package org.example;


import io.joshworks.restclient.http.HttpResponse;
import org.apache.flink.api.common.JobID;
import io.joshworks.restclient.http.JsonNode;
import io.joshworks.restclient.http.Unirest;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.jsonplan.JsonPlanGenerator;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;

import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

import org.apache.flink.kubernetes.operator.autoscaler.topology.JobTopology;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.apache.logging.log4j.util.ProcessIdUtil.getProcessId;

public class Scaler {
    public static long pid = -1;
    private RestClusterClient<?> clusterClient;
    public static Process portForwardProcess = null;

    public Scaler() throws MalformedURLException {
            //Thread.sleep(120000);
            String[] command = {"kubectl", "port-forward", "svc/flink-session-deployment-rest", "8081"};

            // Create a new thread to run the kubectl command
            ExecutorService executor = Executors.newSingleThreadExecutor();
            executor.submit(() -> executeCommand(command));

            URL restEndpointURL = new URL("http://flink-session-deployment-rest:8081");
            Configuration config = new Configuration();
            config.setString("rest.address", "localhost");
            config.setInteger("rest.port", 8081);
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

            //ExecutionConfig executionConfig = program.getOriginalPlan().getExecutionConfig();
            try {
                this.clusterClient = new RestClusterClient<>(config, restEndpointURL);
            } catch (IOException e) {
                System.err.println("Failed to create REST cluster client: " + e.getMessage());
                return;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }




        public void start() throws IOException, ExecutionException, InterruptedException {

            while (true) {
                System.out.println(System.currentTimeMillis());
                Collection<JobStatusMessage> runningJobs = this.clusterClient.listJobs().get();
                for(JobStatusMessage runningJob : runningJobs)
                {

                    JobID jobID = runningJob.getJobId();
                    String json =  clusterClient.getJobDetails(jobID).join().getJsonPlan();
                    System.out.println(json);
                    JobDetailsInfo jobDetailsInfo = clusterClient.getJobDetails(jobID).join();

                    Map<JobVertexID, Integer> maxParallelismMap =
                            jobDetailsInfo.getJobVertexInfos().stream()
                                    .collect(
                                            Collectors.toMap(
                                                    JobDetailsInfo.JobVertexDetailsInfo::getJobVertexID,
                                                    JobDetailsInfo.JobVertexDetailsInfo::getMaxParallelism));
                    JobTopology jobTopology = JobTopology.fromJsonPlan(json,
                            maxParallelismMap,
                            jobDetailsInfo.getJobVertexInfos().stream()
                                    .filter(jvdi -> jvdi.getExecutionState() == ExecutionState.FINISHED)
                                    .map(JobDetailsInfo.JobVertexDetailsInfo::getJobVertexID)
                                    .collect(Collectors.toSet()));


                    FlinkMetricAnalyzer flinkMetricAnalyzer = new FlinkMetricAnalyzer(jobID, jobTopology,jobDetailsInfo);

                    Tuple2<Boolean, ArrayList<String>> result = flinkMetricAnalyzer.start();

                    if(result.f0) {
                        triggerCheckpoint(jobID);
                        Thread.sleep(10000);
                        scale(result.f1.toString().replaceAll("[\\[\\]]", "\""));
                        Thread.sleep(120000);
                    }
                    else
                        System.out.println("---------- No scaling Actions needed ----------");




                }
                Thread.sleep(180000);
            }
        }


        public void scale(String newPipeline) throws IOException, InterruptedException {
            // Build the kubectl command
            System.out.println("New Pipeline Starting Scaling:"+newPipeline);
            String str = "{\"spec\": {\"flinkConfiguration\": {\"pipeline.jobvertex-parallelism-overrides\": \""+newPipeline+"\"}}}";
            ProcessBuilder processBuilder = new ProcessBuilder("kubectl", "patch", "flinkdeployment/flink-session-deployment", "--type=merge", "-p", "{\"spec\": {\"flinkConfiguration\":{\"pipeline.jobvertex-parallelism-overrides\":"+newPipeline+"}}}");

            // Start the process and capture its output
            Process process = processBuilder.start();
            InputStream inputStream = process.getInputStream();

            // Read the output stream and print it to the console
            Scanner scanner = new Scanner(inputStream);
            while (scanner.hasNextLine()) {
                System.out.println(scanner.nextLine());
            }

            // Close the scanner and wait for the process to finish
            scanner.close();
            process.waitFor();
            Thread.sleep(13000);
            String[] command = {"kubectl", "port-forward", "svc/flink-session-deployment-rest", "8081"};

            // Create a new thread to run the kubectl command
            ExecutorService executor = Executors.newSingleThreadExecutor();
            executor.submit(() -> executeCommand(command));
        }

        public void sleepingMessage()
        {
            // Create a separate thread for continuous message printing
            Thread messageThread = new Thread(() -> {
                while (true) {
                    System.out.println("Waiting..");
                    try{
                        Thread.sleep(330000);
                    }catch(InterruptedException e)
                    {
                        System.err.println("Main thread sleep interrupted: " + e.getMessage());
                    }

                }
            });

            // Start the message printing thread
            messageThread.start();

            try {
                // Sleep for 5 seconds in the main thread
                Thread.sleep(90000);
            } catch (InterruptedException e) {
                System.err.println("Main thread sleep interrupted: " + e.getMessage());
            }

            // Interrupt the message printing thread to stop it
            messageThread.interrupt();
        }


    public static void executeCommand(String[] command) {
        try {
            if (portForwardProcess != null) {
                System.out.println("Terminating previous port-forward process");
                portForwardProcess.destroy();
                portForwardProcess = null;
            }

            // Start the kubectl process
            ProcessBuilder processBuilder = new ProcessBuilder(command);
            Process process = processBuilder.start();

            portForwardProcess = process;


            // Wait for the process to complete (optional)
            int exitCode = process.waitFor();
            System.out.println("Process exited with code " + exitCode);
            portForwardProcess = process;

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void triggerCheckpoint(JobID jobID){
        JSONObject object = new JSONObject();
        object.put("checkpointType", "CONFIGURED");

        String url = "http://localhost:8081/jobs/"+jobID+"/checkpoints";

        HttpResponse<JsonNode> response = Unirest.post(url)
                .header("Content-Type", "application/json")
                .body(object)
                .asJson();
    }


}
