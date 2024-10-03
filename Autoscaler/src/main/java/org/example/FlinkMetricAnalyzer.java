package org.example;

import io.joshworks.restclient.http.JsonNode;
import io.joshworks.restclient.http.Unirest;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.kubernetes.operator.autoscaler.topology.JobTopology;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;


public class FlinkMetricAnalyzer {

    private final JobID jobID;

    private final JobTopology jobTopology;

    public FlinkMetricAnalyzer(JobID jobID, JobTopology jobTopology, JobDetailsInfo jobDetailsInfo) throws IOException {
        this.jobID = jobID;
        this.jobTopology = jobTopology;

    }

    public Tuple2<Boolean, ArrayList<String>> start(){
        boolean scalingAction = false;
        ArrayList<String> newPipeline = new ArrayList<>();
        int maxParallelism;
        boolean sourceFlag = false;

        for(JobVertexID jobVertexID : this.jobTopology.getVerticesInTopologicalOrder())
        {

            int currentParallelism = this.jobTopology.getParallelisms().get(jobVertexID);
            if(isBackpressured(jobVertexID)) {
                newPipeline.add(jobVertexID + ":" + currentParallelism);
                continue;
            }
            if(this.jobTopology.isSource(jobVertexID)) {

                int newParallelism = sourceParallelism(jobVertexID);
                if (newParallelism <= 0 || currentParallelism == newParallelism) {

                    newPipeline.add(jobVertexID + ":" + currentParallelism);
                    continue;
                }
                scalingAction = true;
                newPipeline.add(jobVertexID + ":" + newParallelism);

                continue;

            }


            double throughput = getThroughput(jobVertexID);
            double lag_rate = getPendingRecordsRate(jobVertexID);
            double relativeLagChangeRate =  computeRelativeLagChangeRate(throughput, lag_rate);
            double idleness = getIdleness(jobVertexID);
            double busy = getBusy(jobVertexID);
            maxParallelism = this.jobTopology.getMaxParallelisms().get(jobVertexID);


            int newParallelism = (int)Math.ceil(currentParallelism * (relativeLagChangeRate + 1));

            System.out.println("------ Vertex: "+jobVertexID+" ------");
            System.out.println("Throughput: "+ throughput);
            System.out.println("Idleness:"+idleness);
            System.out.println("RelativeLagChangeRate: "+relativeLagChangeRate);
            System.out.println("Target Parallelism:"+newParallelism);
            System.out.println("Current Parallelism:"+currentParallelism);

            if( bottleneck(jobVertexID) && busy>=500 && !Double.isInfinite(relativeLagChangeRate)) {

                if(newParallelism != currentParallelism ) {
                    scalingAction = true;
                    if(relativeLagChangeRate>0)
                    {
                        newPipeline.add(jobVertexID + ":" + newParallelism);
                        continue;
                    }

                    }
                else {
                    scalingAction = true;

                    int i = (currentParallelism + 1);
                    System.out.println(Integer.min(i, maxParallelism));

                    newPipeline.add(jobVertexID + ":" + i);
                    System.out.println(newPipeline);
                    System.out.println(currentParallelism + 1);
                    continue;
                }
                newPipeline.add(jobVertexID + ":" + (currentParallelism));
                continue;

            }

            if((relativeLagChangeRate<=0 || Double.isNaN(relativeLagChangeRate)) && idleness>600) {
                if(currentParallelism == 1) {
                    newPipeline.add(jobVertexID + ":" + (currentParallelism));
                    continue;
                }
                scalingAction = true;
                newPipeline.add(jobVertexID + ":" + (currentParallelism - 1));
                continue;
            }
            newPipeline.add(jobVertexID + ":" + currentParallelism);


        }
        System.out.println(newPipeline);
        return new Tuple2<>(scalingAction,newPipeline);
    }


    public boolean isBackpressured(JobVertexID jobVertexID)
    {
        String query ="avg(flink_taskmanager_job_task_backPressuredTimeMsPerSecond{task_id=\""+jobVertexID+"\"})";
        double result = prometheus_query(query);

        return result >= 500;
    }

    public int sourceParallelism(JobVertexID jobVertexID)
    {
        String query = "deriv(sum(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_lag_max{job_id=\""+this.jobID+"\", task_id=\""+jobVertexID+"\"})[1m:])*60 / rate(sum(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_consumed_total{job_id=\""+this.jobID+"\", task_id=\""+jobVertexID+"\"})[60s:])";
        double relativeLagChangeRate = prometheus_query(query);

        if(Double.isNaN(relativeLagChangeRate))
            return -1;


        int currentParallelism = this.jobTopology.getParallelisms().get(jobVertexID);
        int newParallelism = (int)Math.ceil(currentParallelism * (relativeLagChangeRate + 1));
        int maxParallelism = this.jobTopology.getMaxParallelisms().get(jobVertexID);

        double idleness = getIdleness(jobVertexID);


        System.out.println("------ Vertex: "+jobVertexID+" ------");
        System.out.println("RelativeLagChangeRate: "+relativeLagChangeRate);
        System.out.println("Target Parallelism:"+newParallelism);
        System.out.println("Idleness:"+idleness);

        if(relativeLagChangeRate>0 && idleness<=500) {
            return Integer.min(newParallelism, maxParallelism);
        }
        else if (relativeLagChangeRate<0 && idleness>=600) {
            return (int) Double.max(1.0, currentParallelism-1);
        }

        return 0;
    }

    public double getThroughput(JobVertexID jobVertexID)
    {
        String query = "avg(flink_taskmanager_job_task_numRecordsInPerSecond{job_id=\""+this.jobID+"\",  task_id=\""+jobVertexID+"\"})";
        double recordsInPerSec = prometheus_query(query);

        query = "avg(flink_taskmanager_job_task_busyTimeMsPerSecond{job_id=\""+this.jobID+"\",  task_id=\""+jobVertexID+"\"})";
        double busyTime = prometheus_query(query);
        System.out.println(busyTime+"       " + recordsInPerSec);
        if(Double.isNaN(busyTime))
            System.out.println("There is no busyTime!");

        double throughput = recordsInPerSec/busyTime;

        if(Double.isInfinite(throughput)) {
            return recordsInPerSec;
        }
        return Double.max(0.0, throughput*1000);
    }

    public boolean bottleneck(JobVertexID currentVertex){
        String query = "avg_over_time(flink_taskmanager_job_task_buffers_inPoolUsage{job_id=\""+this.jobID+"\",task_id=\""+currentVertex+"\"}[1m])";
        double inPool = prometheus_query(query);
        query = "avg_over_time(flink_taskmanager_job_task_buffers_outPoolUsage{job_id=\""+this.jobID+"\",task_id=\""+currentVertex+"\"}[1m])";
        double outPool = prometheus_query(query);
        System.out.println("InPoolUsage: "+inPool);
        System.out.println("OutPoolUsage: "+outPool);

        return inPool > 0.5 && outPool <= 0.5 && outPool > 0.1;
    }
    public double getPendingRecordsRate(JobVertexID currentVertex)
    {
        String query = "deriv(((avg(flink_taskmanager_job_task_numRecordsIn{task_id=\""+currentVertex+"\"}/" +
                "flink_taskmanager_job_task_numBytesIn{task_id=\""+currentVertex+"\"}))*sum(sum(flink_taskmanager_job_task_Shuffle_Netty_Input_0_debloatedBufferSize{task_id=\""+currentVertex+"\"}*" +
                "flink_taskmanager_job_task_Shuffle_Netty_Input_Buffers_inputQueueLength{task_id=\""+currentVertex+"\"}*" +
                "max_over_time(flink_taskmanager_job_task_Shuffle_Netty_Input_Buffers_inPoolUsage{task_id=\""+currentVertex+"\"}[1m])) by (subtask_index)))[1m:])";
        return prometheus_query(query);
    }

    public double computeRelativeLagChangeRate(double throughput, double lag)
    {
        return (lag*60)/throughput;
    }

    public boolean isPipelineBackpressured()
    {
        String query = "max(flink_taskmanager_job_task_backPressuredTimeMsPerSecond{job_id=\""+this.jobID+"\"})";
        double backpressure =  prometheus_query(query);

        return backpressure >= 900;
    }
    public double getBusy(JobVertexID currentVertex)
    {
        String query = "flink_taskmanager_job_task_busyTimeMsPerSecond{job_id=\""+this.jobID+"\",  task_id=\""+currentVertex+"\"}";
        double busyTime = prometheus_query(query);
        return busyTime;
    }
    public double getIdleness(JobVertexID currentVertex)
    {
        String query = "avg(flink_taskmanager_job_task_idleTimeMsPerSecond{job_id=\""+this.jobID+"\",task_id=\""+currentVertex+"\"})";

        return prometheus_query(query);
    }
    public double prometheus_query(String query)
    {
        try {


            String url = "http://localhost:9090";

            JsonNode response = Unirest.get(url + "/api/v1/query")
                    .queryString("query", query)
                    .asJson()
                    .body();
            JSONObject value = response.getObject();



            JSONArray resultArray = value.getJSONObject("data")
                    .getJSONArray("result");

            if(resultArray.length()==0)
                return Double.NaN;

            double tmp = resultArray.getJSONObject(0)
                    .getJSONArray("value")
                    .getDouble(1);
            Unirest.close();

            return tmp;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0.0;

    }


}
