
## Prerequisites

- Kubernetes cluster
- `kubectl` configured for your cluster
- Helm 3

## Deployment Steps

### Step 1: Deploy Flink Kubernetes Operator 1.5

- Guide: [Apache Flink Kubernetes Operator 1.5 Documentation](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-release-1.5/docs/try-flink-kubernetes-operator/quick-start/)

### Step 2: Deploy Prometheus for Monitoring

1. Create monitoring namespace:
        
        kubectl create namespace monitoring
        
2. Deploy Prometheus:
     
         kubectl create -f ./Autoscaler/kubernetes-implementation/prometheus -n monitoring

3. Forward the Prometheus service port, for metric collection:

        kubectl port-forward -n monitoring service/prometheus-service 9090:8080


### Step 3: Deploy Kafka
- Install Kafka with Helm
```
        helm install kafka oci://registry-1.docker.io/bitnamicharts/kafka -f ./Experiment/flink-playground/kafka-flink/values.yaml --version 23.0.7
```

### Step 4: Deploy Flink Job

- Deploy the experiment 

        kubectl create -f ./Experiment/flink-playground/FraudDetectionExp.yaml

### Step 5:  Deploy Kafka Producer
- Deploy Python Producer

        kubectl create -f ./Experiment/flink-playground/kafka-producer/producer.yaml

### Step 6:  Run Autoscaler 

Execute main

