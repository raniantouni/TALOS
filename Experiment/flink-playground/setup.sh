#!/bin/bash

#Deploy Flink K8s Operator

kubectl create -f https://github.com/jetstack/cert-manager/releases/download/v1.8.2/cert-manager.yaml
helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator

echo "Flink K8s Operator just Deploye
echo "Kafka Just Deployed"

