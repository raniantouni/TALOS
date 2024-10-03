start:	
	minikube ssh 'sudo ip link set docker0 promisc on'
	
	kubectl apply -f ./flink/
	kubectl create -f jmx-exporter.yaml	
	kubectl apply -f ./kafka/

	kubectl apply -f ./kube-state-metrics/

kafka-start:
	kubectl apply -f ./kafka/

kafka-clean:
	kubectl delete -f ./kafka/

produce-start:
	kubectl create -f kafka-producer.yaml

produce-clean:
	kubectl delete -f kafka-producer.yaml

monitor-start:
	kubectl create namespace monitoring
	kubectl apply -f ./prometheus/
	kubectl apply -f ./grafana/

monitor-clean:
	kubectl delete -f ./prometheus/
	kubectl delete -f ./grafana/
	kubectl delete namespace monitoring

clean:
	
	kubectl delete -f ./flink/
	kubectl delete -f ./kafka/
	kubectl delete -f jmx-exporter.yaml

	kubectl delete -f ./kube-state-metrics/

	# kubectl delete -f kafka-producer.yaml

install:
	mvn clean install