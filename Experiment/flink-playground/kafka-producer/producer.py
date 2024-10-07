from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from uuid import uuid4
from ipaddress import IPv4Address

import time
import random
import threading
import math
import sys

def generateRandomUidList() :
    """Generate a list of UIDs to be used for generating displays/clicks"""
    list = [];
    for i in range(1000):
        list.append(str(uuid4()))
    return list


def generateRandomIpList():
    list = [];
    for i in range(100):
        random.seed(random.randint(1, 0xffffffff))
        list.append(str(IPv4Address(random.getrandbits(32))))
    return list


def getEvent(uid: str, ipAddress: str):
    """Generate a random click event in Json"""
    timestamp = int(time.time())
    return bytes(f'{{ "eventType":"click", "uid":"{uid}", "ip":"{ipAddress}", "timestamp":{timestamp}, "impressionId":"{str(time.time())}"}}', encoding='utf-8')


def producerSend():
    i = 0
    partition_counter = 0
    while True:
        kafkaProducer.poll(0)

        clickValue = getEvent(random.choice(uidList), random.choice(ipList));
        try:
            kafkaProducer.produce('topic', clickValue, partition=partition_counter);
        except BufferError as e:
            print(e, file=sys.stderr)
            time.sleep(1)
            kafkaProducer.poll(0)

        partition_counter += 1
        if(partition_counter == 59):
            partition_counter = 0
        i+=1
        if(i % (recordsPerSecond*sleepDuration) == 0):
            print("Sleeping... Kafka produced "+str(i)+" records total.")
            sys.stdout.flush()

            try:
                time.sleep(sleepDuration)
            except BufferError as e:
                print(e, file=sys.stderr)
                producer.poll(1)


# --------------------------------------------------------------------------------------------------------

kafka_broker = {'bootstrap.servers': 'kafka-0.kafka-headless.default.svc.cluster.local:9092'}

while(True):

    admin_client = AdminClient(kafka_broker)
    topics = admin_client.list_topics().topics

    if not topics:
        print("Couldn't connect to Kafka broker, sleeping...")
        time.sleep(30)
    else:
        try:
            kafkaProducer = Producer({'bootstrap.servers': 'kafka-0.kafka-headless.default.svc.cluster.local:9092'})
            print("Connected to Kafka Broker!")

            sleepDuration = 1
            recordsPerSecond = 1

            uidList = generateRandomUidList()
            ipList = generateRandomIpList()

            x = threading.Thread(target=producerSend)
            x.start()

            median = 6000000;
            inn = -7
            minutes = 1

            while(True):
                cos = math.cos(inn)
                current = int(median + (median * math.cos(2*inn)))
                inn += 0.10
                recordsPerSecond = int(current / 60)
                print("At "+str(minutes)+" minutes: Setting number of records to send in the course of 60 seconds at: "+str(current)+" ("+str(recordsPerSecond)+" records/second)")
                sys.stdout.flush()

                minutes+=1
                time.sleep(60)

        except Exception as e:
            print(e)
            time.sleep(30)
