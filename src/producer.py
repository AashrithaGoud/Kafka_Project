import os
import sys
import json
import time

import pandas as pd
from confluent_kafka import Producer
sys.path.append('/opt/airflow')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def to_json_converter():
    df=pd.read_json("/opt/airflow/data/data.json")
    # df=pd.read_json("E:/Projects/Real-TimeStreamingData/data/data.json")
    data=df.to_dict(orient='records')
    return data

server='pkc-56d1g.eastus.azure.confluent.cloud:9092'
topic='kafka_topic'

config={'bootstrap.servers': server,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': 'QBXWRFGXEVRXKENV',
        'sasl.password': 'iB/oMxhDmE247TzJk2eoIAC7BX61knyY2TCTvWlwqZ+zhlZ6KfOU55FyABPSG5YX',
        'client.id': "Aashritha Marupalli"}

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


def kafka_stream():
    result=to_json_converter()
    producer=Producer(config)
    for i, value in enumerate(result[:2]):
        producer.produce(topic=topic,key=value['product_id'],value=json.dumps(value),callback=delivery_report)
        time.sleep(1)
    producer.flush()
kafka_stream()
