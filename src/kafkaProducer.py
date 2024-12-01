from confluent_kafka import Producer

server = 'pkc-56d1g.eastus.azure.confluent.cloud:9092'
topic = 'kafka_topic'
config = {'bootstrap.servers': server,
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

def produce_data():
    producer = Producer(config)
    producer.produce(topic = topic, key="11111", value= "{'name' : 'Aashritha 3'}", callback=delivery_report)
    producer.flush()

produce_data()
