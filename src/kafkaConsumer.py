import json
from confluent_kafka import Consumer

server = 'pkc-56d1g.eastus.azure.confluent.cloud:9092'
topic = 'kafka_topic'
config = {'bootstrap.servers': server,
          'security.protocol': 'SASL_SSL',
          'sasl.mechanism': 'PLAIN',
          'sasl.username': 'QBXWRFGXEVRXKENV',
          'sasl.password': 'iB/oMxhDmE247TzJk2eoIAC7BX61knyY2TCTvWlwqZ+zhlZ6KfOU55FyABPSG5YX',
          "group.id": "my-group",
          'client.id': "Aashritha Marupalli"}

consumer = Consumer(config)
consumer.subscribe([topic])

print("Starting Kafka Consumer")
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            print("No message received")
            continue
        if msg.error():
            print("Kafka error:", msg.error())
            continue
        try:
            print("Received and decoded data:", msg.value().decode('utf8').replace("'", '"'))
        except json.JSONDecodeError as e:
            print("JSON decoding error:", e)
            continue
except KeyboardInterrupt:
    print("Exiting...")
finally:
    consumer.close()
