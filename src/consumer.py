import json
from azure.storage.blob import BlobServiceClient


def kafka_consumer():
    from confluent_kafka import Consumer
    k_config = {
        'bootstrap.servers': 'pkc-56d1g.eastus.azure.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': 'QBXWRFGXEVRXKENV',
        'sasl.password': 'iB/oMxhDmE247TzJk2eoIAC7BX61knyY2TCTvWlwqZ+zhlZ6KfOU55FyABPSG5YX',
        'client.id': "Aashritha Marupalli",
        "group.id": "my-group",
        'api.version.request': True
    }

    consumer = Consumer(k_config)
    consumer.subscribe(['kafka_topic'])

    connection_string = "DefaultEndpointsProtocol=https;AccountName=amazonsalesdata;AccountKey=WRIoFbg52/RtK6mkTBfew2VwyM1f9VZVKqPrc1dUUjwiIlSwRkhJInuKQ+XxhiA6JTJnWbxt2qYe+ASt5Ca8rw==;EndpointSuffix=core.windows.net"
    container_name = "kafka-output"

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    print("Starting Kafka to Azure Blob pipeline...")
    try:
        while True:
            msg = consumer.poll()
            if msg is None:
                print("No message received")
                continue

            if msg.error():
                print("Kafka error:", msg.error())
                continue

            try:
                data = json.loads(msg.value().decode('utf8'))
                print("Received and decoded data:", data)
            except json.JSONDecodeError as e:
                print("JSON decoding error:", e)
                continue

            blob_name = f"data-lake-folder/message_{msg.offset}.json"
            try:
                blob_client = container_client.get_blob_client(blob=blob_name)
                blob_client.upload_blob(json.dumps(data), overwrite=True)
                print(f"Uploaded to Azure Blob: {blob_name}")
            except Exception as e:
                print("Error uploading blob:", e)
    except KeyboardInterrupt:
        print("Exiting...")
    finally:
        consumer.close()
kafka_consumer()
