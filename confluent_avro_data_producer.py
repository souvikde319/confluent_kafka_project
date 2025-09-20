# Make sure to install required packages:
# pip install confluent-kafka pandas

import time
import pandas as pd
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.
    """
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print(
        '✅ User record {} successfully produced to {} [{}] at offset {}'.format(
            msg.key(), msg.topic(), msg.partition(), msg.offset()
        )
    )


# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'pkc-oxqxx9.us-east-1.aws.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'xxxxxxxxxxxxxxx',
    'sasl.password': 'cfltIm13+xxxxxxxxxxxxxxxxxxx+WhoTJUe55+qJDPl1m4wfe8LXNezw'
}

# Schema Registry client
schema_registry_client = SchemaRegistryClient({
    'url': 'https://psrc-1rpknql.ap-south-1.aws.confluent.cloud',
    'basic.auth.user.info': '{}:{}'.format(
        'XXXXXXXXXXXX',
        'xxxxxxxxxxxxxxxxxxxx/FM1z9LiWazkslZhPT00U8LG9FgSuF1B6A'
    )
})

# Fetch Avro schema
subject_name = 'retail_data_topic-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str
print("Schema from Registry ---")
print(schema_str)
print("=====================")

# Serializers
key_serializer = StringSerializer('utf_8')
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# Producer
producer = SerializingProducer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.serializer': key_serializer,
    'value.serializer': avro_serializer
})

# Load CSV
df = pd.read_csv('retail_data.csv')
df = df.fillna('null')

# Fix CustomerID column to int (schema requires int, not float/string)
df["CustomerID"] = df["CustomerID"].replace("null", -1)  # handle nulls
df["CustomerID"] = df["CustomerID"].astype(int)

print(df.head(5))
print("=====================")

# Produce messages
for index, row in df.iterrows():
    data_value = row.to_dict()
    print(data_value)

    producer.produce(
        topic='retail_data_topic',
        key=str(index),
        value=data_value,
        on_delivery=delivery_report
    )
    producer.flush()
    time.sleep(1)  # throttle sending (optional)

print("🎉 All Data successfully published to Kafka")
