# Make sure to install confluent-kafka python package
# pip install confluent-kafka

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer



# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': 'pkc-oxqxx9.us-east-1.aws.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'xxxxxxxxxxx',
    'sasl.password': 'xxxxxxxxxxxx+y6tWVeBtIKaxPvZRmsPuxVMD1u+WhoTJUe55+qJDPl1m4wfe8LXNezw',
    'group.id': 'G1',
    'auto.offset.reset': 'latest'
}

# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
    'url': 'https://psrc-1rpknql.ap-south-1.aws.confluent.cloud',
    'basic.auth.user.info': '{}:{}'.format(
        'xxxxxxxxxxxxxxx',
        'xxxxxxxxxxxxxxxxxxxxxxxx/FM1z9LiWazkslZhPT00U8LG9FgSuF1B6A'
    )
})

# Fetch the latest Avro schema for the value
subject_name = 'retail_data_topic-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Deserializer for the value
key_deserializer = StringDeserializer('utf_8')
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

# Define the DeserializingConsumer
consumer = DeserializingConsumer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.deserializer': key_deserializer,
    'value.deserializer': avro_deserializer,
    'group.id': kafka_config['group.id'],
    'auto.offset.reset': kafka_config['auto.offset.reset']
    # 'enable.auto.commit': True,
    # 'auto.commit.interval.ms': 5000 # Commit every 5000 ms, i.e., every 5 seconds
})

# Subscribe to the 'retail_data_topic' topic
consumer.subscribe(['retail_data_topic'])

#Continually read messages from Kafka
try:
    while True:
        msg = consumer.poll(1.0) # How many seconds to wait for message

        if msg is None:
            continue
        if msg.error():
            print('Consumer error: {}'.format(msg.error()))
            continue
        
        print('Successfully consumed record from partition {} and offset {}'.format(msg.partition(), msg.offset()))
        print('Key {} and Value {}'.format(msg.key(), msg.value()))
        print("==========================")

except KeyboardInterrupt:
    pass
finally:
    consumer.close()