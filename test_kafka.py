from confluent_kafka import Producer
import json

conf = {
    'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'XGXDR43CWRIHV5HG',
    'sasl.password': 'cflthFPYEOhMt81diPo6zq/9+MUBOyLt6oFOvvKlw0B3De42LE23KIir+Elwegdw'
}

producer = Producer(conf)

topic = "fred_test"

message = {
    "test": "Airflow Kafka working",
    "source": "WSL"
}

producer.produce(topic, json.dumps(message))
producer.flush()

print("✅ Message sent to Confluent!")