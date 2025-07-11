# -*- coding: utf-8 -*-
"""produce_to_confluent.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1P0YuyEcIAU1OghtQtIkpBiAAWX9GKv0-
"""

# !pip install confluent-Kafka

from confluent_kafka import Producer
import json
import time
import pandas as pd

df1 = pd.read_csv("/content/streams1.csv")
df2 = pd.read_csv("/content/streams2.csv")
df3 = pd.read_csv("/content/streams3.csv")

df1.shape, df2.shape, df3.shape

df_all = pd.concat([df1, df2, df3], ignore_index=True)
df_all.head(10)

df_all.shape

json_records = df_all.to_dict(orient='records')

json_records[0]

conf = {
    "bootstrap.servers":"pkc-n3603.us-central1.gcp.confluent.cloud:9092",
    "security.protocol":"SASL_SSL",
    "sasl.mechanisms":"PLAIN",
    "sasl.username":"5GTACUZ7VOTFTJLD",
    "sasl.password":"gq3YAeqz+YNGdChHv5b8dyALBAqeRUU9UKGEVQeGzYpld8XT5ajgUmpNiAmt+yPB",
    "session.timeout.ms":45000,
    "client.id":"ccloud-python-client-a583c27a-ef85-4357-b9f1-48909f2d5673"
}

producer = Producer(conf)

def delivery_status(err, msg):
    if(err):
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

topic = 'streams_play'

for idx, record in enumerate(json_records):
    try:
        key = (str(idx)).encode("utf-8")
        value = json.dumps(record)

        producer.produce(topic, key = key, value = value, callback=delivery_status)
        producer.poll(1)

        time.sleep(0.2)
    except Exception as e:
        print(f"Error occurred: {e}")

producer.flush()

print("Messages send to Kafka successfully")

