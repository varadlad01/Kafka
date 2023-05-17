import argparse
from uuid import uuid4
from six.moves import input
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
#from confluent_kafka.schema_registry import *
import pandas as pd
from typing import List

FILE_PATH = "D:/Varad/docs/Data_Engineer/Kafka/Assignment/restaurant_orders.csv"
columns = ['order_number', 'order_date', 'item_name', 'quantity', 'product_price', 'total_products']
API_KEY = 'NE4MUE4ZHWT7JMCU'
ENDPOINT_SCHEMA_URL  = 'https://psrc-q25x7.us-east-2.aws.confluent.cloud'
API_SECRET_KEY = 'Rh+9eLnLP4WHtejYAltI0jDmuHYj8joOULJF0JMS5wmooctJ3RQGCyV+W8gNGFyZ'
BOOTSTRAP_SERVER = 'pkc-6ojv2.us-west4.gcp.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = 'DWGLTQEJNU5QPPAW'
SCHEMA_REGISTRY_API_SECRET = 'IyNN0VK78zmXVDcT2cnQxHYCWfZsB7ljG8FPkA4V66CbVYNmo4TlvCZx8U/5OTuO'

def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf

def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

    }

class Restaurant:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        
        self.record=record
   
    @staticmethod
    def dict_to_restaurant(data:dict,ctx):
        return Restaurant(record=data)

    def __str__(self):
        return f"{self.record}"

def get_restaurant_instance(file_path):
    df=pd.read_csv(file_path)
    restaurants:List[Restaurant]=[]
    for data in df.values:
        restaurant=Restaurant(dict(zip(columns,data)))
        restaurants.append(restaurant)
        yield restaurant

def restaurant_to_dict(restaurant:Restaurant, ctx):
    return restaurant.record


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main(topic):
    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    # subjects = schema_registry_client.get_subjects()
    # schema=schema_registry_client.get_latest_version(subjects[2])
    # schema_str=schema.schema.schema_str
    schema_str = schema_registry_client.get_latest_version('restaurent-take-away-data-value').schema.schema_str
    string_serializer = StringSerializer('utf_8')
    json_serializer = JSONSerializer(schema_str, schema_registry_client, restaurant_to_dict)

    producer = Producer(sasl_conf())

    print("Producing user records to topic {}. ^C to exit.".format(topic))
    producer.poll(0.0)
    try:
        for restaurant in get_restaurant_instance(file_path=FILE_PATH):

            print(restaurant)
            producer.produce(topic=topic,
                            key=string_serializer(str(uuid4()), restaurant_to_dict),
                            value=json_serializer(restaurant, SerializationContext(topic, MessageField.VALUE)),
                            on_delivery=delivery_report)
            #break
    except KeyboardInterrupt:
        pass
    except ValueError:
        print("Invalid input, discarding record...")
        pass

    print("\nFlushing records...")
    producer.flush()

main("restaurent-take-away-data")
