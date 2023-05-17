import argparse
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient

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

class Restaurent:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        self.record=record
    @staticmethod
    def dict_to_restaurent(data:dict,ctx):
        return Restaurent(record=data)
    def __str__(self):
        return f"{self.record}"

def main(topic):
    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    schema_str = schema_registry_client.get_latest_version('restaurent-take-away-data-value').schema.schema_str
    json_deserializer = JSONDeserializer(schema_str,
                                         from_dict=Restaurent.dict_to_restaurent)

    consumer_conf = sasl_conf()
    consumer_conf.update({
                     'group.id': 'group1',
                     'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    count=0
    while True:
        try:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            car = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            
            if car is not None:
                print("User record {}: car: {}\n"
                      .format(msg.key(), car))
            count=count+1
            print(count)
        
        except KeyboardInterrupt:
            break
    
    consumer.close()

main("restaurent-take-away-data")