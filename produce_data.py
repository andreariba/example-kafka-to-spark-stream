from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
import time, random, json, yaml, socket

class Reading(object):
    def __init__(self, id: str, value: float):
        self.sensor_id = id
        self.value = value

def read_to_dict(read, ctx):
    return {"sensor_id": read.sensor_id, 
            "value": read.value}

def delivery_report(err, event):
    if err is not None:
        print(f'Delivery failed on reading for {event.key().decode("utf8")}: {err}')
    else:
        print(f'Sensor reading for {event.key().decode("utf8")} produced to {event.topic()}')


if __name__=="__main__":

    with open('configuration.yaml','r') as fp:
        config = yaml.load(fp, Loader=yaml.CLoader)
        
    sr_config = {
        'url': config['SCHEMA_REGISTRY_URL']
    }
    
    kafka_config = {
        'bootstrap.servers': config['KAFKA_BOOTSTRAP_SERVERS'],
        'client.id': socket.gethostname()
    }

    topic = config['KAFKA_TOPIC']

    # open and convert the message schema to a string
    with open(config['JSON_SCHEMA'], 'r') as fp:
        schema_json = json.load(fp)  
    schema_str = json.dumps(schema_json)

    # define the list of sensors
    sensor_list = config['SENSOR_LIST']

    # create the client for the schema registry
    schema_registry_client = SchemaRegistryClient(sr_config)

    # message serializer
    json_serializer = JSONSerializer(schema_str, schema_registry_client, read_to_dict)

    # producer loop
    while True:
        try:
            producer = Producer(kafka_config)
            for sensor in sensor_list:
                producer.produce(topic=topic, key=sensor,
                                 value=json_serializer(Reading(id=sensor, value=random.uniform(0, 1)),
                                 SerializationContext(topic, MessageField.VALUE)),
                                 on_delivery=delivery_report)
            producer.flush()
            time.sleep(5)
        except:
            raise