from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from confluent_kafka import KafkaException, KafkaError


class Producer:

    def __init__(self, registry=None, value={}, key={}, key_schema=None,
                 value_schema=None, broker=None, topic=None):
        self.value_schema = avro.loads(value_schema)
        self.key_schema = avro.loads(key_schema)
        self.key = key
        self.value = value
        self.registry_url = registry
        self.broker = broker
        self.topics = topic

    def delivery_report(self, err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

    def execute_producer(self):
        avroProducer = AvroProducer({
            'bootstrap.servers': self.broker,
            'queue.buffering.max.ms': 1,
            'socket.keepalive.enable': True,
            'on_delivery': self.delivery_report,
            'schema.registry.url': self.registry_url
        }, default_key_schema=self.key_schema, default_value_schema=self.value_schema)

        try:
            print("Topics", self.topics)
            avroProducer.produce(topic=self.topics, value=self.value, key=self.key)
            avroProducer.flush()
        except KafkaException as err:
            print("Kafka Error", err)
        except Exception as err:
            # print("Error", KafkaException(err))
            print("********************************* Error ******************************** ")
            print("Key Schema", self.key_schema)
            print("Value Schema", self.value_schema)
            print("Value ", self.value)
