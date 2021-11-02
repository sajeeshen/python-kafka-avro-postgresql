from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from sqlalchemy.orm import sessionmaker
from db import engine, User2
from producer_api import BROKER_URLS, REGISTERY_URL

Session = sessionmaker(bind = engine)
session = Session()

class Consumer:

    def __init__(self, topic_name):
        self.topic = topic_name
        self.consumer = AvroConsumer({
            'bootstrap.servers': BROKER_URLS,
            'group.id': 'groupid',
            'schema.registry.url': REGISTERY_URL})
        self.consumer.subscribe([self.topic])

    def pool_vdg(self, data_type=None):
        """
        Get the values and insert into Cassandra
        :param data_type: Cassandra data type
        :type data_type:str
        :return:
        """
        print(data_type)
        while True:
            try:
                msg = self.consumer.poll(10)

            except SerializerError as e:
                print("Message deserialization failed for {}: {}".format(msg, e))
                break
            if msg is None:
                continue
            if msg.error():
                print("AvroConsumer error: {}".format(msg.error()))
                continue
            if data_type == "avro_topic":
                print("Insert value")
                print("Message", msg.value())
                result = msg.value()
                user = User2(first_name=result.get('first_name'),
                             last_name=result.get('last_name'), address=result.get('address'),
                             age=int(result.get('age')))

                session.add(user)
                session.commit()



