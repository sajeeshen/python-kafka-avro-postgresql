from producer import Producer
REGISTERY_URL = "http://localhost:8081"
BROKER_URLS = "localhost:29092"
KEY_SCHEMA = """
{
   "namespace": "sensit.schema",
   "name": "value",
   "type": "record",
   "fields" : [
     {
       "name" : "first_name",
       "type" : "string"
     },
    {
       "name" : "last_name",
       "type" : "string"
     },
     {
       "name" : "age",
       "type" : "string",
       "default": null
     },
     {
       "name" : "address",
       "type" : "string",
       "default": null
     }

   ]
}
"""

KEY_STR = """
{
   "namespace": "sensit.schema",
   "name": "key",
   "type": "record",
   "fields" : [
     {
       "name" : "rec_id",
       "type" : "string"
     }

   ]
}
"""

dummy_value = {

    'first_name': 'Sajeesh',
    'last_name': 'Namboothiri',
    'age': '10',
    'address': '437/E, 2nd floor, Al Karmama'

}
key = {"rec_id": "123"}



producer = Producer(registry=REGISTERY_URL, broker=BROKER_URLS, key=key,
                    value=dummy_value, key_schema=KEY_STR,
                    value_schema=KEY_SCHEMA, topic="avro_topic")
producer.execute_producer()