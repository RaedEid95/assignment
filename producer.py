import json
import pandas as pd 
from kafka import KafkaProducer
from kafka import KafkaAdminClient





def producer_builder():

    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        api_version=(0,11,5),
        retries = 10,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    return producer
    


def fill_topic(df,producer):

   
        count = 1
        json_data = df.to_dict('records')
        for row in json_data :
            


            producer.send(topic="stable_topic",value=row)
            # print(f"sending record number {count}")
            producer.flush()
            count +=1

def delete_topics(topic_names,admin_client):
    try:
        admin_client.delete_topics(topics=[topic_names])
        print("Topic Deleted Successfully")
    except  Exception as e:
        print(e)




def main(df):

    admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092'])
    delete_topics("stable_topic",admin_client)
    
    producer = producer_builder()

    fill_topic(df,producer)





    
    





    

