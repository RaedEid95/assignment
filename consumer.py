import json
from kafka import KafkaConsumer


def consumer_builder():

    consumer = KafkaConsumer(
     bootstrap_servers='localhost:9092',
     api_version=(0,11,5),
     auto_offset_reset="earliest",
     value_deserializer = lambda v: json.loads(v.decode('ascii')),
     consumer_timeout_ms=1000,
    )

    return consumer




def message_reciver(consumer):
    
    data = []
    
    try:
        
        consumer.subscribe(topics=list(consumer.topics()))
        
    except Exception as e:
        raise(e)
    
    for message in consumer:
        # print ("%s key=%s value=%s" % (message.topic,message.key,
                                            # message.value))
        data.append(message.value)
    consumer.close()

    return data
    

    
def main():

    consumer = consumer_builder()
    return message_reciver(consumer)


if __name__ == "__main__":
    main()




