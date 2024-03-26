from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
import time

def create_topic(admin_client, topic_name):
    topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
    admin_client.create_topics([topic])


def produce_messages(producer, topic):
    for i in range(10):
        producer.produce(topic, value=f"Message i: {i}")
        producer.flush()
        print(f"Produced message: {i}")
        time.sleep(0.5)

def consume_messages(consumer, topic):
    consumer.subscribe([topic])
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            print('None')
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition
                print("EOF")
                continue
            else:
                raise KafkaException(msg.error())
        else:
            print(f"Consumed message: {msg.value().decode('utf-8')}")


if __name__ == '__main__':
    conf = {'bootstrap.servers': 'localhost:9092'}  # Kafka bootstrap server
    admin_client = AdminClient(conf)
    producer = Producer(conf)
    consumer = Consumer({'bootstrap.servers': 'localhost:9092', 'group.id': 'new-group', 'auto.offset.reset': 'earliest'})

    topic = 'new_topic'

    # Create the topic if it does not exist
    if topic not in admin_client.list_topics().topics:
        create_topic(admin_client, topic)
    
    # Start producer and consumer
    produce_messages(producer, topic)
    consume_messages(consumer, topic)

    # Close Kafka resources
    producer.close()
    consumer.close()

    print('ended')

