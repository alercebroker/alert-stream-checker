from confluent_kafka import Producer, Consumer


class LagCalculator:
    def __init__(self, consumer_config, topic):
        self.consumer = Consumer(consumer_config)
        self.topic = topic
        # self.consumer.subscribe([topic])

    def get_lag(self):
        topic_metadata = self.consumer.list_topics(self.topic)
        print(topic_metadata.topics)
        return 1
