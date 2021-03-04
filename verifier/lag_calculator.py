from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from utils import LagCalculatorException


class LagCalculator:
    def __init__(self, consumer_config, topics):
        consumer_config.update(
            {
                "auto.offset.reset": "beginning",
                "enable.auto.commit": "false",
                "enable.partition.eof": "true",
            }
        )
        self.consumer = Consumer(consumer_config)
        self.topics = topics
        self.isEof = self.consume_one()

    def consume_one(self):
        self.consumer.subscribe(topics=self.topics)

        while True:
            msg = self.consumer.poll(timeout=10)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    return True
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                return False

    def get_lag(self):
        partitions = self.consumer.assignment()
        partition_messages = self.get_messages_per_partition(partitions)
        lags = {}
        for topic in self.topics:
            lags[topic] = {}
        positions = self.consumer.position(partitions)
        if len(positions) == 0:
            raise LagCalculatorException(f"No offsets found for topics {self.topics}")
        for pos in positions:
            print(pos)
            if pos.offset == -1001:
                lags[pos.topic][pos.partition] = 0

            else:
                lags[pos.topic][pos.partition] = (
                    partition_messages[pos.topic][pos.partition] - pos.offset + 1
                )

        return lags

    def get_messages_per_partition(self, partitions):
        high_offsets = {}
        for topic in self.topics:
            high_offsets[topic] = {}
        for part in partitions:
            offsets = self.consumer.get_watermark_offsets(part)
            high_offsets[part.topic][part.partition] = offsets[1]
        return high_offsets
