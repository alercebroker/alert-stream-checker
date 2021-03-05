from confluent_kafka import KafkaException
import pytest
from utils.errors import LagCalculatorException
from confluent_kafka import Consumer
from verifier import LagCalculator
from utils.stream import Stream


def consumer_factory(config):
    return Consumer(config)


class MockParser:
    def to_report(self, data):
        return data


class TestGetLag:
    parser = MockParser()

    def test_get_lag_zero(self, kafka_service, consume):
        consume(group_id="lag_zero", topic="test", n=5, max_messages=10)
        stream = Stream("localhost:9094", "lag_zero", "test")
        lag_calculator = LagCalculator(consumer_factory=consumer_factory, stream=stream)
        lag = lag_calculator.get_lag(self.parser)
        assert lag["part_0"] == 0

    def test_get_lag_not_zero(self, kafka_service, consume):
        consume(group_id="lag_not_zero", topic="test", n=5, max_messages=5)
        stream = Stream("localhost:9094", "lag_not_zero", "test")
        lag_calculator = LagCalculator(consumer_factory=consumer_factory, stream=stream)
        lag = lag_calculator.get_lag(self.parser)
        assert lag["part_0"] == 5

    def test_get_lag_not_previously_consumed(self, kafka_service):
        stream = Stream("localhost:9094", "first_consume", "test")
        lag_calculator = LagCalculator(consumer_factory=consumer_factory, stream=stream)
        lag = lag_calculator.get_lag(self.parser)
        assert lag["part_0"] == 10

    def test_get_lag_topic_error(self, kafka_service):
        stream = Stream("localhost:9094", "anything", ["non_existent"])
        with pytest.raises(KafkaException):
            lag_calculator = LagCalculator(
                consumer_factory=consumer_factory, stream=stream
            )
            lag = lag_calculator.get_lag(self.parser)


class TestGetTopicMessages:
    parser = MockParser()

    def test_get_topic_messages(self, kafka_service):
        stream = Stream("localhost:9094", "first_consume", "test")
        lag_calculator = LagCalculator(consumer_factory=consumer_factory, stream=stream)
        messages = lag_calculator.get_messages(self.parser)
        assert messages == 10
