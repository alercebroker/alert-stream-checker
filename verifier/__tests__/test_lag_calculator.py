from ..lag_calculator import LagCalculator
from confluent_kafka import KafkaException
import pytest
from utils.errors import LagCalculatorException


class TestGetLag:
    def test_get_lag_zero(self, kafka_service, consume):
        consume(group_id="lag_zero", topic="test", n=5, max_messages=10)
        consumer_config = {
            "bootstrap.servers": "localhost:9094",
            "group.id": "lag_zero",
        }
        lag_calculator = LagCalculator(consumer_config=consumer_config, topics=["test"])
        lag = lag_calculator.get_lag()
        assert lag["test"][0] == 0

    def test_get_lag_not_zero(self, kafka_service, consume):
        consume(group_id="lag_not_zero", topic="test", n=5, max_messages=5)
        consumer_config = {
            "bootstrap.servers": "localhost:9094",
            "group.id": "lag_not_zero",
        }
        lag_calculator = LagCalculator(consumer_config=consumer_config, topics=["test"])
        lag = lag_calculator.get_lag()
        assert lag["test"][0] == 5

    def test_get_lag_not_previously_consumed(self, kafka_service):
        consumer_config = {
            "bootstrap.servers": "localhost:9094",
            "group.id": "first_consume",
        }
        lag_calculator = LagCalculator(consumer_config=consumer_config, topics=["test"])
        lag = lag_calculator.get_lag()
        assert lag["test"][0] == 10

    def test_get_lag_topic_error(self, kafka_service):
        consumer_config = {
            "bootstrap.servers": "localhost:9094",
            "group.id": "anything",
        }
        with pytest.raises(KafkaException):
            lag_calculator = LagCalculator(
                consumer_config=consumer_config, topics=["non_existent"]
            )
            lag = lag_calculator.get_lag()
            print(lag)
