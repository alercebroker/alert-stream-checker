from ..lag_calculator import LagCalculator


class TestGetLag:
    def test_get_lag_zero(self, kafka_service, consume):
        consume(group_id="lag_zero", topic="test", n=10)
        consumer_config = {
            "bootstrap.servers": "localhost:9094",
            "group.id": "lag_zero",
        }
        lag_calculator = LagCalculator(consumer_config=consumer_config, topic="test")
        lag = lag_calculator.get_lag()
        assert lag == 0

    def test_get_lag_not_zero(self):
        pass

    def test_get_lag_connection_error(self):
        pass
