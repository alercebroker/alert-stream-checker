from utils.result import Result
from utils.stream import Stream
from . import LagCalculator


class StreamChecker:
    def verify(self, config: dict):
        results = []
        for stream_dict in config["streams"]:
            stream = Stream(
                stream_dict["ip"], stream_dict["group_id"], stream_dict["topics"]
            )
            try:
                lag_calculator = LagCalculator(stream)
                has_lag, lag = stream_has_lag(lag_calculator)
                results.append(Result.Ok(value=lag, check_success=not has_lag))
            except Exception as e:
                results.append(Result.Fail(e))
        return Result.combine(results)

    def stream_has_lag(self, lag_calculator: LagCalculator):
        lag = lag_calculator.get_lag()
        for topic in lag_calculator.topics:
            for part in lag[topic]:
                if lag[topic][part] != 0:
                    return True, lag
        return False, lag

    def streams_difference(self, lag_calculators: list):
        messages = {}
        for lag_calculator in lag_calculators:
            stream_messages = lag_calculator.get_topic_messages()
            for topic in stream_messages:
                messages[topic] = stream_messages[topic]
        if all(msg == messages.values()[0] for msg in messages.values()):
            return False, messages
        return True, messages
