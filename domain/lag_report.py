from dataclasses import dataclass


@dataclass
class LagReport:
    topic: str
    offset: int
    total_messages: int
    lag: int
