from dataclasses import dataclass


@dataclass
class Stream:
    bootstrap_servers: str
    group_id: str
    topic: str
