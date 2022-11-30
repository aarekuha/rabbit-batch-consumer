from typing import Union
from dataclasses import dataclass


@dataclass
class RabbitItem:
    routing_key: str
    message: Union[dict, bytes]
