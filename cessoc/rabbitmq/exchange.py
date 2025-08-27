"""Exchange classes to make defining pika exchanges easier"""

from enum import Enum
import re
from typing import Any


class ExchangeType(Enum):
    """Defines pika exchange types"""
    DIRECT = "direct"
    FANOUT = "fanout"
    TOPIC = "topic"


class Exchange:
    """Defines pika exchange attributes"""

    def __init__(
        self,
        name: str,
        exchange_type: ExchangeType = ExchangeType.DIRECT,
        passive: bool = False,
        durable: bool = False,
        auto_delete: bool = True,
        internal: bool = False,
    ) -> None:
        """
        https://pika.readthedocs.io/en/stable/modules/channel.html#pika.channel.Channel.exchange_declare

        :param name: The exchange name consists of a non-empty sequence of these characters: letters, digits, hyphen, underscore, period, or colon
        :param exchange_type: The exchange type to use
        :param passive: Perform a declare or just check to see if it exists
        :param durable: Survive a reboot of RabbitMQ
        :param auto_delete: Remove when no more queues are bound to it
        :param internal: Can only be published to by other exchanges
        """
        super().__init__()

        self.name = name
        self.exchange_type = exchange_type
        self.passive = passive
        self.durable = durable
        self.auto_delete = auto_delete
        self.internal = internal

    def __eq__(self, value: Any) -> bool:
        """
        :param value: Value parameter to be checked against exchange arguments

        :returns: results of comparison (boolean)
        """
        if isinstance(value, Exchange):
            return self is value or (self.name == value.name and self.exchange_type == value.exchange_type and self.passive == value.passive and self.durable == value.durable and self.auto_delete == value.auto_delete and self.internal == value.internal)
        return False

    @property
    def name(self) -> str:
        """The exchange name consists of a non-empty sequence of these characters: letters, digits, hyphen, underscore, period, or colon"""
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        """
        :param value: Checks value against accepted regex

        :raises ValueError: if value fails regex check
        """
        if re.match(r"^[a-zA-Z0-9-_.:]{1,256}$", value):
            self._name = value
        else:
            raise ValueError(
                "The exchange name must consist of a non-empty sequence of these characters: letters, digits, hyphen, underscore, period, or colon. Invalid name '{}'".format(
                    value
                )
            )
