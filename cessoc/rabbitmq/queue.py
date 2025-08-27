"""Queue classes to make defining pika queues easier"""

from typing import List, Dict, Callable, Optional, Tuple, Union, Any
from cessoc.rabbitmq.exchange import Exchange, ExchangeType


class QueueArguments:
    """Defines pika queue arguments attributes"""

    def __init__(self, max_priority: Optional[int] = None) -> None:
        """:param max_priority: Max priority of the queue. https://www.rabbitmq.com/priority.html"""
        super().__init__()

        self.max_priority = max_priority

    def __eq__(self, value: Any) -> bool:
        """
        :param value: check value parameter against queue arguments

        :returns: result of value check
        """
        if isinstance(value, QueueArguments):
            return self is value or (self.max_priority == value.max_priority)
        return False

    @property
    def arguments(self) -> Dict:
        """Returns the dictionary values of the class"""
        attrs = {}
        if self.max_priority:
            attrs["x-max-priority"] = self.max_priority
        return attrs

    @property
    def max_priority(self) -> Union[int, None]:
        """Max priority of the queue. Must be between 1-255. https://www.rabbitmq.com/priority.html"""
        return self._max_priority

    @max_priority.setter
    def max_priority(self, value: Optional[int]) -> None:
        if value is not None and 1 >= value >= 256:
            raise ValueError("Max priority must be between 1 and 255")
        self._max_priority = value


class Queue:
    """Defines pika queue attributes"""

    def __init__(
        self,
        name: str,
        bindings: Optional[Dict[str, Tuple[Dict, Callable]]] = None,
        consumer_tag: Optional[str] = None,
        consume: bool = False,
        passive: bool = False,
        durable: bool = False,
        exclusive: bool = False,
        auto_delete: bool = False,
        arguments: Optional[QueueArguments] = None,
    ) -> None:
        """
        https://pika.readthedocs.io/en/stable/modules/channel.html#pika.channel.Channel.queue_declare

        :param name: The queue name. if empty string, the broker will create a unique queue name
        :param bindings: Dict of routing keys and callbacks. The key should be the routing key and the value should be the callback for the routing key
        :param consumer_tag: Identifies this consumer to the MQ broker. If None, MQ broker will auto assign a tag
        :param consume: Should this queue definition consume from the queue or just ensure the destination exists
        :param passive: Only check to see if the queue exists
        :param durable: Survive reboots of the broker
        :param exclusive: Only allow access by the current connection
        :param auto_delete: Delete after consumer cancels or disconnects
        :param arguments: allows for custom attributes to be attached to queue
        """
        super().__init__()

        self.name = name
        self.bindings = bindings
        self.consumer_tag = consumer_tag
        self.consume = consume
        self.passive = passive
        self.durable = durable
        self.exclusive = exclusive
        self.auto_delete = auto_delete
        self.arguments = arguments

    def __eq__(self, value: Any) -> bool:
        """
        :param value: Value parameter to be checked against queue arguments

        :returns: results of comparison (boolean)
        """
        if isinstance(value, Queue):
            return self is value or (self.name == value.name and self.bindings == value.bindings and self.consumer_tag == value.consumer_tag and self.consume == value.consume and self.passive == value.passive and self.durable == value.durable and self.exclusive == value.exclusive and self.auto_delete == value.auto_delete and self._arguments == value._arguments)
        return False

    @property
    def name(self) -> str:
        """The queue name; if empty string, the broker will create a unique queue name"""
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        if isinstance(value, str):
            self._name = value
        else:
            raise ValueError("Name must be of type 'str' not '{}'".format(type(value)))

    @property
    def consume(self) -> bool:
        """Should this queue definition consume from the queue or just ensure the destination exists"""
        return self._consume

    @consume.setter
    def consume(self, value: bool) -> None:
        # if setting to True, queue bindings must present
        if value is True and (self.bindings is not None and len(self.bindings) == 0):
            raise ValueError(
                "Queue bindings must be specified when enabling the queue to consume"
            )
        self._consume = value

    @property
    def arguments(self) -> Dict:
        """Custom queue arguments"""
        if self._arguments:
            return self._arguments.arguments
        else:
            return {}

    @arguments.setter
    def arguments(self, value: Optional[QueueArguments]) -> None:
        self._arguments = value


class QueueDefinitionManager:
    """Queue Definition Manager to ensure queues are properly defined"""

    def __init__(self) -> None:
        """Initialize exchanges, queue bindings"""
        super().__init__()

        self.default_exchange = "default"

        self.exchanges: Dict[str, Exchange] = {}
        self.queues: Dict[str, Queue] = {}

        self.queue_bindings: Dict[str, List[Queue]] = {self.default_exchange: []}

    def register_exchange(self, exchange: Union[Exchange, List[Exchange]]) -> None:
        """
        Registers a new exchange

        :param exchange: new exchange to register
        :raises ValueError: Raised when the provided exchange already exists and has different instance attribute values
        """
        if type(exchange) is Exchange:
            exchanges = [exchange]
        else:
            exchanges = exchange
        for e in exchanges:
            if e.name in self.exchanges:
                if e != self.exchanges[e.name]:
                    # exchange with same name has already been declared but with different parameters
                    raise ValueError(
                        "Cannot add exchange. Exchange '{}' already exists but has been declared with different parameters".format(
                            e.name
                        )
                    )
                # return early since this exchange has already been declared with same parameters
                continue
            self.exchanges[e.name] = e
            self.queue_bindings[e.name] = []

    def register_queue(
        self, queue: Queue, exchange_name: Optional[Union[str, List[str]]] = None
    ) -> None:
        """
        Registers a new queue and the exchange that is should be bound to

        :param queue: new queue to register
        :param exchange_name: exchange to bind the queue to. A value of None will bind the queue to the default exchange
        :raises ValueError: Raised when the provided queue already exists and has different instance attribute values
        :raises KeyError: Raised when the provided exchange name is not yet registered. Exchange must be registered first
        :raises AttributeError: Raised when the provided queue does not have routing keys for all exchange types except fanout
        """
        exchanges = []
        if queue.name in self.queues:
            if queue != self.queues[queue.name]:
                # queue with same name has already been declared but with different parameters
                raise ValueError(
                    "Cannot add queue. Queue '{}' already exists but has been declared with different parameters".format(
                        queue.name
                    )
                )
            # return early since this queue has already been declared with same parameters
            return

        # exchange checks
        if exchange_name is not None:
            if type(exchange_name) is str:
                exchanges = [exchange_name]
            else:
                exchanges = exchange_name
            for exchange_name in exchanges:
                # exchange must be registered first
                if exchange_name not in self.queue_bindings:
                    raise KeyError(
                        "Cannot bind queue '{queue}' to exchange '{exchange}' because exchange '{exchange}' has not yet been registered. Register the exchange first.".format(
                            queue=queue.name, exchange=exchange_name
                        )
                    )
                # check if queue routing_keys are defined for all exchange types except fanout
                if self.exchanges[
                    exchange_name
                ].exchange_type != ExchangeType.FANOUT and (
                    queue.bindings is None or len(queue.bindings) == 0
                ):
                    raise AttributeError(
                        "Queue routing keys cannot be type None or empty for exchange type '{}'".format(
                            self.exchanges[exchange_name].exchange_type
                        )
                    )
        self.queues[queue.name] = queue
        if exchanges:
            for e in exchanges:
                self.queue_bindings[e].append(queue)
        else:
            self.queue_bindings[self.default_exchange].append(queue)
