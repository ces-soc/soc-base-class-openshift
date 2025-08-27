import ssl
import functools
import json
import threading
import os
import signal
import time
import traceback
import uuid
from concurrent.futures.thread import ThreadPoolExecutor
from typing import List, Dict, Callable, Union, Optional, Tuple
import pika

from pika.channel import Channel
from pika.connection import Connection
from pika.spec import BasicProperties, Basic
from pika.frame import Method

from cessoc.rabbitmq.queue import Queue, QueueDefinitionManager, QueueArguments
from cessoc.rabbitmq.exchange import Exchange, ExchangeType
from cessoc.logging import cessoc_logging


# https://stackoverflow.com/questions/3464061/cast-base-class-to-derived-class-python-or-more-pythonic-way-of-extending-class
class extendProperties(BasicProperties):
    def __init__(self):
        super().__init__()
        self.exchange = None
        self.routing_key = None

    @classmethod
    def from_BasicProperties(cls, oldprop: BasicProperties, delivery_prop: Basic.Deliver):
        newProperty = cls()
        for key, value in oldprop.__dict__.items():
            newProperty.__dict__[key] = value
        newProperty.exchange = delivery_prop.exchange
        newProperty.routing_key = delivery_prop.routing_key
        return newProperty

# FROM EDM SECTION


class Eventhub:
    """Eventhub Base Class abstracts Pika connections/actions"""

    def __init__(
        self,
        prefetch_count: int = 1,
        virtual_host: str = pika.ConnectionParameters.DEFAULT_VIRTUAL_HOST,
        connection_name: Optional[str] = None,
        heartbeat=10,
    ) -> None:
        self.parameters: Dict = {}

        # This line should come after the config manager is intialized because the config_manager configures the root logger
        self._logger = cessoc_logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        # how many messages and threads this service will process at once
        self._prefetch_count = prefetch_count

        # the MQ broker virtual host
        self.virtual_host = virtual_host

        # the MQ endpoint to connect to
        self.mq_endpoint: str = ""

        # the connection name to use to distinguish this connection from others, if None, __class__.__name__ will be used
        self.connection_name = connection_name if connection_name else self.__class__.__name__

        # the number of seconds in between heartbeats
        self.heartbeat = heartbeat

        # the username to use to connect to the MQ endpoint
        self.username: str = ""

        # pika connection https://www.rabbitmq.com/connections.html
        self._connection: Connection = None
        # pika channel https://www.rabbitmq.com/channels.html
        self._channel: Channel = None

        # is the service currently trying to close
        self._closing = False

        # callbacks that will be called when _on_channel_closed is called
        self._on_channel_closed_callbacks: List[Callable] = []

        # manages the registered queues and exchanges
        self._queue_manager = QueueDefinitionManager()

        # callbacks that will be called after the channel is open, useful for sending messages
        self._after_channel_open_callbacks: List[Callable] = []

        # callbacks that will be called after all of the queues are registered and subscribed and the service is ready to receive messages
        self._on_ready_callbacks: List[Callable] = []

        # thread pool manager, max threads is configured with the prefect count
        self._thread_pool_executor = ThreadPoolExecutor(max_workers=self._prefetch_count)
        # list of running threads
        self._tasks: List = []

        # dictionary for callbacks that process reply to messages, key should be the __qualname__ of the method
        self._reply_to_callbacks: Dict[str, Callable] = {}

        # capture keyboard and terminate interupts
        if (
            threading.current_thread() is threading.main_thread()
        ):  # only capture when running as the main thread, call stop if running in a different thread
            signal.signal(signal.SIGINT, self._shutdown_interupt_cb)
            signal.signal(signal.SIGTERM, self._shutdown_interupt_cb)

        self._thread_local = threading.local()
        self._campus = os.environ.get("CAMPUS")
        self._reply_queue_name = None

    def _connect(self, username: str, password: str) -> Connection:
        """Configures and starts the connection the the MQ."""
        self.username = username
        credentials = pika.PlainCredentials(username, password)
        # the SSLOptions tell the pika library to connect using amqps
        # the service will shut down after all the connection_attempts have failed
        parameters = pika.ConnectionParameters(
            host=self.mq_endpoint,
            virtual_host=self.virtual_host,
            heartbeat=self.heartbeat,
            connection_attempts=3,
            retry_delay=10,
            credentials=credentials,
            ssl_options=pika.SSLOptions(context=ssl.create_default_context()),
            client_properties={"connection_name": self.connection_name},
        )

        self._logger.info("Connecting to %s", parameters.host)
        return pika.SelectConnection(
            parameters=parameters,
            on_open_callback=self._on_connection_open,
            on_open_error_callback=self._on_connection_open_error,
            on_close_callback=self._on_connection_closed,
        )

    def _close_connection(self) -> None:
        """Cleanly close the connection to the MQ."""
        self._closing = True
        if self._connection.is_closing or self._connection.is_closed:
            self._logger.info("Connection is closing or already closed")
        else:
            self._logger.info("Closing connection")
            self._connection.close()

    def _on_connection_open(self, _unused_connection: Connection) -> None:
        """Called when a new connection to the MQ has been established. Starts opening a channel."""
        self._logger.info("Connection opened")
        self._open_channel()

    def _on_connection_open_error(self, _unused_connection: Connection, err: Exception) -> None:
        """Called when a connection cannot be established to the MQ. Stops the ioloop."""
        self._logger.error("Connection open failed. Stopping execution.")
        self._logger.error(err)
        self.stop()

    def _on_connection_closed(self, _unused_connection: Connection, reason: Exception) -> None:
        """Called when an established connection to the MQ has been closed cleanly."""
        self._logger.warning("Connection closed: %s", reason)
        self._channel = None
        self._connection.ioloop.stop()

    def _open_channel(self) -> None:
        """Opens a new channel to the MQ. Starts exchange setup."""
        self._logger.debug("Creating a new channel")
        self._connection.channel(on_open_callback=self._on_channel_open)

    def _on_channel_open(self, channel: Channel) -> None:
        """
        Called when a channel has been successfully opened to the MQ. Calls the after
        channel open callbacks and starts queue setup.
        """
        self._logger.info("Channel opened")
        self._channel = channel
        self._channel.add_on_close_callback(self._on_channel_closed)
        self._channel.add_on_cancel_callback(self._on_consumer_cancelled)
        self._channel.add_on_return_callback(self._on_message_reject_cb)
        self._after_channel_open()

        # setup exchanges
        for exchange in self._queue_manager.exchanges.values():
            self._setup_exchange(exchange)

        # setup default exchange queues
        for queue in self._queue_manager.queue_bindings[self._queue_manager.default_exchange]:
            self._setup_queue(queue)

        # must be called here if no queues are being registered
        if self._is_ready():
            self._on_ready()

    def _on_channel_closed(self, channel: Channel, reason: Exception):
        """Called when a channel has been cleanly closed."""
        self._logger.warning("Channel %i was closed: %s", channel, reason)
        for cb in self._on_channel_closed_callbacks:
            cb(reason)
        self._close_connection()

    def register_on_channel_closed_callback(self, callback: Callable):
        """Register a new callback for unexpected channel closures. Method should accept 1 parameter of type 'Exception'"""
        self._on_channel_closed_callbacks.append(callback)

    def _on_consumer_cancelled(self, method_frame: Method) -> None:
        """Called when a consumer has been canceled either by the MQ or this service."""
        self._logger.info("Consumer was cancelled remotely: %r", method_frame)

    def _on_message_reject_cb(
        self, channel: Channel, method: Basic.Return, properties: BasicProperties, body: bytes
    ) -> None:
        """Called when a basic.reject has been issued for a message that this service sent."""
        self._logger.error(
            "message was rejected: '%s'. Exchange: '%s', Route: '%s', 'CorrelationID: '%s'",
            method.reply_text,
            method.exchange,
            method.routing_key,
            properties.correlation_id,
        )

    def stop(self, task_check: int = 2) -> None:
        """
        Stops this service and the ioloop. Cleanly closes all connections, channels, and queues.

        :param task_check: Seconds to wait before checking if all tasks have finished
        """
        self._logger.info("Stopping")

        self._closing = True
        # stop consuming so we don't receive any new messages
        self._stop_consuming_all()
        self._thread_pool_executor.shutdown(wait=False)
        # wait until all threads have returned
        if len(self._tasks) != 0:
            self._logger.info("Pending tasks to complete before shutdown: %s", len(self._tasks))
            # call in 2 seconds, gives the threads time to ack or reply to messages they've already received
            self._connection.ioloop.call_later(task_check, self.stop)
        else:
            # finish closing connections
            self._connection.ioloop.add_callback_threadsafe(self._final_stop)

    def _final_stop(self) -> None:
        """Called once all messages have been properly processed"""
        self._close_connection()
        self._connection.ioloop.stop()
        self._logger.info("Stopped")

    def _stop_consuming_all(self) -> None:
        """Cancels all consumers."""
        if self._channel:
            for queue in self._queue_manager.queues.values():
                # only cancel consumers that are still listening
                if queue.consumer_tag:
                    self._stop_consuming(queue)

    def _stop_consuming(self, queue: Queue) -> None:
        """Cancels the consumer tag"""
        self._logger.debug("Canceling consumer %s", queue.consumer_tag)
        cb = functools.partial(self._on_cancelok, queue=queue)
        self._channel.basic_cancel(queue.consumer_tag, cb)

    def stop_consuming(self, queue_name: str) -> None:
        """Cancels the consumer tag by queue name and thread safe"""
        cb = functools.partial(self._stop_consuming, queue=self._queue_manager.queues[queue_name])
        self._connection.ioloop.add_callback_threadsafe(cb)

    def _on_cancelok(self, _unused_frame: Method, queue: Queue) -> None:
        """Called when a consumer is canceld."""
        self._logger.info("RabbitMQ acknowledged the cancellation of the consumer: %s", queue.consumer_tag)
        queue.consumer_tag = None

    def _setup_exchange(self, exchange: Exchange) -> None:
        """Set up an exchange."""
        self._logger.info("Declaring exchange: %s", exchange.name)
        cb = functools.partial(self._on_exchange_declareok, exchange=exchange)
        self._channel.exchange_declare(
            exchange=exchange.name,
            exchange_type=str(exchange.exchange_type.value),
            passive=exchange.passive,
            auto_delete=exchange.auto_delete,
            internal=exchange.internal,
            callback=cb,
        )

    def _on_exchange_declareok(self, _unused_frame: Method, exchange: Exchange) -> None:
        """Called when an exchanges is successfully declared."""
        self._logger.debug("Exchange declared: %s", exchange.name)
        for queue in self._queue_manager.queue_bindings[exchange.name]:
            self._setup_queue(queue, exchange)

    def _setup_queue(self, queue: Queue, exchange: Optional[Exchange] = None) -> None:
        """Set up a queue."""
        self._logger.info("Declaring queue %s", queue.name)
        cb = functools.partial(self._on_queue_declare_ok, queue=queue, exchange=exchange)
        self._channel.queue_declare(
            queue=queue.name,
            passive=queue.passive,
            durable=queue.durable,
            exclusive=queue.exclusive,
            auto_delete=queue.auto_delete,
            arguments=queue.arguments,
            callback=cb,
        )

    def _on_queue_declare_ok(self, _unused_frame: Method, queue: Queue, exchange: Optional[Exchange] = None) -> None:
        """Called when a queue has been successfully declared. Defines queue binding to an exchange."""
        if queue.consume is False:
            # nothing more to do since we won't be consuming
            return

        if exchange is not None:
            # bind to exchange with routing keys
            if queue.bindings is None or len(queue.bindings) == 0:
                raise AttributeError(
                    "Cannot bind queue '{}' to exchange '{}' with no routing keys".format(queue.name, exchange.name)
                )
            for key in queue.bindings:
                self._logger.info("Binding queue %s to exchange %s with routing key %s", queue.name, exchange.name, key)
                cb = functools.partial(self._on_bindok, queue=queue, routing_key=key)
                self._channel.queue_bind(queue.name, exchange.name, routing_key=key, callback=cb)
        else:
            # bind to default exchange
            self._logger.info("Queue %s bound to the default exchange using the name as the routing key", queue.name)
        self._set_qos(queue)

    def _on_bindok(self, _unused_frame: Method, queue, routing_key: str) -> None:
        """Called when a queue has been successfully bound to an exchange"""
        self._logger.debug("Queue bound '%s' with routing key '%s'", queue.name, routing_key)

    def _set_qos(self, queue: Queue) -> None:
        """Sets the prefect count for the queue"""
        cb = functools.partial(self._on_basic_qos_ok, queue=queue)
        self._channel.basic_qos(prefetch_count=self._prefetch_count, callback=cb)

    def _on_basic_qos_ok(self, _unused_frame: Method, queue: Queue) -> None:
        """Called when the prefect count for a queue has been successfully set. Starting consuming of the queue."""
        self._logger.debug("QOS set to: %d", self._prefetch_count)
        self._start_consuming(queue)

    def _start_consuming(self, queue: Queue) -> None:
        """Starts consuming the queue."""
        self._logger.info("Starting consumer for queue %s", queue.name)
        cb = functools.partial(self._on_message, queue=queue)
        queue.consumer_tag = self._channel.basic_consume(queue.name, cb)
        self._logger.debug("Started consumer %s with tag %s", queue.name, queue.consumer_tag)

        if self._is_ready():
            self._on_ready()

    def _is_ready(self):
        """Check if the service has started all consumers and is ready to receive"""
        for definition in self._queue_manager.queues.values():
            # check if not ready, all definitions that will consume and don't have a consumer tag means we're not ready
            if definition.consume and definition.consumer_tag is None:
                return False
        return True

    def start_consuming(self, queue_name: str) -> None:
        """Starts the consumer tag by queue name and thread safe"""
        cb = functools.partial(self._start_consuming, queue_definition=self._queue_manager.queues[queue_name])
        self._connection.ioloop.add_callback_threadsafe(cb)

    def _on_message(
        self,
        _unused_channel: Channel,
        basic_deliver: Basic.Deliver,
        properties: BasicProperties,
        body: bytes,
        queue: Queue,
    ) -> None:
        """Called when a new message is received. Checks the content encoding and content type. Starts a new thread to process the message."""
        self._logger.debug("Received message # %s from %s", basic_deliver.delivery_tag, properties.app_id)

        if properties.content_encoding != "utf-8":
            self._logger.error(
                "Rejecting message. Content encoding type must be 'utf-8' not '%s'", properties.content_encoding
            )
            self._reject_message(basic_deliver.delivery_tag)
            return
        if properties.content_type != "application/json":
            self._logger.error(
                "Rejecting message. Content type must be 'application/json' not '%s'", properties.content_type
            )
            self._reject_message(basic_deliver.delivery_tag)
            return

        # check if the queue has bindings
        if queue.bindings is None:
            # this code should not be reachable since a check should be done before consuming from a queue with no bindings
            self._logger.error("Rejecting message. Queue %s has no bindings specified", queue.name)
            self._reject_message(basic_deliver.delivery_tag)
            return

        # ensure the queue has bindings for the routing key
        if basic_deliver.routing_key not in queue.bindings:
            self._logger.error(
                "Rejecting message. Received message on routing key '%s' but no binding was specified",
                basic_deliver.routing_key,
            )
            self._reject_message(basic_deliver.delivery_tag)
            return

        if type(queue.bindings[basic_deliver.routing_key]) is dict:
            task = self._thread_pool_executor.submit(
                self._callback_wrapper, queue.bindings[basic_deliver.routing_key]["function"], basic_deliver, properties, body, queue.bindings[basic_deliver.routing_key]["sends_reply"]
            )
        else:
            task = self._thread_pool_executor.submit(
                self._callback_wrapper, queue.bindings[basic_deliver.routing_key], basic_deliver, properties, body
            )
        # track threads and their state
        self._tasks.append(task)
        task.add_done_callback(self._notify_thread_done)

    def _notify_thread_done(self, task) -> None:
        """Called when a thread finishes"""
        self._logger.info("Thread finished")
        self._tasks.remove(task)

    def _callback_wrapper(
        self, cb: Callable, basic_deliver: Basic.Deliver, properties: BasicProperties, body: bytes, reply_expected=True
    ) -> None:
        """Used to help with error handling of the message. Decodes the message body and calls the message callback. Sends reply to if requested."""
        try:
            # measure execution time of the event
            start_time = time.process_time()

            newProperties = extendProperties.from_BasicProperties(oldprop=properties, delivery_prop=basic_deliver)
            response = cb(newProperties, json.loads(body.decode("utf-8"), strict=False))

            end_time = time.process_time()
            self._logger.debug("Processing event took %s seconds", (end_time - start_time))

            if response and properties.reply_to:
                reply_to_headers = None
                if "Reply-To-Headers" in properties.headers:
                    reply_to_headers = properties.headers["Reply-To-Headers"]
                reply_cb = functools.partial(
                    self._publish_message,
                    message=response,
                    routing_key=properties.reply_to,
                    reply_to_callback=properties.headers["Reply-To-Callback"],
                    reply_to_headers=reply_to_headers,
                    correlation_id=properties.correlation_id,
                )
                self._connection.ioloop.add_callback_threadsafe(reply_cb)
            elif response and not properties.reply_to:
                self._logger.warning("Callback returned data but no reply to was requested")
            elif not response and reply_expected and properties.reply_to:
                self._logger.error("Reply-to was requested but no data was returned from the callback")

            cb = functools.partial(self._acknowledge_message, delivery_tag=basic_deliver.delivery_tag)
            self._connection.ioloop.add_callback_threadsafe(cb)
        except UnicodeDecodeError as ex:
            self._logger.error("Could not decode message: %s", ex)
            cb = functools.partial(self._reject_message, delivery_tag=basic_deliver.delivery_tag)
            self._connection.ioloop.add_callback_threadsafe(cb)
        except json.JSONDecodeError as ex:
            self._logger.error("Could not load message json: %s", ex)
            cb = functools.partial(self._reject_message, delivery_tag=basic_deliver.delivery_tag)
            self._connection.ioloop.add_callback_threadsafe(cb)
        except Exception as ex:  # pylint: disable=broad-except
            self._logger.error("Error handling callback: %s", ex)
            self._logger.error("%s", traceback.format_exc())
            cb = functools.partial(self._reject_message, delivery_tag=basic_deliver.delivery_tag)
            self._connection.ioloop.add_callback_threadsafe(cb)

    def _on_reply_to(self, properties: BasicProperties, body: Union[Dict, List]) -> None:
        """Called when the message is a reply to. Calls the reply to callback based on the Reply-To-Callback header."""
        self._logger.debug("Processing reply-to")
        if properties.headers is None or "Reply-To-Callback" not in properties.headers:
            self._logger.error("Reply-to is missing the 'Reply-To-Callback' header. Cannot process reply-to")
            return None

        if properties.headers["Reply-To-Callback"] not in self._reply_to_callbacks:
            self._logger.error(
                "This service has no method '%s' registered as a reply_to_callback. Cannot process reply-to",
                properties.headers["Reply-To-Callback"],
            )
            return None

        callback_name = properties.headers["Reply-To-Callback"]
        self._logger.debug("Calling %s to process reply-to", callback_name)
        return self._reply_to_callbacks[callback_name](properties, body)

    def _reject_message(self, delivery_tag: str) -> None:
        """Rejects and dequeues the message."""
        self._logger.debug("Rejecting message %s", delivery_tag)
        self._channel.basic_reject(delivery_tag, requeue=False)

    def _acknowledge_message(self, delivery_tag: str) -> None:
        """Acknowledges the message."""
        self._logger.debug("Acknowledging message %s", delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def run(self, mq_endpoint: str, username: str = "guest", password: str = "guest") -> None:  # nosec
        """
        Starts the services and the ioloop.
        If username and password is not provided the default guest credentials are used.
        If the mq_endpoint is not specified, the endpoint is expected in the config parameters

        :param username: MQ Username
        :param password: MQ user password
        :param mq_endpoint: MQ endpoint
        """
        self.mq_endpoint = mq_endpoint

        while not self._closing:
            self._connection = self._connect(username, password)
            self._connection.ioloop.start()

    def publish_message_with_callbacks_campus(
        self,
        message: Union[Dict, List],
        routing_key: str,
        reply_to: bool = False,
        reply_to_callback: Optional[Callable] = None,
        reply_to_headers: Optional[Dict] = None,
        correlation_id: Optional[str] = None,
        priority: Optional[int] = None,
        mandatory: bool = True,
    ) -> str:
        """
        Ease of use function to automatically specify the campus name for the exchange

        :param message: JSON message to send
        :param routing_key: Key used to route the message
        :param reply_to: Request a reply
        :param reply_to_callback: The callback to reply to. Must be set when `reply_to` is True
        :param reply_to_headers: Headers added as part of the message properties that will be sent back if `reply_to` is True
        :param correlation_id: The message correlation ID to use
        :param priority: The priority of the message
        :param mandatory: If the message must be routable to a queue

        :returns: The UUID used for the message ID
        """
        return self.publish_message_with_callbacks(
            message,
            routing_key,
            exchange=self._campus,
            reply_to=reply_to,
            reply_to_callback=reply_to_callback,
            reply_to_headers=reply_to_headers,
            correlation_id=correlation_id,
            priority=priority,
            mandatory=mandatory,
        )

    def publish_message_with_callbacks(
        self,
        message: Union[Dict, List],
        routing_key: str,
        exchange: str = "",
        reply_to: bool = False,
        reply_to_callback: Optional[Callable] = None,
        reply_to_headers: Optional[Dict] = None,
        correlation_id: Optional[str] = None,
        priority: Optional[int] = None,
        mandatory: bool = True,
    ) -> str:
        """
        Publishes a message to the MQ using a thread safe callback

        :param message: JSON message to send
        :param routing_key: Key used to route the message
        :param exchange: The exchange to publish to
        :param reply_to: Request a reply
        :param reply_to_callback: The callback to reply to. Must be set when `reply_to` is True
        :param reply_to_headers: Headers added as part of the message properties that will be sent back if `reply_to` is True
        :param correlation_id: The message correlation ID to use
        :param priority: The priority of the message
        :param mandatory: If the message must be routable to a queue

        :returns: The UUID used for the message ID
        """
        if not correlation_id:
            correlation_id = uuid.uuid4().hex
        cb = functools.partial(
            self._publish_message,
            message=message,
            routing_key=routing_key,
            exchange=exchange,
            reply_to=reply_to,
            reply_to_callback=reply_to_callback,
            reply_to_headers=reply_to_headers,
            correlation_id=correlation_id,
            priority=priority,
            mandatory=mandatory,
        )
        self._connection.ioloop.add_callback_threadsafe(cb)
        return correlation_id

    def _publish_message(
        self,
        message: Union[Dict, List],
        routing_key: str,
        exchange: str = "",
        reply_to: bool = False,
        reply_to_callback: Optional[Callable] = None,
        reply_to_headers: Optional[Dict] = None,
        correlation_id: Optional[str] = None,
        priority: Optional[int] = None,
        mandatory: bool = True
    ) -> None:
        """
        Publishes a message to the MQ.

        :param message: JSON message to send
        :param routing_key: Key used to route the message
        :param exchange: The exchange to publish to
        :param reply_to: Request a reply
        :param reply_to_callback: The callback to reply to. Must be set when `reply_to` is True
        :param reply_to_headers: Headers added as part of the message properties that will be sent back if `reply_to` is True
        :param correlation_id: The message correlation ID to use
        :param priority: The priority of the message
        :param mandatory: If the message must be routable to a queue

        :raises AttributeError: Raised when no reply_to callbacks have been registered and reply_to has been requested
        :raises ValueError: Raised when reply_to_callback is not set and reply_to is True
        """
        if self._channel is None or not self._channel.is_open:
            self._logger.error("Channel must be open to publish messages")
            return

        if not correlation_id:
            correlation_id = uuid.uuid4().hex

        reply_to_queue = None
        if reply_to and len(self._reply_to_callbacks) > 0:
            reply_to_queue = self._reply_queue_name  # Set Queue Name from memory. We aren't sure they registered with the _campus or the normal function without this.
        elif reply_to and len(self._reply_to_callbacks) == 0:
            raise AttributeError(
                "Cannot set reply to when there are no reply_to callbacks registered. If you would"
                "like to register a reply_to callback, use the function `register_on_reply_to_callback`"
            )

        if reply_to and not reply_to_callback:
            raise ValueError("reply_to_callback must be set when reply_to is `True`")

        headers: Optional[Dict] = None
        if reply_to_headers is not None:
            if headers is None:
                headers = {}
            headers["Reply-To-Headers"] = reply_to_headers
        if reply_to_callback:
            if headers is None:
                headers = {}
            if isinstance(reply_to_callback, str):
                headers["Reply-To-Callback"] = reply_to_callback
            else:
                headers["Reply-To-Callback"] = reply_to_callback.__qualname__

        properties = pika.BasicProperties(
            app_id=self.__class__.__name__,
            user_id=self.username,
            content_type="application/json",
            content_encoding="utf-8",
            reply_to=reply_to_queue,
            correlation_id=correlation_id,
            priority=priority,
            headers=headers,
        )

        try:
            self._channel.basic_publish(exchange, routing_key, json.dumps(message), properties, mandatory=mandatory)
            self._logger.info(
                "Published message to exchange '%s' with routing key '%s' and correlation id '%s'",
                exchange,
                routing_key,
                correlation_id,
            )
        except pika.exceptions.UnroutableError:
            self._logger.error("Message was unroutable")

    def register_on_message_callback_campus(
        self, queue_name: str, bindings: Dict[str, Tuple[Dict, Callable]], max_priority: Optional[int] = None,
    ) -> None:
        """
        Ease of use function to automatically specify the campus name for the exchange

        :param queue_name: Name of the queue to create and listen to. Campus name will automatically be appended (ie. QUEUE_NAME-CAMPUS)
        :param bindings: Dict of routing keys and callbacks. The key should be the routing key and the value should be the callback for the routing key
        :param max_priority: Max priority of the queue. Can be 1-256. https://www.rabbitmq.com/priority.html
        """
        self.register_on_message_callback(
            f"{queue_name}-{self._campus}",
            bindings=bindings,
            exchange=self._campus.lower(),
            max_priority=max_priority,
        )

    def register_on_message_callback(
        self,
        queue_name: str,
        bindings: Dict[str, Tuple[Dict, Callable]],
        auto_delete_queue: bool = False,
        durable_queue: bool = True,
        exchange: Optional[Union[str, List[str]]] = [],
        passive_exchange: bool = True,
        max_priority: Optional[int] = None,
    ) -> None:
        """
        Registers a callback for processing new messages.
        :param queue_name: Name of the queue to create and listen to
        :param bindings: Dict of routing keys and callbacks. The key should be the routing key and the value should be the callback for the routing key
        :param auto_delete_queue: Delete queue after service stops
        :param durable_queue: Queue survives MQ reboots
        :param exchange: Optional Name of the exchange to bind the queue to or List of exchanges to bind to
        :param passive_exchange: Passively create the exchange
        :param max_priority: Max priority of the queue. Can be 1-256. https://www.rabbitmq.com/priority.html
        """
        for value in bindings.values():
            self._logger.debug("Registering on_message callback %s", value)

        if type(exchange) is str:
            exchanges = [exchange]
        else:
            exchanges = exchange
        if exchanges:
            self._queue_manager.register_exchange([Exchange(e, passive=passive_exchange) for e in exchanges])
        arguments = None
        if max_priority:
            arguments = QueueArguments(max_priority=max_priority)
        for key in bindings:
            if type(bindings[key]) is dict:
                pass
            elif type(bindings[key]) is callable:
                bindings[key] = {"function": bindings[key], "sends_reply": True}
        self._queue_manager.register_queue(
            Queue(
                queue_name,
                bindings=bindings,
                consume=True,
                durable=durable_queue,
                auto_delete=auto_delete_queue,
                arguments=arguments,
            ),
            exchange_name=exchanges,
        )

    def register_on_reply_to_callback(self, callback: Callable) -> None:
        """
        Ease of use function to automatically specify the campus name in the name of the queue.

        Registers a callback for processing reply to messages.

        :param callback: Function to call when a reply-to message is received
        """
        self._logger.debug("Registering on reply_to callback: %s", callback.__qualname__)
        self._reply_to_callbacks[callback.__qualname__] = callback

        # routing key is the same as the queue name
        name = f"replyto.{self.connection_name}"
        if self._reply_queue_name is None:
            self._reply_queue_name = name
        else:
            if name != self._reply_queue_name:
                raise Exception("You cannot use both register_on_reply_to_callback and register_on_reply_to_callback_campus in the same EDM. Please use one other other")

        # call the same method for all reply-tos. The _reply_to_callbacks dict determines what other methods to call
        # delete the queue when all subscribers have closed. We don't want to persist these messages
        self._queue_manager.register_queue(
            Queue(name, bindings={name: self._on_reply_to}, consume=True, auto_delete=True)
        )

    def register_on_reply_to_callback_campus(self, callback: Callable) -> None:
        """
        Registers a callback for processing reply to messages.

        :param callback: Function to call when a reply-to message is received
        """
        self._logger.debug("Registering on reply_to callback: %s", callback.__qualname__)
        self._reply_to_callbacks[callback.__qualname__] = callback

        # routing key is the same as the queue name
        name = f"replyto.{self.connection_name}-{self._campus.lower()}"
        if self._reply_queue_name is None:
            self._reply_queue_name = name
        else:
            if name != self._reply_queue_name:
                raise Exception("You cannot use both register_on_reply_to_callback and register_on_reply_to_callback_campus in the same EDM. Please use one other other")

        # call the same method for all reply-tos. The _reply_to_callbacks dict determines what other methods to call
        # delete the queue when all subscribers have closed. We don't want to persist these messages
        self._queue_manager.register_queue(
            Queue(name, bindings={name: self._on_reply_to}, consume=True, auto_delete=True)
        )

    def ensure_queue(
        self,
        queue_name: str,
        exchange_name: Optional[str] = None,
        exchange_type: ExchangeType = ExchangeType.TOPIC,
        auto_delete: bool = False,
    ) -> None:
        """
        Create the queue if it does not exist

        :param queue_name: Name of the queue
        :param exchange_name: Name of the exchange
        :param exchange_type: Type of the exchange
        :param auto_delete: Auto delete the queue
        """
        if exchange_name:
            self._queue_manager.register_exchange(Exchange(exchange_name, exchange_type=exchange_type))
        self._queue_manager.register_queue(Queue(queue_name, auto_delete=auto_delete))

    def ensure_exchange(
        self, exchange: str, exchange_type: ExchangeType = ExchangeType.TOPIC, auto_delete: bool = False
    ) -> None:
        """
        Create the exchange if it does not exist

        :param exchange: Name of the exchange
        :param exchange_type: Type of the exchange
        :param auto_delete: Auto delete the exchange
        """
        self._queue_manager.register_exchange(Exchange(exchange, exchange_type=exchange_type, auto_delete=auto_delete))

    def register_after_channel_open_callback(self, callback: Callable) -> None:
        """
        Registers a callback for processing after the channel has been opened.

        :param callback: Function to call after this service opens a channel to the MQ
        """
        self._logger.debug("Registering after_channel_open callback: %s", callback)
        self._after_channel_open_callbacks.append(callback)

    def _after_channel_open(self) -> None:
        """Called when the channel has been opened."""
        self._logger.debug("Calling after_channel_open callbacks. %s total", len(self._after_channel_open_callbacks))
        for cb in self._after_channel_open_callbacks:
            cb()

    def register_on_ready_callback(self, callback: Callable) -> None:
        """
        Registers a callback for processing after all queues have been registered and the service is ready to receive messages.
        This method should be preferred over `register_after_channel_open_callback` when this service needs to receive messages.

        :param callback: Function to call after this service is ready to receive messages
        """
        self._logger.debug("Registering on_ready callback: %s", callback)
        self._on_ready_callbacks.append(callback)

    def _on_ready(self) -> None:
        """Called when all queues are registered and the service is ready to receive messages"""
        self._logger.info("EDM ready")
        self._logger.debug("Calling on_ready callbacks. %s total", len(self._on_ready_callbacks))
        for cb in self._on_ready_callbacks:
            cb()

    def _shutdown_interupt_cb(self, interupt_signal: int, frame) -> None:
        """Captures interupt signals"""
        if not self._closing:
            # finish processing any current messages
            self._logger.info("Got KeyboardInterrupt. Closing gracefully.")
            self._logger.info("Send signal again for hard shutdown.")
            self.stop()
        else:
            # close now, interupting any currently processing messages
            self._logger.info("Hard shutdown!")
            os._exit(0)  # pylint: disable=protected-access


# FROM ETL SECTION

def publish_message(
    routing_key: str,
    body: Dict,
    service_name,
    exchange: Optional[str] = None,
    endpoint: Optional[str] = None,
    reply_to: bool = False,
    timeout: int = 300,
) -> Optional[Dict]:
    """
    Sends a message on the eventhub to the exchange on the routing key
    Requires access to `/ces/eventhub/secrets/edm/credentials` and `/ces/eventhub/config/mq_endpoint in parameter store
    :param exchange: The exchange to publish to. if not set then the campus environment variable will be used
    :param routing_key: The routing key to send the message to
    :param body: The body of the message to send
    :param reply_to: if an reply is expected (blocking until reply is received)
    :param timeout: The timeout to wait for a response in seconds
    :return: The dict of the reply if requested
    """

    logger = cessoc_logging.getLogger("cessoc")

    if exchange is None:  # if there is no exchange, use the campus environment variable
        exchange = os.getenv("CAMPUS").lower()
    # Create connection
    if not endpoint:
        # endpoint = os.environ["mq-endpoint"]
        endpoint = "cessoc-rabbitmq-dev.apps.ocp.byu.edu"

    credentials = pika.PlainCredentials(
        os.environ["username"], os.environ["password"]
    )
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            endpoint,
            credentials=credentials,
            ssl_options=pika.SSLOptions(context=ssl.create_default_context()),
            client_properties={"connection_name": service_name},
        )
    )

    # Create channel
    channel = connection.channel()
    channel.exchange_declare(
        exchange, exchange_type="direct", passive=True, durable=True
    )

    # Setup the replyto queue
    reply_to_queue = None
    if reply_to:
        reply_to_queue = f"replyto.{service_name}"
        channel.queue_declare(reply_to_queue, exclusive=True, auto_delete=True)

    # Setup some metadata
    correlation_id = uuid.uuid4().hex
    headers: Optional[Dict] = None
    if reply_to:
        if headers is None:
            headers = {}
        headers[
            "Reply-To-Callback"
        ] = "inline_blocking"  # Removing this errors out the replying EDM

    properties = pika.BasicProperties(
        app_id=service_name,
        user_id="edm",
        content_type="application/json",
        content_encoding="utf-8",
        reply_to=reply_to_queue,
        correlation_id=correlation_id,
        priority=None,
        headers=headers,
    )

    # Publish the message
    try:
        channel.basic_publish(
            exchange, routing_key, json.dumps(body), properties, mandatory=True
        )
        logger.info(
            "Published message to exchange '%s' with routing key '%s' and correlation id '%s'",
            exchange,
            routing_key,
            correlation_id,
        )
    except pika.exceptions.UnroutableError:
        logger.error("Message was unroutable")

    # Consume all messages on the response queue
    if reply_to and reply_to_queue is not None:
        start_time = int(time.time())
        while start_time + timeout > int(
            time.time()
        ):  # Weird looping to accommodate timeout with a generator
            method_frame, properties, reply_body = channel.basic_get(reply_to_queue)
            if (method_frame is not None and properties is not None and reply_body is not None):
                if (
                    properties.correlation_id == correlation_id
                ):  # Only accept the correlated reply
                    logger.info(
                        "Reply received with correlation id: %s", correlation_id
                    )
                    channel.basic_ack(method_frame.delivery_tag)
                    channel.cancel()  # Re-queues anything it may have picked up
                    channel.close()
                    connection.close()
                    return json.loads(reply_body)
                else:  # Reject non-correlated messages
                    logger.debug("Rejecting message")
                    channel.basic_nack(method_frame.delivery_tag)
                channel.cancel()
                break
        logger.error(
            "Message timed out before a reply was received, correlation id: %s",
            correlation_id,
        )

    # Clean up connection and channel
    channel.close()
    connection.close()
    return None
