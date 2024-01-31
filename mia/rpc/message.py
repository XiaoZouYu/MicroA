from __future__ import absolute_import

import asyncio
import time
import pickle
import json
import weakref

import six
from aio_pika.abc import (
    AbstractChannel,
    AbstractQueue,
    AbstractExchange,
    ExchangeType,
    AbstractIncomingMessage,
    DeliveryMode,
    AbstractRobustConnection
)
from aio_pika import connect_robust
from aio_pika.message import Message
from aio_pika.patterns.rpc import RPCMessageType

from mia.abc import AbsSerializer
from mia.log import logger
from mia.scripts import ScriptController
from mia.config import AMQP_URL_CONFIG_KEY, RPC_EXCHANGE_CONFIG_KEY, SERIALIZER_CONFIG_KEY, DEFAULT_SERIALIZER, \
    HEADER_PREFIX
from mia.exceptions import MethodNotFound, ConfigParamsException, RpcMessageError
from aiormq.exceptions import ChannelPreconditionFailed

from typing import Tuple, Dict, Callable, Any, Optional, Union, List

RPC_REPLY_QUEUE_TTL = 300000  # ms (5 min)


class PickleSerializer(AbsSerializer):
    """
    Abstract base class for serializers.
    """
    SERIALIZER = pickle
    CONTENT_TYPE = "application/python-pickle"

    def serialize(self, data: Any) -> bytes:
        return self.SERIALIZER.dumps(data)

    def deserialize(self, data: bytes) -> Any:
        return self.SERIALIZER.loads(data)


class JsonSerializer(AbsSerializer):
    """
    Serializer for Pickle format.
    """
    SERIALIZER = json
    CONTENT_TYPE = "application/json"

    def serialize(self, data: Any) -> bytes:
        return self.SERIALIZER.dumps(data).encode()

    def deserialize(self, data: bytes) -> Any:
        return self.SERIALIZER.loads(data.decode())


class SerializerFactory:
    serializer: AbsSerializer

    def __init__(self, serializer_type: Optional[str] = None):
        if serializer_type == "pickle":
            self.serializer = PickleSerializer()
        elif serializer_type == "json":
            self.serializer = JsonSerializer()
        else:
            raise ConfigParamsException(SERIALIZER_CONFIG_KEY, serializer_type)

    @property
    def content_type(self):
        return self.serializer.CONTENT_TYPE

    def serialize(self, data: Any) -> bytes:
        return self.serializer.serialize(data)

    def serialize_exception(self, exception: Exception) -> Any:
        exc = {
            "exc_type": type(exception).__name__,
            "exc_args": exception.args,
            "value": repr(exception),
        }
        return self.serialize(exc)

    def serialize_message(
            self, payload: Optional[Union[bytes, str, Dict, Tuple]], message_type: RPCMessageType, content_type: str,
            correlation_id: Optional[str], delivery_mode: DeliveryMode,
            **kwargs: Any,
    ) -> Message:
        if not isinstance(payload, bytes):
            payload = self.serialize(payload)

        return Message(
            payload,
            content_type=content_type,
            correlation_id=correlation_id,
            delivery_mode=delivery_mode,
            timestamp=time.time(),
            type=message_type.value,
            **kwargs,
        )

    def deserialize_message(self, message: "AbstractIncomingMessage") -> Any:
        payload = self.serializer.deserialize(message.body)
        if message.type == RPCMessageType.ERROR:
            if payload["exc_type"] == "MethodNotFound":
                payload = MethodNotFound(payload["exc_args"])
            else:
                payload = RpcMessageError(f"rpc return error message: {payload}")
        return payload


class HeaderDecoder(object):
    header_prefix = HEADER_PREFIX

    def _strip_header_name(self, key):
        full_prefix = "{}.".format(self.header_prefix)
        if key.startswith(full_prefix):
            return key[len(full_prefix):]
        return key

    def unpack_message_headers(self, message):
        stripped = {
            self._strip_header_name(k): v
            for k, v in six.iteritems(message.headers)
        }
        return stripped


class HeaderEncoder(object):
    header_prefix = HEADER_PREFIX

    def _get_header_name(self, key):
        return "{}.{}".format(self.header_prefix, key)

    def get_message_headers(self, worker_ctx):
        data = worker_ctx.context_data

        if None in data.values():
            logger.warning(
                'Attempted to publish unserialisable header value. '
                'Headers with a value of `None` will be dropped from '
                'the payload.', UserWarning)

        headers = {self._get_header_name(key): value
                   for key, value in data.items()
                   if value is not None}
        return headers


class RpcMessage(ScriptController):
    connection: AbstractRobustConnection
    channel: AbstractChannel
    serializer: SerializerFactory

    def __init__(self, *args, **kwargs):
        super(RpcMessage, self).__init__(*args, **kwargs)
        # self.queues: Dict[str, "AbstractQueue"] = {}
        self.exchanges: Dict[str, "AbstractExchange"] = {}
        self.queues: List[Tuple[str, "AbstractQueue", "AbstractExchange"]] = []

    @property
    def amqp_url(self):
        url = self.container.config.get(AMQP_URL_CONFIG_KEY)
        if not url:
            raise ConfigParamsException(AMQP_URL_CONFIG_KEY)
        return url

    async def setup(self) -> None:
        self.serializer = SerializerFactory(
            self.container.config.get(SERIALIZER_CONFIG_KEY, DEFAULT_SERIALIZER)
        )
        await self.init_channel()

    async def stop(self) -> None:
        await self.close()

    async def close(self) -> None:
        # 关闭队列
        # await self.close_queue(self.consumer_tag, self.queue, {"From": self.queue.name, "x-match": "any"})
        await asyncio.gather(*(self.close_queue(*pack) for pack in self.queues))

        await self.connection.close()

    async def close_queue(self, tag: str, queue: "AbstractQueue", exchange: "AbstractExchange"):
        try:
            await queue.cancel(tag)
            await queue.unbind(exchange, "", arguments={"From": queue.name, "x-match": "any"})
            await queue.delete()
        except ChannelPreconditionFailed:
            logger.warning(f"还有其他客户端正在监听队列，该队不可被删除 {queue}")
        finally:
            del queue

    async def init_channel(self):
        self.connection = await connect_robust(self.amqp_url)
        self.channel = await self.connection.channel()

    async def handle_result(
            self, message: "AbstractIncomingMessage",
            result: Any, exc_info: Optional[Exception]
    ) -> None:
        message_type = RPCMessageType.ERROR
        if exc_info is None:
            try:
                payload = self.serializer.serialize(result)
                message_type = RPCMessageType.RESULT
            except Exception as e:
                logger.debug("压缩数据出错 %r", e)
                payload = self.serializer.serialize_exception(e)
        else:
            payload = self.serializer.serialize_exception(exc_info)

        try:
            result_message = self.serializer.serialize_message(
                payload,
                content_type=self.serializer.content_type,
                correlation_id=message.correlation_id,
                delivery_mode=message.delivery_mode,
                message_type=message_type
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            result_message = self.serializer.serialize_message(
                self.serializer.serialize_exception(e),
                content_type=self.serializer.content_type,
                correlation_id=message.correlation_id,
                delivery_mode=message.delivery_mode,
                message_type=RPCMessageType.ERROR
            )

        try:
            await self.publish(result_message, message.reply_to)
        except Exception as e:
            logger.warning("错误发送 `reply` 消息 %r, error %r", (result_message, e))
            await message.reject(requeue=False)

        await message.ack()

    async def publish(
            self,
            message: "Message",
            routing_key: str,
            exchange: Optional["AbstractExchange"] = None,
            mandatory: bool = False,
            **kwargs
    ):
        # The default exchange is implicitly bound to every queue, with a routing key equal to the queue name.
        # It is not possible to explicitly bind to, or unbind from the default exchange.
        # It also cannot be deleted.
        exchange = exchange or self.channel.default_exchange
        await exchange.publish(
            message, routing_key, mandatory=mandatory, **kwargs
        )

    async def spawn_queue(
            self,
            queue_name: str,
            routing_key: str,
            callback: Callable,
            exchange: Optional["AbstractExchange"] = None,
            auto_delete: bool = True,
            durable: bool = False,
            **kwargs: Any
    ) -> "AbstractQueue":
        exchange = exchange or await self.get_default_exchange()
        queue = await self.channel.declare_queue(
            queue_name,
            auto_delete=auto_delete,
            durable=durable,
            **kwargs
        )
        await queue.bind(
            exchange,
            routing_key=routing_key,
            arguments={'x-expires': RPC_REPLY_QUEUE_TTL}
        )
        tag = await queue.consume(callback=callback)
        self.queues.append((tag, queue, exchange))
        return weakref.proxy(queue)

    async def spawn_exchange(
            self,
            exchange_name: str = '',
            exchange_type: ExchangeType = ExchangeType.TOPIC,
            auto_delete: bool = True,
            durable: bool = True
    ):
        if exchange_name in self.exchanges:
            return self.exchanges[exchange_name]

        exchange = await self.channel.declare_exchange(
            exchange_name, type=exchange_type, auto_delete=auto_delete, durable=durable
        )
        self.exchanges[exchange.name] = exchange
        return exchange

    async def get_default_exchange(self):
        return await self.spawn_exchange(
            exchange_name = self.container.config.get(RPC_EXCHANGE_CONFIG_KEY, 'mia-rpc')
        )
