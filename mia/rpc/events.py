"""
提供核心消息传递的高级模块。

在这个模块中，事件被视为一种特殊的消息，可由其他服务监听。每个事件包含一个标识符和相关数据。通过实例化 EventDispatcher 类并注入到服务中，可以实现对事件的分派。

事件分派是异步的，它负责通知注册的监听器事件的发生。虽然它能够确保事件被分发，但无法保证是否有服务监听并处理这些事件。

要使服务能够监听事件，服务需要使用 handle_event 入口点声明一个处理程序，并提供目标服务和事件类型的过滤器。这样，当事件被分发时，相关的处理程序将被调用。
"""
from __future__ import absolute_import
import uuid
from aio_pika.patterns.rpc import RPCMessageType

from mia.log import logger
from mia.abc import EventHandlerType
from mia.rpc.message import RpcMessage, HeaderEncoder
from mia.scripts import ScriptDependencyProvider
from mia.rpc.consumer import Rpc

from aio_pika.abc import AbstractExchange, DeliveryMode, AbstractIncomingMessage
from typing import Optional, Any

EVENT_QUEUE_TEMPLATE = 'event.mia-{}'


class EventDispatcher(ScriptDependencyProvider, HeaderEncoder):
    message = RpcMessage()

    def __init__(self, *args: Any, **kwargs: Any):
        super(EventDispatcher, self).__init__(*args, **kwargs)
        self.exchange: Optional[AbstractExchange] = None

    def get_event_exchange_name(self) -> str:
        return EVENT_QUEUE_TEMPLATE.format(self.container.service_name)

    async def start(self) -> None:
        self.exchange = await self.message.spawn_exchange(
            self.get_event_exchange_name()
        )

    def get_dependency(self, worker_ctx: Any) -> Any:
        headers = self.get_message_headers(worker_ctx)

        async def dispatcher(event_key: str, *args: Any, **kwargs: Any) -> None:
            msg = {'args': args, 'kwargs': kwargs}
            await self.message.publish(
                message=self.message.serializer.serialize_message(
                    payload=msg,
                    message_type=RPCMessageType.CALL,
                    content_type=self.message.serializer.content_type,
                    correlation_id=None,
                    delivery_mode=DeliveryMode.PERSISTENT,
                    headers=headers
                ),
                routing_key=event_key,
                exchange=self.exchange
            )

        return dispatcher


class EventHandler(Rpc):
    rpc_message = RpcMessage()

    def __init__(
        self,
        source_service: str,
        event_type: str,
        handler_type: EventHandlerType = EventHandlerType.SERVICE_POOL,
        *args: Any,
        **kwargs: Any
    ):
        super(EventHandler, self).__init__(*args, **kwargs)
        self.source_service = source_service
        self.event_type = event_type
        self.handler_type = handler_type

    def get_event_exchange_name(self) -> str:
        return EVENT_QUEUE_TEMPLATE.format(self.source_service)

    def deserialize_message(self, message: AbstractIncomingMessage) -> Any:
        return self.rpc_message.serializer.deserialize_message(message)

    async def start(self) -> None:
        exclusive = self.handler_type is EventHandlerType.BROADCAST
        service_name = self.container.service_name

        if self.handler_type not in EventHandlerType:
            raise TypeError(f"错误handler_type: {self.handler_type}")

        queue_name = f"{self.handler_type.value}.{self.event_type}"
        if self.handler_type is EventHandlerType.SERVICE_POOL:
            queue_name = f"{queue_name}-{service_name}.{self.method_name}"
        elif self.handler_type is EventHandlerType.BROADCAST:
            queue_name = f"{queue_name}-{service_name}.{self.method_name}-{uuid.uuid4().hex}"

        await self.rpc_message.spawn_queue(
            queue_name, routing_key=self.event_type, callback=self.handle_message,
            exchange=await self.rpc_message.spawn_exchange(
                self.get_event_exchange_name()
            ), durable=True, exclusive=exclusive
        )

    async def handle_result(
        self,
        message: AbstractIncomingMessage,
        worker_ctx: Any,
        result: Any,
        exc_info: Optional[Exception],
    ) -> None:
        logger.debug("处理返回结果 %r => %r error: %r", worker_ctx, result, exc_info)
        if exc_info:
            logger.warning(f"event {self} exec error {exc_info}")
            raise exc_info


event_handler: EventHandler = EventHandler.decorator
