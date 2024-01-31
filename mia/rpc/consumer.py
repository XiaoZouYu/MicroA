from __future__ import absolute_import

from functools import partial

from mia.scripts import ScriptEntrypoint, ScriptController
from mia.exceptions import ContainerBeingKilled, MethodNotFound, MethodExist
from mia.log import logger
from mia.rpc.message import RpcMessage, HeaderDecoder
from aio_pika.abc import AbstractIncomingMessage

from typing import Dict, Callable, Any

DEFAULT_ROUTING_KEY = '{}.*'
RPC_QUEUE_TEMPLATE = 'rpc.mia-{}'


class RpcConsumer(ScriptController):
    message: RpcMessage = RpcMessage()

    def __init__(self, *args, **kwargs):
        super(RpcConsumer, self).__init__(*args, **kwargs)
        self.routes: Dict[str, Callable] = {}

    async def start(self) -> None:
        await self.message.spawn_queue(
            RPC_QUEUE_TEMPLATE.format(self.container.service_name),
            DEFAULT_ROUTING_KEY.format(self.container.service_name),
            self.handle_message,
            durable=True
        )

    async def handle_message(self, message: "AbstractIncomingMessage") -> None:
        logger.debug("处理返回消息 %r", message)

        async def pack_handle_message():
            try:
                await self.routes[message.routing_key](message)
            except Exception as e:
                logger.warning(f"来自于 `routing_key` {message.routing_key} 执行出现错误 {e}")
                await self.handle_result(message, None, e)

        if message.routing_key not in self.routes:
            await self.handle_result(message, None, MethodNotFound(f'{message.routing_key} 不存在'))
        else:
            await self.container.spawn_manager(pack_handle_message())

    async def handle_result(self, *args, **kwargs):
        await self.message.handle_result(*args, **kwargs)

    def register_provider(self, provider: "ScriptEntrypoint") -> None:
        super(RpcConsumer, self).register_provider(provider)
        method_name = f'{self.container.service_name}.{provider.method_name}'
        if method_name in self.routes:
            raise MethodExist(method_name)
        self.routes[method_name] = provider.handle_message

    def unregister_provider(self, provider: "ScriptEntrypoint") -> None:
        super(RpcConsumer, self).unregister_provider(provider)
        method_name = f'{self.container.service_name}.{provider.method_name}'
        del self.routes[method_name]

    def deserialize_message(self, message: "AbstractIncomingMessage"):
        return self.message.serializer.deserialize_message(message)


class Rpc(ScriptEntrypoint, HeaderDecoder):
    rpc_message: RpcConsumer = RpcConsumer()

    async def start(self) -> None:
        self.rpc_message.register_provider(self)

    async def stop(self) -> None:
        self.rpc_message.unregister_provider(self)

    def deserialize_message(self, message: "AbstractIncomingMessage") -> Any:
        return self.rpc_message.deserialize_message(message)

    async def handle_message(self, message: "AbstractIncomingMessage") -> None:
        body = self.deserialize_message(message)
        try:
            args = body['args']
            kwargs = body['kwargs']
        except KeyError:
            logger.warning("无法获取 `args` and `kwargs`")
            await message.reject(requeue=False)
            raise

        self.check_signature(args, kwargs)
        # todo
        # self.unpack_message_headers(message.headers)

        handle_result = partial(self.handle_result, message)
        try:
            await self.container.spawn_worker(self, args, kwargs,
                                              context_data=message.headers,
                                              handle_result=handle_result)
        except ContainerBeingKilled:
            await message.reject(requeue=True)

    async def handle_result(self, message: "AbstractIncomingMessage", worker_ctx, result, exc_info):
        logger.debug("处理返回结果 %r => %r error: %r", worker_ctx, result, exc_info)
        await self.rpc_message.handle_result(message, result, exc_info)
