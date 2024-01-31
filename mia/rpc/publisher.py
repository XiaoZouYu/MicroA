from __future__ import absolute_import

import uuid
import asyncio

from aio_pika.patterns.rpc import RPCMessageType

from mia.scripts import ScriptDependencyProvider, ScriptController
from mia.containers import WorkerContext
from mia.log import logger
from mia.rpc.message import RpcMessage, HeaderEncoder
from aio_pika.abc import AbstractIncomingMessage, DeliveryMode

from typing import Any, Callable, Tuple, Optional, Dict, Coroutine

RPC_REPLY_QUEUE_TEMPLATE = 'rpc.reply-{}-{}'


class RpcReplyListener(ScriptController):
    message = RpcMessage()
    queue_name_format: str = RPC_REPLY_QUEUE_TEMPLATE

    __slots__ = ("futures", "routing_key", "reply_queue_uuid")

    def __init__(self, *args, **kwargs):
        super(RpcReplyListener, self).__init__(*args, **kwargs)
        self.futures: Dict[str, asyncio.Future] = {}
        self.routing_key: Optional[str] = None
        self.reply_queue_uuid: Optional[str] = None

    @property
    def queue_name(self):
        service_name = self.container.service_name
        queue_name = self.queue_name_format.format(service_name, self.reply_queue_uuid)
        return queue_name

    async def setup(self) -> None:
        self.reply_queue_uuid = uuid.uuid4()

    async def start(self) -> None:
        queue_name = self.queue_name
        self.routing_key = str(self.reply_queue_uuid)
        await self.message.spawn_queue(queue_name, self.routing_key, self.handle_message)

    async def handle_message(self, message: "AbstractIncomingMessage") -> None:
        logger.debug("开始处理回调信息 %r", message)
        if not message.correlation_id:
            logger.warning("消息没有correlation_id，这是不能被接收的 %r", message)
            return

        future = self.futures.pop(message.correlation_id, None)
        if future is None:
            logger.warning("不知道这个消息该由谁来处理 %r", message)
            return

        try:
            payload = self.message.serializer.deserialize_message(message)
        except Exception as e:
            logger.error("无法序列化返回的消息 %r", message)
            future.set_exception(e)
            return

        if message.type == RPCMessageType.RESULT.value:
            future.set_result(payload)
        elif message.type == RPCMessageType.ERROR.value:
            if not isinstance(payload, Exception):
                payload = Exception("包装无异常对象 ", payload)
            future.set_exception(payload)
        elif message.type == RPCMessageType.CALL.value:
            future.set_exception(
                asyncio.TimeoutError("超时消息", message),
            )
        else:
            future.set_exception(
                RuntimeError("错误消息类型 %r" % message.type),
            )

    def __remove_future(self, correlation_id: str) -> Callable[[asyncio.Future], None]:
        def do_remove(future: asyncio.Future) -> None:
            logger.debug("移除future %r", future)
            self.futures.pop(correlation_id, None)

        return do_remove

    def create_future(self) -> Tuple[asyncio.Future, str]:
        future = self.container.create_future()
        correlation_id = str(uuid.uuid4())
        self.futures[correlation_id] = future
        future.add_done_callback(self.__remove_future(correlation_id))
        return future, correlation_id

    async def stop(self) -> None:
        logger.debug("取消正在运行的futures %r", self.futures)
        for future in self.futures.values():
            if future.done():
                continue

            future.set_exception(asyncio.CancelledError)

    async def send_message(self, msg, routing_key, *args, **kwargs) -> None:
        result_message = self.message.serializer.serialize_message(
            payload=msg, content_type=self.message.serializer.content_type,
            *args, **kwargs
        )
        await self.message.publish(
            result_message, routing_key=routing_key, mandatory=True, exchange=await self.message.get_default_exchange()
        )


class RpcProxy(ScriptDependencyProvider):
    """
    RPC代理类。

    用于提供对远程服务的RPC调用代理。

    Usage::

        >>> from mia.rpc.publisher import RpcProxy
        >>> class Service:
        ...     name = "service"
        ...     target_service = RpcProxy("service_name")
        ...
        ...     async def test_rpc(self):
        ...         result = await self.target_service.func.wait()

    :attr rpc_reply_listener: 用于监听RPC回复的实例。
    :param target_service: 目标服务的名称。
    :return: 返回一个 `ServiceProxy` 实例，用于处理RPC调用。
    """

    rpc_reply_listener = RpcReplyListener()

    __slots__ = ("target_service",)

    def __init__(self, target_service: str):
        self.target_service = target_service

    def get_dependency(self, worker_ctx: "WorkerContext") -> Any:
        return ServiceProxy(
            worker_ctx,
            self.target_service,
            self.rpc_reply_listener
        )


class ServiceProxy(object):
    __slots__ = ("worker_ctx", "service_name", "reply_listener")

    def __init__(
            self, worker_ctx: "WorkerContext",
            service_name: str,
            reply_listener: "RpcReplyListener"
    ) -> None:
        self.worker_ctx = worker_ctx
        self.service_name = service_name
        self.reply_listener = reply_listener

    def __getattr__(self, name: str) -> "MethodProxy":
        return MethodProxy(
            self.worker_ctx,
            self.service_name,
            name,
            self.reply_listener
        )


class MethodProxy(HeaderEncoder):
    """
    RPC方法实现类。

    这个类的主要目的是实现对远程服务的RPC调用。通过传入上层 `RpcReplyListener` 实例，负责处理消息的发送和监听。

    允许一个服务请求另一个服务执行特定的操作，跨服务调用并获取结果。

    通过这个 `MethodProxy` 类，可以方便地发起对目标服务的方法调用，并等待调用结果。
    该类的实例在初始化时接收当前的 `WorkerContext` 上下文、目标服务的名称、目标方法的名称以及 `RpcReplyListener` 实例。

    通过 `__call__` 方法，可以发起异步的RPC调用，或者通过 `wait` 方法等待调用的结果。底层的 `_call` 方法实现了实际的RPC调用逻辑。

    Usage::

        # 创建 worker_ctx 实例
        >>> worker_ctx = WorkerContext()
        # 创建 rpc_reply_listener 实例，真实的实例需要启动 `start`
        >>> rpc_reply_listener = RpcReplyListener()
        # 创建 MethodProxy 实例
        >>> method_proxy = MethodProxy(worker_ctx, "example_service", "example_method", rpc_reply_listener)
        # 使用 `wait()` 方法等待结果
        >>> result1 = await method_proxy.wait()
        # 使用异步获取
        >>> result2 = await method_proxy()
        ... # 做其他事情
        >>> result2 = await result2

        # 发起 rpc 调用
        result = await method_proxy.wait(arg1, arg2, kwarg1=value1)

    :param worker_ctx: 当前的 WorkerContext 上下文。
    :param service_name: 目标服务的名称。
    :param method_name: 目标服务中要调用的方法名称。
    :param reply_listener: 用于处理RPC回调的监听器。
    """

    __slots__ = ("worker_ctx", "service_name", "method_name", "reply_listener")

    def __init__(
            self, worker_ctx: "WorkerContext",
            service_name: str,
            method_name: str,
            reply_listener: "RpcReplyListener"
    ) -> None:
        self.worker_ctx = worker_ctx
        self.service_name = service_name
        self.method_name = method_name
        self.reply_listener = reply_listener

    def __call__(self, *args, **kwargs) -> Coroutine:
        return self._call(*args, **kwargs)

    async def wait(self, *args, **kwargs) -> Any:
        reply = await self._call(*args, **kwargs)
        return await reply

    async def _call(self, *args, **kwargs) -> asyncio.Future:
        logger.debug("调用 %s", self)
        msg = {'args': args, 'kwargs': kwargs}

        future, correlation_id = self.reply_listener.create_future()
        headers = self.get_message_headers(worker_ctx=self.worker_ctx)
        headers.update({"FROM": self.reply_listener.queue_name})

        await self.reply_listener.send_message(
            msg,
            routing_key=f"{self.service_name}.{self.method_name}",
            message_type=RPCMessageType.CALL,
            correlation_id=correlation_id,
            delivery_mode=DeliveryMode.PERSISTENT,
            reply_to=self.reply_listener.queue_name,
            headers=headers,
            priority=5
        )

        return future
