from __future__ import absolute_import

from mia.abc import AbsServiceContainer
from mia.rpc.publisher import ServiceProxy, RpcReplyListener
from mia.config import AMQP_URL_CONFIG_KEY
from mia.containers import WorkerContext
from mia.scripts import ScriptEntrypoint


class RpcReplyListenerProxy(RpcReplyListener):
    queue_name_format = 'proxy.mia-{}'

    def __init__(self, container, *args, **kwargs):
        super(RpcReplyListenerProxy, self).__init__(*args, **kwargs)
        self.container = container
        self.message.container = container

    @property
    def queue_name(self):
        queue_name = self.queue_name_format.format(self.reply_queue_uuid)
        return queue_name

    async def setup(self) -> None:
        await self.message.setup()
        await super(RpcReplyListenerProxy, self).setup()

    async def start(self) -> None:
        await self.message.start()
        await super(RpcReplyListenerProxy, self).start()
    
    async def stop(self) -> None:
        await self.message.stop()
        await super(RpcReplyListenerProxy, self).stop()


class RpcProxyBase(object):
    _proxy = None

    __slots__ = ("_worker_context", "_reply_listener")

    def __init__(self, rabbitmq_url, context_data=None, reply_listener_cls=RpcReplyListenerProxy):
        container = type(
            'proxy_container',
            (AbsServiceContainer,),
            {}
        )(object, {AMQP_URL_CONFIG_KEY: rabbitmq_url})
        self._worker_context = WorkerContext(
            container, service=None, entrypoint=type('Dummy', (ScriptEntrypoint,), {'method_name': 'call'})(),
            data=context_data or {}, args=(), kwargs={}
        )
        self._reply_listener = reply_listener_cls(container)

    async def __aenter__(self):
        return await self.start()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()

    async def start(self):
        await self._reply_listener.setup()
        await self._reply_listener.start()
        return self._proxy

    async def stop(self):
        await self._reply_listener.stop()


class PackServiceProxy(object):

    __slots__ = ("_worker_context", "_reply_listener")

    def __init__(self, worker_context, reply_listener):
        self._worker_context = worker_context
        self._reply_listener = reply_listener

    def __getattr__(self, item):
        return ServiceProxy(
            self._worker_context, item, self._reply_listener
        )


class ProxyRpc(RpcProxyBase):

    def __init__(self, *args, **kwargs):
        super(ProxyRpc, self).__init__(*args, **kwargs)
        self._proxy = PackServiceProxy(
            self._worker_context, self._reply_listener
        )
