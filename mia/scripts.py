from __future__ import absolute_import

import inspect
import types
import weakref
import asyncio
from functools import partial

from typing import Tuple, Dict, Any, Optional, Set, Union
from mia.abc import AbsScript, AbsServiceContainer
from mia.log import logger
from mia.exceptions import IncorrectSignature
from mia.config import AMQP_URL_CONFIG_KEY
from aio_pika.abc import AbstractIncomingMessage

ENTRYPOINT_SCRIPTS_ATTR = "mia_entrypoint"


class Script(AbsScript):
    _params: Optional[Tuple[Tuple, Dict[str, Any]]] = None
    container: "AbsServiceContainer" = None

    def __new__(cls, *args, **kwargs):
        inst = super(Script, cls).__new__(cls)
        inst._params = (args, kwargs)
        return inst

    async def setup(self) -> None:
        """初始化脚本"""
        pass

    async def start(self) -> None:
        """启动脚本。"""
        pass

    async def stop(self) -> None:
        """停止脚本。"""
        pass

    async def kill(self) -> None:
        """杀死脚本。"""
        pass

    def bind(self, container: "AbsServiceContainer", *_) -> "AbsScript":
        """ 获取一个与 `container` 绑定的该扩展的实例。

        Args:
            container: 要绑定的容器。

        Returns:
            Script: 绑定后的脚本实例。

        Raises:
            RuntimeError: 如果已经绑定，则无法再次绑定。
        """

        def clone(prototype: "Script") -> "Script":
            if prototype.is_bound():
                raise RuntimeError('无法对已绑定的扩展进行 `bind`。')

            cls = type(prototype)
            args, kwargs = prototype._params
            inst = cls(*args, **kwargs)
            inst.container = weakref.proxy(container)
            return inst

        instance = clone(self)

        # 递归处理子扩展
        for name, ext in inspect.getmembers(self, is_script):
            setattr(instance, name, ext.bind(container))
        return instance

    def is_bound(self) -> bool:
        """检查脚本是否已经绑定。"""
        return self.container is not None

    def __repr__(self) -> str:
        if not self.is_bound():
            return f'<{type(self).__name__} [unbound] at 0x{id(self):x}>'

        return f'<{type(self).__name__} at 0x{id(self):x}>'


class ScriptController(Script):
    def __init__(self, *args, **kwargs):
        self._scripts: Set["ScriptEntrypoint"] = set()
        self._scripts_registered: bool = False
        self._scripts_event: asyncio.Event = asyncio.Event()
        super(ScriptController, self).__init__()

    def bind(self, container: Any, *_: Tuple[Any, ...]) -> "AbsScript":
        """
        获取一个与 `container` 绑定的该扩展的实例。

        Args:
            container: 要绑定的容器。

        Returns:
            ScriptController: 绑定后的脚本控制器实例。

        Raises:
            RuntimeError: 如果已经绑定，则无法再次绑定。
        """
        controller_name = type(self)

        # 检查容器中是否已存在相同类型的控制器实例，如果存在则直接返回
        cont = container.controllers.get(controller_name)
        if cont:
            return cont

        instance = super(ScriptController, self).bind(container)

        # 将控制器实例添加到容器的控制器字典中
        container.controllers[controller_name] = instance

        return instance

    def register_provider(self, provider: "ScriptEntrypoint") -> None:
        """注册提供者到该控制器。"""
        self._scripts_registered = True
        logger.debug('注册提供者 %s 到 %s', provider, self)
        self._scripts.add(provider)

    def unregister_provider(self, provider: Any) -> None:
        """注销提供者。"""
        if provider not in self._scripts:
            return

        logger.debug('注销提供者 %s 到 %s', provider, self)

        self._scripts.remove(provider)
        if len(self._scripts) == 0:
            logger.debug('最后一个提供者注销 %s', self)
            self._scripts_event.set()

    async def wait(self) -> None:
        """等待直到所有提供者注销。"""
        if self._scripts_registered:
            logger.debug('开始挂载直到注销 %s', self)
            await self._scripts_event.wait()
            logger.debug('已全部注销 %s', self)

    async def stop(self) -> None:
        """停止控制器。"""
        await self.wait()


def register_entrypoint(fn, entrypoint):
    descriptors = getattr(fn, ENTRYPOINT_SCRIPTS_ATTR, None)

    if descriptors is None:
        descriptors = set()
        setattr(fn, ENTRYPOINT_SCRIPTS_ATTR, descriptors)

    descriptors.add(entrypoint)


class ScriptDependencyProvider(Script):
    attr_name: Optional[str] = None

    def bind(self, container: Union["AbsServiceContainer", Any], attr_name: str) -> "AbsScript":
        """获取一个与 `container` 绑定的该依赖项提供者的实例，使用`attr_name`作为属性名。"""
        instance = super().bind(container)
        instance.attr_name = attr_name
        self.attr_name = attr_name
        return instance

    def get_dependency(self, worker_ctx: Any) -> Any:
        """在工作实例执行之前调用。DependencyProvider 应该返回一个对象，
        该对象将由容器注入到工作实例中。
        """
        # 根据提供依赖项所需的逻辑来实现此方法。
        pass

    def __repr__(self):
        if not self.is_bound():
            return '<{} [unbound] at 0x{:x}>'.format(type(self).__name__, id(self))

        service_name = self.container.service_name
        return '<{} [{}.{}] at 0x{:x}>'.format(
            type(self).__name__, service_name, self.attr_name, id(self))


class ScriptEntrypoint(Script):
    method_name: Optional[str] = None

    def bind(self, container: "AbsServiceContainer", method_name: str) -> "AbsScript":
        """ 获取一个与 `container` 绑定的该入口点脚本的实例。"""
        instance = super(ScriptEntrypoint, self).bind(container)
        instance.method_name = method_name
        return instance

    def check_signature(self, args: Tuple[Any, ...], kwargs: dict) -> None:
        """检查入口点方法的签名是否正确。"""
        service_cls = self.container.service_cls
        fn = getattr(service_cls, self.method_name)
        try:
            service_instance = None  # fn is unbound
            # 使用 inspect 模块获取调用参数，并检查是否会引发 TypeError
            inspect.getcallargs(fn, service_instance, *args, **kwargs)
        except TypeError as exc:
            # 如果发生 TypeError，表示签名不正确，抛出 IncorrectSignature 异常
            raise IncorrectSignature(f"入口点方法 '{self.method_name}' 的签名不正确: {exc}")

    @property
    def amqp_url(self):
        return self.container.config.get(AMQP_URL_CONFIG_KEY, None)

    @classmethod
    def decorator(cls, *args, **kwargs):
        """ 入口点装饰器，用于注册入口点脚本。"""

        def registering_decorator(fn, arg, kwarg):
            instance = cls(*arg, **kwarg)
            register_entrypoint(fn, instance)
            return fn

        if len(args) == 1 and isinstance(args[0], types.FunctionType):
            """ 没有参数
            Usage::
                >>> @rpc
                    def spawn():
                    pass 
            """
            return registering_decorator(args[0], arg=(), kwarg={})
        else:
            """ 有参数
            Usage::
                >>> @rpc('params', ...)
                    def spawn():
                    pass 
            """
            return partial(registering_decorator, arg=args, kwarg=kwargs)

    async def handle_message(self, message: "AbstractIncomingMessage") -> None:
        """rabbit message handel"""
        pass

    def __repr__(self):
        if not self.is_bound():
            return '<{} [unbound] at 0x{:x}>'.format(
                type(self).__name__, id(self))

        service_name = self.container.service_name
        return '<{} [{}.{}] at 0x{:x}>'.format(
            type(self).__name__, service_name, self.method_name, id(self))


def is_script(obj: Any) -> bool:
    return isinstance(obj, Script)


def is_dependency(obj):
    return isinstance(obj, ScriptDependencyProvider)


def is_entrypoint(obj):
    return isinstance(obj, ScriptEntrypoint)


def iter_script(script: "Script"):
    """深度优先迭代器"""
    for _, ext in inspect.getmembers(script, is_script):
        for item in iter_script(ext):
            yield item
        yield ext
