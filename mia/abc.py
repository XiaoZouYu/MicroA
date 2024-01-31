from __future__ import absolute_import

import asyncio
import dataclasses

from functools import partial
from abc import ABC, abstractmethod, abstractproperty
from enum import Enum, IntEnum, unique
from typing import Coroutine, Tuple, Dict, Optional, overload, Any, Union

from aio_pika.patterns.base import CallbackType


class ScriptError(Exception):
    """Script相关错误！"""


@unique
class EventHandlerType(str, Enum):
    SERVICE_POOL = "service_pool"
    SINGLETON = "singleton"
    BROADCAST = "broadcast"


@dataclasses.dataclass(frozen=True)
class AbsWorkerContext:
    container: "AbsServiceContainer"
    service: object
    entrypoint: "AbsScript"
    args: Tuple
    kwargs: Dict
    data: Dict

    @property
    def config(self):
        return self.container.config

    @property
    def service_name(self):
        return self.container.service_name

    def __repr__(self):
        return f'<{type(self).__name__} [{self.service_name}.{self.entrypoint}] at 0x{id(self):x}>'


class AbsServiceContainer:
    service_name: str

    def __init__(self, service_cls, config):
        self.service_cls = service_cls
        self.config = config
        self.loop = asyncio.get_event_loop()

    @abstractmethod
    async def start(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def stop(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def kill(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def wait(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def spawn_manager(self, fn: Coroutine, name: Optional[str] = None) -> asyncio.Task:
        raise NotImplementedError

    @abstractmethod
    async def spawn_worker(
            self,
            entrypoint: "AbsScript",
            args: Tuple,
            kwargs: Dict,
            context_data: Optional[Dict] = None,
            handle_result: Optional[CallbackType] = None
    ) -> "AbsWorkerContext":
        raise NotImplementedError

    def create_future(self) -> asyncio.Future:
        return self.loop.create_future()


class AbsScript:
    @abstractmethod
    async def start(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def stop(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def kill(self) -> None:
        raise NotImplementedError

    def bind(self, container: "AbsServiceContainer", *_) -> "AbsScript":
        raise NotImplementedError
    
    def __call__(self, *args, **kwargs):
        raise ScriptError("不应该调用 `call` 方法，`script` 应该正常使用 `setup`、`start`、`stop`、`kill`等规定方法！")
    
    def __getattr__(self, item):
        raise AttributeError(f"不存在属性 {item}")


class AbsSerializer:
    """
    Abstract base class for serializers.
    """
    SERIALIZER = None
    CONTENT_TYPE = None

    __slots__ = ()

    def serialize(self, data: Any) -> bytes:
        """
        Serialize the given data.

        Args:
            data (Any): The data to be serialized.

        Returns:
            bytes: The serialized data.
        """
        raise NotImplementedError("serialize method must be implemented in derived class")

    def deserialize(self, data: bytes) -> Any:
        """
        Deserialize the given data.

        Args:
            data (bytes): The data to be deserialized.

        Returns:
            Any: The deserialized data.
        """
        raise NotImplementedError("deserialize method must be implemented in derived class")
