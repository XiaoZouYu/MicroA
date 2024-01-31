from __future__ import unicode_literals

import inspect

from typing import Iterable, Optional


def get_module_path(exc_type):
    """ 返回 `exc_type` 的点模块路径，包括类名。

    Usage::

        >>> get_module_path(MethodNotFound)
        >>> "nameko.exceptions.MethodNotFound"

    """
    module = inspect.getmodule(exc_type)
    return "{}.{}".format(module.__class__.__name__, exc_type.__name__)


registry = {}


def deserialize_to_instance(exc_type):
    """ 将 `exc_type` 注册为可反序列化的装饰器实例，而不是 RemoteError。"""
    key = get_module_path(exc_type)
    registry[key] = exc_type
    return exc_type


class ConfigurationError(Exception):
    pass


class CommandError(Exception):
    """Raise from subcommands to report error back to the user"""


class ImportServiceError(Exception):
    """用于表示导入服务时发生的错误的异常类。"""


class IncorrectSignature(Exception):
    """签名相关的异常类。"""


class ContainerBeingKilled(Exception):
    """容器被杀掉后的异常"""


@deserialize_to_instance
class MethodNotFound(Exception):
    """没有找到方法"""


@deserialize_to_instance
class MethodExist(Exception):
    """已存在该方法"""


class ConfigParamsException(Exception):

    def __init__(self, key: str, value: Optional[str] = None):
        self.key = key
        self.value = value
        super(ConfigParamsException, self).__init__()


class RpcMessageError(Exception):
    """Rpc Message Error"""


