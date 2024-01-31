from __future__ import print_function

import asyncio
import sys
import yaml
import os
import re
import six
import inspect
import signal
import socket

from mia.runner import ServiceRunner
from mia.exceptions import ImportServiceError
from mia.scripts import ENTRYPOINT_SCRIPTS_ATTR

from typing import List, Any, Dict


def is_class(obj: Any) -> bool:
    return isinstance(obj, six.class_types)


def is_entrypoint(method: str) -> bool:
    return hasattr(method, ENTRYPOINT_SCRIPTS_ATTR)


def import_service(module_name: str) -> List:
    """
    从Python模块中导入服务类。

    :param module_name: 要导入的Python模块的名称。
    :return services list
    """
    try:
        __import__(module_name)
    except ImportError as exc:
        if module_name.endswith(".py") and os.path.exists(module_name):
            raise ImportServiceError(
                f"导入服务失败，你是不是要说：'{module_name[:-3].replace('/', '.')}'?"
            )

        if re.match(f"^No module named '?{module_name}'?$", str(exc)):
            raise ImportServiceError(exc)

        raise

    module = sys.modules[module_name]

    found_services = [
        potential_service
        for _, potential_service in inspect.getmembers(module, is_class)
        if inspect.getmembers(potential_service, is_entrypoint)
    ]

    if not found_services:
        raise ImportServiceError(
            f"""
            
            在模块 {module_name!r} 中找不到任何看起来像服务的类，
            请按照标准用例构建您自己的微服务。
            
            Usage::
                
                >>> class ExampleService(object):
                ...     name = 'service_name'
            
            """
        )

    return found_services


def run(services: List, config: Dict) -> None:
    """ 异步运行服务的主函数。

    :param services: 要运行的服务类列表。
    :param config: 服务配置。
    """
    service_runner = ServiceRunner(config)
    for service_cls in services:
        service_runner.add_service(service_cls)

    loop = asyncio.get_event_loop()

    async def _run() -> None:
        try:
            await service_runner.start()
            await service_runner.wait()
        except asyncio.CancelledError:
            await service_runner.stop()

    # def bind_socket():
    #     sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #     sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    #     sock.bind((config['host'], config['port']))
    #     sock.setblocking(False)
    #     try:
    #         sock.set_inheritable(True)
    #     except AttributeError:
    #         pass
    # todo bind socket

    try:
        loop.run_until_complete(_run())
    except KeyboardInterrupt:
        loop.run_until_complete(service_runner.kill())


def setup_uvloop():
    # 使用 `uvloop` 加速
    try:
        import uvloop
    except ImportError as error:
        raise ImportError("uvloop 没有下载安装！") from error
    else:
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


def main(args: Any) -> None:
    """
    主程序入口。

    :param args: 命令行参数。
    """
    if '.' not in sys.path:
        sys.path.insert(0, '.')

    config = vars(args)

    if args.config:
        with open(args.config) as f:
            config.update(yaml.safe_load(f))

    services = []
    for path in args.services:
        services.extend(
            import_service(path)
        )

    setup_uvloop()
    run(services, config)
