from __future__ import absolute_import, unicode_literals

import asyncio
import inspect
import six

from mia.abc import AbsServiceContainer, AbsWorkerContext
from mia.utils.concurrency import SpawningSet
from mia.utils.helper import import_from_path
from mia.exceptions import ConfigurationError, ContainerBeingKilled
from mia.scripts import ENTRYPOINT_SCRIPTS_ATTR, is_script, iter_script
from mia.log import logger, timing_logger
from mia.config import SERVICE_CONTAINER_CLS

from typing import MutableMapping, Any, Type

_log_time = timing_logger()

if six.PY2:  # pragma: no cover
    is_method = inspect.ismethod
else:  # pragma: no cover
    is_method = inspect.isfunction


def get_service_name(service_cls):
    service_name = getattr(service_cls, 'name', None)
    if service_name is None:
        raise ConfigurationError(
            'Service class must define a `name` attribute ({}.{})'.format(
                service_cls.__module__, service_cls.__name__))
    if not isinstance(service_name, six.string_types):
        raise ConfigurationError(
            'Service name attribute must be a string ({}.{}.name)'.format(
                service_cls.__module__, service_cls.__name__))
    return service_name


class WorkerContext(AbsWorkerContext):

    @property
    def context_data(self):
        return self.data.copy()


class ServiceContainer(AbsServiceContainer):
    def __init__(self, service_cls, config):
        super().__init__(service_cls, config)

        self.service_name = get_service_name(self.service_cls)
        self.controllers = {}

        # 记录所有的入口点
        self.entrypoints = SpawningSet()
        # 记录所有服务相关的依赖
        self.dependencies = SpawningSet()
        # 所有的`Script`子/孙类
        self.sub_scripts = SpawningSet()

        for attr_name, script in inspect.getmembers(service_cls, is_script):
            # 每个`script`绑定`Container`
            bound = script.bind(self, attr_name)
            self.dependencies.add(bound)
            self.sub_scripts.update(iter_script(bound))

        for method_name, method in inspect.getmembers(service_cls, is_method):
            # 将标记过的方法加入到`entrypoints` todo 需要验证是否是异步
            entrypoints = getattr(method, ENTRYPOINT_SCRIPTS_ATTR, [])
            for entrypoint in entrypoints:
                bound = entrypoint.bind(self, method_name)
                self.entrypoints.add(bound)
                self.sub_scripts.update(iter_script(bound))

        self._died = asyncio.Event()
        self._worker_tasks: MutableMapping[Any, asyncio.Task] = {}
        self._worker_manager = {}

        self._being_killed = False

    @property
    def scripts(self):
        """ 所有和服务相关的脚本扩展。
        :return: SpawningSet
        """
        return SpawningSet(self.entrypoints | self.dependencies | self.sub_scripts)

    async def start(self):
        """ 通过启动容器的所有扩展来启动容器。
        """
        logger.debug('容器启动：%s', self)

        with _log_time('%s 容器被启动了！', self):
            await self.scripts.all.setup()
            await self.scripts.all.start()

    async def stop(self):
        """ 停止容器！

        所有的入口点都应该被正常停止，并且不会启动新的任务。
        扩展脚本都应该有`stop()`，并且可以正常被调用。
        容器应该停止所有的依赖提供者。
        杀掉所有的托管任务`task`。

        :return:
        """
        with _log_time('%s 容器被停止了！', self):
            # 入口点必须在依赖提供者前停止，以确保工人正常正常完成。
            await self.entrypoints.all.stop()

            # 等待工人完成所有的任务
            await asyncio.gather(*self._worker_tasks.values())

            # 如果工人正常退出，那么依赖提供者应该也可以正常安全的退出。
            await self.dependencies.all.stop()

            # 最后，停止所有的脚本扩展。
            await self.sub_scripts.all.stop()

            # 杀掉工作管理员
            self._kill_worker_manager()

            # 关闭事件。
            if not self._died.is_set():
                self._died.set()

    async def kill(self):
        """ 杀掉容器！

        杀掉容器流程：

            `entrypoints` => `tasks` => `scripts` => `managed_threads`

        """
        with _log_time('%s 容器被杀掉了！', self):
            self._being_killed = True
            # 杀掉所有入口点。
            await self.entrypoints.all.kill()

            # 杀掉所有脚本扩展。
            await self.scripts.all.kill()

            # 杀掉所有的任务
            self._kill_worker_tasks()
            self._kill_worker_manager()

            # 关闭事件。
            if not self._died.is_set():
                self._died.set()

    async def wait(self):
        """ 阻塞直到容器停止！

        如果容器是由于异常异常而停止，那么`wait()`将会抛出异常。

        其他被管理的任务抛出的异常将会被`kill()`

        :return:
        """
        return await self._died.wait()

    def _kill_worker_manager(self):
        for task in self._worker_manager.values():  # type: asyncio.Task
            if not task.done():
                task.cancel()

    def _kill_worker_tasks(self):
        for task in self._worker_tasks.values():  # type: asyncio.Task
            if not task.done():
                task.cancel()

    async def spawn_manager(self, fn, name=None) -> asyncio.Task:
        # 创建一个协程任务
        task = asyncio.create_task(fn, name=name)

        # 处理协程任务完成时的回调。
        task.add_done_callback(lambda fut: self._handle_manager_exited(fut))

        # 将 task 与任务关联，以便进行跟踪。
        self._worker_manager[task.get_name()] = task

        return task

    async def spawn_worker(self, entrypoint, args, kwargs, context_data=None, handle_result=None):
        if context_data is None:
            context_data = {}

        if self._being_killed:
            raise ContainerBeingKilled()

        # 实例化服务类。
        service = self.service_cls()

        # 创建 WorkerContext 对象。
        worker_ctx = WorkerContext(
            self, service, entrypoint, args, kwargs, data=context_data
        )

        logger.debug('开始创建工人 %s', worker_ctx)

        # 创建一个协程任务，使用 asyncio.create_task。
        task = asyncio.create_task(self._run_worker(worker_ctx, handle_result))

        # 处理协程任务完成时的回调。
        task.add_done_callback(lambda fut: self._handle_worker_exited(fut, repr(worker_ctx)))

        # 将 WorkerContext 与任务关联，以便进行跟踪。
        self._worker_tasks[repr(worker_ctx)] = task

        # 返回生成的工作上下文。
        return worker_ctx

    async def _run_worker(self, worker_ctx, handle_result):
        logger.debug('初始化 %s', worker_ctx)

        with _log_time('工作工人: %s 耗时 ', worker_ctx):
            for provider in self.dependencies:
                # dependency = DependencyCache(provider, worker_ctx)  todo cache
                dependency = provider.get_dependency(worker_ctx)
                setattr(worker_ctx.service, provider.attr_name, dependency)
            result = exc_info = None
            method_name = worker_ctx.entrypoint.method_name
            method = getattr(worker_ctx.service, method_name)

            try:
                logger.debug("调用处理程序 %s", worker_ctx)

                with _log_time("开始运行处理程序 %s", worker_ctx):
                    if not asyncio.iscoroutinefunction(method):
                        logger.warning("这个方法是同步的，可能会导致事件阻塞，从而影响性能，请谨慎使用！%r", method)
                        result = method(*worker_ctx.args, **worker_ctx.kwargs)
                    else:
                        result = await method(*worker_ctx.args, **worker_ctx.kwargs)
            except Exception as exc:
                logger.error(f"调用程序错误: {exc}", exc_info=True)
                exc_info = exc

            if handle_result is not None:
                with _log_time('处理结果为 %s', worker_ctx):
                    await handle_result(worker_ctx, result, exc_info)

            with _log_time('销毁工人 %s', worker_ctx):
                del exc_info

    def _handle_worker_exited(self, fut, worker_ctx):
        logger.debug("工人回调: %s", fut)
        if worker_ctx in self._worker_tasks:
            del self._worker_tasks[worker_ctx]

    def _handle_manager_exited(self, fut):
        logger.debug("管理者回调: %s", fut)
        self._worker_manager.pop(fut.get_name(), None)

    def __repr__(self):
        service_name = self.service_name
        return '<ServiceContainer [{}] at 0x{:x}>'.format(
            service_name, id(self))


def get_container_cls(config: dict) -> Type["ServiceContainer"]:
    """
    获取服务容器类。

    Args:
        config (dict): 包含配置信息的字典。

    Returns:
        type: 服务容器类。如果导入失败，则返回默认的 ServiceContainer 类。
    """
    # 从配置中获取服务容器类的路径
    class_path = config.get(SERVICE_CONTAINER_CLS)

    # 尝试通过给定的路径导入类，如果导入失败，使用默认的 ServiceContainer 类
    return import_from_path(class_path) or ServiceContainer
