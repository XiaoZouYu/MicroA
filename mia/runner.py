from __future__ import absolute_import

from mia.containers import get_service_name, get_container_cls
from mia.utils.concurrency import SpawningProxy
from mia.log import logger


class ServiceRunner:
    def __init__(self, config: dict):
        """
        初始化 ServiceRunner 实例。

        Args:
            config (dict): 包含配置信息的字典。
        """
        # 用于存储服务名到容器的映射
        self.service_map = {}
        # 配置信息
        self.config = config
        # 获取服务容器类，默认为 ServiceContainer
        self.container_cls = get_container_cls(config)

    @property
    def service_names(self):
        """
        获取已添加服务的名称列表。

        Returns:
            list: 包含已添加服务名称的列表。
        """
        return list(self.service_map.keys())

    @property
    def containers(self):
        """
        获取已添加服务的容器列表。

        Returns:
            list: 包含已添加服务容器的列表。
        """
        return list(self.service_map.values())

    def add_service(self, cls):
        """
        添加服务类到 ServiceRunner 实例。

        Args:
            cls (type): 要添加的服务类。
        """
        service_name = get_service_name(cls)
        # 使用服务容器类创建容器
        container = self.container_cls(cls, self.config)
        # 将服务名与容器映射关系添加到 service_map
        self.service_map[service_name] = container

    async def start(self):
        """
        启动所有已添加的服务。

        Raises:
            Exception: 如果启动服务过程中出现异常，则抛出异常。
        """
        service_names = ', '.join(self.service_names)
        logger.info('开始启动服务: %s。', service_names)

        # 使用 SpawningProxy 启动所有服务
        await SpawningProxy(self.containers).start()

        logger.debug('服务列表: %s 被成功启动。', service_names)

    async def stop(self):
        """
        停止所有已添加的服务。

        Raises:
            Exception: 如果停止服务过程中出现异常，则抛出异常。
        """
        service_names = ', '.join(self.service_names)
        logger.info('开始停止服务: %s。', service_names)

        # 使用 SpawningProxy 停止所有服务
        await SpawningProxy(self.containers).stop()

        logger.debug('服务列表: %s 被成功停止。', service_names)

    async def kill(self):
        """
        杀掉所有已添加的服务。

        Raises:
            Exception: 如果杀掉服务过程中出现异常，则抛出异常。
        """
        service_names = ', '.join(self.service_names)
        logger.info('开始杀掉服务: %s。', service_names)

        # 使用 SpawningProxy 杀掉所有服务
        await SpawningProxy(self.containers).kill()

        logger.debug('服务列表: %s 被成功杀掉。', service_names)

    async def wait(self):
        """
        阻塞直到所有服务停止。

        Raises:
            Exception: 如果等待服务停止过程中出现异常，则抛出异常。
        """
        try:
            # 使用 SpawningProxy 等待所有服务停止
            await SpawningProxy(self.containers, abort_on_error=True).wait()
        except Exception:
            # 如果等待过程中出现异常，尝试停止所有服务并再次抛出异常
            await self.stop()
            raise
