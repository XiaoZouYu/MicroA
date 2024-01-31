from __future__ import absolute_import

import asyncio

from mia.scripts import ScriptEntrypoint

from typing import Optional


class Timer(ScriptEntrypoint):
    """
    计时器入口点。
    这个计时器用于定期执行某些任务，例如定时清理任务或周期性的数据处理。

    每隔 `interval` 秒触发一次，根据前一个任务的完成时间来确定下次触发的时间。

    默认行为是在第一次触发之前等待 `interval` 秒。
    如果希望入口点在服务启动时立即触发，请传递 `eager=True` 参数。

    Usage::

        >>> import time
        >>> from mia.timer import timer
        >>> class TestTimer:
        ...     name = "xxx"
        ...
        ...     @timer(5, eager=True)
        ...     async def timer_func(self):
        ...         print(time.time())

    :param interval: 两次触发之间的时间间隔，以秒为单位。
    :param eager: 如果为 True，入口点将在服务启动时立即触发。默认为 False。
    """

    __slots__ = ("interval", "eager", "should_stop", "worker_complete", "fut")

    def __init__(self, interval, eager=False, *args, **kwargs):
        self.interval: int = interval
        self.eager: bool = eager
        self.should_stop: asyncio.Event = asyncio.Event()
        self.worker_complete: asyncio.Event = asyncio.Event()
        self.fut: Optional[asyncio.Future] = None
        super(Timer, self).__init__(*args, **kwargs)

    async def start(self) -> None:
        self.fut = await self.container.spawn_manager(self._run())

    async def stop(self) -> None:
        self.should_stop.set()
        await self.fut

    async def kill(self) -> None:
        self.fut.cancel()

    async def _run(self):

        async def timeout_stop():
            await self.should_stop.wait()

        while True:
            try:
                await asyncio.wait_for(timeout_stop(), self.interval)
            except asyncio.TimeoutError:
                await self.handle_timer_tick()
                await self.worker_complete.wait()
            else:
                break

    async def handle_timer_tick(self):
        await self.container.spawn_worker(
            self, (), {}, handle_result=self.handle_result)

    async def handle_result(self, worker_ctx, result, exc_info):
        self.worker_complete.set()
        return result, exc_info


timer: Timer = Timer.decorator


__all__ = [
    "timer",
    "Timer"
]
