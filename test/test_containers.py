import asyncio

import pytest
import mock

from mia.containers import ServiceContainer
from mia.scripts import ScriptDependencyProvider, ScriptEntrypoint, ScriptController
from mia.abc import AbsWorkerContext
from mia.testing.utils import get_script

from typing import Any

SyncPackEntrypointException = Exception("sync_pack_entrypoint")


class TestProvider(ScriptDependencyProvider):
    context_key = None
    called = []

    async def start(self) -> None:
        self.called.append("start")

    async def stop(self) -> None:
        self.called.append("stop")

    async def kill(self) -> None:
        self.called.append("kill")

    def get_dependency(self, worker_ctx: "AbsWorkerContext") -> Any:
        self.called.append(("get_dependency", worker_ctx))
        return worker_ctx.data.get(self.context_key)


class Service(object):
    name = "service"

    user = type(
        "User",
        (TestProvider,),
        {
            "context_key": "user",
            "attr_name": "user",
            "called": []
        }
    )()
    admin = type(
        "Admin",
        (TestProvider,),
        {
            "context_key": "admin",
            "attr_name": "admin",
            "called": []
        }
    )()
    # `manager` 增加一个子 `script` 实例
    manager = type(
        "Manager",
        (TestProvider,),
        {
            "context_key": "manager",
            "attr_name": "manager",
            "called": [],
            "script_instance": ScriptController(),
        }
    )()

    @ScriptEntrypoint.decorator
    async def async_pack_entrypoint(self):
        return "async_pack_entrypoint"

    @ScriptEntrypoint.decorator
    def sync_pack_entrypoint(self):
        raise SyncPackEntrypointException


@pytest.fixture
def container(empty_config):
    return ServiceContainer(Service, empty_config)


def test_collects_scripts(container):
    assert len(container.scripts) == 6
    assert len(container.dependencies) == 3
    assert len(container.sub_scripts) == 1
    assert len(container.entrypoints) == 2


def test_starts_scripts(loop, container):
    item: "TestProvider"
    for item in container.dependencies:
        assert item.called == []

    loop.run_until_complete(container.start())

    item: "TestProvider"
    for item in container.dependencies:
        assert item.called == ["start"]


def test_stops_scripts(loop, container):
    item: "TestProvider"
    for item in container.dependencies:
        assert item.called == []

    loop.run_until_complete(container.stop())

    item: "TestProvider"
    for item in container.dependencies:
        assert item.called == ["stop"]


def test_kills_scripts(loop, container):
    item: "TestProvider"
    for item in container.dependencies:
        assert item.called == []

    loop.run_until_complete(container.kill())

    item: "TestProvider"
    for item in container.dependencies:
        assert item.called == ["kill"]

    assert container._died.is_set()


def test_wait_for_container_stop(loop, container):
    async def wait_stop():
        work = asyncio.create_task(container.wait())
        await asyncio.sleep(0.2)
        assert not work.done()
        await container.stop()
        await asyncio.sleep(0.1)
        assert work.done()

    loop.run_until_complete(asyncio.wait_for(wait_stop(), timeout=1))


def test_run_order_scripts(loop, container):
    loop.run_until_complete(container.start())
    loop.run_until_complete(container.stop())
    loop.run_until_complete(container.kill())
    loop.run_until_complete(container.start())

    item: "TestProvider"
    for item in container.dependencies:
        assert item.called == [
            "start", "stop", "kill", "start"
        ]


def test_spawn_manager(loop, container):
    async def time_wait():
        await asyncio.sleep(1)

    async def run():
        work: asyncio.Task = await container.spawn_manager(time_wait(), name="_time_wait")
        assert work.get_name() == "_time_wait"
        await asyncio.sleep(0.5)
        assert container._worker_manager.get("_time_wait") == work
        await asyncio.sleep(0.6)
        assert container._worker_manager.get("_time_wait") != work

    loop.run_until_complete(run())


def test_spawn_worker(loop, container):
    async def _handle_result(worker_ctx, result, exc_info):
        return result, exc_info

    async def _wait_worker():
        await asyncio.gather(*container._worker_tasks.values())

    handle_result = mock.AsyncMock()
    handle_result.side_effect = _handle_result

    first_provider = get_script(container, ScriptDependencyProvider)
    async_pack_entrypoint = get_script(container, ScriptEntrypoint, method_name="async_pack_entrypoint")
    sync_pack_entrypoint = get_script(container, ScriptEntrypoint, method_name="sync_pack_entrypoint")

    async_pack_entrypoint_work = loop.run_until_complete(container.spawn_worker(
        async_pack_entrypoint, (), {}, handle_result=handle_result
    ))
    sync_pack_entrypoint_work = loop.run_until_complete(container.spawn_worker(
        sync_pack_entrypoint, (), {}, handle_result=handle_result
    ))

    loop.run_until_complete(_wait_worker())
    assert not container._worker_tasks

    assert first_provider.called == [
        ("get_dependency", async_pack_entrypoint_work),
        ("get_dependency", sync_pack_entrypoint_work),
    ]

    assert handle_result.call_args_list == [
        mock.call(async_pack_entrypoint_work, "async_pack_entrypoint", None),
        mock.call(sync_pack_entrypoint_work, None, (Exception, SyncPackEntrypointException, mock.ANY))
    ]


def test_multi_stop_and_spawn_manager(loop, container):
    done_tag = False

    async def new_work():
        nonlocal done_tag
        await asyncio.sleep(1)
        done_tag = True

    async def spawn_manager():
        await container.spawn_manager(new_work())
        await asyncio.sleep(0.5)
        await container.stop()
        await asyncio.sleep(0.5)

    loop.run_until_complete(spawn_manager())
    assert done_tag == False
