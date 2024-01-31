from __future__ import absolute_import

import pytest
import asyncio

from unittest.mock import create_autospec

from mia.abc import AbsServiceContainer
from mia.containers import ServiceContainer
from mia.config import DEFAULT_SERIALIZER, SERIALIZER_CONFIG_KEY


@pytest.fixture
def empty_config():
    return {}


@pytest.fixture
def mock_container(empty_config: dict) -> "AbsServiceContainer":
    container: "AbsServiceContainer" = create_autospec(ServiceContainer)
    container.config = empty_config
    container.config[SERIALIZER_CONFIG_KEY] = DEFAULT_SERIALIZER
    container.serializer = container.config[SERIALIZER_CONFIG_KEY]
    container.accept = [DEFAULT_SERIALIZER]
    return container


@pytest.fixture(scope="session")
def loop():
    _loop = asyncio.get_event_loop()
    yield _loop
    _loop.close()




