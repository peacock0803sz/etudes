import tempfile
from pathlib import Path

import pytest

from rdbms.buffer import BufferPool, BufferPoolManager
from rdbms.disk import PAGE_SIZE, DiskManager


@pytest.fixture
def temp_db_file():
    with tempfile.NamedTemporaryFile() as temp_file:
        yield Path(temp_file.name)


@pytest.fixture
def sample_pages():
    hello = bytearray(b"hello") + bytearray(PAGE_SIZE - 5)
    world = bytearray(b"world") + bytearray(PAGE_SIZE - 5)
    return hello, world


@pytest.fixture
def disk_manager(temp_db_file: Path):
    """DiskManagerインスタンスを提供するフィクスチャー"""
    with DiskManager.open(temp_db_file) as dm:
        yield dm


@pytest.fixture
def buffer_pool(pool_size: int = 1):
    return BufferPool(pool_size)


@pytest.fixture
def buffer_pool_manager(disk_manager: DiskManager, buffer_pool: BufferPool):
    return BufferPoolManager(disk_manager, buffer_pool)
