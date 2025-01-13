from pathlib import Path

import pytest

from rdbms.disk import PAGE_SIZE, DiskManager


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
