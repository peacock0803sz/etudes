from pathlib import Path

import pytest

from rdbms.storage.buffer import BufferPool, BufferPoolManager, NoFreeBuffer
from rdbms.storage.disk import DiskManager


def test_buffer_pool_manager_basic_operations(
    buffer_pool_manager: BufferPoolManager, sample_pages: tuple[bytearray, bytearray]
):
    """BufferPoolManagerの基本的な操作をテスト"""
    hello, world = sample_pages

    # 最初のページを作成してデータを書き込む
    buffer1 = buffer_pool_manager.create_page()
    page1_id = buffer1.page_id
    buffer1.page_content = hello
    buffer1.is_dirty = True
    buffer_pool_manager.unpin_page(page1_id)

    # 2つ目のページを作成してデータを書き込む
    buffer2 = buffer_pool_manager.create_page()
    page2_id = buffer2.page_id
    buffer2.page_content = world
    buffer2.is_dirty = True
    buffer_pool_manager.unpin_page(page2_id)

    # データを読み込んで検証
    buffer1 = buffer_pool_manager.fetch_page(page1_id)
    assert bytes(buffer1.page) == bytes(hello)
    buffer_pool_manager.unpin_page(page1_id)

    buffer2 = buffer_pool_manager.fetch_page(page2_id)
    assert bytes(buffer2.page) == bytes(world)
    buffer_pool_manager.unpin_page(page2_id)


def test_buffer_pool_manager_eviction(
    buffer_pool_manager: BufferPoolManager, sample_pages: tuple[bytearray, bytearray]
):
    """ページの追い出しをテスト"""
    hello, world = sample_pages

    # 1つ目のページを作成して解放
    buffer1 = buffer_pool_manager.create_page()
    page1_id = buffer1.page_id
    buffer1.page_content = hello
    buffer1.is_dirty = True
    buffer_pool_manager.unpin_page(page1_id)

    # 2つ目のページを作成（1つ目のページが追い出される）
    buffer2 = buffer_pool_manager.create_page()
    buffer2.page_content = world

    with pytest.raises(NoFreeBuffer):
        # 1つ目のページを再度フェッチして内容を確認(失敗する)
        buffer1 = buffer_pool_manager.fetch_page(page1_id)

    assert bytes(buffer1.page) == bytes(hello)


@pytest.fixture
def larger_buffer_pool(temp_db_file: Path) -> BufferPoolManager:
    """より大きなバッファプール用のフィクスチャー"""
    with DiskManager.open(temp_db_file) as disk:
        pool = BufferPool(3)  # サイズ3のプール
        return BufferPoolManager(disk, pool)


def test_buffer_pool_manager_multiple_pages(
    larger_buffer_pool: BufferPoolManager, sample_pages: tuple[bytearray, bytearray]
):
    """複数ページの同時管理をテスト"""
    hello, world = sample_pages

    # 複数のページを作成して管理
    pages = []
    for i in range(3):
        buffer = larger_buffer_pool.create_page()
        buffer.page_content = hello if i % 2 == 0 else world
        buffer.is_dirty = True
        pages.append(buffer.page_id)
        larger_buffer_pool.unpin_page(buffer.page_id)

    # すべてのページの内容を検証
    for i, page_id in enumerate(pages):
        buffer = larger_buffer_pool.fetch_page(page_id)
        expected = hello if i % 2 == 0 else world
        assert bytes(buffer.page) == bytes(expected)
        larger_buffer_pool.unpin_page(page_id)
