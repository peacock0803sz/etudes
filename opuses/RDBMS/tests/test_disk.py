from rdbms.storage.disk import PAGE_SIZE, DiskManager


def test_disk_manager_basic_operations(
    disk_manager: DiskManager, sample_pages: tuple[bytearray, bytearray]
):
    """DiskManagerの基本的な読み書き操作をテスト"""
    hello, world = sample_pages

    # "hello"ページの作成と書き込み
    hello_page_id = disk_manager.allocate_page()
    disk_manager.write_page_data(hello_page_id, hello)

    # "world"ページの作成と書き込み
    world_page_id = disk_manager.allocate_page()
    disk_manager.write_page_data(world_page_id, world)
    disk_manager.sync()

    # データの読み込みと検証
    buf = bytearray(PAGE_SIZE)
    disk_manager.read_page_data(hello_page_id, buf)
    assert bytes(buf) == bytes(hello)

    disk_manager.read_page_data(world_page_id, buf)
    assert bytes(buf) == bytes(world)


def test_disk_manager_sequential_allocation(disk_manager: DiskManager):
    """ページIDの連続割り当てをテスト"""
    page_id1 = disk_manager.allocate_page()
    page_id2 = disk_manager.allocate_page()

    assert page_id2.value == page_id1.value + 1
