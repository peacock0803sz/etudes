from rdbms.storage.disk import BlockId, Page


def test_creation():
    blk = BlockId("student.tbl", 23)
    assert blk.filename == "student.tbl"
    assert blk.blknum == 23


def test_str_representation():
    blk = BlockId("student.tbl", 23)
    assert str(blk) == "[file student.tbl, block 23]"


def test_equality():
    blk1 = BlockId("student.tbl", 23)
    blk2 = BlockId("student.tbl", 23)
    blk3 = BlockId("student.tbl", 24)
    blk4 = BlockId("course.tbl", 23)

    assert blk1 == blk2
    assert blk1 != blk3
    assert blk1 != blk4


def test_page_creation(page: Page):
    assert page.blocksize == 400
    assert len(page.bb) == 400


def test_int_storage(page: Page):
    page.set_int(0, 12345)
    assert page.get_int(0) == 12345

    page.set_int(100, -54321)
    assert page.get_int(100) == -54321  # 符号付き整数として正しく取得


def test_bytes_storage(page: Page):
    test_bytes = b"hello world"
    page.set_bytes(20, test_bytes)
    assert page.get_bytes(20) == test_bytes


def test_string_storage(page: Page):
    test_string = "Hello, database world!"
    page.set_string(40, test_string)
    assert page.get_string(40) == test_string


def test_multiple_values(page: Page):
    # 複数の値を格納して正しく取り出せるか
    page.set_int(0, 42)

    str_pos = 4
    test_string = "SimpleDB"
    page.set_string(str_pos, test_string)

    str_len = Page.max_length(len(test_string))
    int_pos = str_pos + str_len
    page.set_int(int_pos, 99)

    assert page.get_int(0) == 42
    assert page.get_string(str_pos) == test_string
    assert page.get_int(int_pos) == 99


def test_max_length():
    # 文字列の最大長の計算が正しいか
    assert Page.max_length(10) == 14  # 4バイト（長さ用）+ 10バイト（ASCII文字）
    assert Page.max_length(0) == 4  # 空文字列
