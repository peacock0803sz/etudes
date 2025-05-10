import pytest

from rdbms.storage.disk import Page


@pytest.fixture()
def page():
    return Page(400)  # 400バイトのページを作成
