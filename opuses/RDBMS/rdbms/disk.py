import os
import struct
from contextlib import ContextDecorator
from dataclasses import dataclass, field
from io import BytesIO
from pathlib import Path

PAGE_SIZE = 4096
MAX_PAGE_ID = 2**64 - 1


@dataclass
class PageId:
    value: int

    def is_valid(self):
        if self.value == MAX_PAGE_ID:
            return None
        return self

    def to_int(self):
        return self.value

    @classmethod
    def from_bytes(cls, data: bytes):
        return cls(value=struct.unpack("<Q", data)[0])

    def to_bytes(self):
        return struct.pack("<Q", self.value)

    def __eq__(self, other, /) -> bool:
        if isinstance(other, PageId):
            return self.value == other.value
        return False

    @classmethod
    def default(cls):
        return cls(value=MAX_PAGE_ID)


@dataclass
class DiskManager(ContextDecorator):
    heap_file: BytesIO
    next_page_id: int = field(init=False)

    def __post_init__(self):
        heap_file_size = os.fstat(self.heap_file.fileno()).st_size
        self.next_page_id = heap_file_size // PAGE_SIZE

    @classmethod
    def open(cls, heap_file_path: Path):
        f = heap_file_path.open("rb+")
        return cls(f)

    def read_page_data(self, page_id: PageId, data: bytearray):
        offset = PAGE_SIZE * page_id.to_int()
        self.heap_file.seek(offset)
        read_bytes = self.heap_file.read(len(data))
        data[: len(read_bytes)] = read_bytes

    def write_page_data(self, page_id: PageId, data: bytes):
        offset = PAGE_SIZE * page_id.to_int()
        self.heap_file.seek(offset)
        self.heap_file.write(data)

    def allocate_page(self):
        page_id = self.next_page_id
        self.next_page_id += 1
        return PageId(page_id)

    def sync(self):
        self.heap_file.flush()
        os.fsync(self.heap_file.fileno())

    def close(self):
        if not self.heap_file.closed:
            self.heap_file.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
