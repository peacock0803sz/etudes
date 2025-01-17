from dataclasses import dataclass

from rdbms.storage.disk import PAGE_SIZE, DiskManager, PageId


class BufferPoolError(Exception):
    pass


class NoFreeBuffer(BufferPoolError):
    pass


@dataclass
class BufferId:
    value: int = 0

    def __eq__(self, other):
        if isinstance(other, BufferId):
            return self.value == other.value
        return False

    def __hash__(self):
        return hash(self.value)


class Buffer:
    def __init__(self):
        self.page_id = PageId.default()
        self.page = bytearray(PAGE_SIZE)
        self.is_dirty = False

    @property
    def page_content(self):
        return self.page

    @page_content.setter
    def page_content(self, content):
        self.page[:] = content


class Frame:
    def __init__(self):
        self.usage_count: int = 0
        self.buffer: Buffer = Buffer()
        self.pin_count: int = 0

    @property
    def is_pinned(self) -> bool:
        return self.pin_count > 0


class BufferPool:
    def __init__(self, pool_size: int):
        self.buffers = [Frame() for _ in range(pool_size)]
        self.next_victim_id = BufferId(0)

    def size(self):
        return len(self.buffers)

    def evict(self):
        pool_size = self.size()
        consecutive_pinned = 0

        while True:
            next_victim_id = self.next_victim_id
            frame = self.buffers[next_victim_id.value]

            if frame.usage_count == 0 and not frame.is_pinned:
                return self.next_victim_id

            if not frame.is_pinned:
                frame.usage_count -= 1
                consecutive_pinned = 0
            else:
                consecutive_pinned += 1
                if consecutive_pinned >= pool_size:
                    return None

            self.next_victim_id = self.increment_id(self.next_victim_id)

    def increment_id(self, buffer_id: BufferId) -> BufferId:
        return BufferId((buffer_id.value + 1) % self.size())

    def get_frame(self, buffer_id: BufferId) -> Frame:
        return self.buffers[buffer_id.value]


class BufferPoolManager:
    def __init__(self, disk: DiskManager, pool: BufferPool):
        self.disk = disk
        self.pool = pool
        self.page_table: dict[int, BufferId] = {}

    def fetch_page(self, page_id: PageId):
        # ページがすでにバッファプールにある場合
        if page_id.value in self.page_table:
            buffer_id = self.page_table[page_id.value]
            frame = self.pool.get_frame(buffer_id)
            frame.usage_count += 1
            frame.pin_count += 1
            return frame.buffer

        # 新しいバッファを確保
        buffer_id = self.pool.evict()
        if buffer_id is None:
            raise NoFreeBuffer("No free buffer available in buffer pool")

        frame = self.pool.get_frame(buffer_id)
        evict_page_id = frame.buffer.page_id

        # 古いページが dirty な場合は書き出す
        if frame.buffer.is_dirty:
            self.disk.write_page_data(evict_page_id, frame.buffer.page)

        # 新しいページを読み込む
        frame.buffer.page_id = page_id
        frame.buffer.is_dirty = False
        self.disk.read_page_data(page_id, frame.buffer.page)
        frame.usage_count = 1
        frame.pin_count = 1

        # ページテーブルを更新
        if evict_page_id.value in self.page_table:
            del self.page_table[evict_page_id.value]
        self.page_table[page_id.value] = buffer_id

        return frame.buffer

    def create_page(self):
        buffer_id = self.pool.evict()
        if buffer_id is None:
            raise NoFreeBuffer("No free buffer available in buffer pool")

        frame = self.pool.get_frame(buffer_id)
        evict_page_id = frame.buffer.page_id

        # 古いページが dirty な場合は書き出す
        if frame.buffer.is_dirty:
            self.disk.write_page_data(evict_page_id, frame.buffer.page)

        # 新しいページを作成
        page_id = self.disk.allocate_page()
        frame.buffer = Buffer()
        frame.buffer.page_id = page_id
        frame.buffer.is_dirty = True
        frame.usage_count = 1
        frame.pin_count = 1

        # ページテーブルを更新
        if evict_page_id.value in self.page_table:
            del self.page_table[evict_page_id.value]
        self.page_table[page_id.value] = buffer_id

        return frame.buffer

    def flush(self):
        for page_id, buffer_id in self.page_table.items():
            frame = self.pool.get_frame(buffer_id)
            if frame.buffer.is_dirty:
                self.disk.write_page_data(PageId(page_id), frame.buffer.page)
                frame.buffer.is_dirty = False
        self.disk.sync()

    def unpin_page(self, page_id: PageId):
        if page_id.value in self.page_table:
            buffer_id = self.page_table[page_id.value]
            frame = self.pool.get_frame(buffer_id)
            if frame.pin_count > 0:
                frame.pin_count -= 1
