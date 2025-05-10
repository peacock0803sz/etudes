from dataclasses import dataclass, field

from rdbms.storage.disk import BlockId, FileMgr, Page


@dataclass
class LogMgr:
    fm: "FileMgr"
    logfile: str
    logpage: Page = None
    current_blk: BlockId = None
    latest_lsn: int = 0
    last_saved_lsn: int = 0

    def __post_init__(self):
        self.logpage = Page(block_size=self.fm.block_size())
        logsize = self.fm.length(self.logfile)
        if logsize == 0:
            self.current_blk = self._append_new_block()
        else:
            self.current_blk = BlockId(self.logfile, logsize - 1)
            self.fm.read(self.current_blk, self.logpage)

    def flush(self, lsn: int) -> None:
        if lsn >= self.last_saved_lsn:
            self._flush()

    def iterator(self) -> "LogIterator":
        self._flush()
        return LogIterator(self.fm, self.current_blk)

    def append(self, logrec: bytes) -> int:
        boundary = self.logpage.get_int(0)
        recsize = len(logrec)
        bytesneeded = recsize + 4  # 4はInteger.BYTESに相当

        if boundary - bytesneeded < 4:  # 収まらない場合
            self._flush()  # 次のブロックに移動
            self.current_blk = self._append_new_block()
            boundary = self.logpage.get_int(0)

        recpos = boundary - bytesneeded
        self.logpage.set_bytes(recpos, logrec)
        self.logpage.set_int(0, recpos)  # 新しい境界
        self.latest_lsn += 1
        return self.latest_lsn

    def _append_new_block(self) -> BlockId:
        blk = self.fm.append(self.logfile)
        self.logpage.set_int(0, self.fm.block_size())
        self.fm.write(blk, self.logpage)
        return blk

    def _flush(self) -> None:
        self.fm.write(self.current_blk, self.logpage)
        self.last_saved_lsn = self.latest_lsn


class LogIterator:
    def __init__(self, fm: "FileMgr", blk: BlockId):
        self.fm = fm
        self.blk = blk
        self.p = Page(block_size=fm.block_size())
        self._move_to_block(blk)

    def __iter__(self):
        return self

    def __next__(self):
        if not self.has_next():
            raise StopIteration

        if self.current_pos == self.fm.block_size():
            self.blk = BlockId(self.blk.file_name, self.blk.number - 1)
            self._move_to_block(self.blk)

        rec = self.p.get_bytes(self.current_pos)
        self.current_pos += 4 + len(rec)  # 4はInteger.BYTESに相当
        return rec

    def has_next(self) -> bool:
        return self.current_pos < self.fm.block_size() or self.blk.number > 0

    def _move_to_block(self, blk: BlockId) -> None:
        self.fm.read(blk, self.p)
        self.boundary = self.p.get_int(0)
        self.current_pos = self.boundary


@dataclass
class Buffer:
    fm: "FileMgr"
    lm: "LogMgr"
    contents: Page = None
    blk: BlockId = None
    pins: int = 0
    txnum: int = -1
    lsn: int = -1

    def __post_init__(self):
        self.contents = Page(block_size=self.fm.block_size())

    def block(self) -> BlockId:
        return self.blk

    def is_pinned(self) -> bool:
        return self.pins > 0

    def modifying_tx(self) -> int:
        return self.txnum

    def set_modified(self, txnum: int, lsn: int) -> None:
        self.txnum = txnum
        if lsn >= 0:
            self.lsn = lsn

    def pin(self) -> None:
        self.pins += 1

    def unpin(self) -> None:
        self.pins -= 1

    def assign_to_block(self, b: BlockId) -> None:
        self.flush()
        self.blk = b
        self.fm.read(self.blk, self.contents)
        self.pins = 0

    def flush(self) -> None:
        if self.txnum >= 0:
            self.lm.flush(self.lsn)
            self.fm.write(self.blk, self.contents)
            self.txnum = -1


class BufferAbortException(Exception):
    pass


@dataclass
class BufferMgr:
    fm: "FileMgr"
    lm: "LogMgr"
    numbuffs: int
    bufferpool: list[Buffer] = None
    num_available: int = 0
    MAX_TIME: int = 10000  # 10秒

    def __post_init__(self):
        self.bufferpool = []
        self.num_available = self.numbuffs
        for i in range(self.numbuffs):
            self.bufferpool.append(Buffer(self.fm, self.lm))

    def available(self) -> int:
        return self.num_available

    def flush_all(self, txnum: int) -> None:
        for buff in self.bufferpool:
            if buff.modifying_tx() == txnum:
                buff.flush()

    def unpin(self, buff: Buffer) -> None:
        buff.unpin()
        if not buff.is_pinned():
            self.num_available += 1
            # JavaのnotifyAll()に相当するコードはPythonでは異なる実装が必要
            # ここではシンプルな実装のため省略

    def pin(self, blk: BlockId) -> Buffer:
        import time

        timestamp = time.time() * 1000  # ミリ秒に変換

        buff = self._try_to_pin(blk)
        while buff is None and not self._waiting_too_long(timestamp):
            # Javaのwait()に相当するコードはPythonでは異なる実装が必要
            # ここではシンプルに時間をチェックする
            time.sleep(0.1)  # 100ミリ秒待機
            buff = self._try_to_pin(blk)

        if buff is None:
            raise BufferAbortException()

        return buff

    def _waiting_too_long(self, starttime: int) -> bool:
        import time

        return (time.time() * 1000) - starttime > self.MAX_TIME

    def _try_to_pin(self, blk: BlockId) -> Buffer | None:
        buff = self._find_existing_buffer(blk)
        if buff is None:
            buff = self._choose_unpinned_buffer()
            if buff is None:
                return None
            buff.assign_to_block(blk)

        if not buff.is_pinned():
            self.num_available -= 1

        buff.pin()
        return buff

    def _find_existing_buffer(self, blk: BlockId) -> Buffer | None:
        for buff in self.bufferpool:
            b = buff.block()
            if b is not None and b == blk:
                return buff
        return None

    def _choose_unpinned_buffer(self) -> Buffer | None:
        for buff in self.bufferpool:
            if not buff.is_pinned():
                return buff
        return None


# BufferListクラス
@dataclass
class BufferList:
    bm: "BufferMgr"
    buffers: dict[BlockId, Buffer] = field(default_factory=dict)
    pins: list[BlockId] = field(default_factory=list)

    def get_buffer(self, blk: BlockId) -> Buffer:
        return self.buffers.get(blk)

    def pin(self, blk: BlockId) -> None:
        buff = self.bm.pin(blk)
        self.buffers[blk] = buff
        self.pins.append(blk)

    def unpin(self, blk: BlockId) -> None:
        buff = self.buffers.get(blk)
        self.bm.unpin(buff)
        self.pins.remove(blk)
        if blk not in self.pins:
            del self.buffers[blk]

    def unpin_all(self) -> None:
        for blk in self.pins:
            buff = self.buffers.get(blk)
            self.bm.unpin(buff)
        self.buffers.clear()
        self.pins.clear()
