from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import ClassVar

from rdbms.storage.buffer import Buffer, BufferMgr, FileMgr, LogMgr
from rdbms.storage.disk import BlockId, Page


# LogRecord関連クラス
class LogRecord(ABC):
    # 定数
    CHECKPOINT = 0
    START = 1
    COMMIT = 2
    ROLLBACK = 3
    SETINT = 4
    SETSTRING = 5

    @abstractmethod
    def op(self) -> int:
        pass

    @abstractmethod
    def tx_number(self) -> int:
        pass

    @abstractmethod
    def undo(self, tx) -> None:
        pass

    @staticmethod
    def create_log_record(bytes_data: bytes) -> "LogRecord":
        # 実装略
        return None


@dataclass
class SetStringRecord(LogRecord):
    txnum: int = 0
    offset: int = 0
    val: str = ""
    blk: BlockId | None = None

    def __init__(self, p: Page | None = None):
        if p is not None:
            # データからフィールド抽出
            # 実装略
            pass

    def op(self) -> int:
        return LogRecord.SETSTRING

    def tx_number(self) -> int:
        return self.txnum

    def undo(self, tx) -> None:
        tx.pin(self.blk)
        tx.set_string(self.blk, self.offset, self.val, False)
        tx.unpin(self.blk)

    @staticmethod
    def write_to_log(lm, txnum: int, blk: BlockId, offset: int, val: str) -> int:
        # ログ書き込み実装
        # 実装略
        return 0


# ロック関連クラス
class LockAbortException(Exception):
    pass


class LockTable:
    MAX_TIME = 10000  # 10秒

    def __init__(self):
        self.locks = {}

    def s_lock(self, blk: BlockId) -> None:
        # 実装略
        pass

    def x_lock(self, blk: BlockId) -> None:
        # 実装略
        pass

    def unlock(self, blk: BlockId) -> None:
        # 実装略
        pass


@dataclass
class ConcurrencyMgr:
    txnum: int
    locktbl: ClassVar[LockTable] = LockTable()

    def __init__(self, txnum: int):
        self.txnum = txnum
        self.locks = {}

    def s_lock(self, blk: BlockId) -> None:
        if blk not in self.locks:
            self.locktbl.s_lock(blk)
            self.locks[blk] = "S"

    def x_lock(self, blk: BlockId) -> None:
        if not self.has_xlock(blk):
            self.s_lock(blk)
            self.locktbl.x_lock(blk)
            self.locks[blk] = "X"

    def release(self) -> None:
        for blk in list(self.locks.keys()):
            self.locktbl.unlock(blk)
        self.locks.clear()

    def has_xlock(self, blk: BlockId) -> bool:
        locktype = self.locks.get(blk)
        return locktype is not None and locktype == "X"


# リカバリ関連クラス
@dataclass
class RecoveryMgr:
    tx: "Transaction"
    txnum: int
    lm: "LogMgr"
    bm: "BufferMgr"

    def __init__(self, tx: "Transaction", txnum: int, lm: "LogMgr", bm: "BufferMgr"):
        self.tx = tx
        self.txnum = txnum
        self.lm = lm
        self.bm = bm
        # StartRecordの書き込み略

    def commit(self) -> None:
        # 実装略
        pass

    def rollback(self) -> None:
        # 実装略
        pass

    def recover(self) -> None:
        # 実装略
        pass

    def set_int(self, buff: Buffer, offset: int, newval: int) -> int:
        # 実装略
        return 0

    def set_string(self, buff: Buffer, offset: int, newval: str) -> int:
        # 実装略
        return 0


# トランザクションクラス
@dataclass
class Transaction:
    fm: "FileMgr"
    lm: "LogMgr"
    bm: "BufferMgr"
    txnum: int = 0
    recovery_mgr: RecoveryMgr | None = None
    concur_mgr: ConcurrencyMgr | None = None
    mybuffers: BufferList | None = None

    # クラス変数
    _next_tx_num: ClassVar[int] = 0
    END_OF_FILE: ClassVar[int] = -1

    def __post_init__(self):
        self.txnum = self.next_tx_number()
        self.recovery_mgr = RecoveryMgr(self, self.txnum, self.lm, self.bm)
        self.concur_mgr = ConcurrencyMgr(self.txnum)
        self.mybuffers = BufferList(self.bm)

    @classmethod
    def next_tx_number(cls) -> int:
        cls._next_tx_num += 1
        print(f"new transaction: {cls._next_tx_num}")
        return cls._next_tx_num

    def commit(self) -> None:
        self.recovery_mgr.commit()
        self.concur_mgr.release()
        self.mybuffers.unpin_all()
        print(f"transaction {self.txnum} committed")

    def rollback(self) -> None:
        self.recovery_mgr.rollback()
        self.concur_mgr.release()
        self.mybuffers.unpin_all()
        print(f"transaction {self.txnum} rolled back")

    def recover(self) -> None:
        self.bm.flush_all(self.txnum)
        self.recovery_mgr.recover()

    def pin(self, blk: BlockId) -> None:
        self.mybuffers.pin(blk)

    def unpin(self, blk: BlockId) -> None:
        self.mybuffers.unpin(blk)

    def get_int(self, blk: BlockId, offset: int) -> int:
        self.concur_mgr.s_lock(blk)
        buff = self.mybuffers.get_buffer(blk)
        return buff.contents().get_int(offset)

    def get_string(self, blk: BlockId, offset: int) -> str:
        self.concur_mgr.s_lock(blk)
        buff = self.mybuffers.get_buffer(blk)
        return buff.contents().get_string(offset)

    def set_int(self, blk: BlockId, offset: int, val: int, ok_to_log: bool) -> None:
        self.concur_mgr.x_lock(blk)
        buff = self.mybuffers.get_buffer(blk)
        lsn = -1
        if ok_to_log:
            lsn = self.recovery_mgr.set_int(buff, offset, val)
        p = buff.contents()
        p.set_int(offset, val)
        buff.set_modified(self.txnum, lsn)

    def set_string(self, blk: BlockId, offset: int, val: str, ok_to_log: bool) -> None:
        self.concur_mgr.x_lock(blk)
        buff = self.mybuffers.get_buffer(blk)
        lsn = -1
        if ok_to_log:
            lsn = self.recovery_mgr.set_string(buff, offset, val)
        p = buff.contents()
        p.set_string(offset, val)
        buff.set_modified(self.txnum, lsn)

    def size(self, filename: str) -> int:
        dummyblk = BlockId(filename, self.END_OF_FILE)
        self.concur_mgr.s_lock(dummyblk)
        return self.fm.length(filename)

    def append(self, filename: str) -> BlockId:
        dummyblk = BlockId(filename, self.END_OF_FILE)
        self.concur_mgr.x_lock(dummyblk)
        return self.fm.append(filename)

    def block_size(self) -> int:
        return self.fm.block_size()

    def available_buffs(self) -> int:
        return self.bm.available()
