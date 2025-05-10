import io
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class BlockId:
    """A reference to a specific block of a specific file."""

    filename: str
    blknum: int

    def __str__(self) -> str:
        return f"[file {self.filename}, block {self.blknum}]"


@dataclass
class Page:
    """
    The contents of a disk block.
    Page provides methods to get and set values in the page.
    """

    blocksize: int = 0
    bb: bytearray = field(default_factory=bytearray)
    CHARSET = "ascii"  # 文字エンコーディング

    def __post_init__(self):
        if self.blocksize > 0 and not self.bb:
            # データバッファを作成するコンストラクタ
            self.bb = bytearray(self.blocksize)

    def __init__(self, blocksize_or_bytes=None):
        """
        Page can be initialized either with a blocksize (for data buffers)
        or with a bytearray (for log pages)
        """
        if isinstance(blocksize_or_bytes, int):
            self.blocksize = blocksize_or_bytes
            self.bb = bytearray(self.blocksize)
        elif isinstance(blocksize_or_bytes, (bytearray, bytes)):
            self.blocksize = len(blocksize_or_bytes)
            self.bb = bytearray(blocksize_or_bytes)

    def get_int(self, offset: int) -> int:
        """指定されたオフセットから整数を取得"""
        return int.from_bytes(
            self.bb[offset : offset + 4], byteorder="big", signed=True
        )

    def set_int(self, offset: int, n: int) -> None:
        """指定されたオフセットに整数を設定"""
        self.bb[offset : offset + 4] = n.to_bytes(4, byteorder="big", signed=True)

    def get_bytes(self, offset: int) -> bytes:
        """指定されたオフセットからバイト配列を取得"""
        length = self.get_int(offset)
        return bytes(self.bb[offset + 4 : offset + 4 + length])

    def set_bytes(self, offset: int, b: bytes) -> None:
        """指定されたオフセットにバイト配列を設定"""
        self.set_int(offset, len(b))
        self.bb[offset + 4 : offset + 4 + len(b)] = b

    def get_string(self, offset: int) -> str:
        """指定されたオフセットから文字列を取得"""
        b = self.get_bytes(offset)
        return b.decode(self.CHARSET)

    def set_string(self, offset: int, s: str) -> None:
        """指定されたオフセットに文字列を設定"""
        b = s.encode(self.CHARSET)
        self.set_bytes(offset, b)

    @staticmethod
    def max_length(strlen: int) -> int:
        """文字列の最大長（バイト数）を計算"""
        # ASCIIエンコーディングでは1文字あたり1バイト
        bytes_per_char = 1
        return 4 + (strlen * bytes_per_char)

    def contents(self):
        """ファイルマネージャーが必要とするパッケージプライベートメソッド"""
        return self.bb


@dataclass
class FileMgr:
    """
    The file manager handles interactions with the files that
    comprise a database.
    """

    db_directory: str
    blocksize: int
    is_new: bool = field(init=False)
    open_files: dict[str, io.BufferedRandom] = field(default_factory=dict, init=False)

    def __post_init__(self):
        # ディレクトリパスを作成
        db_dir = Path(self.db_directory)
        self.is_new = not db_dir.exists()

        # データベースが新しい場合はディレクトリを作成
        if self.is_new:
            db_dir.mkdir(parents=True)

        # 残っている一時テーブルを削除
        for filename in db_dir.glob("temp*"):
            filename.unlink()

    def read(self, blk: BlockId, p: Page) -> None:
        """ブロックの内容をページに読み込む"""
        try:
            f = self._get_file(blk.filename)
            f.seek(blk.blknum * self.blocksize)
            f.readinto(p.contents())
        except Exception as e:
            raise RuntimeError(f"cannot read block {blk}: {e}")

    def write(self, blk: BlockId, p: Page) -> None:
        """ページの内容をブロックに書き込む"""
        try:
            f = self._get_file(blk.filename)
            f.seek(blk.blknum * self.blocksize)
            f.write(p.contents())
            f.flush()  # 即時ディスクに書き込む
        except Exception as e:
            raise RuntimeError(f"cannot write block {blk}: {e}")

    def append(self, filename: str) -> BlockId:
        """新しいブロックをファイルに追加"""
        newblknum = self.length(filename)
        blk = BlockId(filename, newblknum)
        b = bytearray(self.blocksize)
        try:
            f = self._get_file(filename)
            f.seek(blk.blknum * self.blocksize)
            f.write(b)
            f.flush()
        except Exception as e:
            raise RuntimeError(f"cannot append block {blk}: {e}")
        return blk

    def length(self, filename: str) -> int:
        """ファイル内のブロック数を返す"""
        try:
            f = self._get_file(filename)
            return int(f.tell() / self.blocksize)
        except Exception as e:
            raise RuntimeError(f"cannot access {filename}: {e}")

    def _get_file(self, filename: str):
        """ファイルを取得またはオープン"""
        if filename not in self.open_files:
            filepath = Path(self.db_directory) / filename
            self.open_files[filename] = open(
                filepath, "a+b"
            )  # バイナリモードで追加と読み取り
        return self.open_files[filename]
