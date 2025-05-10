
# 使用例
def tx_test():
    # この例はSimpleDBのインスタンスを作成し、
    # 図5.3のテストコードと同様のトランザクションを実行します

    # 簡単化のため、必要なコンポーネントが存在すると仮定
    db = SimpleDB("txtest", 400, 8)
    fm = db.file_mgr()
    lm = db.log_mgr()
    bm = db.buffer_mgr()

    # トランザクション1: 値を初期化
    tx1 = Transaction(fm, lm, bm)
    blk = BlockId("testfile", 1)
    tx1.pin(blk)
    tx1.set_int(blk, 80, 1, False)
    tx1.set_string(blk, 40, "one", False)
    tx1.commit()

    # トランザクション2: 値を読み、更新
    tx2 = Transaction(fm, lm, bm)
    tx2.pin(blk)
    ival = tx2.get_int(blk, 80)
    sval = tx2.get_string(blk, 40)
    print(f"initial value at location 80 = {ival}")
    print(f"initial value at location 40 = {sval}")

    newival = ival + 1
    newsval = sval + "!"
    tx2.set_int(blk, 80, newival, True)
    tx2.set_string(blk, 40, newsval, True)
    tx2.commit()

    # トランザクション3: 値を読み、更新してからロールバック
    tx3 = Transaction(fm, lm, bm)
    tx3.pin(blk)
    print(f"new value at location 80 = {tx3.get_int(blk, 80)}")
    print(f"new value at location 40 = {tx3.get_string(blk, 40)}")

    tx3.set_int(blk, 80, 9999, True)
    print(f"pre-rollback value at location 80 = {tx3.get_int(blk, 80)}")
    tx3.rollback()

    # トランザクション4: ロールバック後の値を確認
    tx4 = Transaction(fm, lm, bm)
    tx4.pin(blk)
    print(f"post-rollback at location 80 = {tx4.get_int(blk, 80)}")
    tx4.commit()
