"""核验入口：聚焦题面两个关键语义——撤销连坐与失败提交。

通过则进程退出 0；任一断言失败以非零退出码报告。
核验点：
  1. 撤父支连已释放孙支一并作废，提交仅发存活序号（1、3），
     且 close() 重开后续办结果一致；
  2. 提交冲突（插须缺席 / 改删须在账）失败时旧账、版号、暂存均保留，
     重开后仍可恢复并完成提交。
"""

import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lake import Lake  # noqa: E402


def check_revocation():
    db = os.path.join(tempfile.mkdtemp(prefix="lake_verify_"), "r.db")
    lake = Lake(db)
    lake.init([["10", 5, 5]])
    lake.feed(["b", "R"])
    lake.feed(["s", "C", "R"])
    lake.feed(["i", "C", "1", 1, 20])        # seq 1
    lake.feed(["l", "C"])
    lake.feed(["s", "P", "R"])
    lake.feed(["s", "G", "P"])
    lake.feed(["u", "G", "1", {"b": 99}])    # seq 2（孙支，随后释放）
    lake.feed(["l", "G"])
    lake.close()

    lake = Lake(db)                          # 重开续办
    lake.feed(["r", "P"])                    # 撤父支，已释放孙支同废
    lake.feed(["u", "R", "1", {"a": None}])  # seq 3
    v = lake.feed(["c", "R"])
    assert v == 1, "首个版号应为 1，实际 %r" % v
    assert lake.get("1") == [None, 20], lake.get("1")
    assert lake.get("10") == [5, 5]
    rows = list(lake.changes(1))
    assert rows == [
        [1, "i", "1", None, [1, 20]],
        [3, "u", "1", [1, 20], [None, 20]],
    ], rows
    lake.close()
    print("[1/2] 撤销连坐（含已释放孙支、序号留洞、重开续办）通过")


def check_failed_commit():
    db = os.path.join(tempfile.mkdtemp(prefix="lake_verify_"), "f.db")
    lake = Lake(db)
    lake.init([["k", 0, 0]])
    lake.feed(["b", "R"])
    lake.feed(["s", "C1", "R"])
    lake.feed(["i", "C1", "new", 1, 1])
    lake.feed(["l", "C1"])
    lake.feed(["s", "C2", "R"])
    lake.feed(["i", "C2", "k", 2, 2])        # k 已在账，插须缺席
    lake.feed(["l", "C2"])

    try:
        lake.feed(["c", "R"])
    except ValueError:
        pass
    else:
        raise AssertionError("冲突提交应抛错")
    # 旧账保留、版号未发、暂存保留。
    assert lake.get("k") == [0, 0]
    assert lake.get("new") is None
    try:
        lake.changes(1)
    except ValueError:
        pass
    else:
        raise AssertionError("失败提交不应产生版号")
    lake.close()

    lake = Lake(db)                          # 重开后失败状态仍在
    try:
        lake.feed(["c", "R"])
    except ValueError:
        pass
    else:
        raise AssertionError("重开后重试仍应失败")
    lake.feed(["r", "R"])                    # 撤根清暂存，原账不动
    lake.feed(["b", "R2"])
    lake.feed(["i", "R2", "new", 1, 1])
    v = lake.feed(["c", "R2"])
    assert v == 1, v
    assert lake.get("k") == [0, 0]
    assert lake.get("new") == [1, 1]

    # 改、删缺席键同样必须失败且不动账。
    lake.feed(["b", "R3"])
    lake.feed(["u", "R3", "ghost", {"a": 1}])
    try:
        lake.feed(["c", "R3"])
    except ValueError:
        pass
    else:
        raise AssertionError("更新缺席键应失败")
    lake.feed(["r", "R3"])
    lake.feed(["b", "R4"])
    lake.feed(["d", "R4", "ghost"])
    try:
        lake.feed(["c", "R4"])
    except ValueError:
        pass
    else:
        raise AssertionError("删除缺席键应失败")
    assert lake.get("k") == [0, 0]
    lake.close()
    print("[2/2] 失败提交保留旧账/版号/暂存，且重开可恢复 通过")


def main():
    check_revocation()
    check_failed_commit()
    print("全部核验通过。")


if __name__ == "__main__":
    main()
