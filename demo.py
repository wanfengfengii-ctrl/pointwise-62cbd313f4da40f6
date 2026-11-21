"""题面例行演示：撤父支后已释放孙支的值被一并作废。

流程：
  初始账：键 "10" = [5, 5]
  根 R 上开子支 C，C 写 键 "1" 插入 [1, 20]，随后释放 C；
  另开父支 P，P 下开孙支 G，G 把 "1" 的 b 改成 99，仅释放 G（P 仍打开）；
  close() 后重开 Lake 续办；
  撤销父支 P —— 连已释放的孙支 G 一并作废（seq 2 留洞）；
  根支 R 写 u：把 "1" 的 a 置为 None；
  提交后只发 seq 1、3 两笔，get("1") 得 [None, 20]。
"""

import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lake import Lake  # noqa: E402


def main():
    db = os.path.join(tempfile.mkdtemp(prefix="lake_demo_"), "demo.db")
    print("数据库文件:", db)

    lake = Lake(db)
    lake.init([["10", 5, 5]])
    print('init 后 get("10") =', lake.get("10"))

    lake.feed(["b", "R"])
    lake.feed(["s", "C", "R"])
    lake.feed(["i", "C", "1", 1, 20])       # seq 1
    lake.feed(["l", "C"])

    lake.feed(["s", "P", "R"])
    lake.feed(["s", "G", "P"])
    lake.feed(["u", "G", "1", {"b": 99}])   # seq 2
    lake.feed(["l", "G"])                   # 孙支释放，父支 P 保持打开
    lake.close()
    print("-- close() 完成，重新打开续办 --")

    lake = Lake(db)
    lake.feed(["r", "P"])                   # 撤父支，已释放孙支同废
    lake.feed(["u", "R", "1", {"a": None}])  # seq 3
    version = lake.feed(["c", "R"])
    print("提交版号:", version)

    print('get("1")  =', lake.get("1"))
    print('get("10") =', lake.get("10"))

    print("changes(%d) 行像（[序号, 码, k, 前像, 后像]）:" % version)
    for row in lake.changes(version):
        print("  ", row)

    assert lake.get("1") == [None, 20]
    assert lake.get("10") == [5, 5]
    assert list(lake.changes(version)) == [
        [1, "i", "1", None, [1, 20]],
        [3, "u", "1", [1, 20], [None, 20]],
    ]
    lake.close()
    print("演示断言全部通过。")


if __name__ == "__main__":
    main()
