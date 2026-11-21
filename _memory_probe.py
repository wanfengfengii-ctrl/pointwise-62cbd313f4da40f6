"""内存有界性探针：由 test_lake.MemoryBoundTests 在子进程中调用。

用法: python3 -I _memory_probe.py <lake 模块所在目录>
最后一行打印进程常驻内存峰值（KiB，ru_maxrss）。
"""

import os
import resource
import sys
import tempfile

sys.path.insert(0, sys.argv[1])
from lake import Lake  # noqa: E402

DEPTH = 200
N_KEYS = 20000


def main():
    db = os.path.join(tempfile.mkdtemp(), "m.db")
    lake = Lake(db)
    lake.init([])

    # 空根上建深支链并整根撤销：作废走迭代 CTE，深度不占 Python 栈。
    lake.feed(["b", "R"])
    cur = "R"
    for i in range(DEPTH):
        t = "b%d" % i
        lake.feed(["s", t, cur])
        cur = t
    lake.feed(["i", cur, "deep", 1, 1])
    lake.feed(["r", "R"])

    # 新根上大批量写：逐行流式提交，Python 侧不缓存行。
    lake.feed(["b", "R1"])
    for i in range(N_KEYS):
        lake.feed(["i", "R1", "k%d" % i, i, i])
    v = lake.feed(["c", "R1"])
    assert v == 1, v
    lake.close()

    # 重开续办，核对持久化与变更流。
    lake = Lake(db)
    assert lake.get("k%d" % (N_KEYS - 1)) == [N_KEYS - 1, N_KEYS - 1]
    assert lake.get("deep") is None
    n = sum(1 for _ in lake.changes(1))
    assert n == N_KEYS, n

    # 再开一根走一次深支撤销（含已释放后代作废路径）。
    lake.feed(["b", "R2"])
    cur = "R2"
    for i in range(DEPTH):
        t = "q%d" % i
        lake.feed(["s", t, cur])
        cur = t
    lake.feed(["i", cur, "z", 9, 9])
    lake.feed(["r", "R2"])
    lake.close()

    print(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)


if __name__ == "__main__":
    main()
