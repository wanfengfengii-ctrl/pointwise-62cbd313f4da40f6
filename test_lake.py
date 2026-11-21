"""lake.Lake 的完整回归测试（unittest）。"""

import os
import resource
import subprocess
import sys
import tempfile
import unittest

from lake import Lake


class LakeTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "warehouse.db")

    def tearDown(self):
        self.tmp.cleanup()

    def open(self):
        return Lake(self.db)


class InitGetTests(LakeTestBase):
    def test_init_and_get(self):
        lake = self.open()
        lake.init([["k1", 1, None], ["k2", None, -2]])
        self.assertEqual(lake.get("k1"), [1, None])
        self.assertEqual(lake.get("k2"), [None, -2])
        self.assertIsNone(lake.get("absent"))
        lake.close()

    def test_init_empty_and_reopen(self):
        lake = self.open()
        lake.init([])
        lake.close()
        lake = self.open()
        self.assertIsNone(lake.get("x"))
        lake.close()

    def test_init_only_once(self):
        lake = self.open()
        lake.init([])
        with self.assertRaises(ValueError):
            lake.init([["k", 1, 2]])
        lake.close()

    def test_init_rejects_bad_rows(self):
        lake = self.open()
        bad_rows = [
            [["k", 1]],                       # 列数不对
            [["", 1, 2]],                     # 空键
            [["k", True, 2]],                 # 布尔值
            [["k", 1, 2**63]],                # int64 上溢
            [["k", -(2**63) - 1, 2]],         # int64 下溢
            [["k", "x", 2]],                  # 非整数
            [[1, 1, 2]],                      # 非字符串键
            [["k ", 1, 2]],                   # 非 ASCII（空格是 ASCII，换一个）
        ]
        bad_rows[-1] = [["ké", 1, 2]]
        for rows in bad_rows:
            with self.assertRaises(ValueError):
                lake.init(rows)
        # 全部失败后仍可一次性装入。
        lake.init([["k", 0, 0]])
        self.assertEqual(lake.get("k"), [0, 0])
        lake.close()

    def test_init_duplicate_key_atomic(self):
        lake = self.open()
        with self.assertRaises(ValueError):
            lake.init([["a", 1, 2], ["a", 5, 6], ["b", 3, 4]])
        self.assertIsNone(lake.get("a"))
        # 失败的 init 不占掉唯一一次机会：随后可正常一次性装入。
        lake.init([["a", 1, 2]])
        self.assertEqual(lake.get("a"), [1, 2])
        with self.assertRaises(ValueError):
            lake.init([["c", 7, 8]])   # 第二次 init 一律拒绝
        lake.close()

    def test_get_validates_key(self):
        lake = self.open()
        lake.init([])
        with self.assertRaises(ValueError):
            lake.get("")
        with self.assertRaises(ValueError):
            lake.get(7)
        lake.close()


class BranchRuleTests(LakeTestBase):
    def test_basic_branch_lifecycle(self):
        lake = self.open()
        lake.init([])
        lake.feed(["b", "R"])
        lake.feed(["s", "C", "R"])
        lake.feed(["i", "C", "k1", 1, 2])
        lake.feed(["l", "C"])           # 关支回父级
        self.assertEqual(lake.feed(["c", "R"]), 1)
        self.assertEqual(lake.get("k1"), [1, 2])
        lake.close()

    def test_no_two_open_roots(self):
        lake = self.open()
        lake.init([])
        lake.feed(["b", "R1"])
        with self.assertRaises(ValueError):
            lake.feed(["b", "R2"])
        lake.close()

    def test_committed_root_number_not_reused(self):
        lake = self.open()
        lake.init([])
        lake.feed(["b", "R"])
        lake.feed(["c", "R"])
        with self.assertRaises(ValueError):
            lake.feed(["b", "R"])
        lake.close()

    def test_in_root_number_not_reused(self):
        lake = self.open()
        lake.init([])
        lake.feed(["b", "R"])
        lake.feed(["s", "C", "R"])
        lake.feed(["l", "C"])
        with self.assertRaises(ValueError):
            lake.feed(["s", "C", "R"])   # 已释放的号也不能复用
        lake.feed(["r", "R"])            # 撤根后号可复用
        lake.feed(["b", "R"])
        lake.feed(["c", "R"])
        lake.close()

    def test_sub_must_attach_current(self):
        lake = self.open()
        lake.init([])
        lake.feed(["b", "R"])
        lake.feed(["s", "C", "R"])
        with self.assertRaises(ValueError):
            lake.feed(["s", "X", "R"])   # 当前支是 C，不是 R
        with self.assertRaises(ValueError):
            lake.feed(["s", "Y", "nope"])
        lake.close()

    def test_release_only_current_child(self):
        lake = self.open()
        lake.init([])
        lake.feed(["b", "R"])
        with self.assertRaises(ValueError):
            lake.feed(["l", "R"])        # 根不能释放
        lake.feed(["s", "C", "R"])
        lake.feed(["s", "G", "C"])
        with self.assertRaises(ValueError):
            lake.feed(["l", "C"])        # C 不是当前支
        lake.feed(["l", "G"])
        lake.feed(["l", "C"])
        lake.close()

    def test_revoke_rules(self):
        lake = self.open()
        lake.init([])
        lake.feed(["b", "R"])
        lake.feed(["s", "C", "R"])
        lake.feed(["s", "G", "C"])
        lake.feed(["l", "G"])
        lake.feed(["l", "C"])
        lake.feed(["s", "C2", "R"])
        with self.assertRaises(ValueError):
            lake.feed(["r", "G"])        # 已释放且不是当前支祖先
        with self.assertRaises(ValueError):
            lake.feed(["r", "nope"])
        lake.feed(["r", "C2"])           # 撤销当前支，回到 R
        lake.feed(["r", "R"])            # 撤根，整根作废
        with self.assertRaises(ValueError):
            lake.feed(["c", "R"])        # 已无打开的根
        lake.close()

    def test_commit_only_at_root(self):
        lake = self.open()
        lake.init([])
        lake.feed(["b", "R"])
        lake.feed(["s", "C", "R"])
        with self.assertRaises(ValueError):
            lake.feed(["c", "R"])        # 还在子支上
        with self.assertRaises(ValueError):
            lake.feed(["c", "C"])        # 子支不能提交
        lake.feed(["l", "C"])
        self.assertEqual(lake.feed(["c", "R"]), 1)
        with self.assertRaises(ValueError):
            lake.feed(["c", "R"])        # 没有打开的根
        lake.close()

    def test_writes_only_on_current_branch(self):
        lake = self.open()
        lake.init([])
        lake.feed(["b", "R"])
        lake.feed(["s", "C", "R"])
        with self.assertRaises(ValueError):
            lake.feed(["i", "R", "k", 1, 2])   # R 不是当前支
        lake.feed(["i", "C", "k", 1, 2])
        lake.feed(["l", "C"])
        with self.assertRaises(ValueError):
            lake.feed(["u", "C", "k", {"a": 9}])  # C 已释放
        lake.close()

    def test_invalid_event_does_not_advance(self):
        lake = self.open()
        lake.init([])
        lake.feed(["b", "R"])
        lake.feed(["s", "C", "R"])
        lake.feed(["i", "C", "k", 1, 2])         # seq 1
        with self.assertRaises(ValueError):
            lake.feed(["i", "C", "k", True, 2])  # 布尔，拒绝
        with self.assertRaises(ValueError):
            lake.feed(["bogus", "C"])
        with self.assertRaises(ValueError):
            lake.feed(["i", "C"])                # 缺尾项
        lake.feed(["d", "C", "k"])               # 仍为 seq 2
        lake.feed(["l", "C"])
        lake.feed(["c", "R"])
        self.assertEqual(
            [row[0] for row in lake.changes(1)], [1, 2])
        lake.close()


class RowOpTests(LakeTestBase):
    def test_update_preserves_unspecified_fields(self):
        lake = self.open()
        lake.init([["k", 1, 2]])
        lake.feed(["b", "R"])
        lake.feed(["u", "R", "k", {"b": 20}])
        v = lake.feed(["c", "R"])
        self.assertEqual(v, 1)
        self.assertEqual(lake.get("k"), [1, 20])
        self.assertEqual(
            list(lake.changes(1)),
            [[1, "u", "k", [1, 2], [1, 20]]])
        lake.close()

    def test_update_to_none_and_delete_images(self):
        lake = self.open()
        lake.init([["k", 1, 2]])
        lake.feed(["b", "R"])
        lake.feed(["u", "R", "k", {"a": None}])
        lake.feed(["d", "R", "k"])
        v = lake.feed(["c", "R"])
        self.assertEqual(
            list(lake.changes(v)),
            [[1, "u", "k", [1, 2], [None, 2]],
             [2, "d", "k", [None, 2], None]])
        self.assertIsNone(lake.get("k"))
        lake.close()

    def test_insert_change_image(self):
        lake = self.open()
        lake.init([])
        lake.feed(["b", "R"])
        lake.feed(["i", "R", "k", None, 7])
        lake.feed(["c", "R"])
        self.assertEqual(
            list(lake.changes(1)),
            [[1, "i", "k", None, [None, 7]]])
        lake.close()

    def test_update_rejects_bad_dict(self):
        lake = self.open()
        lake.init([["k", 1, 2]])
        lake.feed(["b", "R"])
        for bad in [{}, {"c": 1}, {"a": "x"}, {"a": True}, []]:
            with self.assertRaises(ValueError):
                lake.feed(["u", "R", "k", bad])
        lake.close()


class FailedCommitTests(LakeTestBase):
    def _staged_conflict(self):
        lake = self.open()
        lake.init([["k", 0, 0]])
        lake.feed(["b", "R"])
        lake.feed(["s", "C1", "R"])
        lake.feed(["i", "C1", "new", 1, 1])
        lake.feed(["l", "C1"])
        lake.feed(["s", "C2", "R"])
        lake.feed(["i", "C2", "k", 2, 2])     # k 已在账，插须缺席 → 提交失败
        return lake

    def test_failed_commit_keeps_ledger_and_staging(self):
        lake = self._staged_conflict()
        with self.assertRaises(ValueError):
            lake.feed(["c", "R"])             # 还在 C2 上，本就不能提交
        lake.feed(["l", "C2"])                # 回根后提交，触发冲突失败
        with self.assertRaises(ValueError):
            lake.feed(["c", "R"])
        # 旧账不动，版号不发，暂存保留可再试。
        self.assertEqual(lake.get("k"), [0, 0])
        self.assertIsNone(lake.get("new"))
        with self.assertRaises(ValueError):
            lake.changes(1)
        with self.assertRaises(ValueError):
            lake.feed(["c", "R"])             # 原样重试仍失败
        # 撤根清掉整根暂存后，另起新根完成补录；原账始终未变。
        lake.feed(["r", "R"])
        lake.feed(["b", "R2"])
        lake.feed(["i", "R2", "new", 1, 1])
        self.assertEqual(lake.feed(["c", "R2"]), 1)
        self.assertEqual(lake.get("new"), [1, 1])
        self.assertEqual(lake.get("k"), [0, 0])
        lake.close()

    def test_failed_update_and_delete_missing_key(self):
        lake = self.open()
        lake.init([["k", 0, 0]])
        lake.feed(["b", "R"])
        lake.feed(["u", "R", "ghost", {"a": 1}])
        with self.assertRaises(ValueError):
            lake.feed(["c", "R"])
        self.assertEqual(lake.get("k"), [0, 0])
        lake.feed(["r", "R"])

        lake.feed(["b", "R2"])
        lake.feed(["d", "R2", "ghost"])
        with self.assertRaises(ValueError):
            lake.feed(["c", "R2"])
        self.assertEqual(lake.get("k"), [0, 0])
        lake.feed(["r", "R2"])
        lake.close()

    def test_failed_commit_survives_reopen(self):
        lake = self._staged_conflict()
        lake.feed(["l", "C2"])
        with self.assertRaises(ValueError):
            lake.feed(["c", "R"])
        lake.close()
        lake = self.open()                    # 重开续办，状态与暂存均持久化
        self.assertEqual(lake.get("k"), [0, 0])
        with self.assertRaises(ValueError):
            lake.feed(["c", "R"])
        # 撤根后原账完好，另起新根完成补录。
        lake.feed(["r", "R"])
        lake.feed(["b", "R2"])
        lake.feed(["i", "R2", "new", 1, 1])
        self.assertEqual(lake.feed(["c", "R2"]), 1)
        self.assertEqual(lake.get("new"), [1, 1])
        lake.close()


class RevocationExampleTests(LakeTestBase):
    def test_spec_example_with_reopen(self):
        """题面例行：撤父支连已释放孙支一并作废，提交仅发 1、3。"""
        lake = self.open()
        lake.init([["10", 5, 5]])
        lake.feed(["b", "R"])
        lake.feed(["s", "C", "R"])            # 子支
        lake.feed(["i", "C", "1", 1, 20])     # seq 1：b=20
        lake.feed(["l", "C"])
        lake.feed(["s", "P", "R"])            # 另开父支
        lake.feed(["s", "G", "P"])            # 孙支
        lake.feed(["u", "G", "1", {"b": 99}])  # seq 2：b=99
        lake.feed(["l", "G"])                 # 仅释放孙支，父支保持打开
        lake.close()

        lake = self.open()                    # 重开续办
        lake.feed(["r", "P"])                 # 撤父支，已释放孙支同废
        lake.feed(["u", "R", "1", {"a": None}])  # seq 3：根写 a=None
        v = lake.feed(["c", "R"])
        self.assertEqual(v, 1)
        self.assertEqual(lake.get("1"), [None, 20])
        self.assertEqual(lake.get("10"), [5, 5])
        self.assertEqual(
            list(lake.changes(1)),
            [[1, "i", "1", None, [1, 20]],
             [3, "u", "1", [1, 20], [None, 20]]])
        lake.close()

    def test_revoke_open_ancestor_kills_released_descendants(self):
        lake = self.open()
        lake.init([])
        lake.feed(["b", "R"])
        lake.feed(["s", "P", "R"])
        lake.feed(["s", "G", "P"])
        lake.feed(["i", "G", "x", 1, 2])      # 已释放孙支的值
        lake.feed(["l", "G"])                 # 仅释放孙支，父支 P 仍打开
        lake.feed(["r", "P"])                 # 撤销打开的祖先 P，孙支同废
        # P/G 已废，根上另写同键不冲突；空穴之外一切正常。
        lake.feed(["i", "R", "x", 3, 4])
        lake.feed(["c", "R"])
        self.assertEqual(lake.get("x"), [3, 4])
        self.assertEqual(
            list(lake.changes(1)),
            [[2, "i", "x", None, [3, 4]]])    # seq 1 留洞
        lake.close()


class VersionAndChangesTests(LakeTestBase):
    def test_empty_commits_increment_versions(self):
        lake = self.open()
        lake.init([])
        lake.feed(["b", "R1"])
        self.assertEqual(lake.feed(["c", "R1"]), 1)
        lake.feed(["b", "R2"])
        self.assertEqual(lake.feed(["c", "R2"]), 2)
        self.assertEqual(list(lake.changes(1)), [])
        self.assertEqual(list(lake.changes(2)), [])
        with self.assertRaises(ValueError):
            lake.changes(0)
        with self.assertRaises(ValueError):
            lake.changes(3)
        lake.close()

    def test_changes_stream_is_lazy(self):
        lake = self.open()
        lake.init([])
        lake.feed(["b", "R"])
        for i in range(500):
            lake.feed(["i", "R", "k%d" % i, i, i])
        lake.feed(["c", "R"])
        gen = lake.changes(1)
        first = next(gen)
        self.assertEqual(first, [1, "i", "k0", None, [0, 0]])
        self.assertEqual(len(list(gen)), 499)
        lake.close()


class MemoryBoundTests(LakeTestBase):
    def test_memory_does_not_grow_with_scale(self):
        # 子进程内跑大规模键数 + 深分支，检查常驻内存有界。
        probe = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             "_memory_probe.py")
        repo = os.path.dirname(probe)
        out = subprocess.run(
            [sys.executable, "-I", probe, repo],
            capture_output=True, text=True, check=True)
        max_kb = int(out.stdout.strip().splitlines()[-1])
        # 20k 键 + 200 深支，常驻内存应远低于 120MiB（SQLite 页缓存默认 2MiB）。
        self.assertLess(max_kb, 120 * 1024, "ru_maxrss=%d KiB" % max_kb)


class ClosedLakeTests(LakeTestBase):
    def test_ops_after_close_raise(self):
        lake = self.open()
        lake.init([])
        lake.close()
        with self.assertRaises(ValueError):
            lake.get("k")
        with self.assertRaises(ValueError):
            lake.feed(["b", "R"])
        with self.assertRaises(ValueError):
            lake.changes(1)
        lake.close()  # 幂等


if __name__ == "__main__":
    unittest.main(verbosity=2)
