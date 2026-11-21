"""lake.Lake —— SQLite 持久化的离线数仓变更补录存储。

约束摘要：
  * init(rows) 一次性、单遍原子装入 [k, a, b]，键唯一；
  * get(k) 读已提交的 [a, b]，缺席返回 None；
  * feed(e) 按序处理分支/行变更事件，非法事件抛 ValueError 且不推进；
  * 仅当前根可提交，提交原子换账、落变更、清暂存，返回自 1 递增的版号；
  * changes(v) 按根内原序号流式返回逐笔变更，未知版号拒绝；
  * close() 后可用新的 Lake 重开同一数据库续办；
  * Python 侧内存不随键数、操作数或分支深度增长。
"""

import sqlite3

_INT64_MIN = -(1 << 63)
_INT64_MAX = (1 << 63) - 1

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS kv (
    k TEXT PRIMARY KEY,
    a INTEGER,
    b INTEGER
);
CREATE TABLE IF NOT EXISTS committed_roots (
    t TEXT PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS roots (
    version INTEGER PRIMARY KEY AUTOINCREMENT,
    t       TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS changes (
    version INTEGER NOT NULL,
    seq     INTEGER NOT NULL,
    code    TEXT NOT NULL,
    k       TEXT NOT NULL,
    ba      INTEGER,
    bb      INTEGER,
    aa      INTEGER,
    ab      INTEGER,
    PRIMARY KEY (version, seq)
);
CREATE TABLE IF NOT EXISTS branches (
    root     TEXT NOT NULL,
    t        TEXT NOT NULL,
    parent   TEXT,
    status   TEXT NOT NULL CHECK (status IN ('open', 'released', 'revoked')),
    PRIMARY KEY (root, t)
);
CREATE TABLE IF NOT EXISTS stage (
    root     TEXT NOT NULL,
    seq      INTEGER NOT NULL,
    branch   TEXT NOT NULL,
    op       TEXT NOT NULL CHECK (op IN ('i', 'u', 'd')),
    k        TEXT NOT NULL,
    ka       INTEGER,
    kb       INTEGER,
    ua_set   INTEGER NOT NULL DEFAULT 0,
    ua       INTEGER,
    ub_set   INTEGER NOT NULL DEFAULT 0,
    ub       INTEGER
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_stage_root_seq ON stage(root, seq);
CREATE INDEX IF NOT EXISTS idx_stage_apply ON stage(root, branch, seq);
"""


def _is_ascii_str(x):
    return isinstance(x, str) and len(x) > 0 and x.isascii()


def _check_name(x, what):
    if not _is_ascii_str(x):
        raise ValueError("%s 必须是非空 ASCII 串" % what)


def _check_scalar(x, what):
    # bool 是 int 的子类，按题意显式拒绝布尔。
    if x is not None and not (isinstance(x, int) and not isinstance(x, bool)
                              and _INT64_MIN <= x <= _INT64_MAX):
        raise ValueError("%s 必须是非布尔 int64 或 None" % what)


class Lake:
    def __init__(self, db):
        self._con = sqlite3.connect(db, isolation_level=None)
        self._con.execute("PRAGMA journal_mode=WAL")
        self._con.execute("PRAGMA foreign_keys=ON")
        self._con.executescript(_SCHEMA)
        self._closed = False

    # ------------------------------------------------------------------ #
    # 内部工具
    # ------------------------------------------------------------------ #
    def _guard(self):
        if self._closed:
            raise ValueError("Lake 已关闭")

    def _meta_get(self, key):
        row = self._con.execute(
            "SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row[0] if row is not None else None

    def _meta_set(self, key, value):
        self._con.execute(
            "INSERT INTO meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value))

    def _meta_del(self, key):
        self._con.execute("DELETE FROM meta WHERE key=?", (key,))

    def _open_root(self):
        return self._meta_get("open_root")

    def _current(self):
        return self._meta_get("current_branch")

    def _branch(self, root, t):
        return self._con.execute(
            "SELECT t, parent, status FROM branches WHERE root=? AND t=?",
            (root, t)).fetchone()

    def _is_ancestor_or_self(self, root, ancestor, descendant):
        """ancestor 是否为 descendant 本身或其祖先（沿 parent 链，迭代 CTE）。"""
        row = self._con.execute(
            "WITH RECURSIVE chain(t, parent) AS ("
            "  SELECT t, parent FROM branches WHERE root=? AND t=?"
            "  UNION ALL"
            "  SELECT b.t, b.parent FROM branches b"
            "  JOIN chain c ON b.root=? AND b.t=c.parent"
            ") SELECT 1 FROM chain WHERE t=? LIMIT 1",
            (root, descendant, root, ancestor)).fetchone()
        return row is not None

    # ------------------------------------------------------------------ #
    # 初始化与读取
    # ------------------------------------------------------------------ #
    def init(self, rows):
        self._guard()
        if self._meta_get("initialized") is not None:
            raise ValueError("init 仅允许一次")
        # 先在事务外做完整校验，任何非法输入都不触碰数据库。
        normalized = []
        seen = set()
        for idx, row in enumerate(rows):
            if not isinstance(row, (list, tuple)) or len(row) != 3:
                raise ValueError("init 第 %d 行必须是 [k, a, b]" % idx)
            k, a, b = row
            _check_name(k, "键")
            _check_scalar(a, "a 值")
            _check_scalar(b, "b 值")
            if k in seen:
                raise ValueError("init 键重复: %r" % k)
            seen.add(k)
            normalized.append((k, a, b))
        con = self._con
        con.execute("BEGIN IMMEDIATE")
        try:
            con.executemany("INSERT INTO kv(k, a, b) VALUES(?, ?, ?)",
                            normalized)
            self._meta_set("initialized", "1")
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

    def get(self, k):
        self._guard()
        _check_name(k, "键")
        row = self._con.execute(
            "SELECT a, b FROM kv WHERE k=?", (k,)).fetchone()
        return [row[0], row[1]] if row is not None else None

    # ------------------------------------------------------------------ #
    # 事件流
    # ------------------------------------------------------------------ #
    def feed(self, e):
        self._guard()
        if not isinstance(e, (list, tuple)) or len(e) < 2:
            raise ValueError("非法事件: %r" % (e,))
        code, t = e[0], e[1]
        if not isinstance(code, str) or code not in ("b", "l", "r", "c",
                                                      "s", "i", "u", "d"):
            raise ValueError("非法事件码: %r" % (code,))
        _check_name(t, "事务号")
        con = self._con
        con.execute("BEGIN IMMEDIATE")
        try:
            if code == "b":
                if len(e) != 2:
                    raise ValueError("b 事件无尾项")
                self._ev_begin(t)
            elif code == "s":
                if len(e) != 3:
                    raise ValueError("s 事件格式为 [s, t, parent]")
                self._ev_sub(t, e[2])
            elif code == "l":
                if len(e) != 2:
                    raise ValueError("l 事件无尾项")
                self._ev_release(t)
            elif code == "r":
                if len(e) != 2:
                    raise ValueError("r 事件无尾项")
                self._ev_revoke(t)
            elif code == "c":
                if len(e) != 2:
                    raise ValueError("c 事件无尾项")
                result = self._ev_commit(t)
            else:
                if code == "i":
                    if len(e) != 5:
                        raise ValueError("i 事件格式为 [i, t, k, a, b]")
                    self._ev_insert(t, e[2], e[3], e[4])
                elif code == "u":
                    if len(e) != 4:
                        raise ValueError("u 事件格式为 [u, t, k, fields]")
                    self._ev_update(t, e[2], e[3])
                else:  # d
                    if len(e) != 3:
                        raise ValueError("d 事件格式为 [d, t, k]")
                    self._ev_delete(t, e[2])
                result = None
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        return result if code == "c" else None

    def _require_current(self, t):
        root = self._open_root()
        if root is None:
            raise ValueError("无打开的根事务")
        cur = self._current()
        if t != cur:
            raise ValueError("写操作仅限当前支: 事件号 %r 当前支 %r" % (t, cur))
        return root, cur

    def _ev_begin(self, t):
        con = self._con
        if self._open_root() is not None:
            raise ValueError("一次仅允许一个打开的根")
        if con.execute("SELECT 1 FROM committed_roots WHERE t=?",
                       (t,)).fetchone() is not None:
            raise ValueError("已提交根号禁止复用: %r" % t)
        con.execute("INSERT INTO branches(root, t, parent, status) "
                    "VALUES(?, ?, NULL, 'open')", (t, t))
        self._meta_set("open_root", t)
        self._meta_set("current_branch", t)

    def _ev_sub(self, t, parent):
        con = self._con
        root = self._open_root()
        if root is None:
            raise ValueError("建子支前必须先开根")
        cur = self._current()
        if parent != cur:
            raise ValueError("子支只能接在当前支上: %r != %r" % (parent, cur))
        if con.execute("SELECT 1 FROM branches WHERE root=? AND t=?",
                       (root, t)).fetchone() is not None:
            raise ValueError("根内号禁止复用: %r" % t)
        con.execute("INSERT INTO branches(root, t, parent, status) "
                    "VALUES(?, ?, ?, 'open')", (root, t, parent))
        self._meta_set("current_branch", t)

    def _ev_release(self, t):
        con = self._con
        root = self._open_root()
        if root is None:
            raise ValueError("无打开的根事务")
        if t == root:
            raise ValueError("根支不能释放")
        cur = self._current()
        if t != cur:
            raise ValueError("仅能释放当前子支")
        row = self._branch(root, t)
        if row is None or row[2] != "open":
            raise ValueError("待释放支不是打开状态")
        con.execute("UPDATE branches SET status='released' "
                    "WHERE root=? AND t=?", (root, t))
        self._meta_set("current_branch", row[1])  # 关支回父级

    def _ev_revoke(self, t):
        con = self._con
        root = self._open_root()
        if root is None:
            raise ValueError("无打开的根事务")
        cur = self._current()
        if t == root:
            # 根是当前支的打开祖先：撤销根则整根（含已释放后代）一并作废，
            # 回到无根状态，根号可复用。
            con.execute("DELETE FROM stage WHERE root=?", (root,))
            con.execute("DELETE FROM branches WHERE root=?", (root,))
            self._meta_del("open_root")
            self._meta_del("current_branch")
            return
        row = self._branch(root, t)
        if row is None:
            raise ValueError("未知事务号: %r" % t)
        if row[2] != "open":
            raise ValueError("只能撤销当前支或打开的祖先支")
        if not self._is_ancestor_or_self(root, t, cur):
            raise ValueError("待撤销支不是当前支或其打开祖先")
        # 连已释放后代一并作废（子树全部置 revoked，深度不占 Python 栈/内存）。
        con.execute(
            "WITH RECURSIVE subtree(t) AS ("
            "  SELECT t FROM branches WHERE root=? AND t=?"
            "  UNION ALL"
            "  SELECT b.t FROM branches b JOIN subtree s"
            "  ON b.root=? AND b.parent=s.t"
            ") UPDATE branches SET status='revoked' "
            "WHERE root=? AND t IN (SELECT t FROM subtree)",
            (root, t, root, root))
        self._meta_set("current_branch", row[1])  # 回到被撤销支的父级

    def _next_seq(self, root):
        row = self._con.execute(
            "SELECT COALESCE(MAX(seq), 0) + 1 FROM stage WHERE root=?",
            (root,)).fetchone()
        return row[0]

    def _ev_insert(self, t, k, a, b):
        root, cur = self._require_current(t)
        _check_name(k, "键")
        _check_scalar(a, "a 值")
        _check_scalar(b, "b 值")
        seq = self._next_seq(root)
        self._con.execute(
            "INSERT INTO stage(root, seq, branch, op, k, ka, kb) "
            "VALUES(?, ?, ?, 'i', ?, ?, ?)",
            (root, seq, cur, k, a, b))

    def _ev_update(self, t, k, fields):
        root, cur = self._require_current(t)
        _check_name(k, "键")
        if not isinstance(fields, dict) or len(fields) == 0:
            raise ValueError("u 的字段必须是非空 a/b 字典")
        extra = set(fields).difference(("a", "b"))
        if extra:
            raise ValueError("u 仅允许字段 a/b，出现: %r" % sorted(extra))
        ua_set, ua, ub_set, ub = 0, None, 0, None
        if "a" in fields:
            _check_scalar(fields["a"], "a 值")
            ua_set, ua = 1, fields["a"]
        if "b" in fields:
            _check_scalar(fields["b"], "b 值")
            ub_set, ub = 1, fields["b"]
        seq = self._next_seq(root)
        self._con.execute(
            "INSERT INTO stage(root, seq, branch, op, k, "
            "ua_set, ua, ub_set, ub) "
            "VALUES(?, ?, ?, 'u', ?, ?, ?, ?, ?)",
            (root, seq, cur, k, ua_set, ua, ub_set, ub))

    def _ev_delete(self, t, k):
        root, cur = self._require_current(t)
        _check_name(k, "键")
        seq = self._next_seq(root)
        self._con.execute(
            "INSERT INTO stage(root, seq, branch, op, k) "
            "VALUES(?, ?, ?, 'd', ?)", (root, seq, cur, k))

    def _ev_commit(self, t):
        con = self._con
        root = self._open_root()
        if root is None or root != t or self._current() != root:
            raise ValueError("仅当前根可提交，且不能在打开的子支上提交")
        # 流式拉取存活暂存（仅撤销支的行在 SQL 侧过滤；已释放支的写入保留），
        # 逐行应用，不在 Python 侧堆积；游标与写操作共用同一连接。
        cur = con.execute(
            "SELECT s.seq, s.op, s.k, s.ka, s.kb, "
            "s.ua_set, s.ua, s.ub_set, s.ub "
            "FROM stage s JOIN branches br "
            "ON br.root=s.root AND br.t=s.branch "
            "WHERE s.root=? AND br.status != 'revoked' ORDER BY s.seq",
            (root,))
        try:
            vcur = con.execute("INSERT INTO roots(t) VALUES(?)", (root,))
            version = vcur.lastrowid
            while True:
                batch = cur.fetchmany(100)
                if not batch:
                    break
                for (seq, op, k, ka, kb, ua_set, ua, ub_set, ub) in batch:
                    if op == "i":
                        if con.execute("SELECT 1 FROM kv WHERE k=?",
                                       (k,)).fetchone() is not None:
                            raise ValueError("插入键已存在: %r" % k)
                        con.execute(
                            "INSERT INTO changes(version, seq, code, k, "
                            "ba, bb, aa, ab) VALUES(?, ?, 'i', ?, NULL, NULL, ?, ?)",
                            (version, seq, k, ka, kb))
                        con.execute("INSERT INTO kv(k, a, b) VALUES(?, ?, ?)",
                                    (k, ka, kb))
                    elif op == "u":
                        old = con.execute(
                            "SELECT a, b FROM kv WHERE k=?", (k,)).fetchone()
                        if old is None:
                            raise ValueError("更新键不在账: %r" % k)
                        na = ua if ua_set else old[0]
                        nb = ub if ub_set else old[1]
                        con.execute(
                            "INSERT INTO changes(version, seq, code, k, "
                            "ba, bb, aa, ab) VALUES(?, ?, 'u', ?, ?, ?, ?, ?)",
                            (version, seq, k, old[0], old[1], na, nb))
                        con.execute("UPDATE kv SET a=?, b=? WHERE k=?",
                                    (na, nb, k))
                    else:  # d
                        old = con.execute(
                            "SELECT a, b FROM kv WHERE k=?", (k,)).fetchone()
                        if old is None:
                            raise ValueError("删除键不在账: %r" % k)
                        con.execute(
                            "INSERT INTO changes(version, seq, code, k, "
                            "ba, bb, aa, ab) VALUES(?, ?, 'd', ?, ?, ?, NULL, NULL)",
                            (version, seq, k, old[0], old[1]))
                        con.execute("DELETE FROM kv WHERE k=?", (k,))
            con.execute("INSERT INTO committed_roots(t) VALUES(?)", (root,))
            con.execute("DELETE FROM stage WHERE root=?", (root,))
            con.execute("DELETE FROM branches WHERE root=?", (root,))
            self._meta_del("open_root")
            self._meta_del("current_branch")
            return version
        except Exception:
            raise  # 外层 feed 统一 ROLLBACK，旧账与暂存原样保留

    # ------------------------------------------------------------------ #
    # 变更历史与关闭
    # ------------------------------------------------------------------ #
    def changes(self, v):
        self._guard()
        if not isinstance(v, int) or isinstance(v, bool) or v < 1:
            raise ValueError("版号必须是正整数")
        con = self._con
        if con.execute("SELECT 1 FROM roots WHERE version=?",
                       (v,)).fetchone() is None:
            raise ValueError("未知版号: %r" % v)

        def stream():
            # 游标分批拉取，调用方内存不随变更总数增长。
            cur = con.execute(
                "SELECT seq, code, k, ba, bb, aa, ab FROM changes "
                "WHERE version=? ORDER BY seq", (v,))
            while True:
                batch = cur.fetchmany(100)
                if not batch:
                    return
                for seq, code, k, ba, bb, aa, ab in batch:
                    before = None if code == "i" else [ba, bb]
                    after = None if code == "d" else [aa, ab]
                    yield [seq, code, k, before, after]

        return stream()

    def close(self):
        if not self._closed:
            self._con.close()
            self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
