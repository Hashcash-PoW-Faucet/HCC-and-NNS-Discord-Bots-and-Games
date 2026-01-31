#!/usr/bin/env python3
import argparse
import sqlite3
import time
from typing import Any, Iterable, List, Tuple, Optional


def fmt_ts(ts: Any) -> str:
    try:
        ts = int(ts)
        if ts <= 0:
            return ""
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
    except Exception:
        return str(ts)


def shorten(s: Any, n: int = 80) -> str:
    s = "" if s is None else str(s)
    s = s.replace("\n", "\\n")
    return s if len(s) <= n else s[: n - 1] + "…"


def print_table(title: str, headers: List[str], rows: List[Tuple[Any, ...]]) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)

    # Convert rows to strings for width calc
    str_rows = []
    for r in rows:
        str_rows.append([("" if v is None else str(v)) for v in r])

    widths = [len(h) for h in headers]
    for r in str_rows:
        for i, cell in enumerate(r):
            widths[i] = max(widths[i], len(cell))

    # cap very wide columns for readability
    maxw = 60
    widths = [min(w, maxw) for w in widths]

    def fmt_row(cells: Iterable[str]) -> str:
        out = []
        for i, c in enumerate(cells):
            c = c if len(c) <= widths[i] else c[: widths[i] - 1] + "…"
            out.append(c.ljust(widths[i]))
        return " | ".join(out)

    print(fmt_row(headers))
    print("-" * (sum(widths) + 3 * (len(widths) - 1)))

    for r in str_rows:
        print(fmt_row(r))


def fetch_all(con: sqlite3.Connection, q: str, params: Tuple[Any, ...] = ()) -> List[Tuple[Any, ...]]:
    cur = con.execute(q, params)
    return cur.fetchall()


def table_exists(con: sqlite3.Connection, name: str) -> bool:
    row = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return bool(row)


def dump_users(con: sqlite3.Connection, limit: Optional[int]) -> None:
    if not table_exists(con, "users"):
        print("Table 'users' not found.")
        return

    q = """
    SELECT discord_id, address, balance, created_at, updated_at, last_withdraw_at
    FROM users
    ORDER BY balance DESC, updated_at DESC
    """
    if limit:
        q += " LIMIT ?"
        rows = fetch_all(con, q, (limit,))
    else:
        rows = fetch_all(con, q)

    pretty = []
    for discord_id, address, balance, created_at, updated_at, last_w in rows:
        pretty.append((
            discord_id,
            address or "",
            balance,
            fmt_ts(created_at),
            fmt_ts(updated_at),
            fmt_ts(last_w),
        ))

    print_table(
        "USERS (sorted by balance desc)",
        ["discord_id", "address", "balance", "created_at", "updated_at", "last_withdraw_at"],
        pretty,
    )


def dump_daily_limits(con: sqlite3.Connection, limit: Optional[int]) -> None:
    if not table_exists(con, "daily_limits"):
        print("Table 'daily_limits' not found.")
        return

    q = """
    SELECT discord_id, day, withdrawn_today
    FROM daily_limits
    ORDER BY day DESC, withdrawn_today DESC
    """
    if limit:
        q += " LIMIT ?"
        rows = fetch_all(con, q, (limit,))
    else:
        rows = fetch_all(con, q)

    print_table(
        "DAILY_LIMITS",
        ["discord_id", "day(UTC)", "withdrawn_today"],
        [(r[0], r[1], r[2]) for r in rows],
    )


def dump_tx_log(con: sqlite3.Connection, limit: int, only_type: Optional[str], only_status: Optional[str]) -> None:
    if not table_exists(con, "tx_log"):
        print("Table 'tx_log' not found.")
        return

    where = []
    params: List[Any] = []

    if only_type:
        where.append("type = ?")
        params.append(only_type)
    if only_status:
        where.append("status = ?")
        params.append(only_status)

    wsql = ("WHERE " + " AND ".join(where)) if where else ""

    q = f"""
    SELECT id, ts, type, from_id, to_id, amount, status, note, error
    FROM tx_log
    {wsql}
    ORDER BY id DESC
    LIMIT ?
    """
    params.append(limit)
    rows = fetch_all(con, q, tuple(params))

    pretty = []
    for rid, ts, typ, from_id, to_id, amount, status, note, error in rows:
        pretty.append((
            rid,
            fmt_ts(ts),
            typ,
            from_id if from_id is not None else "",
            to_id if to_id is not None else "",
            amount,
            status,
            shorten(note, 60),
            shorten(error, 60),
        ))

    print_table(
        f"TX_LOG (last {limit})",
        ["id", "ts", "type", "from_id", "to_id", "amount", "status", "note", "error"],
        pretty,
    )


def dump_schema(con: sqlite3.Connection) -> None:
    rows = fetch_all(
        con,
        "SELECT name, sql FROM sqlite_master WHERE type='table' ORDER BY name",
    )
    pretty = []
    for name, sql in rows:
        pretty.append((name, shorten(sql, 200)))
    print_table("SCHEMA (tables)", ["table", "create_sql"], pretty)


def main() -> None:
    ap = argparse.ArgumentParser(description="Human-readable dump for tipbot.db")
    ap.add_argument("--db", default="tipbot.db", help="Path to tipbot.db (default: tipbot.db)")
    ap.add_argument("--users", action="store_true", help="Dump users table")
    ap.add_argument("--limits", action="store_true", help="Dump daily_limits table")
    ap.add_argument("--tx", action="store_true", help="Dump tx_log table")
    ap.add_argument("--schema", action="store_true", help="Dump DB schema")
    ap.add_argument("--limit", type=int, default=50, help="Limit rows (default: 50). For --tx this is last N entries.")
    ap.add_argument("--tx-type", default="", help="Filter tx_log by type (tip|grant|withdraw)")
    ap.add_argument("--tx-status", default="", help="Filter tx_log by status (ok|pending|failed)")

    args = ap.parse_args()

    # If no section chosen, dump everything
    if not (args.users or args.limits or args.tx or args.schema):
        args.schema = True
        args.users = True
        args.limits = True
        args.tx = True

    con = sqlite3.connect(args.db)
    try:
        con.row_factory = sqlite3.Row  # not strictly needed, but nice
        if args.schema:
            dump_schema(con)
        if args.users:
            dump_users(con, args.limit if args.limit > 0 else None)
        if args.limits:
            dump_daily_limits(con, args.limit if args.limit > 0 else None)
        if args.tx:
            dump_tx_log(
                con,
                limit=max(1, args.limit),
                only_type=args.tx_type.strip() or None,
                only_status=args.tx_status.strip() or None,
            )
    finally:
        con.close()


if __name__ == "__main__":
    main()