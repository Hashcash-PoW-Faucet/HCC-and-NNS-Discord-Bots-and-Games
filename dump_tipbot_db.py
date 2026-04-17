#!/usr/bin/env python3
import argparse
import sqlite3
import time
from typing import Any, Iterable, List, Tuple, Optional

from decimal import Decimal


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


SATS = 100_000_000



def fmt_coin_sat(v: Any) -> str:
    try:
        n = int(v or 0)
        return f"{(Decimal(n) / Decimal(SATS)):.8f}"
    except Exception:
        return str(v)


def fmt_coin_sat_decimal(v: Any) -> str:
    try:
        d = Decimal(str(v or "0"))
        return f"{(d / Decimal(SATS)):.8f}"
    except Exception:
        return str(v)


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


def table_columns(con: sqlite3.Connection, name: str) -> List[str]:
    rows = fetch_all(con, f"PRAGMA table_info({name})")
    # PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
    return [r[1] for r in rows]

def first_existing(cols: List[str], candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in cols:
            return c
    return None


def dump_deposits(con: sqlite3.Connection, limit: int, only_coin: Optional[str], only_status: Optional[str]) -> None:
    candidate_tables = [
        ("deposits", None),
        ("deposit_log", None),
        ("veco_deposits", "VECO"),
        ("nns_deposits", "NNS"),
    ]

    found_any = False
    wanted_coin = (str(only_coin).strip().upper() if only_coin else None)

    for table_name, fixed_coin in candidate_tables:
        if not table_exists(con, table_name):
            continue

        if wanted_coin and fixed_coin and wanted_coin != fixed_coin:
            continue

        found_any = True
        cols = table_columns(con, table_name)
        order_col = first_existing(cols, ["id", "ts", "created_at", "updated_at", "first_seen_ts", "last_update_ts", cols[0]]) or cols[0]

        coin_col = first_existing(cols, ["coin", "currency", "symbol"])
        status_col = first_existing(cols, ["status", "state", "credited"])

        amount_sat_col = first_existing(cols, [
            "amount_sat",
            "credited_sat",
            "value_sat",
            "nns_amount_sat",
            "veco_amount_sat",
        ])
        amount_dec_col = first_existing(cols, [
            "amount",
            "credited_amount",
            "value",
        ])

        where = []
        params: List[Any] = []

        if wanted_coin and coin_col:
            where.append(f"UPPER({coin_col}) = ?")
            params.append(wanted_coin)

        if only_status and status_col:
            status_value = str(only_status).strip()
            if status_col == "credited":
                sv = status_value.lower()
                if sv in ("credited", "ok", "true", "yes", "1"):
                    where.append("credited = 1")
                elif sv in ("pending", "uncredited", "false", "no", "0"):
                    where.append("credited = 0")
                else:
                    where.append("credited = ?")
                    params.append(status_value)
            else:
                where.append(f"{status_col} = ?")
                params.append(status_value)

        wsql = ("WHERE " + " AND ".join(where)) if where else ""

        q = f"SELECT {', '.join(cols)} FROM {table_name} {wsql} ORDER BY {order_col} DESC LIMIT ?"
        params.append(limit)
        rows = fetch_all(con, q, tuple(params))

        pretty_rows: List[Tuple[Any, ...]] = []
        for r in rows:
            out: List[Any] = []
            for i, v in enumerate(r):
                cname = cols[i]
                if cname in ("ts", "created_at", "updated_at", "first_seen_ts", "last_seen_ts", "credited_at", "credited_ts", "last_update_ts"):
                    out.append(fmt_ts(v))
                elif amount_sat_col and cname == amount_sat_col:
                    out.append(fmt_coin_sat(v))
                elif amount_dec_col and cname == amount_dec_col:
                    out.append(str(v if v is not None else ""))
                elif cname == "credited":
                    out.append("credited" if int(v or 0) else "pending")
                elif cname in ("note", "error", "txid", "address", "deposit_address"):
                    out.append(shorten(v, 60))
                elif isinstance(v, str):
                    out.append(shorten(v, 60))
                else:
                    out.append(v)
            if fixed_coin and "coin" not in cols and "currency" not in cols and "symbol" not in cols:
                out.insert(0, fixed_coin)
            pretty_rows.append(tuple(out))

        headers = []
        if fixed_coin and "coin" not in cols and "currency" not in cols and "symbol" not in cols:
            headers.append("coin")
        for c in cols:
            if c == "credited":
                headers.append("status")
            elif c.endswith("_sat"):
                headers.append(c[:-4])
            else:
                headers.append(c)

        print_table(
            f"{table_name.upper()} (last {limit})",
            headers,
            pretty_rows,
        )

    if not found_any:
        print("No deposit tables found. Looked for: deposits, deposit_log, veco_deposits, nns_deposits.")


# === BEGIN: dump_withdrawals ===

def dump_withdrawals(con: sqlite3.Connection, limit: int, only_coin: Optional[str], only_status: Optional[str]) -> None:
    candidate_tables = [
        ("withdrawals", None),
        ("withdraw_log", None),
        ("veco_withdrawals", "VECO"),
        ("nns_withdrawals", "NNS"),
    ]

    found_any = False
    wanted_coin = (str(only_coin).strip().upper() if only_coin else None)

    for table_name, fixed_coin in candidate_tables:
        if not table_exists(con, table_name):
            continue

        if wanted_coin and fixed_coin and wanted_coin != fixed_coin:
            continue

        found_any = True
        cols = table_columns(con, table_name)
        order_col = first_existing(cols, ["id", "ts", "created_at", "updated_at", cols[0]]) or cols[0]

        coin_col = first_existing(cols, ["coin", "currency", "symbol"])
        status_col = first_existing(cols, ["status", "state"])

        amount_sat_col = first_existing(cols, [
            "amount_sat",
            "net_amount_sat",
            "value_sat",
        ])
        fee_sat_col = first_existing(cols, [
            "fee_sat",
            "network_fee_sat",
        ])
        amount_dec_col = first_existing(cols, [
            "amount",
            "net_amount",
            "value",
        ])

        where = []
        params: List[Any] = []

        if wanted_coin and coin_col:
            where.append(f"UPPER({coin_col}) = ?")
            params.append(wanted_coin)

        if only_status and status_col:
            where.append(f"{status_col} = ?")
            params.append(str(only_status).strip())

        wsql = ("WHERE " + " AND ".join(where)) if where else ""

        q = f"SELECT {', '.join(cols)} FROM {table_name} {wsql} ORDER BY {order_col} DESC LIMIT ?"
        params.append(limit)
        rows = fetch_all(con, q, tuple(params))

        pretty_rows: List[Tuple[Any, ...]] = []
        for r in rows:
            out: List[Any] = []
            for i, v in enumerate(r):
                cname = cols[i]
                if cname in ("ts", "created_at", "updated_at"):
                    out.append(fmt_ts(v))
                elif amount_sat_col and cname == amount_sat_col:
                    out.append(fmt_coin_sat(v))
                elif fee_sat_col and cname == fee_sat_col:
                    out.append(fmt_coin_sat(v))
                elif amount_dec_col and cname == amount_dec_col:
                    out.append(str(v if v is not None else ""))
                elif cname in ("note", "error", "txid", "to_address", "address"):
                    out.append(shorten(v, 60))
                elif isinstance(v, str):
                    out.append(shorten(v, 60))
                else:
                    out.append(v)
            if fixed_coin and "coin" not in cols and "currency" not in cols and "symbol" not in cols:
                out.insert(0, fixed_coin)
            pretty_rows.append(tuple(out))

        headers = []
        if fixed_coin and "coin" not in cols and "currency" not in cols and "symbol" not in cols:
            headers.append("coin")
        for c in cols:
            if c.endswith("_sat"):
                headers.append(c[:-4])
            else:
                headers.append(c)

        print_table(
            f"{table_name.upper()} (last {limit})",
            headers,
            pretty_rows,
        )

    if not found_any:
        print("No withdrawal tables found. Looked for: withdrawals, withdraw_log, veco_withdrawals, nns_withdrawals.")



def dump_users(con: sqlite3.Connection, limit: Optional[int]) -> None:
    if not table_exists(con, "users"):
        print("Table 'users' not found.")
        return

    cols = table_columns(con, "users")
    has_veco_internal = "veco_internal_sat" in cols
    has_veco_deposit = "veco_deposit_address" in cols
    has_nns_internal = "nns_internal_sat" in cols
    has_nns_deposit = "nns_deposit_address" in cols

    select_cols = [
        "discord_id",
        "address",
        "balance",
    ]
    if has_veco_internal:
        select_cols.append("veco_internal_sat")
    if has_veco_deposit:
        select_cols.append("veco_deposit_address")
    if has_nns_internal:
        select_cols.append("nns_internal_sat")
    if has_nns_deposit:
        select_cols.append("nns_deposit_address")
    select_cols.extend(["created_at", "updated_at", "last_withdraw_at"])

    order_parts = ["balance DESC"]
    if has_veco_internal:
        order_parts.append("veco_internal_sat DESC")
    if has_nns_internal:
        order_parts.append("nns_internal_sat DESC")
    order_parts.append("updated_at DESC")

    q = f"SELECT {', '.join(select_cols)} FROM users ORDER BY {', '.join(order_parts)}"
    if limit:
        q += " LIMIT ?"
        rows = fetch_all(con, q, (limit,))
    else:
        rows = fetch_all(con, q)

    pretty = []
    for r in rows:
        idx = 0
        discord_id = r[idx]; idx += 1
        address = r[idx]; idx += 1
        balance = r[idx]; idx += 1
        veco_internal_sat = None
        if has_veco_internal:
            veco_internal_sat = r[idx]
            idx += 1
        veco_deposit_address = None
        if has_veco_deposit:
            veco_deposit_address = r[idx]
            idx += 1
        nns_internal_sat = None
        if has_nns_internal:
            nns_internal_sat = r[idx]
            idx += 1
        nns_deposit_address = None
        if has_nns_deposit:
            nns_deposit_address = r[idx]
            idx += 1
        created_at = r[idx]; idx += 1
        updated_at = r[idx]; idx += 1
        last_w = r[idx]; idx += 1

        row_out: List[Any] = [
            discord_id,
            address or "",
            balance,
        ]
        if has_veco_internal:
            row_out.append(fmt_coin_sat(veco_internal_sat if veco_internal_sat is not None else 0))
        if has_veco_deposit:
            row_out.append(veco_deposit_address or "")
        if has_nns_internal:
            row_out.append(fmt_coin_sat(nns_internal_sat if nns_internal_sat is not None else 0))
        if has_nns_deposit:
            row_out.append(nns_deposit_address or "")
        row_out.extend([
            fmt_ts(created_at),
            fmt_ts(updated_at),
            fmt_ts(last_w),
        ])
        pretty.append(tuple(row_out))

    headers = ["discord_id", "address", "balance_hcc"]
    if has_veco_internal:
        headers.append("veco_internal")
    if has_veco_deposit:
        headers.append("veco_deposit_address")
    if has_nns_internal:
        headers.append("nns_internal")
    if has_nns_deposit:
        headers.append("nns_deposit_address")
    headers.extend(["created_at", "updated_at", "last_withdraw_at"])

    print_table(
        "USERS (sorted by balance desc)",
        headers,
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
        # Support comma-separated types and a special shorthand: "lottery"
        t = str(only_type).strip()
        if t.lower() == "lottery":
            # Show lottery-related bookkeeping in tx_log.
            # We match common patterns by type prefix and a fallback note match.
            where.append("(type LIKE 'lottery_%' OR type IN ('lottery_ticket','lottery_payout','lottery_seed','lottery_house_fee') OR note LIKE '%lottery%')")
        else:
            parts = [p.strip() for p in t.split(",") if p.strip()]
            if len(parts) == 1:
                where.append("type = ?")
                params.append(parts[0])
            else:
                where.append("type IN (%s)" % ",".join(["?"] * len(parts)))
                params.extend(parts)
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



def dump_swap_log(con: sqlite3.Connection, limit: int, only_status: Optional[str]) -> None:
    if not table_exists(con, "swap_log"):
        print("Table 'swap_log' not found.")
        return

    cols = table_columns(con, "swap_log")
    # Choose a reasonable ordering column
    order_col = "id" if "id" in cols else ("ts" if "ts" in cols else cols[0])

    where = []
    params: List[Any] = []
    if only_status and "status" in cols:
        where.append("status = ?")
        params.append(only_status)
    wsql = ("WHERE " + " AND ".join(where)) if where else ""

    q = f"SELECT {', '.join(cols)} FROM swap_log {wsql} ORDER BY {order_col} DESC LIMIT ?"
    params.append(limit)
    rows = fetch_all(con, q, tuple(params))

    pretty_rows: List[Tuple[Any, ...]] = []
    for r in rows:
        # r is a tuple; map special formatting for timestamps and long text
        out: List[Any] = []
        for i, v in enumerate(r):
            cname = cols[i]
            if cname in ("ts", "created_at", "updated_at", "first_seen_ts", "last_update_ts"):
                out.append(fmt_ts(v))
            elif cname in ("note", "error"):
                out.append(shorten(v, 60))
            elif isinstance(v, str):
                out.append(shorten(v, 60))
            else:
                out.append(v)
        pretty_rows.append(tuple(out))

    print_table(
        f"SWAP_LOG (last {limit})",
        cols,
        pretty_rows,
    )


def dump_nns_stakes(con: sqlite3.Connection, limit: int) -> None:
    if not table_exists(con, "nns_stakes"):
        print("Table 'nns_stakes' not found.")
        return

    cols = table_columns(con, "nns_stakes")
    order_col = first_existing(cols, ["updated_at", "created_at", "id", cols[0]]) or cols[0]

    amount_col = first_existing(cols, [
        "staked_sat",
        "stake_sat",
        "amount_sat",
        "amount",
        "staked_amount",
        "stake_amount",
        "principal_sat",
        "principal",
    ])

    total_staked = None
    if amount_col:
        try:
            row = con.execute(f"SELECT COALESCE(SUM({amount_col}), 0) FROM nns_stakes").fetchone()
            total_staked = int((row[0] if row else 0) or 0)
        except Exception:
            total_staked = None

    q = f"SELECT {', '.join(cols)} FROM nns_stakes ORDER BY {order_col} DESC LIMIT ?"
    rows = fetch_all(con, q, (max(1, limit),))

    pretty_rows: List[Tuple[Any, ...]] = []
    for r in rows:
        out: List[Any] = []
        for i, v in enumerate(r):
            cname = cols[i]
            if cname in (
                "ts", "created_at", "updated_at", "last_claim_at", "last_reward_at",
                "started_at", "ends_at", "claimed_at", "last_accrual_ts",
            ):
                out.append(fmt_ts(v))
            elif cname in ("note", "error"):
                out.append(shorten(v, 60))
            elif isinstance(v, str):
                out.append(shorten(v, 60))
            else:
                out.append(v)
        pretty_rows.append(tuple(out))

    human_amount_cols = {
        "staked_sat",
        "stake_sat",
        "amount_sat",
        "staked_amount",
        "stake_amount",
        "principal_sat",
        "accrued_reward_sat",
    }

    remainder_idx = cols.index("reward_remainder") if "reward_remainder" in cols else None
    accrued_idx = cols.index("accrued_reward_sat") if "accrued_reward_sat" in cols else None

    pretty_rows_hr: List[Tuple[Any, ...]] = []
    for raw_row, display_row in zip(rows, pretty_rows):
        out: List[Any] = []
        for i, v in enumerate(display_row):
            cname = cols[i]
            if cname in human_amount_cols:
                out.append(fmt_coin_sat(raw_row[i]))
            elif cname == "reward_remainder":
                out.append(str(raw_row[i] if raw_row[i] is not None else "0"))
            else:
                out.append(v)

        if accrued_idx is not None and remainder_idx is not None:
            try:
                accrued_sat = Decimal(str(raw_row[accrued_idx] or 0))
                remainder_sat = Decimal(str(raw_row[remainder_idx] or "0"))
                total_pending_sat = accrued_sat + remainder_sat
                out.append(fmt_coin_sat_decimal(total_pending_sat))
            except Exception:
                out.append("")
        elif accrued_idx is not None:
            try:
                accrued_sat = Decimal(str(raw_row[accrued_idx] or 0))
                out.append(fmt_coin_sat_decimal(accrued_sat))
            except Exception:
                out.append("")

        pretty_rows_hr.append(tuple(out))

    headers = []
    for c in cols:
        if c == "reward_remainder":
            headers.append("reward_remainder_sat")
        elif c.endswith("_sat"):
            headers.append(c[:-4])
        else:
            headers.append(c)
    if accrued_idx is not None:
        headers.append("pending_reward")

    shown_count = len(pretty_rows_hr)
    title = f"NNS_STAKES (showing {shown_count} row(s))"
    if total_staked is not None:
        title += f" | total_staked={fmt_coin_sat(total_staked)} NNS"
    print_table(title, headers, pretty_rows_hr)


def dump_lottery_rounds(con: sqlite3.Connection, limit: int) -> None:
    if not table_exists(con, "lottery_rounds"):
        print("Table 'lottery_rounds' not found.")
        return

    cols = table_columns(con, "lottery_rounds")
    order_col = "id" if "id" in cols else ("created_at" if "created_at" in cols else cols[0])

    q = f"SELECT {', '.join(cols)} FROM lottery_rounds ORDER BY {order_col} DESC LIMIT ?"
    rows = fetch_all(con, q, (max(1, limit),))

    pretty_rows: List[Tuple[Any, ...]] = []
    for r in rows:
        out: List[Any] = []
        for i, v in enumerate(r):
            cname = cols[i]
            if cname in ("ts", "created_at", "updated_at", "ends_at", "start_ts", "end_ts"):
                out.append(fmt_ts(v))
            elif cname in ("commit", "reveal", "error", "note"):
                out.append(shorten(v, 60))
            elif isinstance(v, str):
                out.append(shorten(v, 60))
            else:
                out.append(v)
        pretty_rows.append(tuple(out))

    print_table(f"LOTTERY_ROUNDS (last {min(limit, len(pretty_rows))})", cols, pretty_rows)


def dump_lottery_tickets(con: sqlite3.Connection, limit: int) -> None:
    if not table_exists(con, "lottery_tickets"):
        print("Table 'lottery_tickets' not found.")
        return

    cols = table_columns(con, "lottery_tickets")
    order_col = "id" if "id" in cols else ("created_at" if "created_at" in cols else cols[0])

    q = f"SELECT {', '.join(cols)} FROM lottery_tickets ORDER BY {order_col} DESC LIMIT ?"
    rows = fetch_all(con, q, (max(1, limit),))

    pretty_rows: List[Tuple[Any, ...]] = []
    for r in rows:
        out: List[Any] = []
        for i, v in enumerate(r):
            cname = cols[i]
            if cname in ("ts", "created_at", "updated_at"):
                out.append(fmt_ts(v))
            elif cname in ("note", "meta"):
                out.append(shorten(v, 60))
            elif isinstance(v, str):
                out.append(shorten(v, 60))
            else:
                out.append(v)
        pretty_rows.append(tuple(out))

    print_table(f"LOTTERY_TICKETS (last {min(limit, len(pretty_rows))})", cols, pretty_rows)


def dump_lottery_payouts(con: sqlite3.Connection, limit: int) -> None:
    if not table_exists(con, "lottery_payouts"):
        print("Table 'lottery_payouts' not found.")
        return

    cols = table_columns(con, "lottery_payouts")
    order_col = "id" if "id" in cols else ("created_at" if "created_at" in cols else cols[0])

    q = f"SELECT {', '.join(cols)} FROM lottery_payouts ORDER BY {order_col} DESC LIMIT ?"
    rows = fetch_all(con, q, (max(1, limit),))

    pretty_rows: List[Tuple[Any, ...]] = []
    for r in rows:
        out: List[Any] = []
        for i, v in enumerate(r):
            cname = cols[i]
            if cname in ("ts", "created_at", "updated_at"):
                out.append(fmt_ts(v))
            elif cname in ("note", "error"):
                out.append(shorten(v, 60))
            elif isinstance(v, str):
                out.append(shorten(v, 60))
            else:
                out.append(v)
        pretty_rows.append(tuple(out))

    print_table(f"LOTTERY_PAYOUTS (last {min(limit, len(pretty_rows))})", cols, pretty_rows)


def dump_lottery_state(con: sqlite3.Connection) -> None:
    if not table_exists(con, "lottery_state"):
        print("Table 'lottery_state' not found.")
        return

    cols = table_columns(con, "lottery_state")
    q = f"SELECT {', '.join(cols)} FROM lottery_state ORDER BY id DESC LIMIT 5"
    rows = fetch_all(con, q)

    pretty_rows: List[Tuple[Any, ...]] = []
    for r in rows:
        out: List[Any] = []
        for i, v in enumerate(r):
            cname = cols[i]
            if cname in ("ts", "created_at", "updated_at", "last_tick_ts"):
                out.append(fmt_ts(v))
            elif cname in ("commit", "reveal", "note", "error"):
                out.append(shorten(v, 80))
            elif isinstance(v, str):
                out.append(shorten(v, 80))
            else:
                out.append(v)
        pretty_rows.append(tuple(out))

    print_table("LOTTERY_STATE (last 5)", cols, pretty_rows)


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
    ap.add_argument("--swaps", action="store_true", help="Dump swap_log table (recent swaps)")
    ap.add_argument("--deposits", action="store_true", help="Dump deposits / deposit_log table (recent deposits)")
    ap.add_argument("--withdrawals", action="store_true", help="Dump withdrawals / withdraw_log table (recent withdrawals)")
    ap.add_argument("--withdraw-coin", default="", help="Filter withdrawals by coin/currency if such a column exists")
    ap.add_argument("--withdraw-status", default="", help="Filter withdrawals by status/state if such a column exists")
    ap.add_argument("--deposit-coin", default="", help="Filter deposits by coin/currency if such a column exists")
    ap.add_argument("--deposit-status", default="", help="Filter deposits by status/state if such a column exists")
    ap.add_argument("--nns-stakes", action="store_true", help="Dump nns_stakes table and show total NNS currently staked with the bot")
    ap.add_argument("--schema", action="store_true", help="Dump DB schema")
    ap.add_argument("--limit", type=int, default=50, help="Limit rows (default: 50). Use 0 to show all rows. For --tx this is last N entries.")
    ap.add_argument("--tx-type", default="", help="Filter tx_log by type. Supports comma-separated values (e.g. tip,grant) or 'lottery' for lottery-related entries.")
    ap.add_argument("--tx-status", default="", help="Filter tx_log by status (ok|pending|failed)")
    ap.add_argument("--swap-status", default="", help="Filter swap_log by status if column exists")

    ap.add_argument("--lottery", action="store_true", help="Shortcut: dump only lottery-related entries from tx_log (same as --tx-type lottery)")
    ap.add_argument("--lottery-tables", action="store_true", help="Dump dedicated lottery_* tables (tickets/rounds/payouts/state)")

    args = ap.parse_args()

    if getattr(args, "lottery", False):
        # Lottery data is stored in dedicated tables; also include tx_log filter as a best-effort.
        args.tx = True
        args.tx_type = "lottery"
        # Also dump dedicated lottery tables
        args.lottery_tables = True

    # If no section chosen, dump the common tables only.
    # Lottery tables are intentionally excluded from the default dump and require flags.
    if not (args.users or args.limits or args.tx or args.swaps or args.deposits or args.withdrawals or args.schema or args.nns_stakes or getattr(args, "lottery_tables", False)):
        args.schema = True
        args.users = True
        args.limits = True
        args.tx = True
        args.swaps = True
        args.deposits = True
        args.withdrawals = True
        args.nns_stakes = True

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
        if args.swaps:
            dump_swap_log(
                con,
                limit=max(1, args.limit),
                only_status=args.swap_status.strip() or None,
            )
        if args.deposits:
            dump_deposits(
                con,
                limit=max(1, args.limit),
                only_coin=args.deposit_coin.strip() or None,
                only_status=args.deposit_status.strip() or None,
            )
        if args.withdrawals:
            dump_withdrawals(
                con,
                limit=max(1, args.limit),
                only_coin=args.withdraw_coin.strip() or None,
                only_status=args.withdraw_status.strip() or None,
            )
        if args.nns_stakes:
            if args.limit and args.limit > 0:
                dump_nns_stakes(con, limit=args.limit)
            else:
                dump_nns_stakes(con, limit=10**9)
        if getattr(args, "lottery_tables", False):
            dump_lottery_state(con)
            dump_lottery_rounds(con, limit=max(1, args.limit))
            dump_lottery_tickets(con, limit=max(1, args.limit))
            dump_lottery_payouts(con, limit=max(1, args.limit))
    finally:
        con.close()


if __name__ == "__main__":
    main()