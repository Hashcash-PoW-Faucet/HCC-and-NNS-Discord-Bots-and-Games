#!/usr/bin/env python3
import os
import json
import time
import sqlite3
import asyncio
from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from dotenv import load_dotenv

load_dotenv()

# ---------------------------
# Config
# ---------------------------
DB_PATH = os.environ.get("TIPBOT_DB", "tipbot.db").strip()

VECO_RPC_URL = os.environ.get("VECO_RPC_URL", "http://127.0.0.1:26920/").strip()
VECO_RPC_USER = os.environ.get("VECO_RPC_USER", "").strip()
VECO_RPC_PASSWORD = os.environ.get("VECO_RPC_PASSWORD", "").strip()

VECO_SATS = 100_000_000
VECO_DEPOSIT_CONFS = int(os.environ.get("VECO_DEPOSIT_CONFS", "6"))

POLL_SECONDS = int(os.environ.get("VECO_POLL_SECONDS", "15"))

# Logging: print a heartbeat line at most every N seconds (also logs immediately if something happened)
LOG_HEARTBEAT_SECONDS = int(os.environ.get("VECO_LOG_HEARTBEAT_SECONDS", "600"))
WITHDRAW_BATCH = int(os.environ.get("VECO_WITHDRAW_BATCH", "10"))


# How many uncredited deposits to refresh per loop (for confirmations)
DEPOSIT_REFRESH_BATCH = int(os.environ.get("VECO_DEPOSIT_REFRESH_BATCH", "200"))

# Safety: don’t spam sends too fast
WITHDRAW_SLEEP_BETWEEN = float(os.environ.get("VECO_WITHDRAW_SLEEP", "0.2"))

# ---------------------------
# Helpers
# ---------------------------
def now_ts() -> int:
    return int(time.time())


def should_log_status(seen: int, credited: int, sent: int, failed: int, last_log_ts: int) -> bool:
    """Log only when there was activity, or once per heartbeat interval."""
    if seen or credited or sent or failed:
        return True
    hb = int(LOG_HEARTBEAT_SECONDS)
    if hb <= 0:
        return False
    return (now_ts() - int(last_log_ts)) >= hb


def format_sat_to_veco(sat: int) -> str:
    return f"{(Decimal(int(sat)) / Decimal(VECO_SATS)):.8f}"


def parse_amount_to_sat(amount_any: Any) -> int:
    # JSON-RPC often returns amount as float -> use str() to preserve value reasonably
    d = Decimal(str(amount_any)).quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)
    sat = int(d * Decimal(VECO_SATS))
    return sat


def db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)  # autocommit
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con


def init_db() -> None:
    con = db()
    # State table for watcher
    con.execute("""
    CREATE TABLE IF NOT EXISTS veco_watcher_state (
      id INTEGER PRIMARY KEY CHECK(id=1),
      lastblockhash TEXT,
      updated_at INTEGER NOT NULL
    );
    """)
    # Idempotent deposit tracking
    con.execute("""
    CREATE TABLE IF NOT EXISTS veco_deposits (
      txid TEXT NOT NULL,
      vout INTEGER NOT NULL,
      discord_id INTEGER NOT NULL,
      address TEXT NOT NULL,
      amount_sat INTEGER NOT NULL,
      confirmations INTEGER NOT NULL DEFAULT 0,
      credited INTEGER NOT NULL DEFAULT 0,
      first_seen_ts INTEGER NOT NULL,
      credited_ts INTEGER,
      last_update_ts INTEGER NOT NULL,
      PRIMARY KEY(txid, vout)
    );
    """)
    # Ensure state row exists
    row = con.execute("SELECT id FROM veco_watcher_state WHERE id=1").fetchone()
    if not row:
        con.execute(
            "INSERT INTO veco_watcher_state(id, lastblockhash, updated_at) VALUES(1, NULL, ?)",
            (now_ts(),)
        )
    con.close()


def get_lastblockhash(con: sqlite3.Connection) -> Optional[str]:
    row = con.execute("SELECT lastblockhash FROM veco_watcher_state WHERE id=1").fetchone()
    if not row:
        return None
    return row[0] if row[0] else None


def set_lastblockhash(con: sqlite3.Connection, bh: Optional[str]) -> None:
    con.execute(
        "UPDATE veco_watcher_state SET lastblockhash=?, updated_at=? WHERE id=1",
        (bh, now_ts())
    )


async def veco_rpc_call(method: str, params: Optional[List[Any]] = None) -> Any:
    if not VECO_RPC_URL or not VECO_RPC_USER or not VECO_RPC_PASSWORD:
        raise RuntimeError("VECO RPC not configured (VECO_RPC_URL/USER/PASSWORD)")

    payload = {
        "jsonrpc": "1.0",
        "id": "veco-watcher",
        "method": method,
        "params": params or [],
    }

    auth = aiohttp.BasicAuth(VECO_RPC_USER, VECO_RPC_PASSWORD)
    async with aiohttp.ClientSession(auth=auth) as session:
        async with session.post(VECO_RPC_URL, json=payload, timeout=30) as r:
            txt = await r.text()
            if r.status != 200:
                raise RuntimeError(f"VECO RPC HTTP {r.status}: {txt}")
            data = json.loads(txt)
            if data.get("error"):
                raise RuntimeError(f"VECO RPC error: {data['error']}")
            return data.get("result")


def load_address_map(con: sqlite3.Connection) -> Dict[str, int]:
    # address -> discord_id
    rows = con.execute(
        "SELECT discord_id, veco_deposit_address FROM users WHERE veco_deposit_address IS NOT NULL AND TRIM(veco_deposit_address) != ''"
    ).fetchall()
    m: Dict[str, int] = {}
    for discord_id, addr in rows:
        a = (addr or "").strip()
        if a:
            m[a] = int(discord_id)
    return m


def upsert_deposit_seen(
    con: sqlite3.Connection,
    txid: str,
    vout: int,
    discord_id: int,
    address: str,
    amount_sat: int,
    confirmations: int
) -> None:
    ts = now_ts()
    # Insert if new; if exists update confirmations
    con.execute("""
    INSERT INTO veco_deposits(txid, vout, discord_id, address, amount_sat, confirmations, credited, first_seen_ts, credited_ts, last_update_ts)
    VALUES(?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(txid, vout) DO UPDATE SET
      confirmations=excluded.confirmations,
      last_update_ts=excluded.last_update_ts
    """, (txid, int(vout), int(discord_id), address, int(amount_sat), int(confirmations), 0, ts, None, ts))


def try_credit_deposit(con: sqlite3.Connection, txid: str, vout: int) -> bool:
    """
    Atomically credit if:
    - deposit exists
    - credited=0
    - confirmations >= VECO_DEPOSIT_CONFS
    Returns True if credited.
    """
    row = con.execute(
        "SELECT discord_id, amount_sat, confirmations, credited FROM veco_deposits WHERE txid=? AND vout=?",
        (txid, int(vout))
    ).fetchone()
    if not row:
        return False
    discord_id, amount_sat, confirmations, credited = int(row[0]), int(row[1]), int(row[2]), int(row[3])
    if credited:
        return False
    if confirmations < VECO_DEPOSIT_CONFS:
        return False

    ts = now_ts()
    # credit internal balance + mark credited
    con.execute(
        "UPDATE users SET veco_internal_sat = veco_internal_sat + ?, updated_at=? WHERE discord_id=?",
        (int(amount_sat), ts, int(discord_id))
    )
    con.execute(
        "UPDATE veco_deposits SET credited=1, credited_ts=?, last_update_ts=? WHERE txid=? AND vout=?",
        (ts, ts, txid, int(vout))
    )
    return True


# --- Refresh confirmations for uncredited deposits via gettransaction
async def refresh_uncredited_confirmations(con: sqlite3.Connection) -> int:
    """Refresh confirmations for uncredited deposits via `gettransaction`.

    Some wallet/RPC implementations return a tx in `listsinceblock` only once,
    so confirmations may stay stale in our DB unless we actively refresh.

    Returns number of rows updated.
    """
    rows = con.execute(
        "SELECT txid FROM veco_deposits WHERE credited=0 ORDER BY last_update_ts ASC LIMIT ?",
        (int(DEPOSIT_REFRESH_BATCH),)
    ).fetchall()
    if not rows:
        return 0

    updated = 0
    for (txid,) in rows:
        txid = str(txid)
        try:
            gt = await veco_rpc_call("gettransaction", [txid])
        except Exception:
            continue

        confs = int(gt.get("confirmations") or 0) if isinstance(gt, dict) else 0
        if confs < 0:
            confs = 0

        # Update all vouts we already track for this txid
        cur = con.execute(
            "UPDATE veco_deposits SET confirmations=?, last_update_ts=? WHERE txid=? AND credited=0",
            (confs, now_ts(), txid)
        )
        if cur.rowcount:
            updated += int(cur.rowcount)

    return updated


async def process_deposits() -> Tuple[int, int]:
    """
    Returns: (seen_count, credited_count)
    """
    con = db()
    try:
        addr_map = load_address_map(con)
        last_bh = get_lastblockhash(con)
    finally:
        con.close()

    # Nothing to watch
    if not addr_map:
        return (0, 0)

    # listsinceblock supports blockhash param; we also want new lastblockhash
    # Typical return: { "transactions": [...], "lastblock": "..." }
    # We'll be defensive with params.
    try:
        if last_bh:
            res = await veco_rpc_call("listsinceblock", [last_bh])
        else:
            res = await veco_rpc_call("listsinceblock", [])
    except Exception:
        # Fallback: try with include_watchonly=True (Dash-like)
        if last_bh:
            res = await veco_rpc_call("listsinceblock", [last_bh, 1, True])
        else:
            res = await veco_rpc_call("listsinceblock", [None, 1, True])

    txs = res.get("transactions") if isinstance(res, dict) else None
    new_last = res.get("lastblock") if isinstance(res, dict) else None
    if not isinstance(txs, list):
        txs = []

    seen = 0
    credited = 0

    con2 = db()
    try:
        con2.execute("BEGIN IMMEDIATE;")

        # Update lastblockhash early (so we don’t reprocess endlessly if we crash later)
        if isinstance(new_last, str) and new_last:
            set_lastblockhash(con2, new_last)

        # First pass: upsert all relevant receives
        for t in txs:
            if not isinstance(t, dict):
                continue
            if str(t.get("category") or "").lower() != "receive":
                continue

            addr = str(t.get("address") or "").strip()
            if not addr or addr not in addr_map:
                continue

            txid = str(t.get("txid") or "").strip()
            if not txid:
                continue

            vout = int(t.get("vout") or 0)
            confs = int(t.get("confirmations") or 0)

            amt_sat = parse_amount_to_sat(t.get("amount", 0))

            # only positive deposits
            if amt_sat <= 0:
                continue

            upsert_deposit_seen(con2, txid, vout, addr_map[addr], addr, amt_sat, confs)
            seen += 1

        # Refresh confirmations for uncredited deposits (listsinceblock may not replay txs)
        await refresh_uncredited_confirmations(con2)
        # Second pass: credit matured deposits
        # (Credit based on DB state to avoid double credits)
        rows = con2.execute(
            "SELECT txid, vout FROM veco_deposits WHERE credited=0 AND confirmations >= ?",
            (int(VECO_DEPOSIT_CONFS),)
        ).fetchall()

        for txid, vout in rows:
            if try_credit_deposit(con2, str(txid), int(vout)):
                credited += 1

        con2.execute("COMMIT;")

    except Exception:
        try:
            con2.execute("ROLLBACK;")
        except Exception:
            pass
        raise
    finally:
        con2.close()

    return (seen, credited)


async def process_withdrawals() -> Tuple[int, int]:
    """
    Returns: (sent_count, failed_count)
    """
    con = db()
    try:
        rows = con.execute(
            "SELECT id, discord_id, to_address, amount_sat, fee_sat FROM veco_withdrawals WHERE status='pending' ORDER BY id ASC LIMIT ?",
            (int(WITHDRAW_BATCH),)
        ).fetchall()
    finally:
        con.close()

    if not rows:
        return (0, 0)

    sent = 0
    failed = 0

    for wid, discord_id, to_addr, amount_sat, fee_sat in rows:
        wid = int(wid)
        discord_id = int(discord_id)
        to_addr = str(to_addr).strip()
        amount_sat = int(amount_sat)
        fee_sat = int(fee_sat or 0)

        # Broadcast outside DB lock
        txid: Optional[str] = None
        err: Optional[str] = None
        try:
            # `amount_sat` already stores the net user amount after fee deduction in the tipbot.
            # Send only this net amount on-chain and keep the fee implicitly in the hot wallet.
            amount_str = format_sat_to_veco(amount_sat)
            txid = await veco_rpc_call("sendtoaddress", [to_addr, amount_str])

            if not isinstance(txid, str) or not txid.strip():
                raise RuntimeError("withdraw broadcast returned invalid txid")
            txid = txid.strip()
        except Exception as e:
            err = str(e)[:500]

        # Finalize atomically
        con2 = db()
        try:
            con2.execute("BEGIN IMMEDIATE;")
            row = con2.execute(
                "SELECT status, discord_id, amount_sat, fee_sat FROM veco_withdrawals WHERE id=?",
                (wid,)
            ).fetchone()
            if not row:
                con2.execute("ROLLBACK;")
                continue

            status_now = str(row[0])
            did_now = int(row[1])
            amt_now = int(row[2])
            fee_now = int(row[3] or 0)

            # If it was already processed by another run, skip
            if status_now != "pending":
                con2.execute("ROLLBACK;")
                continue

            if txid:
                con2.execute(
                    "UPDATE veco_withdrawals SET status='sent', txid=?, error=NULL WHERE id=?",
                    (txid, wid)
                )
                con2.execute("COMMIT;")
                sent += 1
            else:
                # mark failed and refund ONLY ONCE
                con2.execute(
                    "UPDATE veco_withdrawals SET status='failed', error=? WHERE id=?",
                    (err or "unknown error", wid)
                )
                con2.execute(
                    "UPDATE users SET veco_internal_sat = veco_internal_sat + ?, updated_at=? WHERE discord_id=?",
                    (int(amt_now) + int(fee_now), now_ts(), did_now)
                )
                con2.execute("COMMIT;")
                failed += 1

        except Exception:
            try:
                con2.execute("ROLLBACK;")
            except Exception:
                pass
            raise
        finally:
            con2.close()

        await asyncio.sleep(WITHDRAW_SLEEP_BETWEEN)

    return (sent, failed)


async def main_loop() -> None:
    init_db()
    print("veco_watcher started")
    print(f"DB={DB_PATH}")
    print(f"RPC={VECO_RPC_URL}")
    print(f"DEPOSIT_CONFS={VECO_DEPOSIT_CONFS} POLL_SECONDS={POLL_SECONDS} WITHDRAW_BATCH={WITHDRAW_BATCH}")

    last_log_ts = 0

    while True:
        try:
            seen, credited = await process_deposits()
            sent, failed = await process_withdrawals()

            if should_log_status(seen, credited, sent, failed, last_log_ts):
                print(
                    f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] "
                    f"deposits: seen={seen} credited={credited} | withdrawals: sent={sent} failed={failed}",
                    flush=True,
                )
                last_log_ts = now_ts()
        except Exception as e:
            print(f"[ERROR] {e}", flush=True)

        await asyncio.sleep(POLL_SECONDS)


if __name__ == "__main__":
    asyncio.run(main_loop())