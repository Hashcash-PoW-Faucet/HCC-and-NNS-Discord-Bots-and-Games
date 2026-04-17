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

NNS_RPC_URL = os.environ.get("NNS_RPC_URL", "http://127.0.0.1:48931/").strip()
NNS_RPC_USER = os.environ.get("NNS_RPC_USER", "").strip()
NNS_RPC_PASSWORD = os.environ.get("NNS_RPC_PASSWORD", "").strip()

NNS_SATS = 100_000_000
NNS_DEPOSIT_CONFS = int(os.environ.get("NNS_DEPOSIT_CONFS", "6"))

POLL_SECONDS = int(os.environ.get("NNS_POLL_SECONDS", "15"))

LOG_HEARTBEAT_SECONDS = int(os.environ.get("NNS_LOG_HEARTBEAT_SECONDS", "600"))
WITHDRAW_BATCH = int(os.environ.get("NNS_WITHDRAW_BATCH", "10"))

NNS_WITHDRAW_FEE_ADDRESS = os.environ.get("NNS_WITHDRAW_FEE_ADDRESS", "").strip()

DEPOSIT_REFRESH_BATCH = int(os.environ.get("NNS_DEPOSIT_REFRESH_BATCH", "200"))

WITHDRAW_SLEEP_BETWEEN = float(os.environ.get("NNS_WITHDRAW_SLEEP", "0.2"))

EXPLORER_SENDRAWTX_URL = os.environ.get("EXPLORER_SENDRAWTX_URL", "").strip()
EXPLORER_TX_API_KEY = os.environ.get("EXPLORER_TX_API_KEY", "").strip()

EXPLORER_REBROADCAST_TIMEOUT = int(os.environ.get("EXPLORER_REBROADCAST_TIMEOUT", "20"))
NNS_RPC_TIMEOUT = int(os.environ.get("NNS_RPC_TIMEOUT", "30"))
NNS_RPC_RETRIES = int(os.environ.get("NNS_RPC_RETRIES", "1"))
NNS_RPC_RETRY_DELAY = float(os.environ.get("NNS_RPC_RETRY_DELAY", "1.5"))
NNS_CONFIRMATION_REFRESH_SLEEP = float(os.environ.get("NNS_CONFIRMATION_REFRESH_SLEEP", "0.05"))


# ---------------------------
# Helpers
# ---------------------------
def now_ts() -> int:
    return int(time.time())


def should_log_status(seen: int, credited: int, sent: int, failed: int, last_log_ts: int) -> bool:
    if seen or credited or sent or failed:
        return True
    hb = int(LOG_HEARTBEAT_SECONDS)
    if hb <= 0:
        return False
    return (now_ts() - int(last_log_ts)) >= hb


def format_sat_to_nns(sat: int) -> str:
    return f"{(Decimal(int(sat)) / Decimal(NNS_SATS)):.8f}"


def parse_amount_to_sat(amount_any: Any) -> int:
    d = Decimal(str(amount_any)).quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)
    sat = int(d * Decimal(NNS_SATS))
    return sat


def db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con


def init_db() -> None:
    con = db()
    con.execute("""
    CREATE TABLE IF NOT EXISTS nns_watcher_state (
      id INTEGER PRIMARY KEY CHECK(id=1),
      lastblockhash TEXT,
      updated_at INTEGER NOT NULL
    );
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS nns_deposits (
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
    row = con.execute("SELECT id FROM nns_watcher_state WHERE id=1").fetchone()
    if not row:
        con.execute(
            "INSERT INTO nns_watcher_state(id, lastblockhash, updated_at) VALUES(1, NULL, ?)",
            (now_ts(),)
        )
    con.close()


def get_lastblockhash(con: sqlite3.Connection) -> Optional[str]:
    row = con.execute("SELECT lastblockhash FROM nns_watcher_state WHERE id=1").fetchone()
    if not row:
        return None
    return row[0] if row[0] else None


def set_lastblockhash(con: sqlite3.Connection, bh: Optional[str]) -> None:
    con.execute(
        "UPDATE nns_watcher_state SET lastblockhash=?, updated_at=? WHERE id=1",
        (bh, now_ts())
    )



_RPC_SESSION: Optional[aiohttp.ClientSession] = None


async def get_rpc_session() -> aiohttp.ClientSession:
    global _RPC_SESSION
    if _RPC_SESSION is None or _RPC_SESSION.closed:
        auth = aiohttp.BasicAuth(NNS_RPC_USER, NNS_RPC_PASSWORD)
        timeout = aiohttp.ClientTimeout(total=NNS_RPC_TIMEOUT)
        connector = aiohttp.TCPConnector(limit=8, limit_per_host=8, keepalive_timeout=30)
        _RPC_SESSION = aiohttp.ClientSession(auth=auth, timeout=timeout, connector=connector)
    return _RPC_SESSION


async def close_rpc_session() -> None:
    global _RPC_SESSION
    if _RPC_SESSION is not None and not _RPC_SESSION.closed:
        await _RPC_SESSION.close()
    _RPC_SESSION = None


async def nns_rpc_call(method: str, params: Optional[List[Any]] = None) -> Any:
    if not NNS_RPC_URL or not NNS_RPC_USER or not NNS_RPC_PASSWORD:
        raise RuntimeError("996-Coin RPC not configured (NNS_RPC_URL/USER/PASSWORD)")

    payload = {
        "jsonrpc": "1.0",
        "id": "nns-watcher",
        "method": method,
        "params": params or [],
    }

    attempts = max(1, int(NNS_RPC_RETRIES) + 1)
    last_err: Optional[Exception] = None

    for attempt in range(1, attempts + 1):
        try:
            session = await get_rpc_session()
            async with session.post(NNS_RPC_URL, json=payload) as r:
                txt = await r.text()
                if r.status != 200:
                    raise RuntimeError(f"996-Coin RPC HTTP {r.status}: {txt}")
                data = json.loads(txt)
                if data.get("error"):
                    raise RuntimeError(f"996-Coin RPC error: {data['error']}")
                return data.get("result")
        except Exception as e:
            last_err = e
            msg = str(e)
            transient = (
                isinstance(e, aiohttp.ClientError)
                or isinstance(e, asyncio.TimeoutError)
                or "work queue depth exceeded" in msg.lower()
                or "cannot connect to host" in msg.lower()
                or "server disconnected" in msg.lower()
            )
            if attempt >= attempts or not transient:
                break
            await asyncio.sleep(max(0.0, float(NNS_RPC_RETRY_DELAY)))

    raise RuntimeError(str(last_err) if last_err else "unknown RPC error")


# ---------------------------
# Explorer rebroadcast helpers
# ---------------------------

async def get_raw_tx_hex(txid: str) -> str:
    """
    Fetch the wallet transaction and return its raw hex for best-effort rebroadcast.
    """
    res = await nns_rpc_call("gettransaction", [str(txid), True])
    if not isinstance(res, dict):
        raise RuntimeError("gettransaction returned invalid result")
    raw_hex = str(res.get("hex") or "").strip()
    if not raw_hex:
        raise RuntimeError("transaction hex not available from gettransaction")
    return raw_hex


async def explorer_rebroadcast_raw_tx(raw_hex: str) -> Dict[str, Any]:
    """
    Best-effort rebroadcast via explorer backend.
    Returns the parsed JSON response. Raises on transport or HTTP errors.
    """
    if not EXPLORER_SENDRAWTX_URL:
        raise RuntimeError("EXPLORER_SENDRAWTX_URL not configured")

    headers = {"Content-Type": "application/json"}
    if EXPLORER_TX_API_KEY:
        headers["X-API-Key"] = EXPLORER_TX_API_KEY

    payload: Dict[str, Any] = {"hex": str(raw_hex).strip()}

    timeout = aiohttp.ClientTimeout(total=EXPLORER_REBROADCAST_TIMEOUT)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(EXPLORER_SENDRAWTX_URL, json=payload, headers=headers) as r:
            txt = await r.text()
            if r.status >= 400:
                raise RuntimeError(f"explorer rebroadcast HTTP {r.status}: {txt}")
            try:
                data = json.loads(txt)
            except Exception as e:
                raise RuntimeError(f"explorer rebroadcast returned invalid JSON: {e}")
            if not isinstance(data, dict):
                raise RuntimeError("explorer rebroadcast returned invalid response type")
            return data


def load_address_map(con: sqlite3.Connection) -> Dict[str, int]:
    rows = con.execute(
        "SELECT discord_id, nns_deposit_address FROM users WHERE nns_deposit_address IS NOT NULL AND TRIM(nns_deposit_address) != ''"
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
    con.execute("""
    INSERT INTO nns_deposits(txid, vout, discord_id, address, amount_sat, confirmations, credited, first_seen_ts, credited_ts, last_update_ts)
    VALUES(?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(txid, vout) DO UPDATE SET
      confirmations=excluded.confirmations,
      last_update_ts=excluded.last_update_ts
    """, (txid, int(vout), int(discord_id), address, int(amount_sat), int(confirmations), 0, ts, None, ts))


def try_credit_deposit(con: sqlite3.Connection, txid: str, vout: int) -> bool:
    row = con.execute(
        "SELECT discord_id, amount_sat, confirmations, credited FROM nns_deposits WHERE txid=? AND vout=?",
        (txid, int(vout))
    ).fetchone()
    if not row:
        return False

    discord_id, amount_sat, confirmations, credited = int(row[0]), int(row[1]), int(row[2]), int(row[3])

    if credited:
        return False
    if confirmations < NNS_DEPOSIT_CONFS:
        return False

    ts = now_ts()
    con.execute(
        "UPDATE users SET nns_internal_sat = nns_internal_sat + ?, updated_at=? WHERE discord_id=?",
        (int(amount_sat), ts, int(discord_id))
    )
    con.execute(
        "UPDATE nns_deposits SET credited=1, credited_ts=?, last_update_ts=? WHERE txid=? AND vout=?",
        (ts, ts, txid, int(vout))
    )
    return True


async def refresh_uncredited_confirmations(con: sqlite3.Connection) -> int:
    rows = con.execute(
        "SELECT txid FROM nns_deposits WHERE credited=0 ORDER BY last_update_ts ASC LIMIT ?",
        (int(DEPOSIT_REFRESH_BATCH),)
    ).fetchall()
    if not rows:
        return 0

    updated = 0
    for (txid,) in rows:
        txid = str(txid)
        try:
            gt = await nns_rpc_call("gettransaction", [txid])
        except Exception:
            if NNS_CONFIRMATION_REFRESH_SLEEP > 0:
                await asyncio.sleep(NNS_CONFIRMATION_REFRESH_SLEEP)
            continue

        confs = int(gt.get("confirmations") or 0) if isinstance(gt, dict) else 0
        if confs < 0:
            confs = 0

        cur = con.execute(
            "UPDATE nns_deposits SET confirmations=?, last_update_ts=? WHERE txid=? AND credited=0",
            (confs, now_ts(), txid)
        )
        if cur.rowcount:
            updated += int(cur.rowcount)

        if NNS_CONFIRMATION_REFRESH_SLEEP > 0:
            await asyncio.sleep(NNS_CONFIRMATION_REFRESH_SLEEP)

    return updated


async def process_deposits() -> Tuple[int, int]:
    con = db()
    try:
        addr_map = load_address_map(con)
        last_bh = get_lastblockhash(con)
    finally:
        con.close()

    if not addr_map:
        return (0, 0)

    try:
        if last_bh:
            res = await nns_rpc_call("listsinceblock", [last_bh])
        else:
            res = await nns_rpc_call("listsinceblock", [])
    except Exception:
        if last_bh:
            res = await nns_rpc_call("listsinceblock", [last_bh, 1, True])
        else:
            res = await nns_rpc_call("listsinceblock", [None, 1, True])

    txs = res.get("transactions") if isinstance(res, dict) else None
    new_last = res.get("lastblock") if isinstance(res, dict) else None
    if not isinstance(txs, list):
        txs = []

    seen = 0
    credited = 0

    con2 = db()
    try:
        con2.execute("BEGIN IMMEDIATE;")

        if isinstance(new_last, str) and new_last:
            set_lastblockhash(con2, new_last)

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

            if amt_sat <= 0:
                continue

            upsert_deposit_seen(con2, txid, vout, addr_map[addr], addr, amt_sat, confs)
            seen += 1

        await refresh_uncredited_confirmations(con2)

        rows = con2.execute(
            "SELECT txid, vout FROM nns_deposits WHERE credited=0 AND confirmations >= ?",
            (int(NNS_DEPOSIT_CONFS),)
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
    con = db()
    try:
        rows = con.execute(
            "SELECT id, discord_id, to_address, amount_sat, fee_sat FROM nns_withdrawals WHERE status='pending' ORDER BY id ASC LIMIT ?",
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

        txid: Optional[str] = None
        err: Optional[str] = None
        rebroadcast_note: Optional[str] = None
        try:
            amount_str = format_sat_to_nns(amount_sat)

            if fee_sat > 0:
                if not NNS_WITHDRAW_FEE_ADDRESS:
                    raise RuntimeError("withdraw fee is set but NNS_WITHDRAW_FEE_ADDRESS is not configured")
                fee_str = format_sat_to_nns(fee_sat)
                outputs = {
                    str(to_addr): float(amount_str),
                    str(NNS_WITHDRAW_FEE_ADDRESS): float(fee_str),
                }
                txid = await nns_rpc_call("sendmany", ["", outputs])
            else:
                txid = await nns_rpc_call("sendtoaddress", [to_addr, amount_str])

            if not isinstance(txid, str) or not txid.strip():
                raise RuntimeError("withdraw broadcast returned invalid txid")
            txid = txid.strip()

            if EXPLORER_SENDRAWTX_URL:
                try:
                    raw_hex = await get_raw_tx_hex(txid)
                    explorer_res = await explorer_rebroadcast_raw_tx(raw_hex)
                    if explorer_res.get("ok") is True:
                        rebroadcast_note = f"explorer rebroadcast ok txid={explorer_res.get('txid', txid)}"
                    else:
                        rebroadcast_note = f"explorer rebroadcast not ok: {explorer_res.get('error', 'unknown error')}"
                except Exception as re:
                    rebroadcast_note = f"explorer rebroadcast failed: {str(re)[:300]}"
        except Exception as e:
            err = str(e)[:500]

        con2 = db()
        try:
            con2.execute("BEGIN IMMEDIATE;")
            row = con2.execute(
                "SELECT status, discord_id, amount_sat, fee_sat FROM nns_withdrawals WHERE id=?",
                (wid,)
            ).fetchone()
            if not row:
                con2.execute("ROLLBACK;")
                continue

            status_now = str(row[0])
            did_now = int(row[1])
            amt_now = int(row[2])
            fee_now = int(row[3] or 0)

            if status_now != "pending":
                con2.execute("ROLLBACK;")
                continue

            if txid:
                con2.execute(
                    "UPDATE nns_withdrawals SET status='sent', txid=?, error=? WHERE id=?",
                    (txid, rebroadcast_note, wid)
                )
                con2.execute("COMMIT;")
                sent += 1
                if rebroadcast_note:
                    print(f"[withdraw {wid}] {rebroadcast_note}", flush=True)
            else:
                con2.execute(
                    "UPDATE nns_withdrawals SET status='failed', error=? WHERE id=?",
                    (err or "unknown error", wid)
                )
                con2.execute(
                    "UPDATE users SET nns_internal_sat = nns_internal_sat + ?, updated_at=? WHERE discord_id=?",
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
    print("nns_watcher started")
    print(f"DB={DB_PATH}")
    print(f"RPC={NNS_RPC_URL}")
    print(f"DEPOSIT_CONFS={NNS_DEPOSIT_CONFS} POLL_SECONDS={POLL_SECONDS} WITHDRAW_BATCH={WITHDRAW_BATCH}")

    last_log_ts = 0

    try:
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
    finally:
        await close_rpc_session()


if __name__ == "__main__":
    asyncio.run(main_loop())