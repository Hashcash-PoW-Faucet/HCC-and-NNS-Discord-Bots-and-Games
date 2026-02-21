# hcc_lottery_bot.py
# Discord Lottery Bot for HCC (tickets/shares), with small house edge and auto rounds.
#
# Requirements:
#   pip install discord.py python-dotenv
#
# .env example:
#   DISCORD_TOKEN=...
#   LOTTERY_DB_PATH=/home/user1/HCC_Tipping_Bot/tipbot.db
#   LOTTERY_CHANNEL_IDS=123,456
#   LOTTERY_ADMIN_IDS=111,222
#
#   # Where to credit the house fee (a discord user id that "owns" the house/treasury)
#   LOTTERY_HOUSE_DISCORD_ID=111
#
#   # Round params
#   LOTTERY_DURATION_MINUTES=1440
#   LOTTERY_HOUSE_FEE_BPS=500
#   LOTTERY_SPLIT_1_BPS=6000
#   LOTTERY_SPLIT_2_BPS=2500
#   LOTTERY_SPLIT_3_BPS=1000
#   LOTTERY_TICKET_CAP=0         # 0 = no cap
#   LOTTERY_CHECK_INTERVAL_SEC=20
#
#   LOTTERY_TICKET_PRICE_HCC=1
#   LOTTERY_REGULAR_POOL_BPS=500
#   LOTTERY_JACKPOT_CHANCE_BPS=300
#   LOTTERY_JACKPOT_PCT_BPS=1000
#   LOTTERY_JACKPOT_CAP_HCC=0
#   LOTTERY_USERS_TABLE=users
#   LOTTERY_USERS_ID_COL=discord_id
#   LOTTERY_USERS_BAL_COL=balance
#   LOTTERY_ENABLE_PREVIEW_DRAW=1
#
# Optional:
#   LOTTERY_ANNOUNCE_CHANNEL_ID=123   # where draws get announced (must be allowed channel)
#
# Notes:
# - This bot debits/credits HCC from your existing tipbot.db "users" table.
#
# Security:
# - House fee goes to LOTTERY_HOUSE_DISCORD_ID.
# - Admin can seed the pot by transferring HCC from the house account into the pot.

import os
import time
import math
import asyncio
import sqlite3
import hashlib
import secrets
from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple

import discord
from discord import app_commands
from dotenv import load_dotenv

load_dotenv()

# ----------------------------
# Config helpers
# ----------------------------


def now_unix() -> int:
    return int(time.time())


def clamp_int(x: int, lo: int, hi: int) -> int:
    try:
        x = int(x)
    except Exception:
        x = lo
    return max(lo, min(hi, x))


def bps_to_frac(bps: int) -> float:
    return float(bps) / 10000.0


def parse_csv_int_set(s: str) -> set[int]:
    out: set[int] = set()
    for part in (s or "").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.add(int(part))
        except Exception:
            pass
    return out

# ----------------------------
# Env / Settings
# ----------------------------


DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN_LOTTERY", "").strip()

DB_PATH = os.environ.get("LOTTERY_DB_PATH", "tipbot.db").strip()

ALLOWED_CHANNEL_IDS = parse_csv_int_set(os.environ.get("LOTTERY_CHANNEL_IDS", ""))
ADMIN_IDS = parse_csv_int_set(os.environ.get("LOTTERY_ADMIN_IDS", ""))

HOUSE_DISCORD_ID = os.environ.get("LOTTERY_HOUSE_DISCORD_ID", "").strip()
HOUSE_DISCORD_ID_INT: Optional[int] = None
try:
    HOUSE_DISCORD_ID_INT = int(HOUSE_DISCORD_ID) if HOUSE_DISCORD_ID else None
except Exception:
    HOUSE_DISCORD_ID_INT = None

ANNOUNCE_CHANNEL_ID = os.environ.get("LOTTERY_ANNOUNCE_CHANNEL_ID", "").strip()
ANNOUNCE_CHANNEL_ID_INT: Optional[int] = None
try:
    ANNOUNCE_CHANNEL_ID_INT = int(ANNOUNCE_CHANNEL_ID) if ANNOUNCE_CHANNEL_ID else None
except Exception:
    ANNOUNCE_CHANNEL_ID_INT = None

DURATION_MIN = clamp_int(int(os.environ.get("LOTTERY_DURATION_MINUTES", "1440")), 5, 60 * 24 * 14)
HOUSE_FEE_BPS = clamp_int(int(os.environ.get("LOTTERY_HOUSE_FEE_BPS", "500")), 0, 3000)

SPLIT_1_BPS = clamp_int(int(os.environ.get("LOTTERY_SPLIT_1_BPS", "6000")), 0, 10000)
SPLIT_2_BPS = clamp_int(int(os.environ.get("LOTTERY_SPLIT_2_BPS", "2500")), 0, 10000)
SPLIT_3_BPS = clamp_int(int(os.environ.get("LOTTERY_SPLIT_3_BPS", "1000")), 0, 10000)

MAX_TICKETS_PER_BUY = clamp_int(int(os.environ.get("LOTTERY_MAX_TICKETS_PER_BUY", "1000")), 1, 100_000)
MAX_TICKETS_PER_MIN = clamp_int(int(os.environ.get("LOTTERY_MAX_TICKETS_PER_MIN", "3000")), 1, 100_000)

TICKET_CAP = clamp_int(int(os.environ.get("LOTTERY_TICKET_CAP", "0")), 0, 1_000_000_000)
CHECK_INTERVAL = clamp_int(int(os.environ.get("LOTTERY_CHECK_INTERVAL_SEC", "20")), 5, 600)

TICKET_PRICE_HCC = clamp_int(int(os.environ.get("LOTTERY_TICKET_PRICE_HCC", "1")), 1, 1_000_000)

# Regular payouts are a small fraction of the TOTAL pot (seed + tickets). The remainder rolls over.
# Jackpot (optional) is an extra payout to the 1st-place winner with a probability.
REGULAR_POOL_BPS = clamp_int(int(os.environ.get("LOTTERY_REGULAR_POOL_BPS", "500")), 0, 5000)  # default 5%
JACKPOT_CHANCE_BPS = clamp_int(int(os.environ.get("LOTTERY_JACKPOT_CHANCE_BPS", "300")), 0, 10000)  # default 3% chance
JACKPOT_PCT_BPS = clamp_int(int(os.environ.get("LOTTERY_JACKPOT_PCT_BPS", "1000")), 0, 5000)  # default 10% of pot
JACKPOT_CAP_HCC = clamp_int(int(os.environ.get("LOTTERY_JACKPOT_CAP_HCC", "0")), 0, 1_000_000_000)  # 0 = no cap

# Admin/testing helpers
# If true, /lottery_preview_draw will simulate the draw without mutating DB balances or round state.
# (Normal auto draws are never dry-run.)
LOTTERY_ENABLE_PREVIEW_DRAW = os.environ.get("LOTTERY_ENABLE_PREVIEW_DRAW", "1").strip() not in ("0", "false", "False")

# Optional overrides for the existing "users" table schema
USERS_TABLE = os.environ.get("LOTTERY_USERS_TABLE", "users").strip()
USERS_ID_COL = os.environ.get("LOTTERY_USERS_ID_COL", "discord_id").strip()

# FIX for your tipbot schema:
USERS_BAL_COL = os.environ.get("LOTTERY_USERS_BAL_COL", "balance").strip()

# ----------------------------
# SQLite
# ----------------------------


def db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con


def log_tx(
    con: sqlite3.Connection,
    *,
    tx_type: str,
    from_id: Optional[int],
    to_id: Optional[int],
    amount: int,
    note: str = "",
    status: str = "ok",
    error: str = "",
    faucet_resp: str = "",
) -> None:
    """
    Write an audit entry into the shared tipbot tx_log table.
    This does NOT change balances; it only records what happened.
    """
    ts = now_unix()
    con.execute(
        "INSERT INTO tx_log(ts, type, from_id, to_id, amount, note, status, faucet_resp, error) "
        "VALUES(?,?,?,?,?,?,?,?,?)",
        (int(ts), str(tx_type), from_id, to_id, int(amount), str(note), str(status), str(faucet_resp), str(error)),
    )


def ensure_tables(con: sqlite3.Connection) -> None:
    con.execute("""
    CREATE TABLE IF NOT EXISTS lottery_state (
        id INTEGER PRIMARY KEY CHECK (id=1),
        active_round_id INTEGER,
        enabled INTEGER NOT NULL DEFAULT 1,
        updated_at INTEGER
    );
    """)
    con.execute("""
    INSERT OR IGNORE INTO lottery_state(id, active_round_id, updated_at) VALUES(1, NULL, strftime('%s','now'));
    """)
    # Migration: add enabled column if missing
    cols = [r[1] for r in con.execute("PRAGMA table_info(lottery_state)").fetchall()]
    if "enabled" not in cols:
        con.execute("ALTER TABLE lottery_state ADD COLUMN enabled INTEGER NOT NULL DEFAULT 1;")
    # Ensure a row exists and enabled has a sane value
    con.execute("UPDATE lottery_state SET enabled=COALESCE(enabled,1) WHERE id=1;")

    con.execute("""
    CREATE TABLE IF NOT EXISTS lottery_rounds (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        starts_at INTEGER NOT NULL,
        ends_at INTEGER NOT NULL,
        status TEXT NOT NULL,                 -- active | closed | paid
        ticket_price_hcc INTEGER NOT NULL,
        house_fee_bps INTEGER NOT NULL,
        split1_bps INTEGER NOT NULL,
        split2_bps INTEGER NOT NULL,
        split3_bps INTEGER NOT NULL,
        ticket_cap INTEGER NOT NULL,
        seed_hcc INTEGER NOT NULL,            -- amount seeded from house into pot at round start (real)
        commit_hash TEXT NOT NULL,            -- sha256(seed_secret)
        reveal_secret TEXT,                   -- secret revealed after draw
        pot_from_tickets_hcc INTEGER NOT NULL DEFAULT 0,
        house_collected_hcc INTEGER NOT NULL DEFAULT 0,
        payout_total_hcc INTEGER NOT NULL DEFAULT 0,
        created_at INTEGER NOT NULL
    );
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS lottery_tickets (
        round_id INTEGER NOT NULL,
        discord_id INTEGER NOT NULL,
        tickets INTEGER NOT NULL,
        PRIMARY KEY(round_id, discord_id)
    );
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS lottery_payouts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        round_id INTEGER NOT NULL,
        discord_id INTEGER NOT NULL,
        place INTEGER NOT NULL,               -- 1,2,3
        amount_hcc INTEGER NOT NULL,
        ts INTEGER NOT NULL
    );
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS lottery_rate_limit (
        discord_id INTEGER PRIMARY KEY,
        window_start INTEGER NOT NULL,
        tickets_used INTEGER NOT NULL
    );
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_lottery_tickets_round ON lottery_tickets(round_id);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_lottery_payouts_round ON lottery_payouts(round_id);")


def ensure_user_row(con: sqlite3.Connection, discord_id: int, bal_col: str) -> None:
    # Insert row if missing (balance defaults to 0)
    con.execute(
        f"INSERT OR IGNORE INTO {USERS_TABLE}({USERS_ID_COL}, {bal_col}) VALUES(?, ?)",
        (discord_id, 0)
    )


def get_balance(con: sqlite3.Connection, discord_id: int, bal_col: str) -> int:
    row = con.execute(
        f"SELECT {bal_col} AS bal FROM {USERS_TABLE} WHERE {USERS_ID_COL}=?",
        (discord_id,)
    ).fetchone()
    if not row:
        return 0
    try:
        return int(row["bal"] or 0)
    except Exception:
        return 0


def add_balance(con: sqlite3.Connection, discord_id: int, delta: int, bal_col: str) -> None:
    ensure_user_row(con, discord_id, bal_col)
    con.execute(
        f"UPDATE {USERS_TABLE} SET {bal_col} = COALESCE({bal_col},0) + ? WHERE {USERS_ID_COL}=?",
        (int(delta), discord_id)
    )


def sub_balance_checked(con: sqlite3.Connection, discord_id: int, amount: int, bal_col: str) -> None:
    # atomic check+deduct
    ensure_user_row(con, discord_id, bal_col)
    row = con.execute(
        f"SELECT COALESCE({bal_col},0) AS bal FROM {USERS_TABLE} WHERE {USERS_ID_COL}=?",
        (discord_id,)
    ).fetchone()
    bal = int(row["bal"] or 0) if row else 0
    if bal < amount:
        raise ValueError("insufficient balance")
    con.execute(
        f"UPDATE {USERS_TABLE} SET {bal_col} = COALESCE({bal_col},0) - ? WHERE {USERS_ID_COL}=?",
        (int(amount), discord_id)
    )


def check_and_consume_rate_limit(con: sqlite3.Connection, discord_id: int, add_tickets: int) -> None:
    now = now_unix()
    row = con.execute(
        "SELECT window_start, tickets_used FROM lottery_rate_limit WHERE discord_id=?",
        (discord_id,)
    ).fetchone()

    if not row:
        con.execute(
            "INSERT INTO lottery_rate_limit(discord_id, window_start, tickets_used) VALUES(?,?,?)",
            (discord_id, now, int(add_tickets))
        )
        if add_tickets > MAX_TICKETS_PER_MIN:
            raise ValueError("rate limit")
        return

    window_start = int(row["window_start"] or 0)
    used = int(row["tickets_used"] or 0)

    if now - window_start >= 60:
        window_start = now
        used = 0

    if used + add_tickets > MAX_TICKETS_PER_MIN:
        # still update window_start reset if it rolled, otherwise keep as-is
        con.execute(
            "UPDATE lottery_rate_limit SET window_start=?, tickets_used=? WHERE discord_id=?",
            (window_start, used, discord_id)
        )
        raise ValueError("rate limit")

    con.execute(
        "UPDATE lottery_rate_limit SET window_start=?, tickets_used=? WHERE discord_id=?",
        (window_start, used + int(add_tickets), discord_id)
    )

# ----------------------------
# Lottery logic
# ----------------------------


@dataclass
class RoundInfo:
    round_id: int
    starts_at: int
    ends_at: int
    status: str
    seed_hcc: int
    pot_tickets_hcc: int
    house_fee_bps: int
    split1_bps: int
    split2_bps: int
    split3_bps: int
    ticket_cap: int
    commit_hash: str


def get_active_round(con: sqlite3.Connection) -> Optional[RoundInfo]:
    st = con.execute("SELECT active_round_id FROM lottery_state WHERE id=1").fetchone()
    rid = int(st["active_round_id"] or 0) if st else 0
    if rid <= 0:
        return None
    row = con.execute(
        "SELECT * FROM lottery_rounds WHERE id=?",
        (rid,)
    ).fetchone()
    if not row:
        return None
    return RoundInfo(
        round_id=int(row["id"]),
        starts_at=int(row["starts_at"]),
        ends_at=int(row["ends_at"]),
        status=str(row["status"]),
        seed_hcc=int(row["seed_hcc"] or 0),
        pot_tickets_hcc=int(row["pot_from_tickets_hcc"] or 0),
        house_fee_bps=int(row["house_fee_bps"] or 0),
        split1_bps=int(row["split1_bps"] or 0),
        split2_bps=int(row["split2_bps"] or 0),
        split3_bps=int(row["split3_bps"] or 0),
        ticket_cap=int(row["ticket_cap"] or 0),
        commit_hash=str(row["commit_hash"] or "")
    )

# --- New helpers for enabled flag ---
def lottery_is_enabled(con: sqlite3.Connection) -> bool:
    row = con.execute("SELECT COALESCE(enabled,1) AS en FROM lottery_state WHERE id=1").fetchone()
    try:
        return int(row["en"] or 1) == 1 if row else True
    except Exception:
        return True


def set_lottery_enabled(con: sqlite3.Connection, enabled: bool) -> None:
    con.execute(
        "UPDATE lottery_state SET enabled=?, updated_at=? WHERE id=1",
        (1 if enabled else 0, now_unix())
    )


def compute_pot_total(r: RoundInfo) -> int:
    return int(r.seed_hcc + r.pot_tickets_hcc)


def round_total_tickets(con: sqlite3.Connection, round_id: int) -> int:
    row = con.execute("SELECT COALESCE(SUM(tickets),0) AS n FROM lottery_tickets WHERE round_id=?", (round_id,)).fetchone()
    return int(row["n"] or 0) if row else 0


def get_user_tickets(con: sqlite3.Connection, round_id: int, discord_id: int) -> int:
    row = con.execute(
        "SELECT tickets FROM lottery_tickets WHERE round_id=? AND discord_id=?",
        (round_id, discord_id)
    ).fetchone()
    return int(row["tickets"] or 0) if row else 0


def start_new_round(con: sqlite3.Connection, seed_hcc: int, *, duration_min: int, house_fee_bps: int,
                    split1: int, split2: int, split3: int, ticket_cap: int) -> int:
    ts = now_unix()
    ends = ts + int(duration_min) * 60

    # commit-reveal secret
    secret_bytes = secrets.token_bytes(32)
    secret_hex = secret_bytes.hex()
    commit_hash = hashlib.sha256(secret_hex.encode("utf-8")).hexdigest()

    cur = con.execute("""
        INSERT INTO lottery_rounds(
            starts_at, ends_at, status, ticket_price_hcc,
            house_fee_bps, split1_bps, split2_bps, split3_bps,
            ticket_cap, seed_hcc, commit_hash, reveal_secret,
            pot_from_tickets_hcc, house_collected_hcc, payout_total_hcc, created_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        ts, ends, "active", TICKET_PRICE_HCC,
        int(house_fee_bps), int(split1), int(split2), int(split3),
        int(ticket_cap), int(seed_hcc), commit_hash, None, 0, 0, 0, ts
    ))
    rid = int(cur.lastrowid)
    con.execute("UPDATE lottery_state SET active_round_id=?, updated_at=? WHERE id=1", (rid, ts))

    # Store secret securely? MVP stores it only in memory is risky across restarts.
    # So we store it *encrypted*? For MVP we store plaintext in DB but only reveal after draw.
    # Here: store plaintext in reveal_secret column already, but it will only be shown after draw.
    con.execute("UPDATE lottery_rounds SET reveal_secret=? WHERE id=?", (secret_hex, rid))

    return rid


def close_round(con: sqlite3.Connection, round_id: int) -> None:
    con.execute("UPDATE lottery_rounds SET status='closed' WHERE id=? AND status='active'", (round_id,))
    con.execute("UPDATE lottery_state SET active_round_id=NULL, updated_at=? WHERE id=1", (now_unix(),))


def weighted_draw_without_replacement(entries: List[Tuple[int, int]], k: int) -> List[int]:
    """
    entries: [(discord_id, tickets), ...]
    returns up to k winners (distinct discord_ids), weighted by tickets.
    """
    rng = secrets.SystemRandom()
    pool = [(uid, w) for (uid, w) in entries if w > 0]
    winners: List[int] = []
    for _ in range(k):
        if not pool:
            break
        total = sum(w for _, w in pool)
        pick = rng.uniform(0, total)
        acc = 0.0
        idx = 0
        for i, (uid, w) in enumerate(pool):
            acc += w
            if pick <= acc:
                idx = i
                break
        winners.append(pool[idx][0])
        pool.pop(idx)
    return winners


def settle_round(con: sqlite3.Connection, bal_col: str, round_id: int, *, dry_run: bool = False) -> Dict[str, any]:
    """
    - Compute pot_total = seed + tickets
    - house_fee = floor(pot_total * house_fee)
    - payout_pool = pot_total - house_fee
    - winners: 3 weighted draws
    - payouts: split bps
    - credit house_fee to HOUSE_DISCORD_ID_INT (if set)
    - credit payouts to winners
    Writes payouts into lottery_payouts and marks round paid.
    """
    row = con.execute("SELECT * FROM lottery_rounds WHERE id=?", (round_id,)).fetchone()
    if not row:
        raise RuntimeError("round not found")

    status = str(row["status"] or "")
    if status not in ("active", "closed"):
        # if already paid, nothing to do
        return {"ok": False, "error": f"round status={status}"}

    # gather participants
    trows = con.execute(
        "SELECT discord_id, tickets FROM lottery_tickets WHERE round_id=?",
        (round_id,)
    ).fetchall()
    entries = [(int(r["discord_id"]), int(r["tickets"] or 0)) for r in trows]
    total_tickets = sum(w for _, w in entries)

    seed_hcc = int(row["seed_hcc"] or 0)
    pot_tickets = int(row["pot_from_tickets_hcc"] or 0)
    pot_total = seed_hcc + pot_tickets

    fee_bps = int(row["house_fee_bps"] or 0)
    # Fee only applies to the ticket pot, NOT the seed
    house_fee = int(math.floor(pot_tickets * bps_to_frac(fee_bps)))

    # Option B:
    # - Regular payouts come from a small fraction of the TOTAL pot.
    # - The remainder rolls over to the next round.
    # - House fee is still taken only from ticket pot (NOT seed).
    # Effective available pot after house fee:
    net_pot = max(0, pot_total - house_fee)

    regular_pool = int(math.floor(pot_total * bps_to_frac(REGULAR_POOL_BPS)))
    regular_pool = max(0, min(regular_pool, net_pot))

    # Optional jackpot: additional payout to 1st-place winner with some probability.
    jackpot_hit = False
    jackpot_amount = 0

    s1 = int(row["split1_bps"] or 0)
    s2 = int(row["split2_bps"] or 0)
    s3 = int(row["split3_bps"] or 0)

    # normalize splits if they exceed 10000
    split_sum = s1 + s2 + s3
    if split_sum <= 0:
        # default safe
        s1, s2, s3 = 6000, 2500, 1000
        split_sum = s1 + s2 + s3
    if split_sum > 10000:
        # scale down proportionally
        s1 = int(round(s1 * 10000 / split_sum))
        s2 = int(round(s2 * 10000 / split_sum))
        s3 = max(0, 10000 - s1 - s2)

    # determine winners
    winners = weighted_draw_without_replacement(entries, 3) if total_tickets > 0 else []

    payouts: List[Tuple[int, int, int]] = []  # (place, discord_id, amount)
    amounts = [0, 0, 0]

    if winners and regular_pool > 0:
        # compute amounts with floor; remainder goes to 1st to avoid dust loss
        a1 = int(math.floor(regular_pool * bps_to_frac(s1)))
        a2 = int(math.floor(regular_pool * bps_to_frac(s2)))
        a3 = int(math.floor(regular_pool * bps_to_frac(s3)))
        used = a1 + a2 + a3
        if used < regular_pool:
            a1 += (regular_pool - used)
        amounts = [a1, a2, a3]

        # only pay for actually existing winners
        for i, uid in enumerate(winners[:3]):
            amt = int(amounts[i]) if i < len(amounts) else 0
            payouts.append((i + 1, int(uid), max(0, amt)))

    paid_sum = sum(a for _, _, a in payouts)

    # Jackpot roll: if we have a 1st-place winner, potentially pay extra from the pot.
    # Jackpot is taken from the net pot (after house fee) but does NOT reduce regular_pool payouts.
    # It reduces carryover.
    if JACKPOT_CHANCE_BPS > 0 and JACKPOT_PCT_BPS > 0 and winners:
        # winners[0] is 1st place by construction of weighted draw
        roll = secrets.randbelow(10000)
        if roll < JACKPOT_CHANCE_BPS:
            jackpot_hit = True
            jackpot_amount = int(math.floor(pot_total * bps_to_frac(JACKPOT_PCT_BPS)))
            if JACKPOT_CAP_HCC > 0:
                jackpot_amount = min(jackpot_amount, JACKPOT_CAP_HCC)
            jackpot_amount = max(0, min(jackpot_amount, net_pot))

    # Carryover is everything left after house fee, regular payouts, and (optional) jackpot.
    carryover_hcc = max(0, net_pot - paid_sum - int(jackpot_amount or 0))

    ts = now_unix()

    # DRY RUN: return the computed result without changing any DB state/balances.
    # IMPORTANT: do not reveal the secret in preview mode.
    if dry_run:
        commit_hash = str(row["commit_hash"] or "")
        return {
            "ok": True,
            "dry_run": True,
            "round_id": round_id,
            "total_tickets": total_tickets,
            "pot_total_hcc": pot_total,
            "house_fee_hcc": house_fee,
            "net_pot_hcc": net_pot,
            "regular_pool_hcc": regular_pool,
            "carryover_hcc": carryover_hcc,
            "jackpot_chance_bps": JACKPOT_CHANCE_BPS,
            "jackpot_pct_bps": JACKPOT_PCT_BPS,
            "jackpot_cap_hcc": JACKPOT_CAP_HCC,
            "jackpot_hit": False,
            "jackpot_amount_hcc": 0,
            "winners": payouts,  # (place, discord_id, amount)
            "commit_hash": commit_hash,
            "reveal_secret": "",  # intentionally hidden
            "ends_at": int(row["ends_at"] or 0),
            "ts": ts,
        }

    # Apply in one transaction
    con.execute("BEGIN IMMEDIATE;")
    try:
        # close if still active
        con.execute("UPDATE lottery_rounds SET status='closed' WHERE id=? AND status='active'", (round_id,))

        # credit house fee
        if house_fee > 0 and HOUSE_DISCORD_ID_INT:
            add_balance(con, HOUSE_DISCORD_ID_INT, house_fee, bal_col)

            log_tx(
                con,
                tx_type="lottery_fee",
                from_id=None,
                to_id=int(HOUSE_DISCORD_ID_INT),
                amount=int(house_fee),
                note=f"lottery house fee: round={round_id} (fee on ticket pot)",
                status="ok",
            )

        # credit winners
        for place, uid, amt in payouts:
            if amt <= 0:
                continue
            add_balance(con, uid, amt, bal_col)

            log_tx(
                con,
                tx_type="lottery_payout",
                from_id=None,
                to_id=int(uid),
                amount=int(amt),
                note=f"lottery payout: round={round_id} place={place}",
                status="ok",
            )

            con.execute(
                "INSERT INTO lottery_payouts(round_id, discord_id, place, amount_hcc, ts) VALUES(?,?,?,?,?)",
                (round_id, uid, place, amt, ts)
            )

        # credit jackpot (extra payout to 1st-place winner)
        if jackpot_hit and jackpot_amount > 0 and winners:
            juid = int(winners[0])
            add_balance(con, juid, int(jackpot_amount), bal_col)
            log_tx(
                con,
                tx_type="lottery_jackpot",
                from_id=None,
                to_id=int(juid),
                amount=int(jackpot_amount),
                note=f"lottery jackpot: round={round_id} (extra payout)",
                status="ok",
            )
            con.execute(
                "INSERT INTO lottery_payouts(round_id, discord_id, place, amount_hcc, ts) VALUES(?,?,?,?,?)",
                (round_id, juid, 99, int(jackpot_amount), ts)
            )

        # persist accounting
        con.execute(
            "UPDATE lottery_rounds SET house_collected_hcc=?, payout_total_hcc=?, status='paid' WHERE id=?",
            (house_fee, sum(a for _, _, a in payouts) + int(jackpot_amount or 0), round_id)
        )

        # detach active round pointer
        st = con.execute("SELECT active_round_id FROM lottery_state WHERE id=1").fetchone()
        if st and int(st["active_round_id"] or 0) == round_id:
            con.execute("UPDATE lottery_state SET active_round_id=NULL, updated_at=? WHERE id=1", (ts,))

        con.execute("COMMIT;")
    except Exception:
        con.execute("ROLLBACK;")
        raise

    # return reveal + winners
    reveal_secret = str(row["reveal_secret"] or "")
    commit_hash = str(row["commit_hash"] or "")
    return {
        "ok": True,
        "round_id": round_id,
        "total_tickets": total_tickets,
        "pot_total_hcc": pot_total,
        "house_fee_hcc": house_fee,
        "net_pot_hcc": net_pot,
        "regular_pool_hcc": regular_pool,
        "carryover_hcc": carryover_hcc,
        "jackpot_hit": bool(jackpot_hit),
        "jackpot_amount_hcc": int(jackpot_amount or 0),
        "winners": payouts,  # (place, discord_id, amount)
        "commit_hash": commit_hash,
        "reveal_secret": reveal_secret,
        "ends_at": int(row["ends_at"] or 0),
    }


def format_pct(x: float) -> str:
    return f"{max(0.0, min(1.0, x))*100.0:.2f}%"


def compute_odds_top3(entries: List[Tuple[int,int]], my_id: int) -> Dict[str, float]:
    # entries: [(discord_id, tickets)]
    w = 0
    W = 0
    others: List[int] = []
    for uid, t in entries:
        t = int(t or 0)
        if t <= 0:
            continue
        W += t
        if uid == my_id:
            w = t
        else:
            others.append(t)

    if w <= 0 or W <= 0:
        return {"p1": 0.0, "p2": 0.0, "p3": 0.0, "pany": 0.0}

    # if too many participants, Monte Carlo
    if len(others) > 500:
        iters = 3000
        win1 = win2 = win3 = 0
        pool = [(uid, int(t)) for (uid, t) in entries if int(t or 0) > 0]
        for _ in range(iters):
            winners = weighted_draw_without_replacement(pool, 3)
            if not winners:
                continue
            if len(winners) > 0 and winners[0] == my_id: win1 += 1
            if len(winners) > 1 and winners[1] == my_id: win2 += 1
            if len(winners) > 2 and winners[2] == my_id: win3 += 1
        p1 = win1 / iters
        p2 = win2 / iters
        p3 = win3 / iters
        pany = min(1.0, p1 + p2 + p3)
        return {"p1": p1, "p2": p2, "p3": p3, "pany": pany}

    # analytic approx (O(n^2), ok for <=~500)
    p1 = w / W

    p2 = 0.0
    for wi in others:
        if W - wi > 0:
            p2 += (wi / W) * (w / (W - wi))

    p3 = 0.0
    n = len(others)
    for i in range(n):
        wi = others[i]
        denom1 = W - wi
        if denom1 <= 0:
            continue
        for j in range(n):
            if i == j:
                continue
            wj = others[j]
            denom2 = W - wi - wj
            if denom2 <= 0:
                continue
            p3 += (wi / W) * (wj / denom1) * (w / denom2)

    pany = min(1.0, p1 + p2 + p3)
    return {"p1": p1, "p2": p2, "p3": p3, "pany": pany}

# ----------------------------
# Discord bot
# ----------------------------


intents = discord.Intents.default()


class LotteryBot(discord.Client):
    def __init__(self):
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)
        self.bal_col: str = USERS_BAL_COL
        self._task: Optional[asyncio.Task] = None
        self._auto_enabled = asyncio.Event()
        self._auto_enabled.set()  # default ON; synced from DB in on_ready

    async def setup_hook(self):
        # sync commands
        await self.tree.sync()

    async def on_ready(self):
        # db init
        con = db()
        try:
            ensure_tables(con)
            if lottery_is_enabled(con):
                self._auto_enabled.set()
            else:
                self._auto_enabled.clear()
        finally:
            con.close()

        print(f"🎟️ LotteryBot connected as {self.user} | DB={DB_PATH} | bal_col={self.bal_col}")

        # auto task
        if self._task is None:
            self._task = asyncio.create_task(self._auto_loop())

    async def _auto_loop(self):
        await self.wait_until_ready()
        while not self.is_closed():
            try:
                await self._tick()
            except Exception as e:
                print(f"[auto_loop] error: {e}")
            await asyncio.sleep(CHECK_INTERVAL)

    async def _tick(self):
        # Hard gate: if disabled via command, do nothing immediately.
        if not self._auto_enabled.is_set():
            return
        con = db()
        try:
            ensure_tables(con)

            # Only run auto-lottery if enabled (DB is source of truth; also sync in-memory flag)
            if not lottery_is_enabled(con):
                self._auto_enabled.clear()
                return

            r = get_active_round(con)
            if not r:
                # auto-start a new round if none active
                # seed=0 by default; you can add via admin /lottery_seed
                rid = start_new_round(
                    con, seed_hcc=0,
                    duration_min=DURATION_MIN,
                    house_fee_bps=HOUSE_FEE_BPS,
                    split1=SPLIT_1_BPS, split2=SPLIT_2_BPS, split3=SPLIT_3_BPS,
                    ticket_cap=TICKET_CAP
                )
                await self._announce_round_start(rid)
                print(f"[auto] started new round id={rid}")
                return

            if r.status != "active":
                return

            # check end conditions
            tcount = round_total_tickets(con, r.round_id)
            time_up = now_unix() >= r.ends_at
            cap_hit = (r.ticket_cap > 0 and tcount >= r.ticket_cap)

            if time_up or cap_hit:
                # Re-check right before settling to avoid races with /stop_lottery
                if not self._auto_enabled.is_set() or not lottery_is_enabled(con):
                    self._auto_enabled.clear()
                    return
                # settle
                close_round(con, r.round_id)
                result = settle_round(con, self.bal_col, r.round_id)

                carry = int(result.get("carryover_hcc") or 0)

                # If auto was disabled during settle, do not start a new round automatically.
                if not self._auto_enabled.is_set() or not lottery_is_enabled(con):
                    self._auto_enabled.clear()
                    return

                # start next round immediately (carryover becomes seed)
                next_id = start_new_round(
                    con, seed_hcc=carry,
                    duration_min=DURATION_MIN,
                    house_fee_bps=HOUSE_FEE_BPS,
                    split1=SPLIT_1_BPS, split2=SPLIT_2_BPS, split3=SPLIT_3_BPS,
                    ticket_cap=TICKET_CAP
                )

                # announce if possible
                await self._announce_draw(result, next_id)
                await self._announce_round_start(next_id)

        finally:
            con.close()

    async def _announce_draw(self, result: Dict[str, any], next_round_id: int):
        if not result.get("ok"):
            return

        # choose channel: explicit announce channel, else first allowed channel we can access
        channel = None
        if ANNOUNCE_CHANNEL_ID_INT:
            channel = self.get_channel(ANNOUNCE_CHANNEL_ID_INT)
        if channel is None and ALLOWED_CHANNEL_IDS:
            for cid in ALLOWED_CHANNEL_IDS:
                c = self.get_channel(cid)
                if c is not None:
                    channel = c
                    break
        if channel is None:
            return

        rid = result["round_id"]
        pot = result["pot_total_hcc"]
        fee = result["house_fee_hcc"]
        net_pot = int(result.get("net_pot_hcc") or 0)
        regular_pool = int(result.get("regular_pool_hcc") or 0)
        jackpot_hit = bool(result.get("jackpot_hit") or False)
        jackpot_amount = int(result.get("jackpot_amount_hcc") or 0)
        carry = int(result.get("carryover_hcc") or 0)
        total_tickets = result["total_tickets"]
        commit_hash = result["commit_hash"]
        reveal_secret = result["reveal_secret"]

        winners = result["winners"]  # list of (place, discord_id, amount)

        # animation
        msg = await channel.send(
            f"🎲 **HCC Lottery Draw** (Round #{rid})\n"
            f"Tickets sold: **{total_tickets}** | Pot: **{pot} HCC**\n"
            f"Drawing winners…"
        )
        await asyncio.sleep(1.2)

        def mention(uid: int) -> str:
            return f"<@{uid}>"

        # reveal commitment at the end
        # show winners from 3rd -> 1st for drama
        place_map = {p: (uid, amt) for (p, uid, amt) in winners}

        # 3rd
        if 3 in place_map:
            uid, amt = place_map[3]
            await msg.edit(content=(
                f"🎲 **HCC Lottery Draw** (Round #{rid})\n"
                f"Tickets sold: **{total_tickets}** | Pot: **{pot} HCC**\n\n"
                f"🥉 **3rd place**: {mention(uid)} — **{amt} HCC**\n"
                f"(…more…)"))
            await asyncio.sleep(1.2)

        # 2nd
        if 2 in place_map:
            uid, amt = place_map[2]
            prefix = (
                f"🎲 **HCC Lottery Draw** (Round #{rid})\n"
                f"Tickets sold: **{total_tickets}** | Pot: **{pot} HCC**\n\n"
            )
            third_line = ""
            if 3 in place_map:
                third_line = f"🥉 3rd place: {mention(place_map[3][0])} — {place_map[3][1]} HCC\n"
            await msg.edit(content=(
                prefix
                + third_line
                + f"🥈 **2nd place**: {mention(uid)} — **{amt} HCC**\n"
                + f"(…more…)"
            ))
            await asyncio.sleep(1.2)

        # 1st
        if 1 in place_map:
            uid, amt = place_map[1]
            lines = []
            if 3 in place_map: lines.append(f"🥉 3rd place: {mention(place_map[3][0])} — {place_map[3][1]} HCC")
            if 2 in place_map: lines.append(f"🥈 2nd place: {mention(place_map[2][0])} — {place_map[2][1]} HCC")
            lines.append(f"🥇 **1st place**: {mention(uid)} — **{amt} HCC**")
            winners_block = "\n".join(lines)

            await msg.edit(content=(
                f"🎲 **HCC Lottery Draw** (Round #{rid})\n"
                f"Tickets sold: **{total_tickets}** | Pot: **{pot} HCC**\n\n"
                f"{winners_block}\n\n"
                f"House fee: **{fee} HCC** → treasury\n"
                f"Net pot (after fee): **{net_pot} HCC**\n"
                f"Regular payouts this round: **{regular_pool} HCC**\n"
                + (f"💥 Jackpot hit! **+{jackpot_amount} HCC** to 1st place\n" if jackpot_hit and jackpot_amount > 0 else f"Jackpot: {JACKPOT_CHANCE_BPS/100:.0f}% chance of {JACKPOT_PCT_BPS/100:.0f}% pot\n")
                + f"Carryover to next round: **{carry} HCC**\n"
                f"Commit: `{commit_hash[:12]}…`  Reveal: `{reveal_secret[:12]}…`\n"
                f"➡️ New round started: **#{next_round_id}** (ends <t:{now_unix() + DURATION_MIN*60}:R>)"
            ))

    async def _announce_round_start(self, round_id: int):
        # pick channel like _announce_draw
        channel = None
        if ANNOUNCE_CHANNEL_ID_INT:
            channel = self.get_channel(ANNOUNCE_CHANNEL_ID_INT)
        if channel is None and ALLOWED_CHANNEL_IDS:
            for cid in ALLOWED_CHANNEL_IDS:
                c = self.get_channel(cid)
                if c is not None:
                    channel = c
                    break
        if channel is None:
            return

        con = db()
        try:
            row = con.execute("SELECT * FROM lottery_rounds WHERE id=?", (round_id,)).fetchone()
            if not row:
                return
            ends_at = int(row["ends_at"] or 0)
            seed_hcc = int(row["seed_hcc"] or 0)
            pot_t = int(row["pot_from_tickets_hcc"] or 0)
            pot = seed_hcc + pot_t
            # fee_bps = int(row["house_fee_bps"] or 0)
            # s1 = int(row["split1_bps"] or 0)
            # s2 = int(row["split2_bps"] or 0)
            # s3 = int(row["split3_bps"] or 0)
            commit_hash = str(row["commit_hash"] or "")

            await channel.send(
                f"🎟️ **HCC Lottery started** (Round #{round_id})\n"
                f"Pot: **{pot} HCC** | Ends <t:{ends_at}:R>\n"
                # f"House fee: **{fee_bps / 100:.2f}%** | Split: {s1 / 100:.0f}/{s2 / 100:.0f}/{s3 / 100:.0f}\n"
                f"Commit: `{commit_hash[:16]}…`"
            )
        finally:
            con.close()


# ----------------------------
# Bot instance
# ----------------------------


bot = LotteryBot()


def channel_allowed(interaction: discord.Interaction) -> bool:
    if not ALLOWED_CHANNEL_IDS:
        return True
    try:
        return int(interaction.channel_id) in ALLOWED_CHANNEL_IDS
    except Exception:
        return False


def is_admin(uid: int) -> bool:
    return (uid in ADMIN_IDS) or (HOUSE_DISCORD_ID_INT is not None and uid == HOUSE_DISCORD_ID_INT)

# Always acknowledge interactions to avoid Discord timeout
async def ensure_ack(interaction: discord.Interaction, *, ephemeral: bool = True) -> None:
    """Acknowledge the interaction ASAP to avoid the 3s Discord timeout."""
    try:
        if interaction.response.is_done():
            return
        await interaction.response.defer(ephemeral=ephemeral, thinking=True)
    except Exception:
        # Ignore "already responded" and any rare edge cases
        return

@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    try:
        await ensure_ack(interaction, ephemeral=True)
        await interaction.followup.send(f"❌ Error: {error}", ephemeral=True)
    except Exception:
        pass
    print(f"[command_error] {repr(error)}")

# ----------------------------
# Commands
# ----------------------------


@bot.tree.command(name="lottery", description="Show current lottery status (pot, time left, your tickets).")
async def lottery_cmd(interaction: discord.Interaction):
    await ensure_ack(interaction, ephemeral=True)
    if not channel_allowed(interaction):
        await interaction.followup.send("❌ This command is not allowed in this channel.", ephemeral=True)
        return

    con = db()
    try:
        ensure_tables(con)
        bal_col = bot.bal_col
        r = get_active_round(con)
        if not r:
            await interaction.followup.send("ℹ️ No active round yet. Try again in a moment.", ephemeral=True)
            return

        pot = compute_pot_total(r)
        total_tickets = round_total_tickets(con, r.round_id)
        my_tickets = get_user_tickets(con, r.round_id, interaction.user.id)
        seconds_left = max(0, r.ends_at - now_unix())

        # rough odds for 1 ticket (very rough)
        odds = (my_tickets / total_tickets) if total_tickets > 0 else 0.0
        odds_txt = f"{odds*100:.2f}%" if total_tickets > 0 else "—"

        msg = (
            f"🎟️ **HCC Lottery — Round #{r.round_id}**\n"
            f"Pot: **{pot} HCC** (Seed {r.seed_hcc}, +Tickets {r.pot_tickets_hcc})\n"
            f"Tickets sold: **{total_tickets}** | Your tickets: **{my_tickets}** (share ~{odds_txt})\n"
            f"Ends: <t:{r.ends_at}:R>\n"
            f"House fee: **{r.house_fee_bps/100:.2f}%** | Split: {r.split1_bps/100:.0f}/{r.split2_bps/100:.0f}/{r.split3_bps/100:.0f}\n"
            f"Commit: `{r.commit_hash[:16]}…`"
        )
        await interaction.followup.send(msg, ephemeral=True)
    finally:
        con.close()


@bot.tree.command(name="buytickets", description="Buy lottery tickets (1 HCC per ticket).")
@app_commands.describe(count="Number of tickets to buy (1 HCC each)")
async def buytickets_cmd(interaction: discord.Interaction, count: int):
    await ensure_ack(interaction, ephemeral=True)
    if not channel_allowed(interaction):
        await interaction.followup.send("❌ This command is not allowed in this channel.", ephemeral=True)
        return

    count = int(count)
    if count <= 0:
        await interaction.followup.send("❌ Count must be positive.", ephemeral=True)
        return
    if count > MAX_TICKETS_PER_BUY:
        await interaction.followup.send(
            f"❌ Too many at once (max {MAX_TICKETS_PER_BUY}).",
            ephemeral=True
        )
        return

    con = db()
    try:
        ensure_tables(con)
        bal_col = bot.bal_col

        r = get_active_round(con)
        if not r or r.status != "active":
            await interaction.followup.send("ℹ️ No active round right now. Try again shortly.", ephemeral=True)
            return

        # cap check
        if r.ticket_cap > 0:
            total_tickets = round_total_tickets(con, r.round_id)
            if total_tickets + count > r.ticket_cap:
                await interaction.followup.send(
                    f"❌ Ticket cap would be exceeded. Remaining: {max(0, r.ticket_cap - total_tickets)}",
                    ephemeral=True
                )
                return

        cost = count * TICKET_PRICE_HCC

        # Atomic purchase: deduct user, increment ticket counts and pot_from_tickets
        con.execute("BEGIN IMMEDIATE;")
        try:
            # anti-spam / rate limit
            check_and_consume_rate_limit(con, interaction.user.id, count)

            sub_balance_checked(con, interaction.user.id, cost, bal_col)

            # upsert tickets
            con.execute("""
                INSERT INTO lottery_tickets(round_id, discord_id, tickets)
                VALUES(?,?,?)
                ON CONFLICT(round_id, discord_id) DO UPDATE SET tickets = tickets + excluded.tickets
            """, (r.round_id, interaction.user.id, count))

            con.execute(
                "UPDATE lottery_rounds SET pot_from_tickets_hcc = pot_from_tickets_hcc + ? WHERE id=?",
                (cost, r.round_id)
            )

            log_tx(
                con,
                tx_type="lottery_ticket",
                from_id=int(interaction.user.id),
                to_id=None,
                amount=int(cost),
                note=f"lottery tickets: round={r.round_id} count={count} price={TICKET_PRICE_HCC}",
                status="ok",
            )

            con.execute("COMMIT;")
        except Exception as e:
            con.execute("ROLLBACK;")
            if isinstance(e, ValueError) and str(e) == "rate limit":
                await interaction.followup.send(
                    f"❌ Rate limit: max **{MAX_TICKETS_PER_MIN}** tickets per minute. Try again in a bit.",
                    ephemeral=True
                )
                return
            if isinstance(e, ValueError) and "insufficient" in str(e):
                bal = get_balance(con, interaction.user.id, bal_col)
                await interaction.followup.send(
                    f"❌ Not enough HCC. Need {cost} HCC, you have {bal} HCC.",
                    ephemeral=True
                )
                return
            raise

        # Respond
        pot_total = compute_pot_total(get_active_round(con))
        my_tickets = get_user_tickets(con, r.round_id, interaction.user.id)
        await interaction.followup.send(
            f"✅ Bought **{count}** ticket(s) for **{cost} HCC**.\n"
            f"Your tickets this round: **{my_tickets}**\n"
            f"Current pot: **{pot_total} HCC**",
            ephemeral=True
        )
    finally:
        con.close()


@bot.tree.command(name="mytickets", description="Show your tickets in the current round.")
async def mytickets_cmd(interaction: discord.Interaction):
    await ensure_ack(interaction, ephemeral=True)
    if not channel_allowed(interaction):
        await interaction.followup.send("❌ This command is not allowed in this channel.", ephemeral=True)
        return

    con = db()
    try:
        ensure_tables(con)
        r = get_active_round(con)
        if not r:
            await interaction.followup.send("ℹ️ No active round.", ephemeral=True)
            return
        my_tickets = get_user_tickets(con, r.round_id, interaction.user.id)
        await interaction.followup.send(
            f"🎫 You have **{my_tickets}** ticket(s) in **Round #{r.round_id}** (ends <t:{r.ends_at}:R>).",
            ephemeral=True
        )
    finally:
        con.close()


@bot.tree.command(name="lastdraw", description="Show the last paid draw results.")
async def lastdraw_cmd(interaction: discord.Interaction):
    await ensure_ack(interaction, ephemeral=True)
    if not channel_allowed(interaction):
        await interaction.followup.send("❌ This command is not allowed in this channel.", ephemeral=True)
        return

    con = db()
    try:
        ensure_tables(con)
        row = con.execute(
            "SELECT id, ends_at, house_collected_hcc, payout_total_hcc, commit_hash, reveal_secret "
            "FROM lottery_rounds WHERE status='paid' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if not row:
            await interaction.followup.send("📭 No draw has been paid yet.", ephemeral=True)
            return

        rid = int(row["id"])
        ends_at = int(row["ends_at"] or 0)
        fee = int(row["house_collected_hcc"] or 0)
        payout_total = int(row["payout_total_hcc"] or 0)
        commit_hash = str(row["commit_hash"] or "")
        reveal_secret = str(row["reveal_secret"] or "")

        prow = con.execute(
            "SELECT discord_id, place, amount_hcc FROM lottery_payouts WHERE round_id=? ORDER BY place ASC",
            (rid,)
        ).fetchall()

        lines = []
        for r2 in prow:
            uid = int(r2["discord_id"])
            place = int(r2["place"])
            amt = int(r2["amount_hcc"] or 0)
            if place == 99:
                lines.append(f"💥 JACKPOT <@{uid}> — **{amt} HCC**")
            else:
                medal = "🥇" if place == 1 else ("🥈" if place == 2 else "🥉")
                lines.append(f"{medal} <@{uid}> — **{amt} HCC**")

        msg = (
            f"🏁 **Last draw — Round #{rid}** (ended <t:{ends_at}:R>)\n"
            + ("\n".join(lines) if lines else "No winners (no tickets sold).") + "\n\n"
            f"House fee: **{fee} HCC** | Total payouts: **{payout_total} HCC**\n"
            f"Commit: `{commit_hash[:16]}…` | Reveal: `{reveal_secret[:16]}…`"
        )
        await interaction.followup.send(msg, ephemeral=True)
    finally:
        con.close()


@bot.tree.command(name="lottery_odds", description="Show approximate odds for 1st/2nd/3rd place this round.")
async def lottery_odds_cmd(interaction: discord.Interaction):
    await ensure_ack(interaction, ephemeral=True)
    if not channel_allowed(interaction):
        await interaction.followup.send("❌ This command is not allowed in this channel.", ephemeral=True)
        return

    con = db()
    try:
        ensure_tables(con)
        r = get_active_round(con)
        if not r:
            await interaction.followup.send("ℹ️ No active round.", ephemeral=True)
            return

        trows = con.execute(
            "SELECT discord_id, tickets FROM lottery_tickets WHERE round_id=?",
            (r.round_id,)
        ).fetchall()
        entries = [(int(x["discord_id"]), int(x["tickets"] or 0)) for x in trows]
        total = sum(t for _, t in entries)
        mine = 0
        for uid, t in entries:
            if uid == interaction.user.id:
                mine = t
                break

        odds = compute_odds_top3(entries, interaction.user.id)

        pot_total = compute_pot_total(r)
        # House fee is taken only from ticket pot
        pot_tickets = int(r.pot_tickets_hcc or 0)
        house_fee_hcc = int(math.floor(pot_tickets * bps_to_frac(HOUSE_FEE_BPS)))
        net_pot = max(0, pot_total - house_fee_hcc)

        regular_pool = int(math.floor(pot_total * bps_to_frac(REGULAR_POOL_BPS)))
        regular_pool = max(0, min(regular_pool, net_pot))

        # Prize amounts this round (regular pool)
        s1, s2, s3 = SPLIT_1_BPS, SPLIT_2_BPS, SPLIT_3_BPS
        split_sum = s1 + s2 + s3
        if split_sum <= 0:
            s1, s2, s3 = 6000, 2500, 1000
            split_sum = s1 + s2 + s3
        if split_sum > 10000:
            s1 = int(round(s1 * 10000 / split_sum))
            s2 = int(round(s2 * 10000 / split_sum))
            s3 = max(0, 10000 - s1 - s2)

        p1_amt = int(math.floor(regular_pool * bps_to_frac(s1)))
        p2_amt = int(math.floor(regular_pool * bps_to_frac(s2)))
        p3_amt = int(math.floor(regular_pool * bps_to_frac(s3)))
        used = p1_amt + p2_amt + p3_amt
        if used < regular_pool:
            p1_amt += (regular_pool - used)

        jackpot_amt = int(math.floor(pot_total * bps_to_frac(JACKPOT_PCT_BPS)))
        if JACKPOT_CAP_HCC > 0:
            jackpot_amt = min(jackpot_amt, JACKPOT_CAP_HCC)
        jackpot_amt = max(0, min(jackpot_amt, net_pot))

        msg = (
            f"📈 **Lottery odds — Round #{r.round_id}**\n"
            f"Your tickets: **{mine}** | Total tickets: **{total}**\n"
            f"Current prizes (regular pool {REGULAR_POOL_BPS/100:.2f}% of pot): **{regular_pool} HCC**\n"
            f"🥇 ~**{p1_amt} HCC**  🥈 ~**{p2_amt} HCC**  🥉 ~**{p3_amt} HCC**\n"
            f"Jackpot: **{JACKPOT_CHANCE_BPS/100:.2f}%** chance of **{JACKPOT_PCT_BPS/100:.2f}%** pot (~{jackpot_amt} HCC)" + (f" cap {JACKPOT_CAP_HCC} HCC" if JACKPOT_CAP_HCC>0 else "") + "\n\n"
            f"🥇 1st: **{format_pct(odds['p1'])}**\n"
            f"🥈 2nd: **{format_pct(odds['p2'])}**\n"
            f"🥉 3rd: **{format_pct(odds['p3'])}**\n"
            f"🎯 Any prize: **{format_pct(odds['pany'])}**\n"
        )
        await interaction.followup.send(msg, ephemeral=True)
    finally:
        con.close()

# ----------------------------
# Admin commands
# ----------------------------


# Admin/testing: preview/dry-run draw
@bot.tree.command(name="lottery_preview_draw", description="(Admin) Dry-run: simulate the draw without changing balances/state.")
async def lottery_preview_draw_cmd(interaction: discord.Interaction):
    await ensure_ack(interaction, ephemeral=True)
    if not channel_allowed(interaction):
        await interaction.followup.send("❌ Not allowed in this channel.", ephemeral=True)
        return
    if not is_admin(interaction.user.id):
        await interaction.followup.send("❌ Admin only.", ephemeral=True)
        return
    if not LOTTERY_ENABLE_PREVIEW_DRAW:
        await interaction.followup.send("❌ Preview draw disabled by config.", ephemeral=True)
        return

    con = db()
    try:
        ensure_tables(con)
        r = get_active_round(con)
        if not r:
            await interaction.followup.send("ℹ️ No active round.", ephemeral=True)
            return

        # Simulate based on current ticket distribution/pot without mutating anything.
        result = settle_round(con, bot.bal_col, r.round_id, dry_run=True)
        if not result.get("ok"):
            await interaction.followup.send(f"❌ Preview failed: {result.get('error','unknown error')}", ephemeral=True)
            return

        winners = result.get("winners", [])
        if not winners:
            winners_txt = "No winners (no tickets sold)."
        else:
            lines = []
            for place, uid, amt in winners:
                medal = "🥇" if place == 1 else ("🥈" if place == 2 else "🥉")
                lines.append(f"{medal} <@{uid}> — **{amt} HCC**")
            winners_txt = "\n".join(lines)

        msg = (
            f"🧪 **DRY-RUN PREVIEW** — Round #{result['round_id']} (no state changes)\n"
            f"Tickets sold: **{result['total_tickets']}**\n"
            f"Pot total: **{result['pot_total_hcc']} HCC** (Seed {r.seed_hcc}, +Tickets {r.pot_tickets_hcc})\n"
            f"House fee (ticket pot only): **{result['house_fee_hcc']} HCC**\n"
            f"Net pot (after fee): **{result['net_pot_hcc']} HCC**\n"
            f"Regular payouts this round: **{result['regular_pool_hcc']} HCC**\n"
            f"Carryover: **{result['carryover_hcc']} HCC**\n\n"
            f"{winners_txt}\n\n"
            f"Commit: `{result['commit_hash'][:16]}…` | Reveal: *(hidden in preview)*\n"
            f"Ends: <t:{r.ends_at}:R>"
        )
        await interaction.followup.send(msg, ephemeral=True)
    finally:
        con.close()


# Admin commands: enable/disable/reset lottery
@bot.tree.command(name="start_lottery", description="(Admin) Enable auto-lottery and start a round if none is active.")
async def start_lottery_cmd(interaction: discord.Interaction):
    await ensure_ack(interaction, ephemeral=True)
    if not channel_allowed(interaction):
        await interaction.followup.send("❌ Not allowed in this channel.", ephemeral=True)
        return
    if not is_admin(interaction.user.id):
        await interaction.followup.send("❌ Admin only.", ephemeral=True)
        return

    con = db()
    try:
        ensure_tables(con)
        set_lottery_enabled(con, True)
        bot._auto_enabled.set()
        r = get_active_round(con)
        if not r:
            rid = start_new_round(
                con, seed_hcc=0,
                duration_min=DURATION_MIN,
                house_fee_bps=HOUSE_FEE_BPS,
                split1=SPLIT_1_BPS, split2=SPLIT_2_BPS, split3=SPLIT_3_BPS,
                ticket_cap=TICKET_CAP
            )
            await interaction.followup.send(f"✅ Auto-lottery enabled. Started Round #{rid}.", ephemeral=True)
            try:
                await bot._announce_round_start(rid)
            except Exception as e:
                print(f"[start_lottery] announce error: {e}")
            return

        # If a round is already active, announce it publicly as well so everyone sees the restart.
        try:
            await bot._announce_round_start(r.round_id)
        except Exception as e:
            print(f"[start_lottery] announce error: {e}")

        await interaction.followup.send(
            f"✅ Auto-lottery enabled. Current round is #{r.round_id}.",
            ephemeral=True
        )
    finally:
        con.close()


@bot.tree.command(name="stop_lottery", description="(Admin) Disable auto-lottery (no auto start/draw).")
async def stop_lottery_cmd(interaction: discord.Interaction):
    await ensure_ack(interaction, ephemeral=True)
    if not channel_allowed(interaction):
        await interaction.followup.send("❌ Not allowed in this channel.", ephemeral=True)
        return
    if not is_admin(interaction.user.id):
        await interaction.followup.send("❌ Admin only.", ephemeral=True)
        return

    con = db()
    try:
        ensure_tables(con)
        set_lottery_enabled(con, False)
        bot._auto_enabled.clear()
        await interaction.followup.send("🛑 Auto-lottery disabled. No auto draw and no new rounds will be started until you run /start_lottery.", ephemeral=True)
    finally:
        con.close()


@bot.tree.command(name="reset_lottery", description="(Admin) Reset the timer for the current round (keeps tickets/pot).")
async def reset_lottery_cmd(interaction: discord.Interaction):
    await ensure_ack(interaction, ephemeral=True)
    if not channel_allowed(interaction):
        await interaction.followup.send("❌ Not allowed in this channel.", ephemeral=True)
        return
    if not is_admin(interaction.user.id):
        await interaction.followup.send("❌ Admin only.", ephemeral=True)
        return

    con = db()
    try:
        ensure_tables(con)
        r = get_active_round(con)
        if not r or r.status != "active":
            await interaction.followup.send("ℹ️ No active round to reset.", ephemeral=True)
            return

        ts = now_unix()
        new_end = ts + int(DURATION_MIN) * 60
        con.execute(
            "UPDATE lottery_rounds SET starts_at=?, ends_at=? WHERE id=? AND status='active'",
            (ts, new_end, r.round_id)
        )
        await interaction.followup.send(
            f"🔄 Reset Round #{r.round_id} timer. New end: <t:{new_end}:R>.",
            ephemeral=True
        )
    finally:
        con.close()


@bot.tree.command(name="lottery_seed", description="(Admin) Seed the current round pot from house account.")
@app_commands.describe(amount="Seed amount in HCC to add to current pot (debited from house account)")
async def lottery_seed_cmd(interaction: discord.Interaction, amount: int):
    await ensure_ack(interaction, ephemeral=True)
    if not channel_allowed(interaction):
        await interaction.followup.send("❌ Not allowed in this channel.", ephemeral=True)
        return
    if not is_admin(interaction.user.id):
        await interaction.followup.send("❌ Admin only.", ephemeral=True)
        return
    if not HOUSE_DISCORD_ID_INT:
        await interaction.followup.send("❌ LOTTERY_HOUSE_DISCORD_ID not set.", ephemeral=True)
        return

    amount = int(amount)
    if amount <= 0:
        await interaction.followup.send("❌ Amount must be positive.", ephemeral=True)
        return

    con = db()
    try:
        ensure_tables(con)
        bal_col = bot.bal_col
        r = get_active_round(con)
        if not r:
            await interaction.followup.send("ℹ️ No active round.", ephemeral=True)
            return

        # debit house, add to round seed_hcc
        con.execute("BEGIN IMMEDIATE;")
        try:
            sub_balance_checked(con, HOUSE_DISCORD_ID_INT, amount, bal_col)
            con.execute("UPDATE lottery_rounds SET seed_hcc = seed_hcc + ? WHERE id=?", (amount, r.round_id))
            log_tx(
                con,
                tx_type="lottery_seed",
                from_id=int(HOUSE_DISCORD_ID_INT),
                to_id=None,
                amount=int(amount),
                note=f"lottery seed: round={r.round_id}",
                status="ok",
            )
            con.execute("COMMIT;")
        except Exception as e:
            con.execute("ROLLBACK;")
            if isinstance(e, ValueError) and "insufficient" in str(e):
                bal = get_balance(con, HOUSE_DISCORD_ID_INT, bal_col)
                await interaction.followup.send(f"❌ House has insufficient HCC. House balance: {bal}", ephemeral=True)
                return
            raise

        r2 = get_active_round(con)
        pot = compute_pot_total(r2) if r2 else amount
        await interaction.followup.send(
            f"✅ Seeded **{amount} HCC** into Round #{r.round_id}. New pot: **{pot} HCC**",
            ephemeral=True
        )
    finally:
        con.close()


@bot.tree.command(name="lottery_draw_now", description="(Admin) Force draw now (closes and pays current round).")
async def lottery_draw_now_cmd(interaction: discord.Interaction):
    await ensure_ack(interaction, ephemeral=True)
    if not channel_allowed(interaction):
        await interaction.followup.send("❌ Not allowed in this channel.", ephemeral=True)
        return
    if not is_admin(interaction.user.id):
        await interaction.followup.send("❌ Admin only.", ephemeral=True)
        return

    con = db()
    try:
        ensure_tables(con)
        bal_col = bot.bal_col
        r = get_active_round(con)
        if not r:
            await interaction.followup.send("ℹ️ No active round.", ephemeral=True)
            return

        close_round(con, r.round_id)
        result = settle_round(con, bal_col, r.round_id)
        carry = int(result.get("carryover_hcc") or 0)

        next_id = start_new_round(
            con, seed_hcc=carry,
            duration_min=DURATION_MIN,
            house_fee_bps=HOUSE_FEE_BPS,
            split1=SPLIT_1_BPS, split2=SPLIT_2_BPS, split3=SPLIT_3_BPS,
            ticket_cap=TICKET_CAP
        )

        await interaction.followup.send(f"✅ Forced draw for Round #{r.round_id}. New round: #{next_id}", ephemeral=True)
        await bot._announce_draw(result, next_id)
    finally:
        con.close()


@bot.tree.command(name="lottery_help", description="Show help for the HCC lottery bot.")
async def lottery_help_cmd(interaction: discord.Interaction):
    await ensure_ack(interaction, ephemeral=True)
    if not channel_allowed(interaction):
        await interaction.followup.send("❌ This command is not allowed in this channel.", ephemeral=True)
        return

    msg = (
        "🎟️ **HCC Lottery Help**\n\n"
        "**Player commands**\n"
        "• `/lottery` — status (pot, time left, your tickets)\n"
        "• `/buytickets count` — buy tickets (1 HCC each)\n"
        "• `/mytickets` — your tickets this round\n"
        "• `/lottery_odds` — approx odds for 1st/2nd/3rd\n"
        "• `/lastdraw` — last paid draw\n\n"
        f"Ticket price: **{TICKET_PRICE_HCC} HCC** | House fee: **{HOUSE_FEE_BPS/100:.2f}%** (tickets only)\n"
        f"Regular payouts: **{REGULAR_POOL_BPS/100:.2f}%** of total pot per round | Jackpot: **{JACKPOT_CHANCE_BPS/100:.0f}%** chance of **{JACKPOT_PCT_BPS/100:.0f}%** pot" + (f" (cap {JACKPOT_CAP_HCC} HCC)" if JACKPOT_CAP_HCC>0 else "") + "\n"
    )
    await interaction.followup.send(msg, ephemeral=True)

# ----------------------------
# Main
# ----------------------------

if not DISCORD_TOKEN:
    raise SystemExit("DISCORD_TOKEN missing in .env")

bot.run(DISCORD_TOKEN)