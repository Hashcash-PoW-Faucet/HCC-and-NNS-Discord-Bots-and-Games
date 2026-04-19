import os
import json
import time
import sqlite3
from typing import Optional, Dict, Any, List

from decimal import Decimal, ROUND_DOWN
from dotenv import load_dotenv

import discord
from discord import app_commands
from discord import ui
import aiohttp
import asyncio
import random

load_dotenv()

# ---------------------------
# Config
# ---------------------------
DISCORD_TOKEN = os.environ.get("NNS_TIPBOT_DISCORD_TOKEN", "").strip()
DB_PATH = os.environ.get("TIPBOT_DB", "tipbot.db").strip()

# Optional: restrict command sync for this bot to one guild
GUILD_ID = os.environ.get("NNS_TIPBOT_GUILD_ID", "").strip()

# Public announcements

PUBLIC_TIP_ANNOUNCEMENTS = os.environ.get("NNS_TIPBOT_PUBLIC_TIP_ANNOUNCEMENTS", "1").strip() == "1"
# Optional role restriction: only members with one of these roles may use the bot.
# You can configure role IDs and/or exact role names as comma-separated lists.
NNS_TIPBOT_ALLOWED_ROLE_IDS = {
    int(x.strip())
    for x in os.environ.get("NNS_TIPBOT_ALLOWED_ROLE_IDS", "").split(",")
    if x.strip().isdigit()
}
NNS_TIPBOT_ALLOWED_ROLE_NAMES = {
    x.strip().lower()
    for x in os.environ.get("NNS_TIPBOT_ALLOWED_ROLE_NAMES", "").split(",")
    if x.strip()
}
print(f"[nns_tipbot] allowed role ids: {sorted(NNS_TIPBOT_ALLOWED_ROLE_IDS)}")
print(f"[nns_tipbot] allowed role names: {sorted(NNS_TIPBOT_ALLOWED_ROLE_NAMES)}")

# NNS RPC (only needed for getnewaddress)
NNS_RPC_URL = os.environ.get("NNS_RPC_URL", "http://127.0.0.1:19996/").strip()
NNS_RPC_USER = os.environ.get("NNS_RPC_USER", "").strip()
NNS_RPC_PASSWORD = os.environ.get("NNS_RPC_PASSWORD", "").strip()
NNS_DEPOSIT_CONFS = int(os.environ.get("NNS_DEPOSIT_CONFS", "6"))
NNS_MIN_WITHDRAW_SAT = int(Decimal(os.environ.get("NNS_MIN_WITHDRAW", "0.10000000")).quantize(
    Decimal("0.00000001"), rounding=ROUND_DOWN
) * Decimal("100000000"))

NNS_WITHDRAW_FEE_BPS = int(os.environ.get("NNS_WITHDRAW_FEE_BPS", "0"))
NNS_WITHDRAW_FEE_ADDRESS = os.environ.get("NNS_WITHDRAW_FEE_ADDRESS", "").strip()


NNS_SATS = 100_000_000

NNS_CLAIM_ENABLED = os.environ.get("NNS_CLAIM_ENABLED", "1").strip() == "1"
NNS_CLAIM_AMOUNT_SAT = int(Decimal(os.environ.get("NNS_CLAIM_AMOUNT", "0.99600000")).quantize(
    Decimal("0.00000001"), rounding=ROUND_DOWN
) * Decimal("100000000"))
NNS_CLAIM_COOLDOWN_SECONDS = int(os.environ.get("NNS_CLAIM_COOLDOWN_SECONDS", str(60 * 60)))
NNS_CLAIM_ALLOWED_CHANNEL_ID = int(os.environ.get("NNS_CLAIM_ALLOWED_CHANNEL_ID", "1494570081043218452").strip() or "0")
NNS_LEVEL_XP_THRESHOLDS = [0, 50, 125, 225, 350, 500, 700, 950, 1250, 1600, 2000, 2500, 3100, 3800, 4600, 5500]
NNS_LEVEL_CLAIM_MULTIPLIERS = {
    1: Decimal("1.00"),
    2: Decimal("1.10"),
    3: Decimal("1.20"),
    4: Decimal("1.30"),
    5: Decimal("1.40"),
    6: Decimal("1.50"),
    7: Decimal("1.60"),
    8: Decimal("1.70"),
    9: Decimal("1.80"),
    10: Decimal("1.90"),
    11: Decimal("2.00"),
    12: Decimal("2.10"),
    13: Decimal("2.20"),
    14: Decimal("2.30"),
    15: Decimal("2.40"),
    16: Decimal("2.50"),
}

NNS_XP_CLAIM = 5
NNS_XP_TIP = 4
NNS_XP_MULTITIP = 6
NNS_XP_AIRDROP_CLAIM = 3
NNS_XP_AIRDROP_CREATE = 8
NNS_XP_STAKE = 3
NNS_XP_CLAIM_STAKING = 2

NNS_XP_MIN_TIP_SAT = int(Decimal("10.00000000") * Decimal(NNS_SATS))
NNS_XP_MIN_STAKE_SAT = int(Decimal("10.00000000") * Decimal(NNS_SATS))
NNS_XP_TIP_DAILY_CAP = 5
NNS_XP_MULTITIP_DAILY_CAP = 5
NNS_XP_AIRDROP_CLAIM_DAILY_CAP = 5
NNS_XP_AIRDROP_CREATE_DAILY_CAP = 2
NNS_XP_STAKE_DAILY_CAP = 2

NNS_XP_CLAIM_STAKING_DAILY_CAP = 3



NNS_XP_DONATION_BOT_USER_ID = int(os.environ.get("NNS_XP_DONATION_BOT_USER_ID", "0").strip() or "0")

# Treasure Chest config
NNS_CHEST_ENABLED = os.environ.get("NNS_CHEST_ENABLED", "1").strip() == "1"
NNS_CHEST_COST_SAT = int(Decimal(os.environ.get("NNS_CHEST_COST", "3.00000000")).quantize(
    Decimal("0.00000001"), rounding=ROUND_DOWN
) * Decimal("100000000"))
NNS_CHEST_COOLDOWN_SECONDS = int(os.environ.get("NNS_CHEST_COOLDOWN_SECONDS", str(30 * 60)))
NNS_CHEST_ALLOWED_CHANNEL_ID = int(os.environ.get("NNS_CHEST_ALLOWED_CHANNEL_ID", "1495253280337432756").strip() or "0")
NNS_XP_CHEST_MIN = int(os.environ.get("NNS_XP_CHEST_MIN", "2"))
NNS_XP_CHEST_MAX = int(os.environ.get("NNS_XP_CHEST_MAX", "5"))
NNS_XP_CHEST_GOLDEN = int(os.environ.get("NNS_XP_CHEST_GOLDEN", "20"))
NNS_CHEST_GOLDEN_CHANCE_PERCENT = Decimal(os.environ.get("NNS_CHEST_GOLDEN_CHANCE_PERCENT", "5").strip() or "5")

NNS_TIP_BLOCKED_RECIPIENT_IDS = {
    int(x.strip())
    for x in os.environ.get("NNS_TIP_BLOCKED_RECIPIENT_IDS", "1487887518442586173").split(",")
    if x.strip().isdigit()
}
print(f"[nns_tipbot] blocked tip recipient ids: {sorted(NNS_TIP_BLOCKED_RECIPIENT_IDS)}")


# Airdrop config
NNS_AIRDROP_DEFAULT_DURATION_MIN = int(os.environ.get("NNS_AIRDROP_DEFAULT_DURATION_MIN", "1440"))
NNS_AIRDROP_MAX_RECIPIENTS = int(os.environ.get("NNS_AIRDROP_MAX_RECIPIENTS", "20"))

# Staking config
NNS_STAKING_ENABLED = os.environ.get("NNS_STAKING_ENABLED", "0").strip() == "1"
NNS_STAKING_APR_FALLBACK = Decimal(os.environ.get("NNS_STAKING_APR_FALLBACK", "0").strip() or "0")
if NNS_STAKING_APR_FALLBACK < Decimal("0"):
    NNS_STAKING_APR_FALLBACK = Decimal("0")
elif NNS_STAKING_APR_FALLBACK > Decimal("1000"):
    NNS_STAKING_APR_FALLBACK = Decimal("1000")
NNS_STAKING_BLOCK_TIME_SECONDS = int(os.environ.get("NNS_STAKING_BLOCK_TIME_SECONDS", "180"))
NNS_STAKING_APR_FACTOR = Decimal(os.environ.get("NNS_STAKING_APR_FACTOR", "0.75").strip() or "0.75")
if NNS_STAKING_APR_FACTOR < Decimal("0"):
    NNS_STAKING_APR_FACTOR = Decimal("0")
elif NNS_STAKING_APR_FACTOR > Decimal("1"):
    NNS_STAKING_APR_FACTOR = Decimal("1")
CURRENT_NNS_STAKING_APR = NNS_STAKING_APR_FALLBACK
NNS_STAKING_INTERVAL_SECONDS = int(os.environ.get("NNS_STAKING_INTERVAL_SECONDS", "180"))
NNS_STAKING_APR_REFRESH_SECONDS = int(os.environ.get("NNS_STAKING_APR_REFRESH_SECONDS", "1800"))
NNS_STAKING_APR_CACHE_FILE = os.environ.get("NNS_STAKING_APR_CACHE_FILE", "nns_staking_apr.json").strip()
SECONDS_PER_YEAR = Decimal("31536000")


# ---------------------------
# Helpers
# ---------------------------
def now_ts() -> int:
    return int(time.time())


def format_sat_to_nns(sat: int) -> str:
    return f"{(Decimal(int(sat)) / Decimal(NNS_SATS)):.8f}"


def parse_nns_to_sat(s: str) -> int:
    s = (s or "").strip()
    if not s:
        raise ValueError("missing amount")
    d = Decimal(s).quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)
    sat = int(d * Decimal(NNS_SATS))
    if sat <= 0:
        raise ValueError("amount must be > 0")
    return sat



def compute_nns_withdraw_fee_sat(amount_sat: int) -> int:
    bps = int(NNS_WITHDRAW_FEE_BPS)
    if bps <= 0:
        return 0
    fee = (int(amount_sat) * bps) // 10_000
    if fee <= 0 < int(amount_sat):
        fee = 1
    return int(fee)


def get_current_staking_apr() -> Decimal:
    apr = CURRENT_NNS_STAKING_APR
    if apr < Decimal("0"):
        return Decimal("0")
    if apr > Decimal("1000"):
        return Decimal("1000")
    return apr


async def refresh_dynamic_staking_apr() -> Decimal:
    global CURRENT_NNS_STAKING_APR

    try:
        path = (NNS_STAKING_APR_CACHE_FILE or "").strip()
        if not path:
            return CURRENT_NNS_STAKING_APR

        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)

        apr = Decimal(str(raw.get("apr_percent", CURRENT_NNS_STAKING_APR) or CURRENT_NNS_STAKING_APR))
        if apr < Decimal("0"):
            apr = Decimal("0")
        elif apr > Decimal("1000"):
            apr = Decimal("1000")

        CURRENT_NNS_STAKING_APR = apr
        return CURRENT_NNS_STAKING_APR
    except FileNotFoundError:
        return CURRENT_NNS_STAKING_APR
    except Exception as e:
        print(f"[nns_tipbot] APR cache refresh failed: {e}")
        return CURRENT_NNS_STAKING_APR


def db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con


def init_db() -> None:
    con = db()

    con.execute("""
    CREATE TABLE IF NOT EXISTS users (
      discord_id INTEGER PRIMARY KEY,
      address TEXT,
      website_secret TEXT,
      balance INTEGER NOT NULL DEFAULT 0,
      veco_internal_sat INTEGER NOT NULL DEFAULT 0,
      veco_deposit_address TEXT,
      nns_internal_sat INTEGER NOT NULL DEFAULT 0,
      nns_deposit_address TEXT,
      last_nns_claim_at INTEGER NOT NULL DEFAULT 0,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      last_withdraw_at INTEGER NOT NULL DEFAULT 0
    );
    """)

    try:
        con.execute("ALTER TABLE users ADD COLUMN nns_internal_sat INTEGER NOT NULL DEFAULT 0;")
    except Exception:
        pass
    try:
        con.execute("ALTER TABLE users ADD COLUMN nns_deposit_address TEXT;")
    except Exception:
        pass
    try:
        con.execute("ALTER TABLE users ADD COLUMN last_nns_claim_at INTEGER NOT NULL DEFAULT 0;")
    except Exception:
        pass

    con.execute("""
    CREATE TABLE IF NOT EXISTS tx_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ts INTEGER NOT NULL,
      type TEXT NOT NULL,
      from_id INTEGER,
      to_id INTEGER,
      amount INTEGER NOT NULL,
      note TEXT,
      status TEXT NOT NULL,
      faucet_resp TEXT,
      error TEXT
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS nns_withdrawals (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ts INTEGER NOT NULL,
      discord_id INTEGER NOT NULL,
      to_address TEXT NOT NULL,
      amount_sat INTEGER NOT NULL,
      fee_sat INTEGER NOT NULL DEFAULT 0,
      txid TEXT,
      status TEXT NOT NULL,
      error TEXT
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS nns_airdrops (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      creator_discord_id INTEGER NOT NULL,
      guild_id INTEGER,
      channel_id INTEGER,
      message_id INTEGER,
      per_user_sat INTEGER NOT NULL,
      limit_count INTEGER NOT NULL,
      claimed_count INTEGER NOT NULL DEFAULT 0,
      remaining_sat INTEGER NOT NULL,
      expires_at INTEGER,
      status TEXT NOT NULL DEFAULT 'active',
      created_at INTEGER NOT NULL,
      ended_at INTEGER
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS nns_airdrop_claims (
      airdrop_id INTEGER NOT NULL,
      discord_id INTEGER NOT NULL,
      claimed_at INTEGER NOT NULL,
      PRIMARY KEY(airdrop_id, discord_id)
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS nns_airdrop_admins (
      discord_id INTEGER PRIMARY KEY,
      created_at INTEGER NOT NULL
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS nns_stakes (
      discord_id INTEGER PRIMARY KEY,
      staked_sat INTEGER NOT NULL DEFAULT 0,
      accrued_reward_sat INTEGER NOT NULL DEFAULT 0,
      reward_remainder TEXT NOT NULL DEFAULT '0',
      last_accrual_ts INTEGER NOT NULL DEFAULT 0,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS nns_profiles (
      discord_id INTEGER PRIMARY KEY,
      xp_total INTEGER NOT NULL DEFAULT 0,
      level INTEGER NOT NULL DEFAULT 1,
      updated_at INTEGER NOT NULL
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS nns_xp_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ts INTEGER NOT NULL,
      discord_id INTEGER NOT NULL,
      source TEXT NOT NULL,
      xp_amount INTEGER NOT NULL,
      note TEXT
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS nns_xp_daily (
      discord_id INTEGER NOT NULL,
      day_key TEXT NOT NULL,
      source TEXT NOT NULL,
      claim_count INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY(discord_id, day_key, source)
    );
    """)

    # Treasure Chest state table
    con.execute("""
    CREATE TABLE IF NOT EXISTS nns_chest_state (
      discord_id INTEGER PRIMARY KEY,
      last_played_at INTEGER NOT NULL DEFAULT 0,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );
    """)

    try:
        cols = con.execute("PRAGMA table_info(nns_stakes)").fetchall()
        reward_remainder_type = None
        for col in cols:
            try:
                if str(col[1]) == "reward_remainder":
                    reward_remainder_type = str(col[2]).upper()
                    break
            except Exception:
                pass
        if reward_remainder_type == "REAL":
            con.execute("ALTER TABLE nns_stakes RENAME TO nns_stakes_old")
            con.execute("""
            CREATE TABLE nns_stakes (
              discord_id INTEGER PRIMARY KEY,
              staked_sat INTEGER NOT NULL DEFAULT 0,
              accrued_reward_sat INTEGER NOT NULL DEFAULT 0,
              reward_remainder TEXT NOT NULL DEFAULT '0',
              last_accrual_ts INTEGER NOT NULL DEFAULT 0,
              created_at INTEGER NOT NULL,
              updated_at INTEGER NOT NULL
            );
            """)
            con.execute("""
            INSERT INTO nns_stakes(discord_id, staked_sat, accrued_reward_sat, reward_remainder, last_accrual_ts, created_at, updated_at)
            SELECT discord_id, staked_sat, accrued_reward_sat, printf('%.18f', COALESCE(reward_remainder, 0)), last_accrual_ts, created_at, updated_at
            FROM nns_stakes_old
            """)
            con.execute("DROP TABLE nns_stakes_old")
    except Exception:
        pass

    con.close()


def get_or_create_user(con: sqlite3.Connection, discord_id: int) -> Dict[str, Any]:
    row = con.execute(
        "SELECT discord_id, nns_internal_sat, nns_deposit_address, last_nns_claim_at, created_at, updated_at "
        "FROM users WHERE discord_id=?",
        (int(discord_id),)
    ).fetchone()

    if row:
        return {
            "discord_id": int(row[0]),
            "nns_internal_sat": int(row[1] or 0),
            "nns_deposit_address": row[2],
            "last_nns_claim_at": int(row[3] or 0),
            "created_at": int(row[4]),
            "updated_at": int(row[5]),
        }

    ts = now_ts()
    con.execute(
        "INSERT INTO users(discord_id, address, website_secret, balance, veco_internal_sat, veco_deposit_address, "
        "nns_internal_sat, nns_deposit_address, last_nns_claim_at, created_at, updated_at, last_withdraw_at) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
        (int(discord_id), None, None, 0, 0, None, 0, None, 0, ts, ts, 0)
    )

    return {
        "discord_id": int(discord_id),
        "nns_internal_sat": 0,
        "nns_deposit_address": None,
        "last_nns_claim_at": 0,
        "created_at": ts,
        "updated_at": ts,
    }


def has_pending_nns_withdraw(con: sqlite3.Connection, discord_id: int) -> bool:
    row = con.execute(
        "SELECT id FROM nns_withdrawals WHERE discord_id=? AND status='pending' ORDER BY id DESC LIMIT 1",
        (int(discord_id),)
    ).fetchone()
    return bool(row)


def format_claim_cooldown(remaining_seconds: int) -> str:
    remaining_seconds = max(0, int(remaining_seconds or 0))
    minutes = remaining_seconds // 60
    seconds = remaining_seconds % 60
    return f"{minutes}m {seconds}s"


def perform_nns_claim(discord_id: int) -> Dict[str, Any]:
    if not NNS_CLAIM_ENABLED:
        raise ValueError("Claiming is currently disabled.")

    claim_amount_sat = int(NNS_CLAIM_AMOUNT_SAT)
    cooldown_seconds = int(NNS_CLAIM_COOLDOWN_SECONDS)
    if claim_amount_sat <= 0:
        raise ValueError("Claim amount is misconfigured.")

    con = db()
    try:
        con.execute("BEGIN IMMEDIATE;")
        user = get_or_create_user(con, int(discord_id))

        last_claim_at = int(user.get("last_nns_claim_at") or 0)
        ts = now_ts()
        next_allowed_at = last_claim_at + cooldown_seconds
        if cooldown_seconds > 0 and ts < next_allowed_at:
            con.execute("ROLLBACK;")
            remaining = next_allowed_at - ts
            raise ValueError(f"You can claim again in **{format_claim_cooldown(remaining)}**.")

        profile = get_or_create_nns_profile(con, int(discord_id))
        level = int(profile.get("level") or 1)
        multiplier = get_nns_claim_multiplier_for_level(level)
        final_claim_amount_sat = int((Decimal(claim_amount_sat) * multiplier).to_integral_value(rounding=ROUND_DOWN))

        con.execute(
            "UPDATE users SET nns_internal_sat = nns_internal_sat + ?, last_nns_claim_at=?, updated_at=? WHERE discord_id=?",
            (final_claim_amount_sat, ts, ts, int(discord_id))
        )
        con.execute(
            "INSERT INTO tx_log(ts, type, from_id, to_id, amount, note, status) VALUES(?,?,?,?,?,?,?)",
            (ts, "claim_nns", None, int(discord_id), final_claim_amount_sat, f"claim cooldown={cooldown_seconds} level={level}", "ok")
        )
        xp_info = grant_nns_xp(con, int(discord_id), "claim_nns", NNS_XP_CLAIM, f"claim level={level}")
        con.execute("COMMIT;")

        return {
            "amount_sat": final_claim_amount_sat,
            "base_amount_sat": claim_amount_sat,
            "claimed_at": ts,
            "next_allowed_at": ts + cooldown_seconds,
            "level": level,
            "multiplier": str(multiplier),
            "xp_total": int(xp_info["xp_total"]),
            "leveled_up": bool(xp_info["leveled_up"]),
            "new_level": int(xp_info["new_level"]),
            "old_level": int(xp_info["old_level"]),
        }
    except Exception:
        try:
            con.execute("ROLLBACK;")
        except Exception:
            pass
        raise
    finally:
        con.close()


def get_or_create_nns_profile(con: sqlite3.Connection, discord_id: int) -> Dict[str, Any]:
    row = con.execute(
        "SELECT discord_id, xp_total, level, updated_at FROM nns_profiles WHERE discord_id=?",
        (int(discord_id),)
    ).fetchone()

    if row:
        return {
            "discord_id": int(row[0]),
            "xp_total": int(row[1] or 0),
            "level": int(row[2] or 1),
            "updated_at": int(row[3] or 0),
        }

    ts = now_ts()
    con.execute(
        "INSERT INTO nns_profiles(discord_id, xp_total, level, updated_at) VALUES(?,?,?,?)",
        (int(discord_id), 0, 1, ts)
    )
    return {
        "discord_id": int(discord_id),
        "xp_total": 0,
        "level": 1,
        "updated_at": ts,
    }


def compute_nns_level_from_xp(xp_total: int) -> int:
    xp_total = max(0, int(xp_total or 0))
    level = 1
    for idx, threshold in enumerate(NNS_LEVEL_XP_THRESHOLDS, start=1):
        if xp_total >= int(threshold):
            level = idx
        else:
            break
    return min(16, max(1, level))


def get_nns_claim_multiplier_for_level(level: int) -> Decimal:
    level = min(10, max(1, int(level or 1)))
    return NNS_LEVEL_CLAIM_MULTIPLIERS.get(level, Decimal("1.00"))


def get_nns_next_level_xp(level: int) -> Optional[int]:
    level = min(10, max(1, int(level or 1)))
    if level >= len(NNS_LEVEL_XP_THRESHOLDS):
        return None
    return int(NNS_LEVEL_XP_THRESHOLDS[level])


def grant_nns_xp(con: sqlite3.Connection, discord_id: int, source: str, xp_amount: int, note: Optional[str] = None) -> Dict[str, Any]:
    xp_amount = int(xp_amount or 0)
    profile = get_or_create_nns_profile(con, int(discord_id))
    old_xp = int(profile.get("xp_total") or 0)
    old_level = int(profile.get("level") or 1)
    ts = now_ts()

    if xp_amount <= 0:
        return {
            "xp_total": old_xp,
            "level": old_level,
            "leveled_up": False,
            "old_level": old_level,
            "new_level": old_level,
            "xp_added": 0,
        }

    new_xp = old_xp + xp_amount
    new_level = compute_nns_level_from_xp(new_xp)

    con.execute(
        "UPDATE nns_profiles SET xp_total=?, level=?, updated_at=? WHERE discord_id=?",
        (int(new_xp), int(new_level), ts, int(discord_id))
    )
    con.execute(
        "INSERT INTO nns_xp_log(ts, discord_id, source, xp_amount, note) VALUES(?,?,?,?,?)",
        (ts, int(discord_id), str(source), int(xp_amount), (note or ""))
    )

    return {
        "xp_total": int(new_xp),
        "level": int(new_level),
        "leveled_up": bool(new_level > old_level),
        "old_level": int(old_level),
        "new_level": int(new_level),
        "xp_added": int(xp_amount),
    }


def get_day_key(ts: Optional[int] = None) -> str:
    return time.strftime("%Y-%m-%d", time.gmtime(int(ts or now_ts())))


# --- Treasure Chest helpers ---
def get_or_create_nns_chest_state(con: sqlite3.Connection, discord_id: int) -> Dict[str, Any]:
    row = con.execute(
        "SELECT discord_id, last_played_at, created_at, updated_at FROM nns_chest_state WHERE discord_id=?",
        (int(discord_id),)
    ).fetchone()

    if row:
        return {
            "discord_id": int(row[0]),
            "last_played_at": int(row[1] or 0),
            "created_at": int(row[2] or 0),
            "updated_at": int(row[3] or 0),
        }

    ts = now_ts()
    con.execute(
        "INSERT INTO nns_chest_state(discord_id, last_played_at, created_at, updated_at) VALUES(?,?,?,?)",
        (int(discord_id), 0, ts, ts)
    )
    return {
        "discord_id": int(discord_id),
        "last_played_at": 0,
        "created_at": ts,
        "updated_at": ts,
    }


def get_chest_cooldown_remaining(con: sqlite3.Connection, discord_id: int) -> int:
    state = get_or_create_nns_chest_state(con, int(discord_id))
    last_played_at = int(state.get("last_played_at") or 0)
    next_allowed_at = last_played_at + int(NNS_CHEST_COOLDOWN_SECONDS)
    return max(0, next_allowed_at - now_ts())


def get_chest_embed(user: discord.abc.User, status_text: str, reveal: Optional[str] = None, golden: bool = False) -> discord.Embed:
    title = "🧰 Treasure Chest"
    if golden:
        title = "✨ Golden Chest"

    desc = (
        f"Player: {user.mention}\n"
        f"Cost: **{format_sat_to_nns(NNS_CHEST_COST_SAT)} NNS** (burned)\n"
        f"Pick one chest below."
    )
    if status_text:
        desc += f"\n\n{status_text}"
    if reveal:
        desc += f"\n\n{reveal}"

    embed = discord.Embed(title=title, description=desc)
    if golden:
        embed.color = discord.Color.gold()
    else:
        embed.color = discord.Color.orange()
    embed.set_footer(text="One play every 30 minutes")
    return embed


def perform_nns_chest_play(discord_id: int, chosen_index: int) -> Dict[str, Any]:
    if not NNS_CHEST_ENABLED:
        raise ValueError("Treasure Chest is currently disabled.")
    if int(NNS_CHEST_COST_SAT) <= 0:
        raise ValueError("Treasure Chest cost is misconfigured.")
    if int(chosen_index) not in (0, 1, 2):
        raise ValueError("Invalid chest selection.")

    con = db()
    try:
        con.execute("BEGIN IMMEDIATE;")

        user = get_or_create_user(con, int(discord_id))
        state = get_or_create_nns_chest_state(con, int(discord_id))
        balance_sat = int(user.get("nns_internal_sat") or 0)
        if balance_sat < int(NNS_CHEST_COST_SAT):
            con.execute("ROLLBACK;")
            raise ValueError(
                f"Insufficient NNS balance. You need **{format_sat_to_nns(NNS_CHEST_COST_SAT)} NNS** but only have **{format_sat_to_nns(balance_sat)} NNS**."
            )

        last_played_at = int(state.get("last_played_at") or 0)
        next_allowed_at = last_played_at + int(NNS_CHEST_COOLDOWN_SECONDS)
        ts = now_ts()
        if ts < next_allowed_at:
            con.execute("ROLLBACK;")
            remaining = next_allowed_at - ts
            raise ValueError(f"You can play Treasure Chest again in **{format_claim_cooldown(remaining)}**.")

        winning_index = random.randint(0, 2)
        golden_roll = Decimal(str(random.uniform(0, 100)))
        is_golden = golden_roll < NNS_CHEST_GOLDEN_CHANCE_PERCENT
        xp_reward = int(NNS_XP_CHEST_GOLDEN if is_golden else random.randint(int(NNS_XP_CHEST_MIN), int(NNS_XP_CHEST_MAX)))
        won = int(chosen_index) == int(winning_index)

        con.execute(
            "UPDATE users SET nns_internal_sat = nns_internal_sat - ?, updated_at=? WHERE discord_id=?",
            (int(NNS_CHEST_COST_SAT), ts, int(discord_id))
        )
        con.execute(
            "UPDATE nns_chest_state SET last_played_at=?, updated_at=? WHERE discord_id=?",
            (ts, ts, int(discord_id))
        )
        con.execute(
            "INSERT INTO tx_log(ts, type, from_id, to_id, amount, note, status) VALUES(?,?,?,?,?,?,?)",
            (ts, "chest_burn_nns", int(discord_id), None, int(NNS_CHEST_COST_SAT), f"treasure chest pick={int(chosen_index)+1}", "ok")
        )

        xp_info = {
            "xp_total": 0,
            "level": 1,
            "leveled_up": False,
            "old_level": 1,
            "new_level": 1,
            "xp_added": 0,
        }
        if won and xp_reward > 0:
            xp_info = grant_nns_xp(con, int(discord_id), "chest_win_nns", int(xp_reward), f"treasure chest golden={int(is_golden)}")

        con.execute("COMMIT;")

        return {
            "won": bool(won),
            "golden": bool(is_golden),
            "xp_reward": int(xp_reward if won else 0),
            "winning_index": int(winning_index),
            "chosen_index": int(chosen_index),
            "cost_sat": int(NNS_CHEST_COST_SAT),
            "next_allowed_at": ts + int(NNS_CHEST_COOLDOWN_SECONDS),
            "xp_info": xp_info,
        }
    except Exception:
        try:
            con.execute("ROLLBACK;")
        except Exception:
            pass
        raise
    finally:
        con.close()


def grant_nns_xp_capped(
    con: sqlite3.Connection,
    discord_id: int,
    source: str,
    xp_amount: int,
    daily_cap: int,
    note: Optional[str] = None,
) -> Dict[str, Any]:
    daily_cap = int(daily_cap or 0)
    if daily_cap <= 0:
        return grant_nns_xp(con, discord_id, source, xp_amount, note)

    day_key = get_day_key()
    row = con.execute(
        "SELECT claim_count FROM nns_xp_daily WHERE discord_id=? AND day_key=? AND source=?",
        (int(discord_id), day_key, str(source))
    ).fetchone()
    current_count = int(row[0] or 0) if row else 0

    if current_count >= daily_cap:
        profile = get_or_create_nns_profile(con, int(discord_id))
        level = int(profile.get("level") or 1)
        xp_total = int(profile.get("xp_total") or 0)
        return {
            "xp_total": xp_total,
            "level": level,
            "leveled_up": False,
            "old_level": level,
            "new_level": level,
            "xp_added": 0,
            "cap_reached": True,
            "daily_count": current_count,
            "daily_cap": daily_cap,
        }

    con.execute(
        "INSERT INTO nns_xp_daily(discord_id, day_key, source, claim_count) VALUES(?,?,?,1) "
        "ON CONFLICT(discord_id, day_key, source) DO UPDATE SET claim_count = claim_count + 1",
        (int(discord_id), day_key, str(source))
    )

    info = grant_nns_xp(con, int(discord_id), source, xp_amount, note)
    info["cap_reached"] = False
    info["daily_count"] = current_count + 1
    info["daily_cap"] = daily_cap
    return info



def get_tip_xp_amount_for_recipient(recipient_id: int) -> int:
    base_xp = int(NNS_XP_TIP)
    if int(NNS_XP_DONATION_BOT_USER_ID or 0) > 0 and int(recipient_id) == int(NNS_XP_DONATION_BOT_USER_ID):
        return base_xp * 2
    return base_xp


def is_tip_blocked_recipient(discord_id: int) -> bool:
    return int(discord_id) in NNS_TIP_BLOCKED_RECIPIENT_IDS

# Role gate helper
def get_role_gate_error(interaction: discord.Interaction) -> Optional[str]:
    # No restriction configured.
    if not NNS_TIPBOT_ALLOWED_ROLE_IDS and not NNS_TIPBOT_ALLOWED_ROLE_NAMES:
        return None

    # Guild admins / managers always bypass the role gate.
    perms = getattr(interaction.user, "guild_permissions", None)
    if perms and (getattr(perms, "administrator", False) or getattr(perms, "manage_guild", False)):
        return None

    guild = getattr(interaction, "guild", None)
    member = None

    # Prefer a real guild member object, because component interactions / persistent views
    # can sometimes be awkward depending on cache state.
    if guild is not None:
        try:
            member = guild.get_member(int(interaction.user.id))
        except Exception:
            member = None

    if member is None:
        member = interaction.user

    roles = getattr(member, "roles", None)
    if roles is None:
        print(f"[nns_tipbot] role gate: no roles available for user {getattr(interaction.user, 'id', '?')}")
        return "This bot can only be used inside the server by members with an allowed role."

    matched_role = None
    role_ids = []
    role_names = []

    for role in roles:
        try:
            rid = int(role.id)
            role_ids.append(rid)
            if rid in NNS_TIPBOT_ALLOWED_ROLE_IDS:
                matched_role = f"id:{rid}"
                break
        except Exception:
            pass
        try:
            rname = str(role.name).strip().lower()
            role_names.append(rname)
            if rname in NNS_TIPBOT_ALLOWED_ROLE_NAMES:
                matched_role = f"name:{rname}"
                break
        except Exception:
            pass

    if matched_role is not None:
        print(
            f"[nns_tipbot] role gate allow user={int(interaction.user.id)} matched={matched_role} roles={role_ids} role_names={role_names}"
        )
        return None

    print(
        f"[nns_tipbot] role gate deny user={int(interaction.user.id)} roles={role_ids} role_names={role_names} allowed_ids={sorted(NNS_TIPBOT_ALLOWED_ROLE_IDS)} allowed_names={sorted(NNS_TIPBOT_ALLOWED_ROLE_NAMES)}"
    )
    return "You are not allowed to use this feature yet. Please get verified first. See the how-to channel for details."


def is_admin(interaction: discord.Interaction, con: Optional[sqlite3.Connection] = None) -> bool:
    perms = getattr(interaction.user, "guild_permissions", None)
    if perms and (getattr(perms, "administrator", False) or getattr(perms, "manage_guild", False)):
        return True

    should_close = False
    if con is None:
        con = db()
        should_close = True

    try:
        row = con.execute(
            "SELECT 1 FROM nns_airdrop_admins WHERE discord_id=?",
            (int(interaction.user.id),)
        ).fetchone()
        return bool(row)
    finally:
        if should_close:
            con.close()


def fmt_duration_compact(seconds: int) -> str:
    seconds = max(0, int(seconds or 0))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    parts: List[str] = []
    if h:
        parts.append(f"{h}h")
    if m:
        parts.append(f"{m}m")
    if not h and not m:
        parts.append(f"{s}s")
    return " ".join(parts)


def has_user_claimed_airdrop(con: sqlite3.Connection, airdrop_id: int, discord_id: int) -> bool:
    row = con.execute(
        "SELECT 1 FROM nns_airdrop_claims WHERE airdrop_id=? AND discord_id=?",
        (int(airdrop_id), int(discord_id))
    ).fetchone()
    return bool(row)


def get_or_create_stake(con: sqlite3.Connection, discord_id: int) -> Dict[str, Any]:
    row = con.execute(
        "SELECT discord_id, staked_sat, accrued_reward_sat, reward_remainder, last_accrual_ts, created_at, updated_at "
        "FROM nns_stakes WHERE discord_id=?",
        (int(discord_id),)
    ).fetchone()

    if row:
        return {
            "discord_id": int(row[0]),
            "staked_sat": int(row[1] or 0),
            "accrued_reward_sat": int(row[2] or 0),
            "reward_remainder": str(row[3] or "0"),
            "last_accrual_ts": int(row[4] or 0),
            "created_at": int(row[5]),
            "updated_at": int(row[6]),
        }

    ts = now_ts()
    con.execute(
        "INSERT INTO nns_stakes(discord_id, staked_sat, accrued_reward_sat, reward_remainder, last_accrual_ts, created_at, updated_at) "
        "VALUES(?,?,?,?,?,?,?)",
        (int(discord_id), 0, 0, "0", ts, ts, ts)
    )
    return {
        "discord_id": int(discord_id),
        "staked_sat": 0,
        "accrued_reward_sat": 0,
        "reward_remainder": "0",
        "last_accrual_ts": ts,
        "created_at": ts,
        "updated_at": ts,
    }


def accrue_stake_position(con: sqlite3.Connection, discord_id: int, ts_now: Optional[int] = None) -> Dict[str, Any]:
    ts_now = int(ts_now or now_ts())
    stake = get_or_create_stake(con, discord_id)

    staked_sat = int(stake.get("staked_sat") or 0)
    accrued_reward_sat = int(stake.get("accrued_reward_sat") or 0)
    reward_remainder = Decimal(str(stake.get("reward_remainder") or "0"))
    last_accrual_ts = int(stake.get("last_accrual_ts") or ts_now)

    if ts_now <= last_accrual_ts:
        return stake

    elapsed = ts_now - last_accrual_ts
    elapsed = min(elapsed, max(1, 2 * int(NNS_STAKING_INTERVAL_SECONDS)))
    reward_sat_to_add = 0
    new_remainder = reward_remainder

    current_apr = get_current_staking_apr()

    if staked_sat > 0 and current_apr > 0:
        raw_reward = (
            (Decimal(staked_sat) * current_apr * Decimal(elapsed))
            / Decimal("100")
            / SECONDS_PER_YEAR
        )
        raw_reward += reward_remainder
        reward_sat_to_add = int(raw_reward.to_integral_value(rounding=ROUND_DOWN))
        new_remainder = raw_reward - Decimal(reward_sat_to_add)

    new_accrued = accrued_reward_sat + reward_sat_to_add

    con.execute(
        "UPDATE nns_stakes SET accrued_reward_sat=?, reward_remainder=?, last_accrual_ts=?, updated_at=? WHERE discord_id=?",
        (int(new_accrued), str(new_remainder), ts_now, ts_now, int(discord_id))
    )

    return {
        "discord_id": int(discord_id),
        "staked_sat": staked_sat,
        "accrued_reward_sat": int(new_accrued),
        "reward_remainder": str(new_remainder),
        "last_accrual_ts": ts_now,
        "created_at": int(stake.get("created_at") or ts_now),
        "updated_at": ts_now,
    }


def accrue_all_stakes_once() -> int:
    ts = now_ts()
    con = db()
    try:
        con.execute("BEGIN IMMEDIATE;")
        rows = con.execute("SELECT discord_id FROM nns_stakes WHERE staked_sat > 0").fetchall()
        updated = 0
        for row in rows:
            accrue_stake_position(con, int(row[0]), ts)
            updated += 1
        con.execute("COMMIT;")
        return updated
    except Exception:
        try:
            con.execute("ROLLBACK;")
        except Exception:
            pass
        raise
    finally:
        con.close()


async def refresh_airdrop_message(bot_client: discord.Client, airdrop_id: int) -> None:
    con = db()
    try:
        con.row_factory = sqlite3.Row
        row = con.execute(
            "SELECT * FROM nns_airdrops WHERE id=?",
            (int(airdrop_id),)
        ).fetchone()
        recent_claims = con.execute(
            "SELECT discord_id FROM nns_airdrop_claims WHERE airdrop_id=? ORDER BY claimed_at DESC LIMIT 5",
            (int(airdrop_id),)
        ).fetchall()
    finally:
        con.close()

    if not row:
        return

    channel_id = int(row["channel_id"] or 0)
    message_id = int(row["message_id"] or 0)
    if not channel_id or not message_id:
        return

    try:
        channel = await bot_client.fetch_channel(channel_id)
        message = await channel.fetch_message(message_id)
    except Exception:
        return

    remaining_sat = int(row["remaining_sat"] or 0)
    claimed_count = int(row["claimed_count"] or 0)
    limit_count = int(row["limit_count"] or 0)
    expires_at = int(row["expires_at"] or 0) if row["expires_at"] is not None else 0
    status = str(row["status"] or "active")

    now = now_ts()
    expired = bool(expires_at and now >= expires_at)
    depleted = remaining_sat <= 0 or claimed_count >= limit_count
    closed = status != "active" or expired or depleted

    creator_id = int(row["creator_discord_id"])
    title = "🎁 NNS Airdrop"
    desc = (
        f"Host: <@{creator_id}>\n"
        f"Per user: **{format_sat_to_nns(int(row['per_user_sat']))} NNS**\n"
        f"Claimed: **{claimed_count}/{limit_count}**\n"
        f"Remaining: **{format_sat_to_nns(remaining_sat)} NNS**"
    )
    if expires_at:
        rem = max(0, expires_at - now)
        desc += f"\nEnds in: **{fmt_duration_compact(rem)}**"
    if 'recent_claims' in locals() and recent_claims:
        recent_mentions = ", ".join(f"<@{int(r[0])}>" for r in recent_claims)
        desc += f"\nRecent claimers: {recent_mentions}"

    embed = discord.Embed(title=title, description=desc)
    btn_label = "Grab NNS"
    if closed:
        if expired:
            btn_label = "Expired"
        elif depleted:
            btn_label = "All claimed"
        else:
            btn_label = "Ended"

    view = AirdropClaimView(int(row["id"]), disabled=closed, button_label=btn_label)
    try:
        await message.edit(embed=embed, view=view)
    except Exception:
        return


async def maybe_close_expired_airdrops(bot_client: discord.Client) -> None:
    con = db()
    rows = []
    try:
        con.row_factory = sqlite3.Row
        rows = con.execute(
            "SELECT * FROM nns_airdrops WHERE status='active' AND expires_at IS NOT NULL AND expires_at <= ?",
            (now_ts(),)
        ).fetchall()
        if not rows:
            return

        for row in rows:
            con.execute("BEGIN IMMEDIATE;")
            try:
                current = con.execute(
                    "SELECT status, remaining_sat, creator_discord_id FROM nns_airdrops WHERE id=?",
                    (int(row["id"]),)
                ).fetchone()
                if not current:
                    con.execute("ROLLBACK;")
                    continue
                if str(current[0]) != "active":
                    con.execute("ROLLBACK;")
                    continue

                refund_sat = int(current[1] or 0)
                creator_now = int(current[2])
                ts = now_ts()

                if refund_sat > 0:
                    get_or_create_user(con, creator_now)
                    con.execute(
                        "UPDATE users SET nns_internal_sat = nns_internal_sat + ?, updated_at=? WHERE discord_id=?",
                        (refund_sat, ts, creator_now)
                    )
                    con.execute(
                        "INSERT INTO tx_log(ts, type, from_id, to_id, amount, note, status) VALUES(?,?,?,?,?,?,?)",
                        (ts, "airdrop_refund_nns", None, creator_now, refund_sat, f"expired airdrop #{int(row['id'])}", "ok")
                    )

                con.execute(
                    "UPDATE nns_airdrops SET status='expired', remaining_sat=0, ended_at=? WHERE id=?",
                    (ts, int(row["id"]))
                )
                con.execute("COMMIT;")
            except Exception:
                try:
                    con.execute("ROLLBACK;")
                except Exception:
                    pass
                raise
    finally:
        con.close()

    for row in rows:
        await refresh_airdrop_message(bot_client, int(row["id"]))


def perform_nns_tip(sender_id: int, recipient_id: int, amount_sat: int, note: Optional[str] = None) -> None:
    if amount_sat <= 0:
        raise ValueError("Amount must be positive.")
    if int(sender_id) == int(recipient_id):
        raise ValueError("You cannot tip yourself.")

    con = db()
    try:
        con.execute("BEGIN IMMEDIATE;")

        sender = get_or_create_user(con, int(sender_id))
        get_or_create_user(con, int(recipient_id))

        sender_bal_sat = int(sender.get("nns_internal_sat") or 0)
        if sender_bal_sat < int(amount_sat):
            con.execute("ROLLBACK;")
            raise ValueError(f"Insufficient NNS balance. You have **{format_sat_to_nns(sender_bal_sat)}** NNS.")

        ts = now_ts()

        con.execute(
            "UPDATE users SET nns_internal_sat = nns_internal_sat - ?, updated_at=? WHERE discord_id=?",
            (int(amount_sat), ts, int(sender_id))
        )
        con.execute(
            "UPDATE users SET nns_internal_sat = nns_internal_sat + ?, updated_at=? WHERE discord_id=?",
            (int(amount_sat), ts, int(recipient_id))
        )

        con.execute(
            "INSERT INTO tx_log(ts, type, from_id, to_id, amount, note, status) VALUES(?,?,?,?,?,?,?)",
            (ts, "tip_nns", int(sender_id), int(recipient_id), int(amount_sat), (note or ""), "ok")
        )

        con.execute("COMMIT;")
    except Exception:
        try:
            con.execute("ROLLBACK;")
        except Exception:
            pass
        raise
    finally:
        con.close()


async def nns_rpc_call(method: str, params: Optional[List[Any]] = None) -> Any:
    if not NNS_RPC_URL or not NNS_RPC_USER or not NNS_RPC_PASSWORD:
        raise RuntimeError("NNS RPC not configured (NNS_RPC_URL/USER/PASSWORD)")

    payload = {
        "jsonrpc": "1.0",
        "id": "nns-tipbot",
        "method": method,
        "params": params or [],
    }

    auth = aiohttp.BasicAuth(NNS_RPC_USER, NNS_RPC_PASSWORD)
    async with aiohttp.ClientSession(auth=auth) as session:
        async with session.post(NNS_RPC_URL, json=payload, timeout=30) as r:
            txt = await r.text()
            if r.status != 200:
                raise RuntimeError(f"NNS RPC HTTP {r.status}: {txt}")
            data = await r.json()
            if data.get("error"):
                raise RuntimeError(f"NNS RPC error: {data['error']}")
            return data.get("result")


# ---------------------------
# Discord bot
# ---------------------------
intents = discord.Intents.default()


class NNSTipBot(discord.Client):
    def __init__(self):
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)

    async def setup_hook(self):
        init_db()

        if GUILD_ID:
            guild = discord.Object(id=int(GUILD_ID))
            self.tree.copy_global_to(guild=guild)
            synced = await self.tree.sync(guild=guild)
            print(f"Synced {len(synced)} commands to guild {GUILD_ID}")
        else:
            synced = await self.tree.sync()
            print(f"Synced {len(synced)} global commands")

        con = db()
        try:
            con.row_factory = sqlite3.Row
            rows = con.execute(
                "SELECT id, status, expires_at, remaining_sat, claimed_count, limit_count FROM nns_airdrops"
            ).fetchall()
        finally:
            con.close()

        now = now_ts()
        for row in rows:
            expires_at = int(row["expires_at"] or 0) if row["expires_at"] is not None else 0
            expired = bool(expires_at and now >= expires_at)
            depleted = int(row["remaining_sat"] or 0) <= 0 or int(row["claimed_count"] or 0) >= int(row["limit_count"] or 0)
            disabled = str(row["status"] or "active") != "active" or expired or depleted
            label = "Grab NNS"
            if disabled:
                if expired:
                    label = "Expired"
                elif depleted:
                    label = "All claimed"
                else:
                    label = "Ended"
            self.add_view(AirdropClaimView(int(row["id"]), disabled=disabled, button_label=label))

        self.loop.create_task(self.airdrop_expiry_loop())
        if NNS_STAKING_ENABLED:
            await refresh_dynamic_staking_apr()
            self.loop.create_task(self.staking_apr_refresh_loop())
            self.loop.create_task(self.staking_accrual_loop())
    async def staking_apr_refresh_loop(self):
        await self.wait_until_ready()
        await asyncio.sleep(25)
        interval = max(300, int(NNS_STAKING_APR_REFRESH_SECONDS))
        while not self.is_closed():
            try:
                apr = await refresh_dynamic_staking_apr()
                print(f"[staking_apr_refresh_loop] current APR set to {apr}%")
            except Exception as e:
                print(f"[staking_apr_refresh_loop] {e}")
            await asyncio.sleep(interval)

    async def airdrop_expiry_loop(self):
        await self.wait_until_ready()
        while not self.is_closed():
            try:
                await maybe_close_expired_airdrops(self)
            except Exception as e:
                print(f"[airdrop_expiry_loop] {e}")
            await asyncio.sleep(60)

    async def staking_accrual_loop(self):
        await self.wait_until_ready()
        interval = max(30, int(NNS_STAKING_INTERVAL_SECONDS))
        while not self.is_closed():
            try:
                updated = accrue_all_stakes_once()
                if updated > 0:
                    print(f"[staking_accrual_loop] updated {updated} stake positions")
            except Exception as e:
                print(f"[staking_accrual_loop] {e}")
            await asyncio.sleep(interval)


bot = NNSTipBot()


class AirdropClaimView(ui.View):
    def __init__(self, airdrop_id: int, disabled: bool = False, button_label: str = "Grab NNS"):
        super().__init__(timeout=None)
        self.airdrop_id = int(airdrop_id)
        button = ui.Button(
            label=button_label,
            style=discord.ButtonStyle.green,
            custom_id=f"nns_airdrop_claim:{self.airdrop_id}",
            disabled=disabled,
        )
        button.callback = self.claim_callback
        self.add_item(button)

    async def claim_callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        role_gate_error = get_role_gate_error(interaction)
        if role_gate_error:
            print(f"[nns_tipbot] denied airdrop claim airdrop_id={self.airdrop_id} user={int(interaction.user.id)}")
            await interaction.followup.send(role_gate_error, ephemeral=True)
            return

        con = db()
        try:
            con.row_factory = sqlite3.Row
            con.execute("BEGIN IMMEDIATE;")

            row = con.execute(
                "SELECT * FROM nns_airdrops WHERE id=?",
                (self.airdrop_id,)
            ).fetchone()
            if not row:
                con.execute("ROLLBACK;")
                await interaction.followup.send("Airdrop not found.", ephemeral=True)
                return

            status = str(row["status"] or "active")
            expires_at = int(row["expires_at"] or 0) if row["expires_at"] is not None else 0
            now = now_ts()
            if status != "active":
                con.execute("ROLLBACK;")
                await interaction.followup.send("This airdrop is no longer active.", ephemeral=True)
                return
            if expires_at and now >= expires_at:
                con.execute("ROLLBACK;")
                await interaction.followup.send("This airdrop has expired.", ephemeral=True)
                return
            if has_user_claimed_airdrop(con, self.airdrop_id, interaction.user.id):
                con.execute("ROLLBACK;")
                await interaction.followup.send("You already claimed this airdrop.", ephemeral=True)
                return

            per_user_sat = int(row["per_user_sat"] or 0)
            remaining_sat = int(row["remaining_sat"] or 0)
            claimed_count = int(row["claimed_count"] or 0)
            limit_count = int(row["limit_count"] or 0)

            if per_user_sat <= 0 or remaining_sat < per_user_sat or claimed_count >= limit_count:
                con.execute("ROLLBACK;")
                await interaction.followup.send("This airdrop is already exhausted.", ephemeral=True)
                return

            get_or_create_user(con, interaction.user.id)
            ts = now_ts()
            new_claimed_count = claimed_count + 1
            new_remaining_sat = remaining_sat - per_user_sat
            new_status = "active"
            if new_remaining_sat <= 0 or new_claimed_count >= limit_count:
                new_status = "claimed_out"
                new_remaining_sat = max(0, new_remaining_sat)

            con.execute(
                "UPDATE users SET nns_internal_sat = nns_internal_sat + ?, updated_at=? WHERE discord_id=?",
                (per_user_sat, ts, int(interaction.user.id))
            )
            con.execute(
                "INSERT INTO nns_airdrop_claims(airdrop_id, discord_id, claimed_at) VALUES(?,?,?)",
                (self.airdrop_id, int(interaction.user.id), ts)
            )
            con.execute(
                "UPDATE nns_airdrops SET claimed_count=?, remaining_sat=?, status=?, ended_at=? WHERE id=?",
                (
                    new_claimed_count,
                    new_remaining_sat,
                    new_status,
                    ts if new_status != "active" else None,
                    self.airdrop_id,
                )
            )
            con.execute(
                "INSERT INTO tx_log(ts, type, from_id, to_id, amount, note, status) VALUES(?,?,?,?,?,?,?)",
                (ts, "airdrop_claim_nns", int(row["creator_discord_id"]), int(interaction.user.id), per_user_sat,
                 f"airdrop #{self.airdrop_id}", "ok")
            )
            con.execute("COMMIT;")
        except Exception:
            try:
                con.execute("ROLLBACK;")
            except Exception:
                pass
            raise
        finally:
            con.close()

        xp_info = {
            "leveled_up": False,
            "new_level": 1,
            "cap_reached": False,
            "daily_cap": NNS_XP_AIRDROP_CLAIM_DAILY_CAP,
        }
        con_xp = db()
        try:
            con_xp.execute("BEGIN IMMEDIATE;")
            xp_info = grant_nns_xp_capped(
                con_xp,
                interaction.user.id,
                "airdrop_claim_nns",
                NNS_XP_AIRDROP_CLAIM,
                NNS_XP_AIRDROP_CLAIM_DAILY_CAP,
                f"airdrop #{self.airdrop_id}",
            )
            con_xp.execute("COMMIT;")
        except Exception as xp_err:
            try:
                con_xp.execute("ROLLBACK;")
            except Exception:
                pass
            print(f"[nns_tipbot] airdrop claim XP grant failed for user={int(interaction.user.id)} airdrop_id={self.airdrop_id}: {xp_err}")
        finally:
            con_xp.close()

        claim_msg = f"Claimed ✅ You received **{format_sat_to_nns(per_user_sat)} NNS** from airdrop #{self.airdrop_id}."
        if xp_info.get("leveled_up"):
            claim_msg += f"\n🚀 Level up! You reached **Level {int(xp_info['new_level'])}**."
        elif xp_info.get("cap_reached"):
            claim_msg += f"\nℹ️ Airdrop-claim XP cap reached for today (**{int(xp_info.get('daily_cap') or 0)}x/day**)."
        await interaction.followup.send(claim_msg, ephemeral=True)
        await refresh_airdrop_message(bot, self.airdrop_id)

        if PUBLIC_TIP_ANNOUNCEMENTS and interaction.channel is not None:
            try:
                await interaction.channel.send(
                    f"🎁 {interaction.user.mention} claimed **{format_sat_to_nns(per_user_sat)} NNS** from airdrop #{self.airdrop_id}."
                )
            except Exception:
                pass


class ChestGameView(ui.View):
    def __init__(self, owner_id: int, *, disabled: bool = False):
        super().__init__(timeout=180)
        self.owner_id = int(owner_id)
        self.resolved = False

        labels = ["🧰 Chest 1", "🧰 Chest 2", "🧰 Chest 3"]
        for idx, label in enumerate(labels):
            button = ui.Button(
                label=label,
                style=discord.ButtonStyle.blurple,
                custom_id=f"nns_chest:{self.owner_id}:{idx}",
                disabled=disabled,
                row=0,
            )
            button.callback = self.make_callback(idx)
            self.add_item(button)

    def disable_all(self) -> None:
        for item in self.children:
            if isinstance(item, ui.Button):
                item.disabled = True

    def make_callback(self, chosen_index: int):
        async def callback(interaction: discord.Interaction):
            if int(interaction.user.id) != int(self.owner_id):
                await interaction.response.send_message("Only the player who started this game can open a chest.", ephemeral=True)
                return

            if self.resolved:
                await interaction.response.send_message("This Treasure Chest game is already finished.", ephemeral=True)
                return

            try:
                result = perform_nns_chest_play(interaction.user.id, int(chosen_index))
            except Exception as e:
                await interaction.response.send_message(str(e), ephemeral=True)
                return

            self.resolved = True
            self.disable_all()

            won = bool(result.get("won"))
            golden = bool(result.get("golden"))
            xp_reward = int(result.get("xp_reward") or 0)
            winning_index = int(result.get("winning_index") or 0)
            next_allowed_at = int(result.get("next_allowed_at") or 0)
            xp_info = result.get("xp_info") or {}

            labels = ["Chest 1", "Chest 2", "Chest 3"]
            reveal_parts = []
            for idx, name in enumerate(labels):
                if idx == winning_index:
                    if golden:
                        reveal_parts.append(f"**{name}: ✨ GOLDEN XP**")
                    else:
                        reveal_parts.append(f"**{name}: 🎉 XP**")
                else:
                    reveal_parts.append(f"{name}: empty")
            reveal_text = "\n".join(reveal_parts)

            if won:
                result_text = f"✅ {interaction.user.mention} opened the winning chest and gained **{xp_reward} XP**!"
                if golden:
                    result_text += " **Golden chest!**"
            else:
                result_text = f"❌ {interaction.user.mention} picked the wrong chest. Better luck next time."

            result_text += f"\nBurned: **{format_sat_to_nns(NNS_CHEST_COST_SAT)} NNS**"
            result_text += f"\nNext play: <t:{next_allowed_at}:R>"
            if xp_info.get("leveled_up"):
                result_text += f"\n🚀 Level up! Reached **Level {int(xp_info.get('new_level') or 1)}**."

            private_result_text = "Treasure Chest finished. The public result has been posted in this channel."
            private_embed = get_chest_embed(interaction.user, private_result_text, None, golden=False)

            await interaction.response.edit_message(embed=private_embed, view=self)

            public_result = result_text
            if interaction.channel is not None:
                await interaction.channel.send(public_result)

        return callback

    async def on_timeout(self):
        if self.resolved:
            return
        self.disable_all()

# ---------------------------
# Commands
# ---------------------------
@bot.tree.command(name="help", description="Show NNS tip bot commands including airdrops.")
async def help_cmd(interaction: discord.Interaction):
    text = (
        "**NNS TipBot**\n"
        "\n"
        f"• `/deposit` – Show/create your personal NNS deposit address ({NNS_DEPOSIT_CONFS} confs)\n"
        "• `/withdraw <to_address> <amount>` – Request an NNS withdrawal\n"
        "• `/withdraw_status <id>` – Check status / txid of an NNS withdrawal\n"
        "• `/tip @user <amount>` – Tip NNS to another user\n"
        "• `/multitip <amount> <users> [note]` – Tip the same NNS amount to multiple users\n"
        "• `/balances` – Show your internal NNS balance\n"
        "• `/claim` – Claim free NNS every hour\n"
        "• `/treasure_chest` – Open a public 3-chest XP game for 1 NNS (burned)\n"
        "• `/profile` – Show your level, XP, and claim multiplier\n"
        "• `/leaderboard` – Show the top NNS levels\n"
        "• `/stake <amount>` – Move internal NNS into staking\n"
        "• `/unstake <amount|all>` – Unstake NNS and claim the proportional reward\n"
        "• `/stake_balance` – Show your staked balance and pending reward\n"
        "• `/claim_staking` – Claim accrued staking rewards without unstaking\n"
        "• `/start_airdrop <per_user> <limit> [duration_min]` – Start a button airdrop from your internal NNS balance\n"
        "• `/list_airdrops` – Show active NNS airdrops\n"
        "• `/end_airdrop <id>` – End your own airdrop and refund the remainder\n"
        "\n"
    )
    await interaction.response.send_message(text, ephemeral=True)
@bot.tree.command(name="treasure_chest", description="Start a public Treasure Chest game in the configured chest channel.")
async def treasure_chest(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True, thinking=False)

    role_gate_error = get_role_gate_error(interaction)
    if role_gate_error:
        await interaction.followup.send(role_gate_error, ephemeral=True)
        return

    if not NNS_CHEST_ENABLED:
        await interaction.followup.send("Treasure Chest is currently disabled.", ephemeral=True)
        return

    if int(NNS_CHEST_ALLOWED_CHANNEL_ID or 0) > 0 and int(interaction.channel_id or 0) != int(NNS_CHEST_ALLOWED_CHANNEL_ID):
        await interaction.followup.send(
            f"Treasure Chest can only be played in <#{int(NNS_CHEST_ALLOWED_CHANNEL_ID)}>.",
            ephemeral=True,
        )
        return

    con = db()
    try:
        user = get_or_create_user(con, interaction.user.id)
        balance_sat = int(user.get("nns_internal_sat") or 0)
        if balance_sat < int(NNS_CHEST_COST_SAT):
            await interaction.followup.send(
                f"Insufficient NNS balance. You need **{format_sat_to_nns(NNS_CHEST_COST_SAT)} NNS** but only have **{format_sat_to_nns(balance_sat)} NNS**.",
                ephemeral=True,
            )
            return

        remaining = get_chest_cooldown_remaining(con, interaction.user.id)
        if remaining > 0:
            await interaction.followup.send(
                f"You can play Treasure Chest again in **{format_claim_cooldown(remaining)}**.",
                ephemeral=True,
            )
            return
    finally:
        con.close()

    embed = get_chest_embed(
        interaction.user,
        "Choose one chest below. Only you can see this selection. The result will be posted publicly.",
        "Possible reward: **2–5 XP**.\nGolden chest chance: **5%** for **20 XP**.",
        golden=False,
    )
    view = ChestGameView(interaction.user.id)
    await interaction.followup.send(embed=embed, view=view, ephemeral=True)



@bot.tree.command(name="balances", description="Show your internal NNS balance.")
async def balances(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True, thinking=False)

    con = db()
    try:
        u = get_or_create_user(con, interaction.user.id)
        bal = int(u.get("nns_internal_sat") or 0)
        dep = u.get("nns_deposit_address") or "(not created)"
        await interaction.followup.send(
            f"NNS (internal): **{format_sat_to_nns(bal)}**\n"
            f"NNS deposit address: `{dep}`",
            ephemeral=True,
        )
    finally:
        con.close()


@bot.tree.command(name="profile", description="Show your NNS level, XP, and claim multiplier.")
async def profile(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True, thinking=False)

    con = db()
    try:
        profile_row = get_or_create_nns_profile(con, interaction.user.id)
        xp_total = int(profile_row.get("xp_total") or 0)
        level = int(profile_row.get("level") or 1)
        next_level_xp = get_nns_next_level_xp(level)
        multiplier = get_nns_claim_multiplier_for_level(level)

        if next_level_xp is None:
            next_line = "Next level: **max level reached**"
        else:
            remaining = max(0, int(next_level_xp) - xp_total)
            next_line = f"Next level: **{remaining} XP remaining** (at {next_level_xp} XP)"

        await interaction.followup.send(
            f"Level: **{level}**\n"
            f"XP: **{xp_total}**\n"
            f"Claim multiplier: **{multiplier}x**\n"
            f"{next_line}",
            ephemeral=True,
        )
    finally:
        con.close()


@bot.tree.command(name="leaderboard", description="Show the top NNS levels on this bot.")
async def leaderboard(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True, thinking=True)

    con = db()
    try:
        rows = con.execute(
            "SELECT discord_id, xp_total, level FROM nns_profiles ORDER BY level DESC, xp_total DESC, discord_id ASC LIMIT 10"
        ).fetchall()
    finally:
        con.close()

    if not rows:
        await interaction.followup.send("No leaderboard data yet.", ephemeral=True)
        return

    lines: List[str] = []
    for idx, row in enumerate(rows, start=1):
        discord_id = int(row[0])
        xp_total = int(row[1] or 0)
        level = int(row[2] or 1)
        lines.append(f"`#{idx}` <@{discord_id}> — Level **{level}**, XP **{xp_total}**")

    await interaction.followup.send("\n".join(lines), ephemeral=True)

@bot.tree.command(name="claim", description="Claim free NNS every hour.")
async def claim(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True, thinking=True)
    role_gate_error = get_role_gate_error(interaction)
    if role_gate_error:
        await interaction.followup.send(role_gate_error, ephemeral=True)
        return

    if int(NNS_CLAIM_ALLOWED_CHANNEL_ID or 0) > 0 and int(interaction.channel_id or 0) != int(NNS_CLAIM_ALLOWED_CHANNEL_ID):
        await interaction.followup.send(
            f"`/claim` can only be used in <#{int(NNS_CLAIM_ALLOWED_CHANNEL_ID)}>.",
            ephemeral=True,
        )
        return

    try:
        res = perform_nns_claim(interaction.user.id)
    except Exception as e:
        await interaction.followup.send(str(e), ephemeral=True)
        return

    amount_sat = int(res["amount_sat"])
    next_allowed_at = int(res["next_allowed_at"])
    level = int(res.get("level") or 1)
    multiplier = str(res.get("multiplier") or "1.00")
    leveled_up = bool(res.get("leveled_up"))
    new_level = int(res.get("new_level") or level)

    msg = (
        f"Claimed ✅ You received **{format_sat_to_nns(amount_sat)} NNS** "
        f"at **Level {level}** (**{multiplier}x**). "
        f"You can claim again <t:{next_allowed_at}:R>."
    )
    if leveled_up:
        msg += f"\n🚀 Level up! You reached **Level {new_level}**."

    await interaction.followup.send(msg, ephemeral=True)

    if interaction.channel is not None:
        try:
            public_msg = (
                f"🎉 {interaction.user.mention} claimed **{format_sat_to_nns(amount_sat)} NNS** "
                f"with `/claim` at **Level {level}** (**{multiplier}x**)."
            )
            if leveled_up:
                public_msg += f" 🚀 They reached **Level {new_level}**!"
            await interaction.channel.send(public_msg)
        except Exception:
            pass


@bot.tree.command(name="deposit", description="Show (or create) your personal NNS deposit address.")
async def deposit(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True, thinking=True)

    con = db()
    try:
        u = get_or_create_user(con, interaction.user.id)
        addr = (u.get("nns_deposit_address") or "").strip()
    finally:
        con.close()

    if addr:
        await interaction.followup.send(
            f"Your NNS deposit address:\n`{addr}`\n\n"
            f"Credits are added to your internal NNS balance after **{NNS_DEPOSIT_CONFS} confirmations**.",
            ephemeral=True,
        )
        return

    try:
        label = f"nns-tipbot:{interaction.user.id}"
        new_addr = await nns_rpc_call("getnewaddress", [label])
        if not isinstance(new_addr, str) or not new_addr.strip():
            raise RuntimeError("getnewaddress returned an invalid address")
        new_addr = new_addr.strip()
    except Exception as e:
        await interaction.followup.send(f"Could not create NNS deposit address ❌ `{e}`", ephemeral=True)
        return

    con2 = db()
    try:
        con2.execute("BEGIN IMMEDIATE;")
        get_or_create_user(con2, interaction.user.id)
        con2.execute(
            "UPDATE users SET nns_deposit_address=?, updated_at=? WHERE discord_id=?",
            (new_addr, now_ts(), interaction.user.id),
        )
        con2.execute("COMMIT;")
    except Exception:
        try:
            con2.execute("ROLLBACK;")
        except Exception:
            pass
        raise
    finally:
        con2.close()

    await interaction.followup.send(
        f"Created your NNS deposit address ✅\n`{new_addr}`\n\n"
        f"Credits are added to your internal NNS balance after **{NNS_DEPOSIT_CONFS} confirmations**.",
        ephemeral=True,
    )


@bot.tree.command(name="withdraw", description="Request an on-chain NNS withdrawal from your internal balance.")
@app_commands.describe(to_address="Destination NNS address", amount="Amount (NNS, up to 8 decimals)")
async def withdraw(interaction: discord.Interaction, to_address: str, amount: str):
    await interaction.response.defer(ephemeral=True, thinking=True)

    to_address = (to_address or "").strip()
    if not to_address or len(to_address) < 20:
        await interaction.followup.send("Invalid NNS address.", ephemeral=True)
        return

    con_chk = db()
    try:
        row = con_chk.execute(
            "SELECT discord_id FROM users WHERE nns_deposit_address IS NOT NULL AND TRIM(nns_deposit_address)=? LIMIT 1",
            (to_address,),
        ).fetchone()
    finally:
        try:
            con_chk.close()
        except Exception:
            pass

    if row is not None:
        await interaction.followup.send(
            "Safety check: you cannot withdraw to a bot-managed NNS deposit address. "
            "Please withdraw to an external NNS address you control.",
            ephemeral=True,
        )
        return

    try:
        amt_sat = parse_nns_to_sat(amount)
    except Exception as e:
        await interaction.followup.send(f"Invalid amount: `{e}`", ephemeral=True)
        return

    fee_sat = 0
    if int(NNS_WITHDRAW_FEE_BPS) > 0:
        if not NNS_WITHDRAW_FEE_ADDRESS:
            await interaction.followup.send(
                "Bot misconfigured: NNS withdrawal fee is enabled but NNS_WITHDRAW_FEE_ADDRESS is not set.",
                ephemeral=True,
            )
            return
        fee_sat = compute_nns_withdraw_fee_sat(amt_sat)

    net_sat = int(amt_sat) - int(fee_sat)
    if net_sat <= 0:
        await interaction.followup.send(
            "Amount too small after fee. Please enter a larger withdrawal amount.",
            ephemeral=True,
        )
        return

    if net_sat < int(NNS_MIN_WITHDRAW_SAT):
        await interaction.followup.send(
            f"Minimum NNS withdraw (after fee) is **{format_sat_to_nns(NNS_MIN_WITHDRAW_SAT)}** NNS.",
            ephemeral=True,
        )
        return

    con = db()
    try:
        con.execute("BEGIN IMMEDIATE;")
        u = get_or_create_user(con, interaction.user.id)
        bal_sat = int(u.get("nns_internal_sat") or 0)

        if has_pending_nns_withdraw(con, interaction.user.id):
            con.execute("ROLLBACK;")
            await interaction.followup.send(
                "You already have a pending NNS withdrawal. Please wait until it is processed.",
                ephemeral=True,
            )
            return

        if bal_sat < amt_sat:
            con.execute("ROLLBACK;")
            await interaction.followup.send(
                f"Insufficient NNS balance. You have **{format_sat_to_nns(bal_sat)}** NNS.",
                ephemeral=True,
            )
            return

        con.execute(
            "UPDATE users SET nns_internal_sat = nns_internal_sat - ?, updated_at=? WHERE discord_id=?",
            (amt_sat, now_ts(), interaction.user.id),
        )

        cur = con.execute(
            "INSERT INTO nns_withdrawals(ts, discord_id, to_address, amount_sat, fee_sat, txid, status, error) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (now_ts(), interaction.user.id, to_address, int(net_sat), int(fee_sat), None, "pending", None),
        )
        wid = int(cur.lastrowid)

        con.execute("COMMIT;")

    except Exception:
        try:
            con.execute("ROLLBACK;")
        except Exception:
            pass
        raise
    finally:
        con.close()

    status = "pending"
    con_s = db()
    try:
        row_s = con_s.execute(
            "SELECT status, txid FROM nns_withdrawals WHERE id=?",
            (wid,),
        ).fetchone()
        if row_s:
            status = str(row_s[0] or "pending")
            txid = (row_s[1] or "").strip() or None
    finally:
        try:
            con_s.close()
        except Exception:
            pass

    fee_line = ""
    if int(fee_sat) > 0:
        fee_line = f"• Fee (admin): **{format_sat_to_nns(fee_sat)} NNS**\n"

    await interaction.followup.send(
        f"Withdrawal queued ✅\n"
        f"• ID: `{wid}`\n"
        f"• Status: `{status}` (txid will appear after broadcast)\n"
        f"• You receive: **{format_sat_to_nns(net_sat)} NNS**\n"
        + fee_line +
        f"• Total debited: **{format_sat_to_nns(amt_sat)} NNS**\n"
        f"• To: `{to_address}`\n\n"
        f"Use `/withdraw_status {wid}` to check status/txid later.",
        ephemeral=True,
    )


@bot.tree.command(name="withdraw_status", description="Check status/txid of a NNS withdrawal request.")
@app_commands.describe(withdraw_id="Withdrawal ID from /withdraw")
async def withdraw_status(interaction: discord.Interaction, withdraw_id: int):
    await interaction.response.defer(ephemeral=True, thinking=True)

    wid = int(withdraw_id)
    con = db()
    try:
        row = con.execute(
            "SELECT ts, discord_id, to_address, amount_sat, fee_sat, txid, status, error "
            "FROM nns_withdrawals WHERE id=?",
            (wid,),
        ).fetchone()
        if not row:
            await interaction.followup.send("Not found.", ephemeral=True)
            return

        ts, did, to_addr, amt_sat, fee_sat, txid, status, err = row
        if int(did) != int(interaction.user.id):
            await interaction.followup.send("Not found.", ephemeral=True)
            return

        txid = (txid or "").strip()
        status = str(status or "pending")
        err = (err or "").strip()

        lines = [
            f"Withdrawal `{wid}`",
            f"• Status: `{status}`",
            f"• To: `{to_addr}`",
            f"• Amount (user): **{format_sat_to_nns(int(amt_sat))} NNS**",
        ]
        if int(fee_sat or 0) > 0:
            lines.append(f"• Fee (admin): **{format_sat_to_nns(int(fee_sat))} NNS**")
        if txid:
            lines.append(f"• Txid: `{txid}`")
        if ts:
            try:
                lines.append(f"• Created: <t:{int(ts)}:R>")
            except Exception:
                pass
        if err and status == "failed":
            lines.append(f"• Error: `{err[:300]}`")

        await interaction.followup.send("\n".join(lines), ephemeral=True)
    finally:
        try:
            con.close()
        except Exception:
            pass


@bot.tree.command(name="tip", description="Tip internal NNS to another user.")
@app_commands.describe(user="Recipient", amount="Amount to tip in NNS (up to 8 decimals)", note="Optional note")
async def tip(interaction: discord.Interaction, user: discord.User, amount: str, note: Optional[str] = None):
    await interaction.response.defer(ephemeral=True, thinking=True)

    role_gate_error = get_role_gate_error(interaction)
    if role_gate_error:
        await interaction.followup.send(role_gate_error, ephemeral=True)
        return

    if is_tip_blocked_recipient(int(user.id)):
        await interaction.followup.send(
            "Tipping to this account is disabled. Please use another destination.",
            ephemeral=True,
        )
        return

    try:
        amount_sat = parse_nns_to_sat(amount)
        perform_nns_tip(interaction.user.id, user.id, amount_sat, note)
        xp_info = {
            "leveled_up": False,
            "new_level": 1,
            "cap_reached": False,
            "daily_count": 0,
            "daily_cap": NNS_XP_TIP_DAILY_CAP,
            "xp_skipped_reason": "below_min" if int(amount_sat) < int(NNS_XP_MIN_TIP_SAT) else "none",
        }
        if int(amount_sat) >= int(NNS_XP_MIN_TIP_SAT):
            con_xp = db()
            try:
                con_xp.execute("BEGIN IMMEDIATE;")
                xp_info = grant_nns_xp_capped(
                    con_xp,
                    interaction.user.id,
                    "tip_nns",
                    get_tip_xp_amount_for_recipient(int(user.id)),
                    NNS_XP_TIP_DAILY_CAP,
                    f"tip to {int(user.id)}",
                )
                con_xp.execute("COMMIT;")
            except Exception as xp_err:
                try:
                    con_xp.execute("ROLLBACK;")
                except Exception:
                    pass
                print(f"[nns_tipbot] tip XP grant failed for user={int(interaction.user.id)}: {xp_err}")
            finally:
                con_xp.close()
    except Exception as e:
        await interaction.followup.send(str(e), ephemeral=True)
        return

    if PUBLIC_TIP_ANNOUNCEMENTS and interaction.channel is not None:
        try:
            note_txt = f" — {note}" if note else ""
            await interaction.channel.send(
                f"💸 {interaction.user.mention} tipped {user.mention} **{format_sat_to_nns(amount_sat)}** NNS{note_txt}"
            )
        except Exception:
            pass

    tip_msg = f"Tip sent ✅ You tipped {user.mention} **{format_sat_to_nns(amount_sat)}** NNS."
    if int(NNS_XP_DONATION_BOT_USER_ID or 0) > 0 and int(user.id) == int(NNS_XP_DONATION_BOT_USER_ID) and int(amount_sat) >= int(NNS_XP_MIN_TIP_SAT):
        tip_msg += "\n🎁 Donation bonus: **double XP** awarded for tipping the donation bot."
    if xp_info.get("leveled_up"):
        tip_msg += f"\n🚀 Level up! You reached **Level {int(xp_info['new_level'])}**."
    elif xp_info.get("cap_reached"):
        tip_msg += f"\nℹ️ Tip XP cap reached for today (**{int(xp_info.get('daily_cap') or 0)}x/day**)."
    elif xp_info.get("xp_skipped_reason") == "below_min":
        tip_msg += f"\nℹ️ No XP awarded. Minimum for tip XP is **10 NNS**."
    await interaction.followup.send(tip_msg, ephemeral=True)


@bot.tree.command(name="multitip", description="Tip the same NNS amount to multiple users.")
@app_commands.describe(
    amount="Amount of NNS to tip EACH user (up to 8 decimals)",
    users="Space-separated @mentions or user IDs (e.g. @a @b @c)",
    note="Optional note"
)
async def multitip(interaction: discord.Interaction, amount: str, users: str, note: Optional[str] = None):
    await interaction.response.defer(ephemeral=True, thinking=True)

    role_gate_error = get_role_gate_error(interaction)
    if role_gate_error:
        await interaction.followup.send(role_gate_error, ephemeral=True)
        return

    try:
        amount_sat = parse_nns_to_sat(amount)
    except Exception as e:
        await interaction.followup.send(f"Invalid NNS amount: `{e}`", ephemeral=True)
        return

    raw = (users or "").strip()
    if not raw:
        await interaction.followup.send("Please provide at least one recipient (@mention or user id).", ephemeral=True)
        return

    ids: List[int] = []
    for tok in raw.split():
        t = tok.strip()
        if not t:
            continue
        if t.startswith("<@") and t.endswith(">"):
            t2 = t[2:-1]
            if t2.startswith("!"):
                t2 = t2[1:]
            if t2.isdigit():
                ids.append(int(t2))
            continue
        if t.isdigit():
            ids.append(int(t))

    seen = set()
    uniq_ids: List[int] = []
    for did in ids:
        if did not in seen:
            seen.add(did)
            uniq_ids.append(did)

    uniq_ids = [did for did in uniq_ids if did != interaction.user.id]

    blocked_ids = [did for did in uniq_ids if is_tip_blocked_recipient(did)]
    if blocked_ids:
        blocked_mentions = " ".join(f"<@{did}>" for did in blocked_ids)
        await interaction.followup.send(
            f"Multi-tip blocked. These recipients cannot receive tips: {blocked_mentions}",
            ephemeral=True,
        )
        return

    if not uniq_ids:
        await interaction.followup.send("No valid recipients found (or you only included yourself).", ephemeral=True)
        return

    MAX_RECIPIENTS = 10
    if len(uniq_ids) > MAX_RECIPIENTS:
        await interaction.followup.send(
            f"Too many recipients. Max is {MAX_RECIPIENTS} per /multitip.",
            ephemeral=True,
        )
        return

    total_sat = int(amount_sat) * len(uniq_ids)

    con = db()
    try:
        con.execute("BEGIN IMMEDIATE;")

        sender = get_or_create_user(con, interaction.user.id)
        sender_bal_sat = int(sender.get("nns_internal_sat") or 0)
        if sender_bal_sat < total_sat:
            con.execute("ROLLBACK;")
            await interaction.followup.send(
                f"Insufficient NNS balance. You have **{format_sat_to_nns(sender_bal_sat)}** NNS but need **{format_sat_to_nns(total_sat)}** NNS.",
                ephemeral=True,
            )
            return

        ts = now_ts()

        con.execute(
            "UPDATE users SET nns_internal_sat = nns_internal_sat - ?, updated_at=? WHERE discord_id=?",
            (total_sat, ts, interaction.user.id),
        )

        for rid in uniq_ids:
            get_or_create_user(con, rid)
            con.execute(
                "UPDATE users SET nns_internal_sat = nns_internal_sat + ?, updated_at=? WHERE discord_id=?",
                (int(amount_sat), ts, rid),
            )
            con.execute(
                "INSERT INTO tx_log(ts, type, from_id, to_id, amount, note, status) VALUES(?,?,?,?,?,?,?)",
                (ts, "tip_nns", interaction.user.id, rid, int(amount_sat), (note or ""), "ok"),
            )

        con.execute("COMMIT;")

        xp_info = {
            "leveled_up": False,
            "new_level": 1,
            "cap_reached": False,
            "daily_count": 0,
            "daily_cap": NNS_XP_MULTITIP_DAILY_CAP,
            "xp_skipped_reason": "below_min" if int(amount_sat) < int(NNS_XP_MIN_TIP_SAT) else "none",
        }
        if int(amount_sat) >= int(NNS_XP_MIN_TIP_SAT):
            con_xp = db()
            try:
                con_xp.execute("BEGIN IMMEDIATE;")
                xp_info = grant_nns_xp_capped(
                    con_xp,
                    interaction.user.id,
                    "multitip_nns",
                    (int(NNS_XP_MULTITIP) * 2) if (int(NNS_XP_DONATION_BOT_USER_ID or 0) > 0 and int(NNS_XP_DONATION_BOT_USER_ID) in uniq_ids) else NNS_XP_MULTITIP,
                    NNS_XP_MULTITIP_DAILY_CAP,
                    f"multitip recipients={len(uniq_ids)}",
                )
                con_xp.execute("COMMIT;")
            except Exception as xp_err:
                try:
                    con_xp.execute("ROLLBACK;")
                except Exception:
                    pass
                print(f"[nns_tipbot] multitip XP grant failed for user={int(interaction.user.id)}: {xp_err}")
            finally:
                con_xp.close()

    except Exception as e:
        try:
            con.execute("ROLLBACK;")
        except Exception:
            pass
        await interaction.followup.send(f"Multi-tip failed ❌ {e}", ephemeral=True)
        return
    finally:
        con.close()

    mention_list = " ".join([f"<@{rid}>" for rid in uniq_ids])

    if PUBLIC_TIP_ANNOUNCEMENTS and interaction.channel is not None:
        try:
            note_txt = f" — {note}" if note else ""
            await interaction.channel.send(
                f"💸 {interaction.user.mention} multi-tipped **{format_sat_to_nns(amount_sat)}** NNS to {mention_list} "
                f"(total {format_sat_to_nns(total_sat)} NNS){note_txt}"
            )
        except Exception:
            pass

    multitip_msg = (
        f"Multi-tip sent ✅\n"
        f"Each: **{format_sat_to_nns(amount_sat)}** NNS\n"
        f"Recipients ({len(uniq_ids)}): {mention_list}\n"
        f"Total: **{format_sat_to_nns(total_sat)}** NNS"
    )
    if int(NNS_XP_DONATION_BOT_USER_ID or 0) > 0 and int(NNS_XP_DONATION_BOT_USER_ID) in uniq_ids and int(amount_sat) >= int(NNS_XP_MIN_TIP_SAT):
        multitip_msg += "\n🎁 Donation bonus: **double XP** awarded because the donation bot was included."
    if xp_info.get("leveled_up"):
        multitip_msg += f"\n🚀 Level up! You reached **Level {int(xp_info['new_level'])}**."
    elif xp_info.get("cap_reached"):
        multitip_msg += f"\nℹ️ Multi-tip XP cap reached for today (**{int(xp_info.get('daily_cap') or 0)}x/day**)."
    elif xp_info.get("xp_skipped_reason") == "below_min":
        multitip_msg += f"\nℹ️ No XP awarded. Minimum for multi-tip XP is **10 NNS** per recipient."
    await interaction.followup.send(multitip_msg, ephemeral=True)


@bot.tree.command(name="start_airdrop", description="Start a button-based NNS airdrop from your internal balance.")
@app_commands.describe(
    per_user="Amount of NNS each user receives",
    limit="Maximum number of claims",
    duration_min="Optional duration in minutes"
)
async def start_airdrop(interaction: discord.Interaction, per_user: str, limit: int, duration_min: Optional[int] = None):
    await interaction.response.defer(ephemeral=True, thinking=True)
    role_gate_error = get_role_gate_error(interaction)
    if role_gate_error:
        await interaction.followup.send(role_gate_error, ephemeral=True)
        return

    try:
        per_user_sat = parse_nns_to_sat(per_user)
    except Exception as e:
        await interaction.followup.send(f"Invalid NNS amount: `{e}`", ephemeral=True)
        return

    if limit <= 0:
        await interaction.followup.send("limit must be > 0", ephemeral=True)
        return
    if limit > int(NNS_AIRDROP_MAX_RECIPIENTS):
        await interaction.followup.send(
            f"limit too large. Max is {int(NNS_AIRDROP_MAX_RECIPIENTS)}.",
            ephemeral=True,
        )
        return

    if duration_min is None:
        duration_min = int(NNS_AIRDROP_DEFAULT_DURATION_MIN)
    if duration_min <= 0:
        await interaction.followup.send("duration_min must be > 0", ephemeral=True)
        return

    if interaction.channel is None:
        await interaction.followup.send("This command must be used in a channel.", ephemeral=True)
        return

    total_sat = int(per_user_sat) * int(limit)
    airdrop_id = 0

    con = db()
    try:
        con.execute("BEGIN IMMEDIATE;")
        u = get_or_create_user(con, interaction.user.id)
        bal_sat = int(u.get("nns_internal_sat") or 0)
        if bal_sat < total_sat:
            con.execute("ROLLBACK;")
            await interaction.followup.send(
                f"Insufficient NNS balance. You need **{format_sat_to_nns(total_sat)} NNS** but have **{format_sat_to_nns(bal_sat)} NNS**.",
                ephemeral=True,
            )
            return

        ts = now_ts()
        expires_at = ts + int(duration_min) * 60

        con.execute(
            "UPDATE users SET nns_internal_sat = nns_internal_sat - ?, updated_at=? WHERE discord_id=?",
            (total_sat, ts, int(interaction.user.id))
        )
        cur = con.execute(
            "INSERT INTO nns_airdrops(creator_discord_id, guild_id, channel_id, message_id, per_user_sat, limit_count, claimed_count, remaining_sat, expires_at, status, created_at, ended_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                int(interaction.user.id),
                int(interaction.guild_id) if interaction.guild_id else None,
                int(interaction.channel_id) if interaction.channel_id else None,
                None,
                int(per_user_sat),
                int(limit),
                0,
                int(total_sat),
                int(expires_at),
                "active",
                ts,
                None,
            )
        )
        airdrop_id = int(cur.lastrowid)
        con.execute(
            "INSERT INTO tx_log(ts, type, from_id, to_id, amount, note, status) VALUES(?,?,?,?,?,?,?)",
            (ts, "airdrop_fund_nns", int(interaction.user.id), None, total_sat, f"airdrop #{airdrop_id}", "ok")
        )
        con.execute("COMMIT;")
    except Exception:
        try:
            con.execute("ROLLBACK;")
        except Exception:
            pass
        raise
    finally:
        con.close()

    embed = discord.Embed(
        title="🎁 NNS Airdrop",
        description=(
            f"Host: {interaction.user.mention}\n"
            f"Per user: **{format_sat_to_nns(per_user_sat)} NNS**\n"
            f"Claims: **0/{int(limit)}**\n"
            f"Remaining: **{format_sat_to_nns(total_sat)} NNS**\n"
            f"Ends in: **{fmt_duration_compact(int(duration_min) * 60)}**"
        ),
    )
    view = AirdropClaimView(int(airdrop_id))

    try:
        msg = await interaction.channel.send(embed=embed, view=view)
    except Exception as e:
        con_refund = db()
        try:
            con_refund.execute("BEGIN IMMEDIATE;")
            row = con_refund.execute(
                "SELECT creator_discord_id, remaining_sat, status FROM nns_airdrops WHERE id=?",
                (int(airdrop_id),)
            ).fetchone()
            if row and str(row[2] or "") == "active":
                creator_id = int(row[0])
                refund_sat = int(row[1] or 0)
                ts = now_ts()
                if refund_sat > 0:
                    get_or_create_user(con_refund, creator_id)
                    con_refund.execute(
                        "UPDATE users SET nns_internal_sat = nns_internal_sat + ?, updated_at=? WHERE discord_id=?",
                        (refund_sat, ts, creator_id)
                    )
                    con_refund.execute(
                        "INSERT INTO tx_log(ts, type, from_id, to_id, amount, note, status) VALUES(?,?,?,?,?,?,?)",
                        (ts, "airdrop_refund_nns", None, creator_id, refund_sat, f"message post failed for airdrop #{int(airdrop_id)}", "ok")
                    )
                con_refund.execute(
                    "UPDATE nns_airdrops SET status='failed', remaining_sat=0, ended_at=? WHERE id=?",
                    (ts, int(airdrop_id))
                )
            con_refund.execute("COMMIT;")
        except Exception:
            try:
                con_refund.execute("ROLLBACK;")
            except Exception:
                pass
            raise
        finally:
            con_refund.close()

        await interaction.followup.send(
            f"Airdrop creation failed ❌ The reserved NNS was refunded. Error: `{e}`",
            ephemeral=True,
        )
        return

    con2 = db()
    try:
        con2.execute(
            "UPDATE nns_airdrops SET channel_id=?, message_id=? WHERE id=?",
            (int(msg.channel.id), int(msg.id), int(airdrop_id))
        )
    finally:
        con2.close()

    xp_info = {
        "leveled_up": False,
        "new_level": 1,
        "cap_reached": False,
        "daily_cap": NNS_XP_AIRDROP_CREATE_DAILY_CAP,
    }
    con_xp = db()
    try:
        con_xp.execute("BEGIN IMMEDIATE;")
        xp_info = grant_nns_xp_capped(
            con_xp,
            interaction.user.id,
            "airdrop_create_nns",
            NNS_XP_AIRDROP_CREATE,
            NNS_XP_AIRDROP_CREATE_DAILY_CAP,
            f"airdrop #{airdrop_id}",
        )
        con_xp.execute("COMMIT;")
    except Exception as xp_err:
        try:
            con_xp.execute("ROLLBACK;")
        except Exception:
            pass
        print(f"[nns_tipbot] airdrop create XP grant failed for user={int(interaction.user.id)} airdrop_id={airdrop_id}: {xp_err}")
    finally:
        con_xp.close()

    airdrop_msg = f"Airdrop started ✅ Posted airdrop #{airdrop_id} with **{format_sat_to_nns(total_sat)} NNS** reserved."
    if xp_info.get("leveled_up"):
        airdrop_msg += f"\n🚀 Level up! You reached **Level {int(xp_info['new_level'])}**."
    elif xp_info.get("cap_reached"):
        airdrop_msg += f"\nℹ️ Airdrop-create XP cap reached for today (**{int(xp_info.get('daily_cap') or 0)}x/day**)."
    await interaction.followup.send(airdrop_msg, ephemeral=True)


@bot.tree.command(name="list_airdrops", description="List active NNS airdrops.")
async def list_airdrops(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True, thinking=True)

    con = db()
    try:
        con.row_factory = sqlite3.Row
        rows = con.execute(
            "SELECT * FROM nns_airdrops WHERE status='active' AND remaining_sat > 0 ORDER BY created_at DESC LIMIT 20"
        ).fetchall()
    finally:
        con.close()

    now = now_ts()
    lines: List[str] = []
    for row in rows:
        expires_at = int(row["expires_at"] or 0) if row["expires_at"] is not None else 0
        remaining_sat = int(row["remaining_sat"] or 0)
        if expires_at and now >= expires_at:
            continue
        if remaining_sat <= 0:
            continue
        rem = max(0, expires_at - now) if expires_at else 0
        lines.append(
            f"`#{int(row['id'])}` • **{format_sat_to_nns(int(row['per_user_sat']))} NNS** each • "
            f"claims **{int(row['claimed_count'])}/{int(row['limit_count'])}** • "
            f"remaining **{format_sat_to_nns(remaining_sat)} NNS** • "
            f"ends in **{fmt_duration_compact(rem)}**"
        )

    if not lines:
        await interaction.followup.send("No active airdrops.", ephemeral=True)
        return

    await interaction.followup.send("\n".join(lines), ephemeral=True)


@bot.tree.command(name="end_airdrop", description="End your NNS airdrop and refund the remaining amount.")
@app_commands.describe(airdrop_id="Airdrop id")
async def end_airdrop(interaction: discord.Interaction, airdrop_id: int):
    await interaction.response.defer(ephemeral=True, thinking=True)
    role_gate_error = get_role_gate_error(interaction)
    if role_gate_error:
        await interaction.followup.send(role_gate_error, ephemeral=True)
        return

    con = db()
    refund_sat = 0
    try:
        con.row_factory = sqlite3.Row
        con.execute("BEGIN IMMEDIATE;")
        row = con.execute(
            "SELECT * FROM nns_airdrops WHERE id=?",
            (int(airdrop_id),)
        ).fetchone()
        if not row:
            con.execute("ROLLBACK;")
            await interaction.followup.send("Airdrop not found.", ephemeral=True)
            return

        creator_id = int(row["creator_discord_id"])
        if creator_id != int(interaction.user.id) and not is_admin(interaction, con):
            con.execute("ROLLBACK;")
            await interaction.followup.send("You can only end your own airdrop.", ephemeral=True)
            return

        if str(row["status"] or "") != "active":
            con.execute("ROLLBACK;")
            await interaction.followup.send("This airdrop is already closed.", ephemeral=True)
            return

        refund_sat = int(row["remaining_sat"] or 0)
        ts = now_ts()
        if refund_sat > 0:
            get_or_create_user(con, creator_id)
            con.execute(
                "UPDATE users SET nns_internal_sat = nns_internal_sat + ?, updated_at=? WHERE discord_id=?",
                (refund_sat, ts, creator_id)
            )
            con.execute(
                "INSERT INTO tx_log(ts, type, from_id, to_id, amount, note, status) VALUES(?,?,?,?,?,?,?)",
                (ts, "airdrop_refund_nns", None, creator_id, refund_sat, f"manual end airdrop #{int(airdrop_id)}", "ok")
            )

        con.execute(
            "UPDATE nns_airdrops SET status='ended', remaining_sat=0, ended_at=? WHERE id=?",
            (ts, int(airdrop_id))
        )
        con.execute("COMMIT;")
    except Exception:
        try:
            con.execute("ROLLBACK;")
        except Exception:
            pass
        raise
    finally:
        con.close()

    await refresh_airdrop_message(bot, int(airdrop_id))
    await interaction.followup.send(
        f"Airdrop ended ✅ Refunded **{format_sat_to_nns(refund_sat)} NNS**.",
        ephemeral=True,
    )


@bot.tree.command(name="stake_balance", description="Show your staked NNS and accrued staking reward.")
async def stake_balance(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True, thinking=True)

    con = db()
    try:
        con.execute("BEGIN IMMEDIATE;")
        stake = accrue_stake_position(con, interaction.user.id, now_ts())
        con.execute("COMMIT;")
    except Exception:
        try:
            con.execute("ROLLBACK;")
        except Exception:
            pass
        raise
    finally:
        con.close()

    await interaction.followup.send(
        f"Staked: **{format_sat_to_nns(int(stake['staked_sat']))} NNS**\n"
        f"Accrued reward: **{format_sat_to_nns(int(stake['accrued_reward_sat']))} NNS**\n"
        f"APR: **{get_current_staking_apr():.4f}%** (auto-adjusted)",
        ephemeral=True,
    )


@bot.tree.command(name="stake", description="Move internal NNS into staking.")
@app_commands.describe(amount="Amount of NNS to stake (up to 8 decimals)")
async def stake(interaction: discord.Interaction, amount: str):
    await interaction.response.defer(ephemeral=True, thinking=True)

    role_gate_error = get_role_gate_error(interaction)
    if role_gate_error:
        await interaction.followup.send(role_gate_error, ephemeral=True)
        return

    if not NNS_STAKING_ENABLED:
        await interaction.followup.send("Staking is currently disabled.", ephemeral=True)
        return

    try:
        amount_sat = parse_nns_to_sat(amount)
    except Exception as e:
        await interaction.followup.send(f"Invalid NNS amount: `{e}`", ephemeral=True)
        return

    con = db()
    try:
        con.execute("BEGIN IMMEDIATE;")
        user_row = get_or_create_user(con, interaction.user.id)
        stake_row = accrue_stake_position(con, interaction.user.id, now_ts())

        liquid_sat = int(user_row.get("nns_internal_sat") or 0)
        if liquid_sat < amount_sat:
            con.execute("ROLLBACK;")
            await interaction.followup.send(
                f"Insufficient NNS balance. You have **{format_sat_to_nns(liquid_sat)} NNS**.",
                ephemeral=True,
            )
            return

        ts = now_ts()
        new_staked_sat = int(stake_row.get("staked_sat") or 0) + int(amount_sat)

        con.execute(
            "UPDATE users SET nns_internal_sat = nns_internal_sat - ?, updated_at=? WHERE discord_id=?",
            (int(amount_sat), ts, int(interaction.user.id))
        )
        con.execute(
            "UPDATE nns_stakes SET staked_sat=?, updated_at=? WHERE discord_id=?",
            (int(new_staked_sat), ts, int(interaction.user.id))
        )
        con.execute(
            "INSERT INTO tx_log(ts, type, from_id, to_id, amount, note, status) VALUES(?,?,?,?,?,?,?)",
            (ts, "stake_nns", int(interaction.user.id), None, int(amount_sat), "stake", "ok")
        )
        con.execute("COMMIT;")
    except Exception:
        try:
            con.execute("ROLLBACK;")
        except Exception:
            pass
        raise
    finally:
        con.close()

    xp_info = {
        "leveled_up": False,
        "new_level": 1,
        "cap_reached": False,
        "daily_cap": NNS_XP_STAKE_DAILY_CAP,
        "xp_skipped_reason": "below_min" if int(amount_sat) < int(NNS_XP_MIN_STAKE_SAT) else "none",
    }
    if int(amount_sat) >= int(NNS_XP_MIN_STAKE_SAT):
        con_xp = db()
        try:
            con_xp.execute("BEGIN IMMEDIATE;")
            xp_info = grant_nns_xp_capped(
                con_xp,
                interaction.user.id,
                "stake_nns",
                NNS_XP_STAKE,
                NNS_XP_STAKE_DAILY_CAP,
                f"stake {amount_sat}",
            )
            con_xp.execute("COMMIT;")
        except Exception as xp_err:
            try:
                con_xp.execute("ROLLBACK;")
            except Exception:
                pass
            print(f"[nns_tipbot] stake XP grant failed for user={int(interaction.user.id)}: {xp_err}")
        finally:
            con_xp.close()

    stake_msg = f"Staked ✅ Moved **{format_sat_to_nns(amount_sat)} NNS** into staking."
    if xp_info.get("leveled_up"):
        stake_msg += f"\n🚀 Level up! You reached **Level {int(xp_info['new_level'])}**."
    elif xp_info.get("cap_reached"):
        stake_msg += f"\nℹ️ Stake XP cap reached for today (**{int(xp_info.get('daily_cap') or 0)}x/day**)."
    elif xp_info.get("xp_skipped_reason") == "below_min":
        stake_msg += f"\nℹ️ No XP awarded. Minimum for stake XP is **10 NNS**."
    await interaction.followup.send(stake_msg, ephemeral=True)


@bot.tree.command(name="unstake", description="Unstake NNS and claim the proportional accrued reward.")
@app_commands.describe(amount="Amount of NNS to unstake, or 'all'")
async def unstake(interaction: discord.Interaction, amount: str):
    await interaction.response.defer(ephemeral=True, thinking=True)

    if not NNS_STAKING_ENABLED:
        await interaction.followup.send("Staking is currently disabled.", ephemeral=True)
        return

    amount_raw = (amount or "").strip().lower()

    con = db()
    try:
        con.execute("BEGIN IMMEDIATE;")
        get_or_create_user(con, interaction.user.id)
        stake_row = accrue_stake_position(con, interaction.user.id, now_ts())

        current_staked_sat = int(stake_row.get("staked_sat") or 0)
        current_reward_sat = int(stake_row.get("accrued_reward_sat") or 0)

        if current_staked_sat <= 0:
            con.execute("ROLLBACK;")
            await interaction.followup.send("You currently have no staked NNS.", ephemeral=True)
            return

        if amount_raw == "all":
            unstake_sat = current_staked_sat
        else:
            try:
                unstake_sat = parse_nns_to_sat(amount_raw)
            except Exception as e:
                con.execute("ROLLBACK;")
                await interaction.followup.send(f"Invalid NNS amount: `{e}`", ephemeral=True)
                return

        if unstake_sat <= 0 or unstake_sat > current_staked_sat:
            con.execute("ROLLBACK;")
            await interaction.followup.send(
                f"Invalid unstake amount. You currently have **{format_sat_to_nns(current_staked_sat)} NNS** staked.",
                ephemeral=True,
            )
            return

        reward_share_sat = 0
        if current_reward_sat > 0:
            reward_share_sat = (current_reward_sat * int(unstake_sat)) // int(current_staked_sat)

        new_staked_sat = current_staked_sat - int(unstake_sat)
        new_reward_sat = current_reward_sat - int(reward_share_sat)
        ts = now_ts()
        payout_sat = int(unstake_sat) + int(reward_share_sat)

        con.execute(
            "UPDATE users SET nns_internal_sat = nns_internal_sat + ?, updated_at=? WHERE discord_id=?",
            (int(payout_sat), ts, int(interaction.user.id))
        )
        if new_staked_sat <= 0:
            con.execute(
                "UPDATE nns_stakes SET staked_sat=0, accrued_reward_sat=0, reward_remainder='0', last_accrual_ts=?, updated_at=? WHERE discord_id=?",
                (ts, ts, int(interaction.user.id))
            )
        else:
            con.execute(
                "UPDATE nns_stakes SET staked_sat=?, accrued_reward_sat=?, updated_at=? WHERE discord_id=?",
                (int(new_staked_sat), int(new_reward_sat), ts, int(interaction.user.id))
            )
        con.execute(
            "INSERT INTO tx_log(ts, type, from_id, to_id, amount, note, status) VALUES(?,?,?,?,?,?,?)",
            (ts, "unstake_nns", int(interaction.user.id), None, int(unstake_sat), f"unstake reward={int(reward_share_sat)}", "ok")
        )
        con.execute("COMMIT;")
    except Exception:
        try:
            con.execute("ROLLBACK;")
        except Exception:
            pass
        raise
    finally:
        con.close()

    await interaction.followup.send(
        f"Unstaked ✅ Returned **{format_sat_to_nns(unstake_sat)} NNS** and **{format_sat_to_nns(reward_share_sat)} NNS** reward.",
        ephemeral=True,
    )


# New command: claim_staking

@bot.tree.command(name="claim_staking", description="Claim accrued staking rewards without unstaking.")
async def claim_staking(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True, thinking=True)

    role_gate_error = get_role_gate_error(interaction)
    if role_gate_error:
        await interaction.followup.send(role_gate_error, ephemeral=True)
        return

    if not NNS_STAKING_ENABLED:
        await interaction.followup.send("Staking is currently disabled.", ephemeral=True)
        return

    con = db()
    try:
        con.execute("BEGIN IMMEDIATE;")
        get_or_create_user(con, interaction.user.id)
        stake_row = accrue_stake_position(con, interaction.user.id, now_ts())

        current_staked_sat = int(stake_row.get("staked_sat") or 0)
        current_reward_sat = int(stake_row.get("accrued_reward_sat") or 0)

        if current_staked_sat <= 0:
            con.execute("ROLLBACK;")
            await interaction.followup.send("You currently have no staked NNS.", ephemeral=True)
            return

        if current_reward_sat <= 0:
            con.execute("ROLLBACK;")
            await interaction.followup.send("No staking reward available to claim yet.", ephemeral=True)
            return

        ts = now_ts()
        con.execute(
            "UPDATE users SET nns_internal_sat = nns_internal_sat + ?, updated_at=? WHERE discord_id=?",
            (int(current_reward_sat), ts, int(interaction.user.id))
        )
        con.execute(
            "UPDATE nns_stakes SET accrued_reward_sat=0, updated_at=? WHERE discord_id=?",
            (ts, int(interaction.user.id))
        )
        con.execute(
            "INSERT INTO tx_log(ts, type, from_id, to_id, amount, note, status) VALUES(?,?,?,?,?,?,?)",
            (ts, "claim_staking_nns", int(interaction.user.id), None, int(current_reward_sat), "claim staking reward", "ok")
        )
        con.execute("COMMIT;")
    except Exception:
        try:
            con.execute("ROLLBACK;")
        except Exception:
            pass
        raise
    finally:
        con.close()

    xp_info = {
        "leveled_up": False,
        "new_level": 1,
        "cap_reached": False,
        "daily_cap": NNS_XP_CLAIM_STAKING_DAILY_CAP,
        "xp_skipped_reason": "below_min" if int(current_reward_sat) < int(NNS_XP_MIN_STAKE_SAT) else "none",
    }
    if int(current_reward_sat) >= int(NNS_XP_MIN_STAKE_SAT):
        con_xp = db()
        try:
            con_xp.execute("BEGIN IMMEDIATE;")
            xp_info = grant_nns_xp_capped(
                con_xp,
                interaction.user.id,
                "claim_staking_nns",
                NNS_XP_CLAIM_STAKING,
                NNS_XP_CLAIM_STAKING_DAILY_CAP,
                f"claim staking {current_reward_sat}",
            )
            con_xp.execute("COMMIT;")
        except Exception as xp_err:
            try:
                con_xp.execute("ROLLBACK;")
            except Exception:
                pass
            print(f"[nns_tipbot] claim_staking XP grant failed for user={int(interaction.user.id)}: {xp_err}")
        finally:
            con_xp.close()

    claim_staking_msg = f"Staking reward claimed ✅ Received **{format_sat_to_nns(current_reward_sat)} NNS** without unstaking."
    if xp_info.get("leveled_up"):
        claim_staking_msg += f"\n🚀 Level up! You reached **Level {int(xp_info['new_level'])}**."
    elif xp_info.get("cap_reached"):
        claim_staking_msg += f"\nℹ️ Claim-staking XP cap reached for today (**{int(xp_info.get('daily_cap') or 0)}x/day**)."
    elif xp_info.get("xp_skipped_reason") == "below_min":
        claim_staking_msg += f"\nℹ️ No XP awarded. Minimum for claim-staking XP is **10 NNS**."
    await interaction.followup.send(claim_staking_msg, ephemeral=True)


def main():
    if not DISCORD_TOKEN:
        raise SystemExit("Missing NNS_TIPBOT_DISCORD_TOKEN")
    bot.run(DISCORD_TOKEN)


if __name__ == "__main__":
    main()