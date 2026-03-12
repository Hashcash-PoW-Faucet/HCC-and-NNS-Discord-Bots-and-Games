import os
import re
import json
import time
import sqlite3
from typing import Optional, Dict, Any, Tuple, List

from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from dataclasses import dataclass

from dotenv import load_dotenv

import discord
from discord import app_commands
import aiohttp

import base64

# Optional encryption (recommended). If unavailable or key missing, we fall back to plaintext.
try:
    from cryptography.fernet import Fernet  # type: ignore
except Exception:
    Fernet = None  # type: ignore

load_dotenv()


# ---------------------------
# Constants
# ---------------------------
VECO_SATS = 100_000_000  # 1 VECO = 1e8 sats


# --- VECO helpers (integer-only: sats) ---
def format_sat_to_veco(sat: int) -> str:
    return f"{(Decimal(int(sat)) / VECO_SATS):.8f}"


def parse_veco_to_sat(s: str) -> int:
    s = (s or "").strip()
    if not s:
        raise ValueError("missing amount")
    d = Decimal(s).quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)
    sat = int(d * VECO_SATS)
    if sat <= 0:
        raise ValueError("amount must be > 0")
    return sat


#
# ---------------------------
# Config
# ---------------------------
DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN", "").strip()

FAUCET_API_BASE = os.environ.get("FAUCET_API_BASE", "http://127.0.0.1:8000").rstrip("/")
TIPBOT_TREASURY_SECRET = os.environ.get("TIPBOT_TREASURY_SECRET", "").strip()

DB_PATH = os.environ.get("TIPBOT_DB", "tipbot.db").strip()

# VECO RPC (node wallet)
VECO_RPC_URL = os.environ.get("VECO_RPC_URL", "http://127.0.0.1:26920/").strip()
VECO_RPC_USER = os.environ.get("VECO_RPC_USER", "").strip()
VECO_RPC_PASSWORD = os.environ.get("VECO_RPC_PASSWORD", "").strip()
VECO_DEPOSIT_CONFS = int(os.environ.get("VECO_DEPOSIT_CONFS", "6"))
VECO_MIN_WITHDRAW_SAT = parse_veco_to_sat(os.environ.get("VECO_MIN_WITHDRAW", "0.10000"))

# VECO withdraw fee (paid to admin address). Fee is taken from the requested amount.
VECO_WITHDRAW_FEE_BPS = int(os.environ.get("VECO_WITHDRAW_FEE_BPS", "100"))  # 1.00%
VECO_WITHDRAW_FEE_ADDRESS = os.environ.get("VECO_WITHDRAW_FEE_ADDRESS", "").strip()

# Withdraw policy
MIN_WITHDRAW = int(os.environ.get("MIN_WITHDRAW", "1"))
WITHDRAW_COOLDOWN = int(os.environ.get("WITHDRAW_COOLDOWN", "60"))  # seconds
MAX_WITHDRAW_PER_DAY = int(os.environ.get("MAX_WITHDRAW_PER_DAY", "200"))

# Optional: announce tips/withdraws publicly (non-ephemeral)
PUBLIC_TIP_ANNOUNCEMENTS = os.environ.get("PUBLIC_TIP_ANNOUNCEMENTS", "1").strip() == "1"
PUBLIC_WITHDRAW_ANNOUNCEMENTS = os.environ.get("PUBLIC_WITHDRAW_ANNOUNCEMENTS", "1").strip() == "1"
PUBLIC_SHOW_ADDRESS = os.environ.get("PUBLIC_SHOW_ADDRESS", "0").strip() == "1"

# Optional: speed up slash-command sync by limiting to one guild during development
GUILD_ID = os.environ.get("GUILD_ID", "").strip()

# HCC address format (derived from sha256(secret).hexdigest()[:40])
ADDR_RE = re.compile(r"^[0-9a-fA-F]{40}$")

# Hashcash website account linking (C2): store user private keys for one-click deposits.
# Recommended: set TIPBOT_FERNET_KEY (Fernet key). If not set, secrets are stored plaintext.
TIPBOT_FERNET_KEY = os.environ.get("TIPBOT_FERNET_KEY", "").strip()

# ---------------------------
# Faucet (Discord claim into internal HCC balance)
# ---------------------------
FAUCET_AMOUNT = int(os.environ.get("FAUCET_AMOUNT", "4"))
FAUCET_COOLDOWN_SECONDS = int(os.environ.get("FAUCET_COOLDOWN_SECONDS", str(2 * 3600)))
FAUCET_ALLOWED_CHANNEL_ID = os.environ.get("FAUCET_ALLOWED_CHANNEL_ID", "").strip()
PUBLIC_CLAIM_ANNOUNCEMENTS = os.environ.get("PUBLIC_CLAIM_ANNOUNCEMENTS", "1").strip() == "1"
PUBLIC_CLAIM_SHOW_ADDRESS = os.environ.get("PUBLIC_CLAIM_SHOW_ADDRESS", "0").strip() == "1"

# ---------------------------
# AMM Swap (HCC <-> VECO)
# ---------------------------
DEFAULT_POOL_HCC = int(os.environ.get("AMM_INIT_HCC", "40000"))
DEFAULT_POOL_VECO = int(os.environ.get("AMM_INIT_VECO", "20000"))  # VECO (human)
DEFAULT_POOL_FEE_BPS = int(os.environ.get("AMM_FEE_BPS", "75"))    # 0.75%
DEFAULT_SLIPPAGE_BPS = int(os.environ.get("AMM_DEFAULT_SLIPPAGE_BPS", "100"))  # 1.00%
QUOTE_TTL_SECONDS = int(os.environ.get("AMM_QUOTE_TTL", "30"))


#
# ---------------------------
# Helpers
# ---------------------------
def now_ts() -> int:
    return int(time.time())


def day_key(ts: Optional[int] = None) -> str:
    ts = ts or now_ts()
    return time.strftime("%Y-%m-%d", time.gmtime(ts))


# Faucet helpers
def fmt_duration_hms(seconds: int) -> str:
    """Format a duration in seconds as '<h>h <m>m <s>s'."""
    seconds = max(0, int(seconds or 0))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h}h {m}m {s}s"


def channel_allowed(interaction: discord.Interaction) -> bool:
    """Optionally restrict faucet-style commands to a single channel."""
    if not FAUCET_ALLOWED_CHANNEL_ID:
        return True
    try:
        return interaction.channel_id == int(FAUCET_ALLOWED_CHANNEL_ID)
    except Exception:
        return False


def normalize_addr(addr: str) -> str:
    addr = (addr or "").strip()
    if not ADDR_RE.match(addr):
        raise ValueError("bad address format")
    return addr.lower()


@dataclass(frozen=True)
class AmmQuote:
    amount_out: int
    fee_amount: int
    price_impact_bps: int


def quote_hcc_to_veco(amount_in_hcc: int, R_hcc: int, R_veco_sat: int, fee_bps: int) -> AmmQuote:
    if amount_in_hcc <= 0:
        raise ValueError("amount must be > 0")
    if R_hcc <= 0 or R_veco_sat <= 0:
        raise ValueError("pool reserves must be > 0")

    fee = (amount_in_hcc * fee_bps) // 10_000
    dx = amount_in_hcc - fee
    if dx <= 0:
        raise ValueError("amount too small after fee")

    k = R_hcc * R_veco_sat
    new_R_hcc = R_hcc + dx
    new_R_veco = k // new_R_hcc
    out = R_veco_sat - new_R_veco
    if out <= 0:
        raise ValueError("insufficient liquidity")

    # price impact (bps)
    p0 = (R_veco_sat * 1_000_000) // R_hcc
    pe = (out * 1_000_000) // max(1, amount_in_hcc)
    impact_bps = 0
    if p0 > 0 and pe < p0:
        impact_bps = ((p0 - pe) * 10_000) // p0

    return AmmQuote(amount_out=out, fee_amount=fee, price_impact_bps=int(impact_bps))


def quote_veco_to_hcc(amount_in_sat: int, R_hcc: int, R_veco_sat: int, fee_bps: int) -> AmmQuote:
    if amount_in_sat <= 0:
        raise ValueError("amount must be > 0")
    if R_hcc <= 0 or R_veco_sat <= 0:
        raise ValueError("pool reserves must be > 0")

    fee = (amount_in_sat * fee_bps) // 10_000
    dx = amount_in_sat - fee
    if dx <= 0:
        raise ValueError("amount too small after fee")

    k = R_hcc * R_veco_sat
    new_R_veco = R_veco_sat + dx
    new_R_hcc = k // new_R_veco
    out = R_hcc - new_R_hcc
    if out <= 0:
        raise ValueError("insufficient liquidity")

    # price impact (bps) vs spot, excluding fee
    # spot_out = dx * (R_hcc / R_veco_sat)
    impact_bps = 0
    if R_veco_sat > 0 and R_hcc > 0 and out > 0 and dx > 0:
        spot_out = (Decimal(dx) * Decimal(R_hcc)) / Decimal(R_veco_sat)
        if spot_out > 0:
            impact = (spot_out - Decimal(out)) / spot_out
            if impact < 0:
                impact = Decimal("0")
            impact_bps = int((impact * Decimal("10000")).to_integral_value(rounding=ROUND_HALF_UP))
            if impact_bps < 0:
                impact_bps = 0

    return AmmQuote(amount_out=out, fee_amount=fee, price_impact_bps=int(impact_bps))


def apply_slippage_min_out(amount_out: int, slippage_bps: int) -> int:
    slippage_bps = int(slippage_bps)
    if slippage_bps < 0 or slippage_bps > 5_000:
        raise ValueError("slippage out of range")
    return (int(amount_out) * (10_000 - slippage_bps)) // 10_000


# --- Slippage percent helper ---
def percent_to_bps(pct_str: str) -> int:
    """Convert a user-facing percent string (e.g. '1', '0.5') to bps (1% = 100 bps)."""
    s = (pct_str or "").strip().replace("%", "")
    if not s:
        raise ValueError("missing slippage")
    d = Decimal(s)
    if d < 0:
        raise ValueError("slippage must be >= 0")
    # clamp to 50% max to avoid extreme values
    if d > Decimal("50"):
        raise ValueError("slippage too large")
    # 1% = 100 bps
    bps = int((d * Decimal("100")).to_integral_value(rounding=ROUND_DOWN))
    return int(bps)


# --- VECO withdrawal fee helper ---
def compute_withdraw_fee_sat(amount_sat: int) -> int:
    """Compute withdrawal fee in sats (taken from amount)."""
    bps = int(VECO_WITHDRAW_FEE_BPS)
    if bps <= 0:
        return 0
    fee = (int(amount_sat) * bps) // 10_000
    if fee <= 0 and int(amount_sat) > 0:
        fee = 1
    return int(fee)


# --- Private key encryption helpers ---
def _get_fernet():
    if not TIPBOT_FERNET_KEY or Fernet is None:
        return None
    try:
        return Fernet(TIPBOT_FERNET_KEY.encode("utf-8"))
    except Exception:
        return None


def encrypt_secret(secret: str) -> str:
    secret = (secret or "").strip()
    if not secret:
        return ""
    f = _get_fernet()
    if f is None:
        # plaintext fallback (centralized system; user opted in)
        return "plain:" + secret
    token = f.encrypt(secret.encode("utf-8"))
    return "fernet:" + token.decode("utf-8")


def decrypt_secret(stored: Optional[str]) -> str:
    stored = (stored or "").strip()
    if not stored:
        return ""
    if stored.startswith("plain:"):
        return stored[len("plain:"):]
    if stored.startswith("fernet:"):
        token = stored[len("fernet:"):].encode("utf-8")
        f = _get_fernet()
        if f is None:
            # key missing -> cannot decrypt
            return ""
        try:
            return f.decrypt(token).decode("utf-8")
        except Exception:
            return ""
    # Unknown format
    return ""


def db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)  # autocommit
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
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      last_withdraw_at INTEGER NOT NULL DEFAULT 0
    );
    """)
    # Backward-compatible migration (older DBs): add website_secret column if missing.
    try:
        con.execute("ALTER TABLE users ADD COLUMN website_secret TEXT;")
    except Exception:
        pass
    # Backward-compatible migration: add VECO columns if missing.
    try:
        con.execute("ALTER TABLE users ADD COLUMN veco_internal_sat INTEGER NOT NULL DEFAULT 0;")
    except Exception:
        pass
    try:
        con.execute("ALTER TABLE users ADD COLUMN veco_deposit_address TEXT;")
    except Exception:
        pass
    con.execute("""
    CREATE TABLE IF NOT EXISTS daily_limits (
      discord_id INTEGER NOT NULL,
      day TEXT NOT NULL,
      withdrawn_today INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY(discord_id, day)
    );
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS tx_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ts INTEGER NOT NULL,
      type TEXT NOT NULL,          -- tip | grant | withdraw | deposit
      from_id INTEGER,
      to_id INTEGER,
      amount INTEGER NOT NULL,
      note TEXT,
      status TEXT NOT NULL,        -- ok | pending | failed
      faucet_resp TEXT,
      error TEXT
    );
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS amm_pool (
      id INTEGER PRIMARY KEY CHECK(id=1),
      hcc_reserve INTEGER NOT NULL,
      veco_reserve_sat INTEGER NOT NULL,
      fee_bps INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS swap_quotes (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ts INTEGER NOT NULL,
      expires_at INTEGER NOT NULL,
      discord_id INTEGER NOT NULL,
      from_asset TEXT NOT NULL,
      to_asset TEXT NOT NULL,
      amount_in INTEGER NOT NULL,
      amount_out INTEGER NOT NULL,
      min_out INTEGER NOT NULL,
      fee_amount INTEGER NOT NULL,
      price_impact_bps INTEGER NOT NULL
    );
    """)

    # Faucet cooldown table
    con.execute("""
    CREATE TABLE IF NOT EXISTS faucet_claims (
      discord_id INTEGER PRIMARY KEY,
      last_claim_at INTEGER NOT NULL DEFAULT 0
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS swap_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ts INTEGER NOT NULL,
      discord_id INTEGER NOT NULL,
      from_asset TEXT NOT NULL,
      to_asset TEXT NOT NULL,
      amount_in INTEGER NOT NULL,
      amount_out INTEGER NOT NULL,
      fee_amount INTEGER NOT NULL,
      price_impact_bps INTEGER NOT NULL,
      status TEXT NOT NULL,
      error TEXT
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS veco_withdrawals (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ts INTEGER NOT NULL,
      discord_id INTEGER NOT NULL,
      to_address TEXT NOT NULL,
      amount_sat INTEGER NOT NULL,
      fee_sat INTEGER NOT NULL DEFAULT 0,
      txid TEXT,
      status TEXT NOT NULL,   -- pending | sent | failed
      error TEXT
    );
    """)

    # Ensure there is exactly one pool row (id=1). Defaults can be overridden via env.
    row = con.execute("SELECT id FROM amm_pool WHERE id=1").fetchone()
    if not row:
        ts = now_ts()
        con.execute(
            "INSERT INTO amm_pool(id, hcc_reserve, veco_reserve_sat, fee_bps, updated_at) VALUES(1,?,?,?,?)",
            (int(DEFAULT_POOL_HCC), int(DEFAULT_POOL_VECO) * VECO_SATS, int(DEFAULT_POOL_FEE_BPS), ts)
        )
    con.close()


def get_or_create_user(con: sqlite3.Connection, discord_id: int) -> Dict[str, Any]:
    row = con.execute(
        "SELECT discord_id, address, website_secret, balance, veco_internal_sat, veco_deposit_address, created_at, updated_at, last_withdraw_at FROM users WHERE discord_id=?",
        (discord_id,)
    ).fetchone()
    if row:
        return {
            "discord_id": int(row[0]),
            "address": row[1],
            "website_secret": row[2],
            "balance": int(row[3]),
            "veco_internal_sat": int(row[4]),
            "veco_deposit_address": row[5],
            "created_at": int(row[6]),
            "updated_at": int(row[7]),
            "last_withdraw_at": int(row[8]),
        }
    ts = now_ts()
    con.execute(
        "INSERT INTO users(discord_id, address, website_secret, balance, veco_internal_sat, veco_deposit_address, created_at, updated_at, last_withdraw_at) VALUES(?,?,?,?,?,?,?, ?, 0)",
        (discord_id, None, None, 0, 0, None, ts, ts)
    )
    return {
        "discord_id": discord_id,
        "address": None,
        "website_secret": None,
        "balance": 0,
        "veco_internal_sat": 0,
        "veco_deposit_address": None,
        "created_at": ts,
        "updated_at": ts,
        "last_withdraw_at": 0,
    }


def get_withdrawn_today(con: sqlite3.Connection, discord_id: int, dk: str) -> int:
    row = con.execute(
        "SELECT withdrawn_today FROM daily_limits WHERE discord_id=? AND day=?",
        (discord_id, dk)
    ).fetchone()
    if not row:
        con.execute(
            "INSERT INTO daily_limits(discord_id, day, withdrawn_today) VALUES(?,?,0)",
            (discord_id, dk)
        )
        return 0
    return int(row[0])


def set_withdrawn_today(con: sqlite3.Connection, discord_id: int, dk: str, val: int) -> None:
    con.execute(
        "UPDATE daily_limits SET withdrawn_today=? WHERE discord_id=? AND day=?",
        (int(val), discord_id, dk)
    )


# Faucet cooldown helpers
def get_last_claim_at(con: sqlite3.Connection, discord_id: int) -> int:
    row = con.execute(
        "SELECT last_claim_at FROM faucet_claims WHERE discord_id=?",
        (int(discord_id),)
    ).fetchone()
    if row and row[0] is not None:
        return int(row[0])
    return 0


def set_last_claim_at(con: sqlite3.Connection, discord_id: int, ts: int) -> None:
    con.execute(
        "INSERT INTO faucet_claims(discord_id, last_claim_at) VALUES(?,?) "
        "ON CONFLICT(discord_id) DO UPDATE SET last_claim_at=excluded.last_claim_at",
        (int(discord_id), int(ts))
    )


def has_pending_withdraw(con: sqlite3.Connection, discord_id: int) -> bool:
    row = con.execute(
        "SELECT id FROM tx_log WHERE type='withdraw' AND from_id=? AND status='pending' ORDER BY id DESC LIMIT 1",
        (discord_id,)
    ).fetchone()
    return bool(row)


def has_pending_veco_withdraw(con: sqlite3.Connection, discord_id: int) -> bool:
    row = con.execute(
        "SELECT id FROM veco_withdrawals WHERE discord_id=? AND status='pending' ORDER BY id DESC LIMIT 1",
        (discord_id,)
    ).fetchone()
    return bool(row)


# ---------------------------
# AMM pool helpers
# ---------------------------
def get_pool(con: sqlite3.Connection) -> Dict[str, int]:
    row = con.execute("SELECT hcc_reserve, veco_reserve_sat, fee_bps, updated_at FROM amm_pool WHERE id=1").fetchone()
    if not row:
        raise RuntimeError("AMM pool not initialized")
    return {
        "hcc_reserve": int(row[0]),
        "veco_reserve_sat": int(row[1]),
        "fee_bps": int(row[2]),
        "updated_at": int(row[3]),
    }


def set_pool(con: sqlite3.Connection, hcc_reserve: int, veco_reserve_sat: int, fee_bps: int) -> None:
    con.execute(
        "UPDATE amm_pool SET hcc_reserve=?, veco_reserve_sat=?, fee_bps=?, updated_at=? WHERE id=1",
        (int(hcc_reserve), int(veco_reserve_sat), int(fee_bps), now_ts())
    )


#
# ---------------------------
# VECO RPC helper (wallet-based)
# ---------------------------
async def veco_rpc_call(method: str, params: Optional[List[Any]] = None) -> Any:
    """Call VECO JSON-RPC. Requires VECO_RPC_URL/USER/PASSWORD."""
    if not VECO_RPC_URL or not VECO_RPC_USER or not VECO_RPC_PASSWORD:
        raise RuntimeError("VECO RPC not configured (set VECO_RPC_URL/USER/PASSWORD)")

    payload = {
        "jsonrpc": "1.0",
        "id": "tipbot",
        "method": method,
        "params": params or [],
    }

    auth = aiohttp.BasicAuth(VECO_RPC_USER, VECO_RPC_PASSWORD)
    async with aiohttp.ClientSession(auth=auth) as session:
        async with session.post(VECO_RPC_URL, json=payload, timeout=30) as r:
            txt = await r.text()
            if r.status != 200:
                raise RuntimeError(f"VECO RPC HTTP {r.status}: {txt}")
            try:
                data = json.loads(txt)
            except Exception:
                raise RuntimeError(f"VECO RPC invalid JSON: {txt}")
            if data.get("error"):
                raise RuntimeError(f"VECO RPC error: {data['error']}")
            return data.get("result")


# ---------------------------
# Hashcash backend API calls (matches your FastAPI app.py)
# ---------------------------
async def api_get_me(session: aiohttp.ClientSession, secret: str) -> Dict[str, Any]:
    url = f"{FAUCET_API_BASE}/me"
    headers = {"Authorization": f"Bearer {secret}"}
    async with session.get(url, headers=headers, timeout=20) as r:
        txt = await r.text()
        if r.status != 200:
            raise RuntimeError(f"/me failed ({r.status}): {txt}")
        return json.loads(txt)


async def api_transfer(
    session: aiohttp.ClientSession,
    sender_secret: str,
    to_address: str,
    amount: int
) -> Dict[str, Any]:
    url = f"{FAUCET_API_BASE}/transfer"
    headers = {"Authorization": f"Bearer {sender_secret}", "Content-Type": "application/json"}
    payload = {"to_address": to_address, "amount": amount}
    async with session.post(url, headers=headers, json=payload, timeout=30) as r:
        txt = await r.text()
        if r.status != 200:
            raise RuntimeError(f"/transfer failed ({r.status}): {txt}")
        return json.loads(txt)


# ---------------------------
# Discord bot
# ---------------------------
intents = discord.Intents.default()


class TipBot(discord.Client):
    def __init__(self):
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)
        self.treasury_address: Optional[str] = None

    async def setup_hook(self):
        init_db()

        if os.getenv("PURGE_GLOBAL", "0") == "1":
            self.tree.clear_commands(guild=None)
            await self.tree.sync()
            print("Purged global commands.")
            raise SystemExit(0)

        # Resolve treasury address once (bonus protection)
        if TIPBOT_TREASURY_SECRET:
            try:
                async with aiohttp.ClientSession() as session:
                    me = await api_get_me(session, TIPBOT_TREASURY_SECRET)
                    addr = me.get("account_id")
                    if isinstance(addr, str) and ADDR_RE.match(addr):
                        self.treasury_address = addr.lower()
            except Exception:
                self.treasury_address = None

        # Slash command sync
        if GUILD_ID:
            guild = discord.Object(id=int(GUILD_ID))
            self.tree.copy_global_to(guild=guild)
            await self.tree.sync(guild=guild)
        else:
            await self.tree.sync()


bot = TipBot()
# In-memory ephemeral swap sessions (per user). Safe to lose on restart.
SWAP_SESSIONS: Dict[int, Dict[str, Any]] = {}


def is_admin(interaction: discord.Interaction) -> bool:
    # simple admin gate: Administrator OR Manage Guild
    perms = getattr(interaction.user, "guild_permissions", None)
    if not perms:
        return False
    return bool(getattr(perms, "administrator", False) or getattr(perms, "manage_guild", False))


# ---------------------------
# Commands
# ---------------------------
@bot.tree.command(name="help", description="Show tip bot commands.")
async def help_cmd(interaction: discord.Interaction):
    text = (
        "**HCC TipBot (HCC + VECO + Swap)**\n"
        "\n"
        "**HCC (website ↔ Discord internal)**\n"
        "• `/hcc_link_account` – Link your Hashcash website account\n"
        "• `/hcc_unlink_account` – Unlink your website account\n"
        "• `/hcc_deposit <amount|max>` – Move HCC from your website account into your Discord balance\n"
        "\n"
        "**HCC (internal ↔ address)**\n"
        "• `/hcc_register_address <address>` – Set your HCC withdrawal address (40 hex)\n"
        "• `/tip @user <amount> [note]` – Tip HCC to another user (internal)\n"
        "• `/hcc_withdraw <amount>` – Withdraw HCC to your registered address (via treasury)\n"
        f"• `/claim` – Claim {FAUCET_AMOUNT} HCC (cooldown: {int(FAUCET_COOLDOWN_SECONDS)//3600}h)\n"
        "• `/whoami` – Show your registered addresses and limits\n"
        "\n"
        "**VECO (on-chain ↔ Discord internal)**\n"
        f"• `/veco_deposit` – Show (or create) your personal VECO deposit address (credits after {VECO_DEPOSIT_CONFS} confs)\n"
        "• `/balances` – Show your internal HCC + internal VECO balances\n"
        "• `/veco_withdraw <to_address> <amount>` – Request a VECO on-chain withdrawal from your internal balance\n"
        "• `/veco_withdraw_status <id>` – Check status / txid of a VECO withdrawal\n"
        "\n"
        "**Swap (internal AMM: HCC ⇄ VECO)**\n"
        "• `/ui_swap` – Open the swap UI (ephemeral)\n"
        "• `/swap <from_asset> <amount> [slippage_pct]` – Swap via command (no UI)\n"
        "\n"
        "**Limits / Policies**\n"
        f"• HCC min withdraw: `{MIN_WITHDRAW}` | cooldown: `{WITHDRAW_COOLDOWN}s` | max/day: `{MAX_WITHDRAW_PER_DAY}`\n"
        f"• VECO min withdraw (after fee): `{format_sat_to_veco(VECO_MIN_WITHDRAW_SAT)}` VECO\n"
        f"• VECO withdrawal fee: `{VECO_WITHDRAW_FEE_BPS/100:.2f}%` (taken from amount; paid to operator)\n"
        "\n"
        "Notes:\n"
        "• HCC uses an internal ledger; `/hcc_deposit` moves credits from your website account into Discord.\n"
        "• VECO deposits are real on-chain transfers to your personal deposit address.\n"
        "• Safety: withdrawing VECO to any bot-managed deposit address is blocked.\n"
    )
    await interaction.response.send_message(text, ephemeral=True)


# ---------------------------
# Faucet claim command
# ---------------------------

@bot.tree.command(name="claim", description="Claim HCC into your internal balance (cooldown).")
async def claim(interaction: discord.Interaction):
    if not channel_allowed(interaction):
        await interaction.response.send_message("This command is not allowed in this channel.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True, thinking=True)

    t = now_ts()
    con = db()
    to_addr = None
    new_bal = None

    try:
        con.execute("BEGIN IMMEDIATE;")

        u = get_or_create_user(con, interaction.user.id)

        # Require HCC withdraw address to be set before claiming
        to_addr = (u.get("address") or "").strip()
        if not to_addr:
            con.execute("ROLLBACK;")
            await interaction.followup.send(
                "Not registered yet. Please set your HCC withdraw address first with `/hcc_register_address <address>`.\n"
                "Then you can claim with `/claim`.",
                ephemeral=True,
            )
            return

        # basic sanity of stored address
        try:
            to_addr = normalize_addr(to_addr)
        except Exception:
            con.execute("ROLLBACK;")
            await interaction.followup.send(
                "Your stored HCC address has an invalid format. Please re-register using `/hcc_register_address <address>`.",
                ephemeral=True,
            )
            return

        # Cooldown
        last_claim = get_last_claim_at(con, interaction.user.id)
        if last_claim and t < last_claim + int(FAUCET_COOLDOWN_SECONDS):
            rem = (last_claim + int(FAUCET_COOLDOWN_SECONDS)) - t
            con.execute("ROLLBACK;")
            await interaction.followup.send(
                f"Cooldown ⏳ Try again in ~{fmt_duration_hms(rem)}.",
                ephemeral=True,
            )
            return

        # Credit internal balance + persist cooldown
        con.execute(
            "UPDATE users SET balance = balance + ?, updated_at=? WHERE discord_id=?",
            (int(FAUCET_AMOUNT), t, interaction.user.id),
        )
        set_last_claim_at(con, interaction.user.id, t)

        # Log (best effort)
        try:
            con.execute(
                "INSERT INTO tx_log(ts, type, from_id, to_id, amount, note, status) VALUES(?,?,?,?,?,?,?)",
                (t, "faucet_claim", None, interaction.user.id, int(FAUCET_AMOUNT), "faucet /claim", "ok"),
            )
        except Exception:
            pass

        row = con.execute("SELECT balance FROM users WHERE discord_id=?", (interaction.user.id,)).fetchone()
        if row and row[0] is not None:
            new_bal = int(row[0])

        con.execute("COMMIT;")

    except Exception:
        try:
            con.execute("ROLLBACK;")
        except Exception:
            pass
        raise
    finally:
        try:
            con.close()
        except Exception:
            pass

    # Optional public announcement
    if PUBLIC_CLAIM_ANNOUNCEMENTS and interaction.channel is not None:
        try:
            extra = ""
            if PUBLIC_CLAIM_SHOW_ADDRESS and to_addr:
                extra = f" (addr {to_addr[:6]}…{to_addr[-6:]})"
            await interaction.channel.send(
                f"{interaction.user.mention} claimed **{int(FAUCET_AMOUNT)} HCC** into their TipBot balance!{extra}"
            )
        except Exception:
            pass

    bal_txt = f"Your TipBot balance is now **{new_bal} HCC**." if new_bal is not None else ""
    await interaction.followup.send(
        f"Claim successful ✅ Added **{int(FAUCET_AMOUNT)} HCC** to your internal balance.\n"
        f"Withdraw anytime with `/hcc_withdraw <amount>` to your registered address (`{to_addr}`).\n"
        f"{bal_txt}",
        ephemeral=True,
    )


@bot.tree.command(
    name="hcc_register_address",
    description="Register your HCC address (40 hex). No existence check."
)
@app_commands.describe(address="Your HCC address (40 hex characters)")
async def hcc_register_address(interaction: discord.Interaction, address: str):
    await interaction.response.defer(ephemeral=True, thinking=True)

    try:
        addr = normalize_addr(address)
    except Exception:
        await interaction.followup.send(
            "Invalid address format. Expected **40 hex characters** (0-9, a-f).",
            ephemeral=True
        )
        return

    # Bonus protection: do not allow registering the treasury address
    if bot.treasury_address and addr == bot.treasury_address:
        await interaction.followup.send(
            "That address is the **bot treasury** address. Please register your own HCC address.",
            ephemeral=True
        )
        return

    con = db()
    try:
        con.execute("BEGIN IMMEDIATE;")
        u = get_or_create_user(con, interaction.user.id)
        con.execute(
            "UPDATE users SET address=?, updated_at=? WHERE discord_id=?",
            (addr, now_ts(), interaction.user.id)
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
        f"Registered ✅\nStored address: `{addr}`\nYou can withdraw with `/hcc_withdraw <amount>`.",
        ephemeral=True
    )


"""
@bot.tree.command(name="balance", description="Show your TipBot balance.")
async def balance(interaction: discord.Interaction):
    con = db()
    try:
        u = get_or_create_user(con, interaction.user.id)
        addr = u.get("address") or "(not set)"
        await interaction.response.send_message(
            f"Balance: **{u['balance']}** HCC\nRegistered HCC address: `{addr}`",
            ephemeral=True
        )
    finally:
        con.close()
"""


# ---------------------------
# Website account linking and deposit commands
# ---------------------------

class LinkAccountModal(discord.ui.Modal, title="Link Website Account"):
    website_secret = discord.ui.TextInput(
        label="Website private key",
        style=discord.TextStyle.paragraph,
        placeholder="Paste your private key from your website account.",
        required=True,
        max_length=4000,
    )

    def __init__(self, requester_id: int):
        super().__init__()
        self.requester_id = int(requester_id)

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.requester_id:
            await interaction.response.send_message("This modal is not for you.", ephemeral=True)
            return

        secret = str(self.website_secret.value or "").strip()
        if not secret:
            await interaction.response.send_message("Missing secret.", ephemeral=True)
            return

        # Verify the secret via /me
        try:
            async with aiohttp.ClientSession() as session:
                me = await api_get_me(session, secret)
                acct = me.get("account_id")
        except Exception as e:
            await interaction.response.send_message(f"Link failed ❌ Could not verify secret: `{e}`", ephemeral=True)
            return

        if not isinstance(acct, str) or not ADDR_RE.match(acct):
            await interaction.response.send_message("Link failed ❌ API endpoint /me returned an invalid account_id.", ephemeral=True)
            return

        # Prevent linking the bot treasury (user could shoot themselves in the foot)
        if bot.treasury_address and acct.lower() == bot.treasury_address:
            await interaction.response.send_message(
                "Safety check: this secret belongs to the **bot treasury** account. Please link your own website account.",
                ephemeral=True,
            )
            return

        stored = encrypt_secret(secret)
        con = db()
        try:
            con.execute("BEGIN IMMEDIATE;")
            get_or_create_user(con, interaction.user.id)
            con.execute(
                "UPDATE users SET website_secret=?, updated_at=? WHERE discord_id=?",
                (stored, now_ts(), interaction.user.id),
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

        enc_note = "encrypted" if (stored.startswith("fernet:") and _get_fernet() is not None) else "plaintext"
        key_hint = "" if enc_note == "encrypted" else " (Tip: set TIPBOT_FERNET_KEY to encrypt secrets at rest.)"

        await interaction.response.send_message(
            f"Linked ✅ Website Hashcash account: `{acct.lower()}`\nYou can now use `/hcc_deposit`.",
            ephemeral=True,
        )


@bot.tree.command(name="hcc_link_account", description="Link your HCC website account for direct deposits (note: private key is stored encrypted).")
async def hcc_link_account(interaction: discord.Interaction):
    await interaction.response.send_modal(LinkAccountModal(interaction.user.id))


@bot.tree.command(name="hcc_unlink_account", description="Remove your HCC website account secret.")
async def hcc_unlink_account(interaction: discord.Interaction):
    con = db()
    try:
        con.execute("BEGIN IMMEDIATE;")
        get_or_create_user(con, interaction.user.id)
        con.execute("UPDATE users SET website_secret=NULL, updated_at=? WHERE discord_id=?", (now_ts(), interaction.user.id))
        con.execute("COMMIT;")
    except Exception:
        try:
            con.execute("ROLLBACK;")
        except Exception:
            pass
        raise
    finally:
        con.close()

    await interaction.response.send_message("Unlinked ✅ Your website account was unlinked.", ephemeral=True)


@bot.tree.command(name="hcc_deposit", description="Deposit HCC from your website account into your Discord balance.")
@app_commands.describe(amount="Amount to deposit (integer) or 'max'")
async def hcc_deposit(interaction: discord.Interaction, amount: str):
    await interaction.response.defer(ephemeral=True, thinking=True)

    if not bot.treasury_address:
        await interaction.followup.send(
            "Bot treasury address not resolved. Please ensure TIPBOT_TREASURY_SECRET is set and the Hashcash backend is reachable.",
            ephemeral=True,
        )
        return

    # Load linked secret
    con = db()
    try:
        u = get_or_create_user(con, interaction.user.id)
        stored = u.get("website_secret")
    finally:
        con.close()

    secret = decrypt_secret(stored)
    if not secret:
        await interaction.followup.send(
            "No Hashcash website account linked. Use `/hcc_link_account` first (ephemeral).",
            ephemeral=True,
        )
        return

    # Determine deposit amount
    try:
        async with aiohttp.ClientSession() as session:
            me = await api_get_me(session, secret)
            account_balance = int(me.get("credits") or me.get("balance") or me.get("hcc") or 0)
    except Exception as e:
        await interaction.followup.send(f"Deposit failed ❌ Could not read website account balance: `{e}`", ephemeral=True)
        return

    amt_str = (amount or "").strip().lower()
    if amt_str == "max":
        dep_amount = account_balance
    else:
        try:
            dep_amount = int(amt_str)
        except Exception:
            await interaction.followup.send("Amount must be an integer or `max`.", ephemeral=True)
            return

    if dep_amount <= 0:
        await interaction.followup.send("Nothing to deposit.", ephemeral=True)
        return

    if dep_amount > account_balance:
        await interaction.followup.send(
            f"Insufficient deposit account balance. Balance of linked account: **{account_balance}** HCC.",
            ephemeral=True,
        )
        return

    # 1) Perform account transfer: user -> bot treasury
    try:
        async with aiohttp.ClientSession() as session:
            resp = await api_transfer(session, secret, bot.treasury_address, dep_amount)
    except Exception as e:
        await interaction.followup.send(f"Deposit failed ❌ HCC transfer failed: `{e}`", ephemeral=True)
        return

    # 2) Credit internal ledger (atomic)
    con2 = db()
    try:
        con2.execute("BEGIN IMMEDIATE;")
        get_or_create_user(con2, interaction.user.id)
        ts = now_ts()
        con2.execute(
            "UPDATE users SET balance = balance + ?, updated_at=? WHERE discord_id=?",
            (dep_amount, ts, interaction.user.id),
        )
        try:
            con2.execute(
                "INSERT INTO tx_log(ts, type, from_id, to_id, amount, note, status, faucet_resp) VALUES(?,?,?,?,?,?,?,?,?)",
                (ts, "deposit", None, interaction.user.id, dep_amount, "deposit from website account", "ok", json.dumps(resp, ensure_ascii=False)),
            )
        except Exception:
            # fallback if schema differs
            con2.execute(
                "INSERT INTO tx_log(ts, type, from_id, to_id, amount, note, status, faucet_resp) VALUES(?,?,?,?,?,?,?,?)",
                (ts, "deposit", None, interaction.user.id, dep_amount, "deposit from website account", "ok", json.dumps(resp, ensure_ascii=False)),
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

    # Show new internal balance
    con3 = db()
    try:
        u3 = get_or_create_user(con3, interaction.user.id)
        new_bal = int(u3.get("balance") or 0)
    finally:
        con3.close()

    await interaction.followup.send(
        f"Deposit successful ✅\nMoved **{dep_amount} HCC** from your website account into your Discord balance.\n"
        f"Your TipBot balance: **{new_bal} HCC**\n\n"
        "To cash out to your HCC address: use `/hcc_withdraw` in the TipBot.",
        ephemeral=True,
    )



@bot.tree.command(name="tip", description="Tip HCC to another user.")
@app_commands.describe(user="Recipient", amount="Amount to tip", note="Optional note")
async def tip(interaction: discord.Interaction, user: discord.User, amount: int, note: Optional[str] = None):
    await interaction.response.defer(ephemeral=True, thinking=True)

    if amount <= 0:
        await interaction.followup.send("Amount must be positive.", ephemeral=True)
        return
    if user.id == interaction.user.id:
        await interaction.followup.send("You cannot tip yourself.", ephemeral=True)
        return

    con = db()
    try:
        con.execute("BEGIN IMMEDIATE;")

        sender = get_or_create_user(con, interaction.user.id)
        recipient = get_or_create_user(con, user.id)

        if sender["balance"] < amount:
            con.execute("ROLLBACK;")
            await interaction.followup.send(
                f"Insufficient balance. You have **{sender['balance']}**.",
                ephemeral=True
            )
            return

        # Update balances
        con.execute("UPDATE users SET balance = balance - ?, updated_at=? WHERE discord_id=?",
                    (amount, now_ts(), interaction.user.id))
        con.execute("UPDATE users SET balance = balance + ?, updated_at=? WHERE discord_id=?",
                    (amount, now_ts(), user.id))

        # Log
        con.execute(
            "INSERT INTO tx_log(ts, type, from_id, to_id, amount, note, status) VALUES(?,?,?,?,?,?,?)",
            (now_ts(), "tip", interaction.user.id, user.id, amount, (note or ""), "ok")
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

    # Public announcement (optional)
    if PUBLIC_TIP_ANNOUNCEMENTS and interaction.channel is not None:
        try:
            note_txt = f" — {note}" if note else ""
            await interaction.channel.send(
                f"💸 {interaction.user.mention} tipped {user.mention} **{amount}** HCC{note_txt}"
            )
        except Exception:
            pass

    await interaction.followup.send(
        f"Tip sent ✅ You tipped {user.mention} **{amount}** HCC.",
        ephemeral=True
    )


# ---- multitip ----
@bot.tree.command(name="multitip", description="Tip the same HCC amount to multiple users.")
@app_commands.describe(
    amount="Amount of HCC to tip EACH user",
    users="Space-separated @mentions or user IDs (e.g. @a @b @c)",
    note="Optional note"
)
async def multitip(interaction: discord.Interaction, amount: int, users: str, note: Optional[str] = None):
    """Tip the same amount to multiple recipients in one atomic DB transaction.

    We accept a single `users` string because Discord slash commands don't support variadic args.
    Supported formats inside `users`:
      - Mentions: <@123> or <@!123>
      - Raw IDs: 1234567890
    """
    await interaction.response.defer(ephemeral=True, thinking=True)

    if amount <= 0:
        await interaction.followup.send("Amount must be positive.", ephemeral=True)
        return

    raw = (users or "").strip()
    if not raw:
        await interaction.followup.send("Please provide at least one recipient (@mention or user id).", ephemeral=True)
        return

    # Extract IDs from mentions or raw numeric tokens
    ids: List[int] = []
    for tok in raw.split():
        t = tok.strip()
        if not t:
            continue
        # mention variants
        if t.startswith("<@") and t.endswith(">"):
            t2 = t[2:-1]
            if t2.startswith("!"):
                t2 = t2[1:]
            if t2.isdigit():
                ids.append(int(t2))
            continue
        # raw id
        if t.isdigit():
            ids.append(int(t))

    # De-duplicate while preserving order
    seen: set[int] = set()
    uniq_ids: List[int] = []
    for did in ids:
        if did not in seen:
            seen.add(did)
            uniq_ids.append(did)

    # Remove self if present
    uniq_ids = [did for did in uniq_ids if did != interaction.user.id]

    if not uniq_ids:
        await interaction.followup.send("No valid recipients found (or you only included yourself).", ephemeral=True)
        return

    # Hard cap to prevent abuse / huge locks
    MAX_RECIPIENTS = 10
    if len(uniq_ids) > MAX_RECIPIENTS:
        await interaction.followup.send(
            f"Too many recipients. Max is {MAX_RECIPIENTS} per /multitip.",
            ephemeral=True,
        )
        return

    total = int(amount) * len(uniq_ids)

    con = db()
    try:
        con.execute("BEGIN IMMEDIATE;")

        sender = get_or_create_user(con, interaction.user.id)
        if int(sender.get("balance") or 0) < total:
            con.execute("ROLLBACK;")
            await interaction.followup.send(
                f"Insufficient balance. You have **{sender['balance']}** HCC but need **{total}** HCC.",
                ephemeral=True,
            )
            return

        ts = now_ts()

        # Debit sender once
        con.execute(
            "UPDATE users SET balance = balance - ?, updated_at=? WHERE discord_id=?",
            (total, ts, interaction.user.id),
        )

        # Credit each recipient + log each transfer
        for rid in uniq_ids:
            get_or_create_user(con, rid)
            con.execute(
                "UPDATE users SET balance = balance + ?, updated_at=? WHERE discord_id=?",
                (int(amount), ts, rid),
            )
            con.execute(
                "INSERT INTO tx_log(ts, type, from_id, to_id, amount, note, status) VALUES(?,?,?,?,?,?,?)",
                (ts, "tip", interaction.user.id, rid, int(amount), (note or ""), "ok"),
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

    # Best-effort: build mention list for response
    mention_list = " ".join([f"<@{rid}>" for rid in uniq_ids])

    # Optional public announcement
    if PUBLIC_TIP_ANNOUNCEMENTS and interaction.channel is not None:
        try:
            note_txt = f" — {note}" if note else ""
            await interaction.channel.send(
                f"💸 {interaction.user.mention} multi-tipped **{amount}** HCC to {mention_list} (total {total} HCC){note_txt}"
            )
        except Exception:
            pass

    await interaction.followup.send(
        f"Multi-tip sent ✅\n"
        f"Each: **{amount}** HCC\n"
        f"Recipients ({len(uniq_ids)}): {mention_list}\n"
        f"Total: **{total}** HCC",
        ephemeral=True,
    )


@bot.tree.command(name="hcc_withdraw", description="Withdraw to your registered HCC address (via bot treasury).")
@app_commands.describe(amount="Amount to withdraw")
async def hcc_withdraw(interaction: discord.Interaction, amount: int):
    await interaction.response.defer(ephemeral=True, thinking=True)

    if amount < MIN_WITHDRAW:
        await interaction.followup.send(f"Minimum withdraw is **{MIN_WITHDRAW}**.", ephemeral=True)
        return
    if amount <= 0:
        await interaction.followup.send("Amount must be positive.", ephemeral=True)
        return

    if not TIPBOT_TREASURY_SECRET:
        await interaction.followup.send("Bot misconfigured: `TIPBOT_TREASURY_SECRET` missing.", ephemeral=True)
        return

    ts = now_ts()
    dk = day_key(ts)

    # 1) Reserve balance + create pending tx (atomic)
    con = db()
    pending_tx_id = None
    to_addr = None

    try:
        con.execute("BEGIN IMMEDIATE;")

        u = get_or_create_user(con, interaction.user.id)
        to_addr = u.get("address")

        if not to_addr:
            con.execute("ROLLBACK;")
            await interaction.followup.send("No HCC address registered. Use `/hcc_register_address` first.", ephemeral=True)
            return

        # basic sanity
        try:
            to_addr = normalize_addr(to_addr)
        except Exception:
            con.execute("ROLLBACK;")
            await interaction.followup.send("Your stored HCC address has an invalid format. Please re-register.", ephemeral=True)
            return

        # bonus protection
        if bot.treasury_address and to_addr == bot.treasury_address:
            con.execute("ROLLBACK;")
            await interaction.followup.send("Safety check: your withdraw address equals the bot treasury address.", ephemeral=True)
            return

        # cooldown
        last_w = int(u.get("last_withdraw_at", 0) or 0)
        if last_w and (ts - last_w) < WITHDRAW_COOLDOWN:
            rem = WITHDRAW_COOLDOWN - (ts - last_w)
            con.execute("ROLLBACK;")
            await interaction.followup.send(f"Withdraw cooldown ⏳ Try again in ~{rem}s.", ephemeral=True)
            return

        # only one pending withdraw at a time
        if has_pending_withdraw(con, interaction.user.id):
            con.execute("ROLLBACK;")
            await interaction.followup.send("You already have a pending withdraw. Please wait a moment and try again.", ephemeral=True)
            return

        # balance check
        if u["balance"] < amount:
            con.execute("ROLLBACK;")
            await interaction.followup.send(f"Insufficient balance. You have **{u['balance']}**.", ephemeral=True)
            return

        # daily limit (only counts successful withdraws; we check current withdrawn_today here)
        withdrawn_today = get_withdrawn_today(con, interaction.user.id, dk)
        if withdrawn_today + amount > MAX_WITHDRAW_PER_DAY:
            con.execute("ROLLBACK;")
            await interaction.followup.send(
                f"Daily withdraw limit reached. Today: {withdrawn_today}/{MAX_WITHDRAW_PER_DAY}.",
                ephemeral=True
            )
            return

        # reserve: subtract balance + update last_withdraw_at
        con.execute("UPDATE users SET balance = balance - ?, last_withdraw_at=?, updated_at=? WHERE discord_id=?",
                    (amount, ts, ts, interaction.user.id))

        cur = con.execute(
            "INSERT INTO tx_log(ts, type, from_id, to_id, amount, note, status) VALUES(?,?,?,?,?,?,?)",
            (ts, "withdraw", interaction.user.id, None, amount, to_addr, "pending")
        )
        pending_tx_id = int(cur.lastrowid)

        con.execute("COMMIT;")

    except Exception:
        try:
            con.execute("ROLLBACK;")
        except Exception:
            pass
        raise
    finally:
        con.close()

    # 2) Call website account /transfer (outside DB lock)
    ok = False
    resp = None
    err_msg = None

    try:
        async with aiohttp.ClientSession() as session:
            resp = await api_transfer(session, TIPBOT_TREASURY_SECRET, to_addr, amount)
            ok = True
    except Exception as e:
        err_msg = str(e)

    # 3) Finalize tx: success -> mark ok + increment daily withdrawn; failure -> refund balance
    con2 = db()
    try:
        con2.execute("BEGIN IMMEDIATE;")

        if ok:
            con2.execute(
                "UPDATE tx_log SET status='ok', faucet_resp=?, error=NULL WHERE id=?",
                (json.dumps(resp, ensure_ascii=False), pending_tx_id)
            )
            # increment daily withdrawn on success
            w = get_withdrawn_today(con2, interaction.user.id, dk)
            set_withdrawn_today(con2, interaction.user.id, dk, w + amount)

            con2.execute("COMMIT;")

            # Optional public announcement
            if PUBLIC_WITHDRAW_ANNOUNCEMENTS and interaction.channel is not None:
                try:
                    extra = ""
                    if PUBLIC_SHOW_ADDRESS:
                        extra = f" (addr {to_addr[:6]}…{to_addr[-6:]})"
                    await interaction.channel.send(
                        f"🏧 {interaction.user.mention} withdrew **{amount}** HCC{extra}"
                    )
                except Exception:
                    pass

            await interaction.followup.send(
                f"Withdraw successful ✅ Sent **{amount}** HCC to `{to_addr}`.\n",
                ephemeral=True
            )
            return

        # failed -> refund balance, mark failed
        con2.execute(
            "UPDATE users SET balance = balance + ?, updated_at=? WHERE discord_id=?",
            (amount, now_ts(), interaction.user.id)
        )
        con2.execute(
            "UPDATE tx_log SET status='failed', error=? WHERE id=?",
            ((err_msg or "unknown error")[:500], pending_tx_id)
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

    # Friendly hints for common errors
    hint = ""
    if err_msg:
        if "unknown recipient address" in err_msg or "(404)" in err_msg:
            hint = (
                "\n\nHint: Your HCC address is **unknown** to the Hashcash server. "
                "Create it first via PoW signup and ensure the 40-hex address is correct."
            )
        elif "insufficient HCC" in err_msg or "(400)" in err_msg:
            hint = "\n\nHint: Bot treasury has insufficient HCC right now. Ask admin to top it up."

    await interaction.followup.send(
        f"Withdraw failed ❌ The reserved amount was refunded.\nError: `{err_msg}`{hint}",
        ephemeral=True
    )


@bot.tree.command(name="whoami", description="Show your registered HCC / VECO addresses and basic info.")
async def whoami(interaction: discord.Interaction):
    con = db()
    try:
        u = get_or_create_user(con, interaction.user.id)
        dk = day_key()
        wd = get_withdrawn_today(con, interaction.user.id, dk)

        # Withdraw target address (user-provided)
        withdraw_addr = u.get("address") or "(not set)"

        # Deposit source address (linked website account)
        deposit_addr = "(not linked)"
        stored = u.get("website_secret")
        if stored:
            secret = decrypt_secret(stored)
            if not secret:
                deposit_addr = "(linked, but cannot decrypt)"
            else:
                try:
                    async with aiohttp.ClientSession() as session:
                        me = await api_get_me(session, secret)
                        acct = me.get("account_id")
                        if isinstance(acct, str) and ADDR_RE.match(acct):
                            deposit_addr = acct.lower()
                        else:
                            deposit_addr = "(linked, but invalid account_id)"
                except Exception:
                    deposit_addr = "(linked, but cannot reach backend)"

        last_claim = get_last_claim_at(con, interaction.user.id)
        if not last_claim or now_ts() >= last_claim + int(FAUCET_COOLDOWN_SECONDS):
            faucet_cd = "ready ✅"
        else:
            rem = (last_claim + int(FAUCET_COOLDOWN_SECONDS)) - now_ts()
            faucet_cd = f"~{fmt_duration_hms(rem)} remaining"

        await interaction.response.send_message(
            f"HCC withdraw address: `{withdraw_addr}`\n"
            f"HCC website source: `{deposit_addr}`\n"
            f"VECO deposit address: `{(u.get('veco_deposit_address') or '(not created)')}`\n"
            f"Balances: **{u['balance']} HCC** | **{format_sat_to_veco(int(u.get('veco_internal_sat') or 0))} VECO**\n"
            f"HCC withdraw today: `{wd}/{MAX_WITHDRAW_PER_DAY}`\n"
            f"HCC cooldown: `{WITHDRAW_COOLDOWN}s` | HCC min withdraw: `{MIN_WITHDRAW}`\n"
            f"Faucet claim: {faucet_cd}\n",
            ephemeral=True,
        )
    finally:
        con.close()


# ---------------------------
# Admin commands
# ---------------------------
@bot.tree.command(name="pool_status", description="(Admin) Show AMM pool reserves and fee.")
async def pool_status(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True, thinking=True)
    if not is_admin(interaction):
        await interaction.followup.send("Admin only.", ephemeral=True)
        return
    con = db()
    try:
        p = get_pool(con)
        await interaction.followup.send(
            f"AMM Pool (x*y=k)\n"
            f"• HCC reserve: **{p['hcc_reserve']}**\n"
            f"• VECO reserve: **{format_sat_to_veco(p['veco_reserve_sat'])}** VECO\n"
            f"• Fee: **{p['fee_bps']/100:.2f}%** ({p['fee_bps']} bps)",
            ephemeral=True,
        )
    finally:
        con.close()


@bot.tree.command(name="pool_init", description="(Admin) Initialize/reset AMM pool reserves.")
@app_commands.describe(hcc_reserve="HCC reserve (integer)", veco_reserve="VECO reserve (decimal allowed)", fee_bps="Fee in bps (e.g., 75 = 0.75%)")
async def pool_init(interaction: discord.Interaction, hcc_reserve: int = DEFAULT_POOL_HCC, veco_reserve: str = str(DEFAULT_POOL_VECO), fee_bps: int = DEFAULT_POOL_FEE_BPS):
    await interaction.response.defer(ephemeral=True, thinking=True)
    if not is_admin(interaction):
        await interaction.followup.send("Admin only.", ephemeral=True)
        return
    if hcc_reserve <= 0:
        await interaction.followup.send("hcc_reserve must be > 0", ephemeral=True)
        return
    try:
        veco_sat = parse_veco_to_sat(veco_reserve)
    except Exception as e:
        await interaction.followup.send(f"Invalid VECO amount: `{e}`", ephemeral=True)
        return
    if veco_sat <= 0:
        await interaction.followup.send("veco_reserve must be > 0", ephemeral=True)
        return
    if fee_bps < 0 or fee_bps > 1_000:
        await interaction.followup.send("fee_bps out of range (0..1000)", ephemeral=True)
        return

    con = db()
    try:
        con.execute("BEGIN IMMEDIATE;")
        set_pool(con, hcc_reserve, veco_sat, fee_bps)
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
        f"Pool updated ✅\n"
        f"• HCC reserve: **{hcc_reserve}**\n"
        f"• VECO reserve: **{format_sat_to_veco(veco_sat)}**\n"
        f"• Fee: **{fee_bps/100:.2f}%** ({fee_bps} bps)",
        ephemeral=True,
    )
@bot.tree.command(name="grant", description="(Admin) Grant internal HCC to a user.")
@app_commands.describe(user="Recipient", amount="Amount to grant", note="Optional note")
async def grant(interaction: discord.Interaction, user: discord.User, amount: int, note: Optional[str] = None):
    await interaction.response.defer(ephemeral=True, thinking=True)

    if not is_admin(interaction):
        await interaction.followup.send("Admin only.", ephemeral=True)
        return

    if amount <= 0:
        await interaction.followup.send("Amount must be positive.", ephemeral=True)
        return

    con = db()
    try:
        con.execute("BEGIN IMMEDIATE;")
        get_or_create_user(con, user.id)
        con.execute("UPDATE users SET balance = balance + ?, updated_at=? WHERE discord_id=?",
                    (amount, now_ts(), user.id))
        con.execute(
            "INSERT INTO tx_log(ts, type, from_id, to_id, amount, note, status) VALUES(?,?,?,?,?,?,?)",
            (now_ts(), "grant", interaction.user.id, user.id, amount, (note or ""), "ok")
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

    await interaction.followup.send(f"Granted ✅ {user.mention} received **{amount}** HCC.", ephemeral=True)


# ---------------------------
# AMM user commands
# ---------------------------

@bot.tree.command(name="balances", description="Show your HCC and internal VECO balances.")
async def balances(interaction: discord.Interaction):
    con = db()
    try:
        u = get_or_create_user(con, interaction.user.id)
        hcc = int(u.get("balance") or 0)
        vs = int(u.get("veco_internal_sat") or 0)
        await interaction.response.send_message(
            f"HCC (internal): **{hcc}**\nVECO (internal): **{format_sat_to_veco(vs)}**",
            ephemeral=True,
        )
    finally:
        con.close()


# ---------------------------
# VECO deposit/withdraw commands
# ---------------------------

@bot.tree.command(name="veco_deposit", description="Show (or create) your personal VECO deposit address.")
async def veco_deposit(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True, thinking=True)

    con = db()
    try:
        u = get_or_create_user(con, interaction.user.id)
        addr = (u.get("veco_deposit_address") or "").strip()
    finally:
        con.close()

    if addr:
        await interaction.followup.send(
            f"Your VECO deposit address:\n`{addr}`\n\n"
            f"Credits are added to your internal VECO balance after **{VECO_DEPOSIT_CONFS} confirmations**.",
            ephemeral=True,
        )
        return

    # Create a new address in the VECO wallet
    try:
        label = f"discord:{interaction.user.id}"
        new_addr = await veco_rpc_call("getnewaddress", [label])
        if not isinstance(new_addr, str) or not new_addr.strip():
            raise RuntimeError("getnewaddress returned an invalid address")
        new_addr = new_addr.strip()
    except Exception as e:
        await interaction.followup.send(f"Could not create VECO deposit address ❌ `{e}`", ephemeral=True)
        return

    # Persist in DB
    con2 = db()
    try:
        con2.execute("BEGIN IMMEDIATE;")
        get_or_create_user(con2, interaction.user.id)
        con2.execute(
            "UPDATE users SET veco_deposit_address=?, updated_at=? WHERE discord_id=?",
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
        f"Created your VECO deposit address ✅\n`{new_addr}`\n\n"
        f"Credits are added to your internal VECO balance after **{VECO_DEPOSIT_CONFS} confirmations**.",
        ephemeral=True,
    )


@bot.tree.command(name="veco_withdraw", description="Request an on-chain VECO withdrawal from your internal balance.")
@app_commands.describe(to_address="Destination VECO address", amount="Amount (VECO, up to 8 decimals)")
async def veco_withdraw(interaction: discord.Interaction, to_address: str, amount: str):
    await interaction.response.defer(ephemeral=True, thinking=True)

    to_address = (to_address or "").strip()
    if not to_address or len(to_address) < 20:
        await interaction.followup.send("Invalid VECO address.", ephemeral=True)
        return

    # Safety: prevent circular withdrawals to any known deposit address (would just loop back via watcher)
    con_chk = db()
    try:
        row = con_chk.execute(
            "SELECT discord_id FROM users WHERE veco_deposit_address IS NOT NULL AND TRIM(veco_deposit_address)=? LIMIT 1",
            (to_address,),
        ).fetchone()
    finally:
        try:
            con_chk.close()
        except Exception:
            pass

    if row is not None:
        await interaction.followup.send(
            "Safety check: you cannot withdraw to a bot-managed VECO deposit address (this would create a deposit/withdraw loop). "
            "Please withdraw to an external VECO address you control.",
            ephemeral=True,
        )
        return

    try:
        amt_sat = parse_veco_to_sat(amount)
    except Exception as e:
        await interaction.followup.send(f"Invalid amount: `{e}`", ephemeral=True)
        return

    fee_sat = 0
    if int(VECO_WITHDRAW_FEE_BPS) > 0:
        if not VECO_WITHDRAW_FEE_ADDRESS:
            await interaction.followup.send(
                "Bot misconfigured: VECO withdrawal fee is enabled but VECO_WITHDRAW_FEE_ADDRESS is not set.",
                ephemeral=True,
            )
            return
        fee_sat = compute_withdraw_fee_sat(amt_sat)

    net_sat = int(amt_sat) - int(fee_sat)
    if net_sat <= 0:
        await interaction.followup.send(
            "Amount too small after fee. Please enter a larger withdrawal amount.",
            ephemeral=True,
        )
        return

    if net_sat < int(VECO_MIN_WITHDRAW_SAT):
        await interaction.followup.send(
            f"Minimum VECO withdraw (after fee) is **{format_sat_to_veco(VECO_MIN_WITHDRAW_SAT)}** VECO.",
            ephemeral=True,
        )
        return

    # Reserve internal balance + create pending withdrawal
    con = db()
    try:
        con.execute("BEGIN IMMEDIATE;")
        u = get_or_create_user(con, interaction.user.id)
        bal_sat = int(u.get("veco_internal_sat") or 0)

        if has_pending_veco_withdraw(con, interaction.user.id):
            con.execute("ROLLBACK;")
            await interaction.followup.send(
                "You already have a pending VECO withdrawal. Please wait until it is processed.",
                ephemeral=True,
            )
            return

        if bal_sat < amt_sat:
            con.execute("ROLLBACK;")
            await interaction.followup.send(
                f"Insufficient VECO balance. You have **{format_sat_to_veco(bal_sat)}** VECO.",
                ephemeral=True,
            )
            return

        con.execute(
            "UPDATE users SET veco_internal_sat = veco_internal_sat - ?, updated_at=? WHERE discord_id=?",
            (amt_sat, now_ts(), interaction.user.id),
        )

        cur = con.execute(
            "INSERT INTO veco_withdrawals(ts, discord_id, to_address, amount_sat, fee_sat, txid, status, error) "
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

    # Read back status/txid (watcher may have already broadcast)
    txid = None
    status = "pending"
    con_s = db()
    try:
        row_s = con_s.execute(
            "SELECT status, txid FROM veco_withdrawals WHERE id=?",
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
        fee_line = f"• Fee (admin): **{format_sat_to_veco(fee_sat)} VECO**\n"

    tx_line = ""
    if txid:
        tx_line = f"• Txid: `{txid}`\n"
    else:
        tx_line = f"• Status: `{status}` (txid will appear after broadcast)\n"

    await interaction.followup.send(
        f"Withdrawal queued ✅\n"
        f"• ID: `{wid}`\n"
        + tx_line +
        f"• You receive: **{format_sat_to_veco(net_sat)} VECO**\n"
        + fee_line +
        f"• Total debited: **{format_sat_to_veco(amt_sat)} VECO**\n"
        f"• To: `{to_address}`\n\n"
        f"Use `/veco_withdraw_status {wid}` to check status/txid later.",
        ephemeral=True,
    )


# ---- /veco_withdraw_status ----

@bot.tree.command(name="veco_withdraw_status", description="Check status/txid of a VECO withdrawal request.")
@app_commands.describe(withdraw_id="Withdrawal ID from /veco_withdraw")
async def veco_withdraw_status(interaction: discord.Interaction, withdraw_id: int):
    await interaction.response.defer(ephemeral=True, thinking=True)

    wid = int(withdraw_id)
    con = db()
    try:
        row = con.execute(
            "SELECT ts, discord_id, to_address, amount_sat, fee_sat, txid, status, error FROM veco_withdrawals WHERE id=?",
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
            f"• Amount (user): **{format_sat_to_veco(int(amt_sat))} VECO**",
        ]
        if int(fee_sat or 0) > 0:
            lines.append(f"• Fee (admin): **{format_sat_to_veco(int(fee_sat))} VECO**")
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


#
# --- UI classes ---

# --- Swap UI panel (ephemeral) ---
class SwapAmountModal(discord.ui.Modal, title="Swap: Enter Amount"):
    amount = discord.ui.TextInput(
        label="Amount",
        style=discord.TextStyle.short,
        placeholder="HCC: integer (e.g. 10) | VECO: decimal (e.g. 1.25)",
        required=True,
        max_length=50,
    )

    def __init__(self, view: "SwapPanelView"):
        super().__init__()
        self._view = view

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self._view.requester_id:
            await interaction.response.send_message("This modal is not for you.", ephemeral=True)
            return
        val = str(self.amount.value or "").strip()
        self._view.state["amount"] = val
        await self._view.render(interaction, replace_message=True)


class SwapSlippageModal(discord.ui.Modal, title="Swap: Slippage"):
    slippage_pct = discord.ui.TextInput(
        label="Slippage tolerance (%)",
        style=discord.TextStyle.short,
        placeholder="Example: 1 or 0.5",
        required=True,
        max_length=20,
    )

    def __init__(self, view: "SwapPanelView"):
        super().__init__()
        self._view = view

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self._view.requester_id:
            await interaction.response.send_message("This modal is not for you.", ephemeral=True)
            return
        try:
            bps = percent_to_bps(str(self.slippage_pct.value or ""))
        except Exception as e:
            await interaction.response.send_message(f"Invalid slippage: `{e}`", ephemeral=True)
            return
        self._view.state["slippage_bps"] = int(bps)
        await self._view.render(interaction, replace_message=True)


class SwapPanelView(discord.ui.View):
    def __init__(self, requester_id: int):
        super().__init__(timeout=120)
        self.requester_id = int(requester_id)
        # state lives in memory; also mirrored into SWAP_SESSIONS
        st = SWAP_SESSIONS.get(self.requester_id) or {
            "from": "HCC",
            "amount": "",
            "slippage_bps": int(DEFAULT_SLIPPAGE_BPS),
        }
        SWAP_SESSIONS[self.requester_id] = st
        self.state = st

    def _other_asset(self, a: str) -> str:
        return "VECO" if a == "HCC" else "HCC"

    def _parse_amount_in(self) -> Tuple[Optional[int], Optional[str]]:
        fa = str(self.state.get("from") or "HCC").upper()
        amt_s = str(self.state.get("amount") or "").strip()
        if not amt_s:
            return None, None
        if fa == "HCC":
            if not amt_s.isdigit():
                raise ValueError("HCC amount must be an integer")
            v = int(amt_s)
            if v <= 0:
                raise ValueError("amount must be > 0")
            return v, "HCC"
        # VECO
        v_sat = parse_veco_to_sat(amt_s)
        return v_sat, "VECO"

    def _build_embed(self, u: Dict[str, Any], p: Dict[str, int], quote_text: str) -> discord.Embed:
        fa = str(self.state.get("from") or "HCC").upper()
        ta = self._other_asset(fa)
        slip_bps = int(self.state.get("slippage_bps") or DEFAULT_SLIPPAGE_BPS)
        slip_pct = Decimal(slip_bps) / Decimal(100)

        hcc_bal = int(u.get("balance") or 0)
        veco_bal = int(u.get("veco_internal_sat") or 0)

        # Spot price (no trade) derived from reserves
        # VECO per HCC = R_veco / R_hcc
        spot_veco_per_hcc = Decimal(p["veco_reserve_sat"]) / Decimal(VECO_SATS) / Decimal(max(1, p["hcc_reserve"]))
        # HCC per VECO = R_hcc / R_veco
        spot_hcc_per_veco = Decimal(p["hcc_reserve"]) / (Decimal(p["veco_reserve_sat"]) / Decimal(VECO_SATS) if p["veco_reserve_sat"] else Decimal("1"))

        e = discord.Embed(title="Swap (HCC ⇄ VECO)")
        e.add_field(name="From", value=fa, inline=True)
        e.add_field(name="To", value=ta, inline=True)
        e.add_field(name="Amount", value=(str(self.state.get("amount") or "(enter amount)")), inline=False)
        e.add_field(name="Slippage", value=f"{slip_pct}%", inline=True)
        e.add_field(name="Pool fee", value=f"{Decimal(p['fee_bps'])/Decimal(100)}%", inline=True)
        e.add_field(name="Your balances", value=f"HCC: **{hcc_bal}**\nVECO: **{format_sat_to_veco(veco_bal)}**", inline=False)
        e.add_field(
            name="Spot price (pool)",
            value=(
                f"1 HCC ≈ **{spot_veco_per_hcc:.8f} VECO**\n"
                f"1 VECO ≈ **{spot_hcc_per_veco:.4f} HCC**"
            ),
            inline=False,
        )
        e.add_field(name="Quote", value=quote_text, inline=False)
        return e
    @discord.ui.button(label="Max", style=discord.ButtonStyle.gray, custom_id="swap_max")
    async def max_amount(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.requester_id:
            await interaction.response.send_message("This swap panel is not for you.", ephemeral=True)
            return

        con = db()
        try:
            u = get_or_create_user(con, self.requester_id)
            fa = str(self.state.get("from") or "HCC").upper()
            if fa == "HCC":
                bal = int(u.get("balance") or 0)
                self.state["amount"] = str(max(0, bal))
            else:
                bal_sat = int(u.get("veco_internal_sat") or 0)
                self.state["amount"] = format_sat_to_veco(max(0, bal_sat))
        finally:
            try:
                con.close()
            except Exception:
                pass

        await self.render(interaction, replace_message=True)

    async def render(self, interaction: discord.Interaction, replace_message: bool = False) -> None:
        if interaction.user.id != self.requester_id:
            await interaction.response.send_message("This swap panel is not for you.", ephemeral=True)
            return

        # Compute quote
        quote_text = "Enter an amount to see a quote."
        can_swap = False
        con = db()
        try:
            u = get_or_create_user(con, self.requester_id)
            p = get_pool(con)
            fa = str(self.state.get("from") or "HCC").upper()
            slip_bps = int(self.state.get("slippage_bps") or DEFAULT_SLIPPAGE_BPS)

            amt_in, parsed_asset = self._parse_amount_in()
            if amt_in is not None and parsed_asset is not None:
                fee_bps = int(p["fee_bps"])
                if fa == "HCC":
                    if int(u.get("balance") or 0) < amt_in:
                        quote_text = "Insufficient HCC balance."
                    else:
                        q = quote_hcc_to_veco(int(amt_in), p["hcc_reserve"], p["veco_reserve_sat"], fee_bps)
                        min_out = apply_slippage_min_out(q.amount_out, slip_bps)
                        quote_text = (
                            f"Pay: **{amt_in} HCC**\n"
                            f"Receive (est.): **{format_sat_to_veco(q.amount_out)} VECO**\n"
                            f"Min received: **{format_sat_to_veco(min_out)} VECO**\n"
                            f"Fee: **{q.fee_amount} HCC**\n"
                            f"Price impact: **{q.price_impact_bps/100:.2f}%**"
                        )
                        can_swap = True
                else:
                    if int(u.get("veco_internal_sat") or 0) < amt_in:
                        quote_text = "Insufficient VECO balance."
                    else:
                        q = quote_veco_to_hcc(int(amt_in), p["hcc_reserve"], p["veco_reserve_sat"], fee_bps)
                        min_out = apply_slippage_min_out(q.amount_out, slip_bps)
                        quote_text = (
                            f"Pay: **{format_sat_to_veco(amt_in)} VECO**\n"
                            f"Receive (est.): **{q.amount_out} HCC**\n"
                            f"Min received: **{min_out} HCC**\n"
                            f"Fee: **{format_sat_to_veco(q.fee_amount)} VECO**\n"
                            f"Price impact: **{q.price_impact_bps/100:.2f}%**"
                        )
                        can_swap = True

            embed = self._build_embed(u, p, quote_text)
        except Exception as e:
            # If parsing fails, show error but keep panel alive
            con.close()
            embed = discord.Embed(title="Swap (HCC ⇄ VECO)", description=f"⚠️ {e}")
            can_swap = False
        finally:
            try:
                con.close()
            except Exception:
                pass

        # Enable/disable swap button based on quote validity
        for child in self.children:
            if isinstance(child, discord.ui.Button) and child.custom_id == "swap_do":
                child.disabled = not can_swap

        if replace_message:
            # Edit the existing ephemeral message
            await interaction.response.edit_message(embed=embed, view=self)
        else:
            await interaction.response.send_message(embed=embed, view=self, ephemeral=True)

    async def _execute_swap(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.requester_id:
            await interaction.response.send_message("This swap panel is not for you.", ephemeral=True)
            return

        con = db()
        try:
            con.execute("BEGIN IMMEDIATE;")

            u = get_or_create_user(con, self.requester_id)
            p = get_pool(con)

            fa = str(self.state.get("from") or "HCC").upper()
            slip_bps = int(self.state.get("slippage_bps") or DEFAULT_SLIPPAGE_BPS)
            amt_in, _ = self._parse_amount_in()
            if amt_in is None:
                con.execute("ROLLBACK;")
                await interaction.response.send_message("Enter an amount first.", ephemeral=True)
                return

            fee_bps = int(p["fee_bps"])
            if fa == "HCC":
                amt_in_hcc = int(amt_in)
                if int(u.get("balance") or 0) < amt_in_hcc:
                    con.execute("ROLLBACK;")
                    await interaction.response.send_message("Insufficient HCC balance.", ephemeral=True)
                    return
                q = quote_hcc_to_veco(amt_in_hcc, p["hcc_reserve"], p["veco_reserve_sat"], fee_bps)
                min_out = apply_slippage_min_out(q.amount_out, slip_bps)

                out = int(q.amount_out)
                if out < min_out:
                    con.execute("ROLLBACK;")
                    await interaction.response.send_message("Slippage too high. Refresh the quote.", ephemeral=True)
                    return

                new_hcc_res = p["hcc_reserve"] + (amt_in_hcc - int(q.fee_amount))
                new_veco_res = p["veco_reserve_sat"] - out

                con.execute(
                    "UPDATE users SET balance = balance - ?, veco_internal_sat = veco_internal_sat + ?, updated_at=? WHERE discord_id=?",
                    (amt_in_hcc, out, now_ts(), self.requester_id)
                )
                set_pool(con, new_hcc_res, new_veco_res, fee_bps)

                con.execute(
                    "INSERT INTO swap_log(ts, discord_id, from_asset, to_asset, amount_in, amount_out, fee_amount, price_impact_bps, status, error) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (now_ts(), self.requester_id, "HCC", "VECO", amt_in_hcc, out, int(q.fee_amount), int(q.price_impact_bps), "ok", None)
                )

                con.execute("COMMIT;")
                await interaction.response.send_message(
                    f"Swap successful ✅\nYou swapped **{amt_in_hcc} HCC** → **{format_sat_to_veco(out)} VECO**\n"
                    f"Fee: {int(q.fee_amount)} HCC | Price impact: {q.price_impact_bps/100:.2f}%",
                    ephemeral=True,
                )
                return

            # VECO -> HCC
            amt_in_sat = int(amt_in)
            if int(u.get("veco_internal_sat") or 0) < amt_in_sat:
                con.execute("ROLLBACK;")
                await interaction.response.send_message("Insufficient VECO balance.", ephemeral=True)
                return

            q = quote_veco_to_hcc(amt_in_sat, p["hcc_reserve"], p["veco_reserve_sat"], fee_bps)
            min_out = apply_slippage_min_out(q.amount_out, slip_bps)

            out_hcc = int(q.amount_out)
            if out_hcc < min_out:
                con.execute("ROLLBACK;")
                await interaction.response.send_message("Slippage too high. Refresh the quote.", ephemeral=True)
                return

            new_veco_res = p["veco_reserve_sat"] + (amt_in_sat - int(q.fee_amount))
            new_hcc_res = p["hcc_reserve"] - out_hcc

            con.execute(
                "UPDATE users SET veco_internal_sat = veco_internal_sat - ?, balance = balance + ?, updated_at=? WHERE discord_id=?",
                (amt_in_sat, out_hcc, now_ts(), self.requester_id)
            )
            set_pool(con, new_hcc_res, new_veco_res, fee_bps)

            con.execute(
                "INSERT INTO swap_log(ts, discord_id, from_asset, to_asset, amount_in, amount_out, fee_amount, price_impact_bps, status, error) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                (now_ts(), self.requester_id, "VECO", "HCC", amt_in_sat, out_hcc, int(q.fee_amount), int(q.price_impact_bps), "ok", None)
            )

            con.execute("COMMIT;")
            await interaction.response.send_message(
                f"Swap successful ✅\nYou swapped **{format_sat_to_veco(amt_in_sat)} VECO** → **{out_hcc} HCC**\n"
                f"Fee: {format_sat_to_veco(int(q.fee_amount))} VECO | Price impact: {q.price_impact_bps/100:.2f}%",
                ephemeral=True,
            )

        except Exception as e:
            try:
                con.execute("ROLLBACK;")
            except Exception:
                pass
            await interaction.response.send_message(f"Swap failed ❌ `{e}`", ephemeral=True)
        finally:
            try:
                con.close()
            except Exception:
                pass

    @discord.ui.button(label="Flip", style=discord.ButtonStyle.blurple, custom_id="swap_flip")
    async def flip(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.requester_id:
            await interaction.response.send_message("This swap panel is not for you.", ephemeral=True)
            return
        fa = str(self.state.get("from") or "HCC").upper()
        self.state["from"] = self._other_asset(fa)
        await self.render(interaction, replace_message=True)

    @discord.ui.button(label="Enter amount", style=discord.ButtonStyle.gray, custom_id="swap_amount")
    async def enter_amount(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.requester_id:
            await interaction.response.send_message("This swap panel is not for you.", ephemeral=True)
            return
        await interaction.response.send_modal(SwapAmountModal(self))

    @discord.ui.button(label="Slippage", style=discord.ButtonStyle.gray, custom_id="swap_slip")
    async def slippage(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.requester_id:
            await interaction.response.send_message("This swap panel is not for you.", ephemeral=True)
            return
        await interaction.response.send_modal(SwapSlippageModal(self))

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.gray, custom_id="swap_refresh")
    async def refresh(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.requester_id:
            await interaction.response.send_message("This swap panel is not for you.", ephemeral=True)
            return
        await self.render(interaction, replace_message=True)

    @discord.ui.button(label="Swap", style=discord.ButtonStyle.green, custom_id="swap_do")
    async def do_swap(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._execute_swap(interaction)

    @discord.ui.button(label="Close", style=discord.ButtonStyle.red, custom_id="swap_close")
    async def close(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.requester_id:
            await interaction.response.send_message("This swap panel is not for you.", ephemeral=True)
            return
        # Disable all buttons when closing
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                child.disabled = True
        await interaction.response.edit_message(content="Swap panel closed.", embed=None, view=self)
        self.stop()


# --- ConfirmSwapView for CLI swap (/swap) ---
class ConfirmSwapView(discord.ui.View):
    def __init__(self, requester_id: int, quote_id: int):
        super().__init__(timeout=QUOTE_TTL_SECONDS)
        self.requester_id = int(requester_id)
        self.quote_id = int(quote_id)

    @discord.ui.button(label="Confirm Swap", style=discord.ButtonStyle.green)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.requester_id:
            await interaction.response.send_message("This swap confirmation is not for you.", ephemeral=True)
            return

        con = db()
        try:
            con.execute("BEGIN IMMEDIATE;")

            # Load quote
            q = con.execute(
                "SELECT expires_at, discord_id, from_asset, to_asset, amount_in, amount_out, min_out, fee_amount, price_impact_bps "
                "FROM swap_quotes WHERE id=?",
                (self.quote_id,)
            ).fetchone()
            if not q:
                con.execute("ROLLBACK;")
                await interaction.response.send_message("Quote not found (expired). Please request a new quote.", ephemeral=True)
                return

            expires_at = int(q[0])
            discord_id = int(q[1])
            from_asset = str(q[2])
            to_asset = str(q[3])
            amount_in = int(q[4])
            amount_out = int(q[5])
            min_out = int(q[6])
            fee_amount = int(q[7])
            impact_bps = int(q[8])

            if discord_id != interaction.user.id:
                con.execute("ROLLBACK;")
                await interaction.response.send_message("Quote owner mismatch.", ephemeral=True)
                return

            if now_ts() > expires_at:
                con.execute("ROLLBACK;")
                await interaction.response.send_message("Quote expired. Please request a new quote.", ephemeral=True)
                return

            # Load user + pool
            u = get_or_create_user(con, interaction.user.id)
            p = get_pool(con)

            # Re-quote against current reserves (protects against stale quotes)
            fee_bps = int(p["fee_bps"])
            if from_asset == "HCC" and to_asset == "VECO":
                if int(u.get("balance") or 0) < amount_in:
                    con.execute("ROLLBACK;")
                    await interaction.response.send_message("Insufficient HCC balance.", ephemeral=True)
                    return
                q2 = quote_hcc_to_veco(amount_in, p["hcc_reserve"], p["veco_reserve_sat"], fee_bps)
                out2 = int(q2.amount_out)
                if out2 < min_out:
                    con.execute("ROLLBACK;")
                    await interaction.response.send_message("Slippage too high. Please refresh the quote.", ephemeral=True)
                    return

                # Apply swap
                new_hcc_res = p["hcc_reserve"] + (amount_in - q2.fee_amount)
                new_veco_res = p["veco_reserve_sat"] - out2

                con.execute("UPDATE users SET balance = balance - ?, veco_internal_sat = veco_internal_sat + ?, updated_at=? WHERE discord_id=?",
                            (amount_in, out2, now_ts(), interaction.user.id))
                set_pool(con, new_hcc_res, new_veco_res, fee_bps)

                con.execute(
                    "INSERT INTO swap_log(ts, discord_id, from_asset, to_asset, amount_in, amount_out, fee_amount, price_impact_bps, status, error) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (now_ts(), interaction.user.id, "HCC", "VECO", amount_in, out2, int(q2.fee_amount), int(q2.price_impact_bps), "ok", None)
                )

                con.execute("COMMIT;")

                await interaction.response.send_message(
                    f"Swap successful ✅\nYou swapped **{amount_in} HCC** → **{format_sat_to_veco(out2)} VECO**\n"
                    f"Fee: {int(q2.fee_amount)} HCC | Price impact: {q2.price_impact_bps/100:.2f}%",
                    ephemeral=True,
                )
                self.stop()
                return

            if from_asset == "VECO" and to_asset == "HCC":
                if int(u.get("veco_internal_sat") or 0) < amount_in:
                    con.execute("ROLLBACK;")
                    await interaction.response.send_message("Insufficient VECO balance.", ephemeral=True)
                    return
                q2 = quote_veco_to_hcc(amount_in, p["hcc_reserve"], p["veco_reserve_sat"], fee_bps)
                out2 = int(q2.amount_out)
                if out2 < min_out:
                    con.execute("ROLLBACK;")
                    await interaction.response.send_message("Slippage too high. Please refresh the quote.", ephemeral=True)
                    return

                new_veco_res = p["veco_reserve_sat"] + (amount_in - q2.fee_amount)
                new_hcc_res = p["hcc_reserve"] - out2

                con.execute("UPDATE users SET veco_internal_sat = veco_internal_sat - ?, balance = balance + ?, updated_at=? WHERE discord_id=?",
                            (amount_in, out2, now_ts(), interaction.user.id))
                set_pool(con, new_hcc_res, new_veco_res, fee_bps)

                con.execute(
                    "INSERT INTO swap_log(ts, discord_id, from_asset, to_asset, amount_in, amount_out, fee_amount, price_impact_bps, status, error) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (now_ts(), interaction.user.id, "VECO", "HCC", amount_in, out2, int(q2.fee_amount), int(q2.price_impact_bps), "ok", None)
                )

                con.execute("COMMIT;")

                await interaction.response.send_message(
                    f"Swap successful ✅\nYou swapped **{format_sat_to_veco(amount_in)} VECO** → **{out2} HCC**\n"
                    f"Fee: {format_sat_to_veco(int(q2.fee_amount))} VECO | Price impact: {q2.price_impact_bps/100:.2f}%",
                    ephemeral=True,
                )
                self.stop()
                return

            con.execute("ROLLBACK;")
            await interaction.response.send_message("Unsupported swap direction.", ephemeral=True)

        except Exception as e:
            try:
                con.execute("ROLLBACK;")
            except Exception:
                pass
            await interaction.response.send_message(f"Swap failed ❌ `{e}`", ephemeral=True)
        finally:
            con.close()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.gray)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.requester_id:
            await interaction.response.send_message("This is not for you.", ephemeral=True)
            return
        await interaction.response.send_message("Cancelled.", ephemeral=True)
        self.stop()




@bot.tree.command(name="ui_swap", description="Open the swap UI (ephemeral).")
async def swap_ui(interaction: discord.Interaction):
    view = SwapPanelView(interaction.user.id)
    await view.render(interaction, replace_message=False)


@bot.tree.command(name="sawap", description="...")
async def sawap(interaction: discord.Interaction):
    gif_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "giphy-1.gif")

    if not os.path.exists(gif_path):
        await interaction.response.send_message(
            "GIF file not found: `giphy-1.gif`",
            ephemeral=True,
        )
        return

    file = discord.File(gif_path, filename="giphy-1.gif")
    await interaction.response.send_message(file=file, ephemeral=True)


@bot.tree.command(name="swap", description="Swap between internal HCC (integer) and internal VECO (8 decimals).")
@app_commands.describe(
    from_asset="Asset you pay (HCC or VECO)",
    amount="Amount you pay (HCC integer, VECO decimal)",
    slippage_pct="Max slippage in percent (e.g., 1 or 0.5)"
)
async def swap(interaction: discord.Interaction, from_asset: str, amount: str, slippage_pct: str = "1"):
    await interaction.response.defer(ephemeral=True, thinking=True)

    fa = (from_asset or "").strip().upper()
    if fa not in ("HCC", "VECO"):
        await interaction.followup.send("from_asset must be `HCC` or `VECO`.", ephemeral=True)
        return

    try:
        slippage_bps = percent_to_bps(str(slippage_pct))
    except Exception:
        slippage_bps = int(DEFAULT_SLIPPAGE_BPS)

    # Parse input amount
    try:
        if fa == "HCC":
            amt_in = int(str(amount).strip())
            if amt_in <= 0:
                raise ValueError("amount must be > 0")
            ta = "VECO"
        else:
            amt_in = parse_veco_to_sat(str(amount))
            ta = "HCC"
    except Exception as e:
        await interaction.followup.send(f"Invalid amount: `{e}`", ephemeral=True)
        return

    con = db()
    try:
        u = get_or_create_user(con, interaction.user.id)
        p = get_pool(con)

        fee_bps = int(p["fee_bps"])
        if fa == "HCC":
            if int(u.get("balance") or 0) < amt_in:
                await interaction.followup.send("Insufficient HCC balance.", ephemeral=True)
                return
            q = quote_hcc_to_veco(amt_in, p["hcc_reserve"], p["veco_reserve_sat"], fee_bps)
            min_out = apply_slippage_min_out(q.amount_out, slippage_bps)

            # Store quote
            ts = now_ts()
            exp = ts + QUOTE_TTL_SECONDS
            con.execute(
                "INSERT INTO swap_quotes(ts, expires_at, discord_id, from_asset, to_asset, amount_in, amount_out, min_out, fee_amount, price_impact_bps) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                (ts, exp, interaction.user.id, "HCC", "VECO", int(amt_in), int(q.amount_out), int(min_out), int(q.fee_amount), int(q.price_impact_bps))
            )
            quote_id = int(con.execute("SELECT last_insert_rowid()").fetchone()[0])

            await interaction.followup.send(
                f"**Quote (HCC → VECO)**\n"
                f"Pay: **{amt_in} HCC**\n"
                f"Receive (est.): **{format_sat_to_veco(q.amount_out)} VECO**\n"
                f"Min received (@ {Decimal(slippage_bps)/Decimal(100):.2f}% slippage): **{format_sat_to_veco(min_out)} VECO**\n"
                f"Fee: **{q.fee_amount} HCC** | Price impact: **{q.price_impact_bps/100:.2f}%**\n"
                f"(Quote expires in {QUOTE_TTL_SECONDS}s)",
                view=ConfirmSwapView(interaction.user.id, quote_id),
                ephemeral=True,
            )
            return

        # VECO -> HCC
        if int(u.get("veco_internal_sat") or 0) < amt_in:
            await interaction.followup.send("Insufficient VECO balance.", ephemeral=True)
            return
        q = quote_veco_to_hcc(amt_in, p["hcc_reserve"], p["veco_reserve_sat"], fee_bps)
        min_out = apply_slippage_min_out(q.amount_out, slippage_bps)

        ts = now_ts()
        exp = ts + QUOTE_TTL_SECONDS
        con.execute(
            "INSERT INTO swap_quotes(ts, expires_at, discord_id, from_asset, to_asset, amount_in, amount_out, min_out, fee_amount, price_impact_bps) "
            "VALUES(?,?,?,?,?,?,?,?,?,?)",
            (ts, exp, interaction.user.id, "VECO", "HCC", int(amt_in), int(q.amount_out), int(min_out), int(q.fee_amount), int(q.price_impact_bps))
        )
        quote_id = int(con.execute("SELECT last_insert_rowid()").fetchone()[0])

        await interaction.followup.send(
            f"**Quote (VECO → HCC)**\n"
            f"Pay: **{format_sat_to_veco(amt_in)} VECO**\n"
            f"Receive (est.): **{q.amount_out} HCC**\n"
            f"Min received (@ {Decimal(slippage_bps)/Decimal(100):.2f}% slippage): **{min_out} HCC**\n"
            f"Fee: **{format_sat_to_veco(q.fee_amount)} VECO** | Price impact: **{q.price_impact_bps/100:.2f}%**\n"
            f"(Quote expires in {QUOTE_TTL_SECONDS}s)",
            view=ConfirmSwapView(interaction.user.id, quote_id),
            ephemeral=True,
        )

    except Exception as e:
        await interaction.followup.send(f"Swap quote failed ❌ `{e}`", ephemeral=True)
    finally:
        con.close()


def main():
    if not DISCORD_TOKEN:
        raise SystemExit("Missing DISCORD_TOKEN")
    bot.run(DISCORD_TOKEN)


if __name__ == "__main__":
    main()
