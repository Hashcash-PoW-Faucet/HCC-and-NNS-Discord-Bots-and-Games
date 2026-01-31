import os
import re
import json
import time
import sqlite3
from typing import Optional, Dict, Any

from dotenv import load_dotenv

import discord
from discord import app_commands
import aiohttp

load_dotenv()

# ---------------------------
# Config
# ---------------------------
DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN", "").strip()

FAUCET_API_BASE = os.environ.get("FAUCET_API_BASE", "http://127.0.0.1:8000").rstrip("/")
TIPBOT_TREASURY_SECRET = os.environ.get("TIPBOT_TREASURY_SECRET", "").strip()

DB_PATH = os.environ.get("TIPBOT_DB", "tipbot.db").strip()

# Withdraw policy (your requested values)
MIN_WITHDRAW = int(os.environ.get("MIN_WITHDRAW", "1"))
WITHDRAW_COOLDOWN = int(os.environ.get("WITHDRAW_COOLDOWN", "60"))  # seconds
MAX_WITHDRAW_PER_DAY = int(os.environ.get("MAX_WITHDRAW_PER_DAY", "200"))

# Optional: announce tips/withdraws publicly (non-ephemeral)
PUBLIC_TIP_ANNOUNCEMENTS = os.environ.get("PUBLIC_TIP_ANNOUNCEMENTS", "1").strip() == "1"
PUBLIC_WITHDRAW_ANNOUNCEMENTS = os.environ.get("PUBLIC_WITHDRAW_ANNOUNCEMENTS", "1").strip() == "1"
PUBLIC_SHOW_ADDRESS = os.environ.get("PUBLIC_SHOW_ADDRESS", "0").strip() == "1"

# Optional: speed up slash-command sync by limiting to one guild during development
GUILD_ID = os.environ.get("GUILD_ID", "").strip()

# Faucet address format (derived from sha256(secret).hexdigest()[:40])
ADDR_RE = re.compile(r"^[0-9a-fA-F]{40}$")


# ---------------------------
# Helpers
# ---------------------------
def now_ts() -> int:
    return int(time.time())


def day_key(ts: Optional[int] = None) -> str:
    ts = ts or now_ts()
    return time.strftime("%Y-%m-%d", time.gmtime(ts))


def normalize_addr(addr: str) -> str:
    addr = (addr or "").strip()
    if not ADDR_RE.match(addr):
        raise ValueError("bad address format")
    return addr.lower()


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
      balance INTEGER NOT NULL DEFAULT 0,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      last_withdraw_at INTEGER NOT NULL DEFAULT 0
    );
    """)
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
      type TEXT NOT NULL,          -- tip | grant | withdraw
      from_id INTEGER,
      to_id INTEGER,
      amount INTEGER NOT NULL,
      note TEXT,
      status TEXT NOT NULL,        -- ok | pending | failed
      faucet_resp TEXT,
      error TEXT
    );
    """)
    con.close()


def get_or_create_user(con: sqlite3.Connection, discord_id: int) -> Dict[str, Any]:
    row = con.execute(
        "SELECT discord_id, address, balance, created_at, updated_at, last_withdraw_at FROM users WHERE discord_id=?",
        (discord_id,)
    ).fetchone()
    if row:
        return {
            "discord_id": int(row[0]),
            "address": row[1],
            "balance": int(row[2]),
            "created_at": int(row[3]),
            "updated_at": int(row[4]),
            "last_withdraw_at": int(row[5]),
        }
    ts = now_ts()
    con.execute(
        "INSERT INTO users(discord_id, address, balance, created_at, updated_at, last_withdraw_at) VALUES(?,?,?,?,?,0)",
        (discord_id, None, 0, ts, ts)
    )
    return {
        "discord_id": discord_id,
        "address": None,
        "balance": 0,
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


def has_pending_withdraw(con: sqlite3.Connection, discord_id: int) -> bool:
    row = con.execute(
        "SELECT id FROM tx_log WHERE type='withdraw' AND from_id=? AND status='pending' ORDER BY id DESC LIMIT 1",
        (discord_id,)
    ).fetchone()
    return bool(row)


# ---------------------------
# Faucet API calls (matches your FastAPI app.py)
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
        "**HCC TipBot**\n"
        "• `/register_address <address>` – Register your 40-hex HCC address (no existence check).\n"
        "• `/balance` – Show your discord account's internal HCC balance.\n"
        "• `/tip @user <amount> [note]` – Tip HCC to another user.\n"
        "• `/withdraw <amount>` – Withdraw HCC from your discord account to your registered HCC address.\n\n"
        f"Withdraw policy:\n"
        f"• Min: `{MIN_WITHDRAW}`\n"
        f"• Cooldown: `{WITHDRAW_COOLDOWN}s`\n"
        f"• Max per day: `{MAX_WITHDRAW_PER_DAY}`\n\n"
        "Notes:\n"
        "• This bot uses an internal ledger (no deposits).\n"
        "• If your HCC address does not exist yet, withdraw will fail (unknown recipient).\n"
    )
    await interaction.response.send_message(text, ephemeral=True)


@bot.tree.command(
    name="register_address",
    description="Register your HCC address (40 hex). No existence check."
)
@app_commands.describe(address="Your HCC address (40 hex characters)")
async def register_address(interaction: discord.Interaction, address: str):
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
        f"Registered ✅\nStored address: `{addr}`\nYou can withdraw with `/withdraw <amount>`.",
        ephemeral=True
    )


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


@bot.tree.command(name="withdraw", description="Withdraw to your registered HCC address (via bot treasury).")
@app_commands.describe(amount="Amount to withdraw")
async def withdraw(interaction: discord.Interaction, amount: int):
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
            await interaction.followup.send("No HCC address registered. Use `/register_address` first.", ephemeral=True)
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

    # 2) Call faucet /transfer (outside DB lock)
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

    # Friendly hints for common faucet errors
    hint = ""
    if err_msg:
        if "unknown recipient address" in err_msg or "(404)" in err_msg:
            hint = (
                "\n\nHint: Your HCC address is **unknown** to the faucet server. "
                "Create it first via PoW signup and ensure the 40-hex address is correct."
            )
        elif "insufficient HCC" in err_msg or "(400)" in err_msg:
            hint = "\n\nHint: Bot treasury has insufficient HCC right now. Ask admin to top it up."

    await interaction.followup.send(
        f"Withdraw failed ❌ The reserved amount was refunded.\nError: `{err_msg}`{hint}",
        ephemeral=True
    )


@bot.tree.command(name="whoami", description="Show your registered HCC address and basic limits.")
async def whoami(interaction: discord.Interaction):
    con = db()
    try:
        u = get_or_create_user(con, interaction.user.id)
        addr = u.get("address") or "(not set)"
        dk = day_key()
        wd = get_withdrawn_today(con, interaction.user.id, dk)
        await interaction.response.send_message(
            f"Registered HCC address: `{addr}`\n"
            f"TipBot balance: **{u['balance']}**\n"
            f"Withdraw today: `{wd}/{MAX_WITHDRAW_PER_DAY}`\n"
            f"Withdraw cooldown: `{WITHDRAW_COOLDOWN}s` | Min withdraw: `{MIN_WITHDRAW}`",
            ephemeral=True
        )
    finally:
        con.close()


# ---------------------------
# Admin commands
# ---------------------------
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


def main():
    if not DISCORD_TOKEN:
        raise SystemExit("Missing DISCORD_TOKEN")
    bot.run(DISCORD_TOKEN)


if __name__ == "__main__":
    main()