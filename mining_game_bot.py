import discord
from discord.ext import commands, tasks
from discord.ui import View, Button
import json
import os
import asyncio
# from core.tx_utils import safe_append_tx, get_nonce
from dotenv import load_dotenv

# Load .env early so module-level os.getenv() reads the intended values.
load_dotenv(override=True)
import math
import time
import sqlite3
# from typing import Optional, Dict, Any, Tuple, List

# --- MinerGame Constants ---
MINER_GAME_VERSION = "1.5.0"
RIG_BASE_BUILD_COST = 10
ASIC_COST = 45
GPU_COST = 15
MAX_RIG_LEVEL = 12
MAX_ASICS_PER_LEVEL = {lvl: min(1 + (lvl - 1) * 1, 12) for lvl in range(1, MAX_RIG_LEVEL + 1)}
MAX_GPUS_PER_LEVEL = {lvl: min(2 + (lvl - 1) * 2, 24) for lvl in range(1, MAX_RIG_LEVEL + 1)}
BASE_UPGRADE_TIME_MINUTES = 60

# --- New: simple CPUs ---
CPU_COST = int(os.getenv("CPU_COST", "2"))
MAX_CPUS_PER_LEVEL = {lvl: min(8 * lvl, 80) for lvl in range(1, MAX_RIG_LEVEL + 1)}

# --- Overclock (temporary boost) ---
# Duration is fixed: 24h
OVERCLOCK_DURATION_SECONDS = int(os.getenv("OVERCLOCK_DURATION_SECONDS", str(24 * 3600)))

# Boost is a percentage applied multiplicatively to the device's raw contribution while active.
OVERCLOCK_BOOST_PCT_ASIC = float(os.getenv("OVERCLOCK_BOOST_PCT_ASIC", "25"))  # e.g. 25 => +25%
OVERCLOCK_BOOST_PCT_GPU = float(os.getenv("OVERCLOCK_BOOST_PCT_GPU",  "20"))  # e.g. 20 => +20%

# Cost per device per overclock activation
OVERCLOCK_COST_ASIC = int(os.getenv("OVERCLOCK_COST_ASIC", "20"))
OVERCLOCK_COST_GPU = int(os.getenv("OVERCLOCK_COST_GPU",  "10"))

# --- New: CPU overclock (all CPUs together) ---
# CPUs share one effective overclock state: when activated, all CPUs of a rig
# get the same temporary boost. Cost scales linearly with the number of CPUs.
OVERCLOCK_BOOST_PCT_CPU = float(os.getenv("OVERCLOCK_BOOST_PCT_CPU", "35"))   # +35% hashrate
OVERCLOCK_COST_CPU_PER_DEVICE = int(os.getenv("OVERCLOCK_COST_CPU_PER_DEVICE", "1"))  # 1 Ħ per CPU

# --- Power Plant (extends Overclock duration) ---
POWER_PLANT_MAX_LEVEL = int(os.getenv("POWER_PLANT_MAX_LEVEL", "3"))

# Upgrade costs for levels 0->1, 1->2, 2->3 (override via .env if desired)
POWER_PLANT_COST_L1 = int(os.getenv("POWER_PLANT_COST_L1", "100"))
POWER_PLANT_COST_L2 = int(os.getenv("POWER_PLANT_COST_L2", "240"))
POWER_PLANT_COST_L3 = int(os.getenv("POWER_PLANT_COST_L3", "360"))


ACCELERATOR_COST_PER_HOUR =int(os.getenv("ACCELERATOR_COST_PER_HOUR", "2"))
# How many seconds of upgrade time are shaved off per accelerator click (default: 1h)
ACCELERATOR_STEP_SECONDS = int(os.getenv("ACCELERATOR_STEP_SECONDS", str(3600)))

# Automatic pool payout: total Ħ paid out every interval (default: 60 Ħ / 4 hours)
FACTORY_FILE = os.getenv("FACTORY_FILE", "mining_game_state.json").strip()
if not FACTORY_FILE:
    FACTORY_FILE = "mining_game_state.json"
PAYOUT_INTERVAL_SECONDS = int(os.getenv("PAYOUT_INTERVAL_SECONDS", str(4 * 3600)))

PAYOUT_TOTAL_PER_INTERVAL_AND_PLAYER = int(os.getenv("PAYOUT_TOTAL_PER_INTERVAL_AND_PLAYER", "7"))

EFFECTIVE_POWER_MODE = os.getenv("EFFECTIVE_POWER_MODE", "sqrt").strip().lower()  # sqrt
GAME_TREASURY_DISCORD_ID = int((os.getenv("GAME_TREASURY_DISCORD_ID", "0") or "0").strip())


TIPBOT_DB_PATH = os.getenv("TIPBOT_DB", "tipbot.db").strip()
# Automatic pool payout: total Ħ paid out every interval.
# Total payout is computed as: PAYOUT_TOTAL_PER_INTERVAL_AND_PLAYER * NUM_PLAYERS.
# --- Optional: name cache for webhook leaderboards (ID -> display name) ---
NAMES_CACHE_FILE = os.getenv("NAMES_CACHE_FILE", "names_cache.json").strip() or "names_cache.json"


def infer_num_players(data: dict) -> int:
    if not isinstance(data, dict):
        return 0
    for k in ("users", "players", "rigs", "miners"):
        v = data.get(k)
        if isinstance(v, dict):
            return len(v)
    # fallback: mapping user_id -> rig
    if data and all(isinstance(v, dict) for v in data.values()) and any(str(k).isdigit() for k in data.keys()):
        return len(data)
    return 0


def payout_total_for_state(data: dict) -> int:
    return PAYOUT_TOTAL_PER_INTERVAL_AND_PLAYER * infer_num_players(data)


def _load_names_cache() -> dict:
    try:
        if os.path.exists(NAMES_CACHE_FILE):
            with open(NAMES_CACHE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, dict) else {}
    except Exception:
        pass
    return {}


def _save_names_cache(cache: dict) -> None:
    # atomic-ish write to avoid partial files
    try:
        tmp = f"{NAMES_CACHE_FILE}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
        os.replace(tmp, NAMES_CACHE_FILE)
    except Exception:
        # don't crash the bot because of cache IO
        pass


def update_names_cache_for_user(user: discord.abc.User) -> None:
    """Persist a best-effort mapping discord_id -> display name.

    This enables external scripts (webhook leaderboard) to render nice names in code blocks
    without needing a bot token.
    """
    try:
        uid = str(int(user.id))
        # Prefer guild display name (nickname) when available; otherwise global name/username
        name = getattr(user, "display_name", None) or getattr(user, "global_name", None) or getattr(user, "name", None) or uid
        name = str(name).strip() or uid
        cache = _load_names_cache()
        if cache.get(uid) != name:
            cache[uid] = name
            _save_names_cache(cache)
    except Exception:
        pass


def stars_to_multiplier(stars: int) -> float:
    return min(1.0 + 0.5 * math.log2(1 + stars), 5.0)


def _clamp_float(x: float, lo: float, hi: float) -> float:
    try:
        v = float(x)
    except Exception:
        v = float(lo)
    return max(float(lo), min(float(hi), v))


def is_overclock_active(dev: dict, now_ts: int) -> bool:
    try:
        until = int(dev.get("overclock_until") or 0)
        return until > int(now_ts or time.time())
    except Exception:
        return False


def overclock_multiplier_for_device(dev: dict, now_ts: int, boost_pct: float) -> float:
    if not dev or not isinstance(dev, dict):
        return 1.0
    if not is_overclock_active(dev, now_ts):
        return 1.0
    pct = _clamp_float(boost_pct, 0.0, 500.0)
    return 1.0 + (pct / 100.0)


def overclock_remaining_seconds(dev: dict, now_ts: int) -> int:
    try:
        until = int(dev.get("overclock_until") or 0)
        return max(0, int(until - int(now_ts or time.time())))
    except Exception:
        return 0


def power_plant_level(rig: dict) -> int:
    try:
        lvl = int((rig or {}).get("power_plant_level", 0) or 0)
    except Exception:
        lvl = 0
    return max(0, min(int(POWER_PLANT_MAX_LEVEL), lvl))


def overclock_duration_seconds_for_rig(rig: dict) -> int:
    """Compute OC duration based on Power Plant level.

    Level 0: 24h (default)
    Level 1: 48h
    Level 2: 72h
    Level 3: 96h
    """
    lvl = power_plant_level(rig)
    return int(24 * 3600 * (1 + lvl))


def power_plant_upgrade_cost(next_level: int) -> int:
    next_level = int(next_level)
    if next_level == 1:
        return int(POWER_PLANT_COST_L1)
    if next_level == 2:
        return int(POWER_PLANT_COST_L2)
    if next_level == 3:
        return int(POWER_PLANT_COST_L3)
    return int(POWER_PLANT_COST_L3)


# --- TipBot DB helpers ---
def db() -> sqlite3.Connection:
    con = sqlite3.connect(TIPBOT_DB_PATH, timeout=30, isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con


def ensure_user_row(con: sqlite3.Connection, discord_id: int) -> None:
    row = con.execute("SELECT 1 FROM users WHERE discord_id=?", (discord_id,)).fetchone()
    if row:
        return
    ts = int(time.time())
    con.execute(
        "INSERT INTO users(discord_id, address, balance, created_at, updated_at, last_withdraw_at) VALUES(?,?,?,?,?,0)",
        (discord_id, None, 0, ts, ts)
    )


def get_user_balance(discord_id: int) -> int:
    con = db()
    try:
        ensure_user_row(con, int(discord_id))
        row = con.execute("SELECT balance FROM users WHERE discord_id=?", (int(discord_id),)).fetchone()
        return int(row[0] or 0) if row else 0
    finally:
        con.close()


def transfer_internal(from_id: int, to_id: int, amount: int, note: str) -> bool:
    """Move Ħ inside TipBot ledger (users.balance). Returns True on success."""
    amount = int(amount)
    if amount <= 0:
        return False
    if int(from_id) == int(to_id):
        return False
    con = db()
    try:
        con.execute("BEGIN IMMEDIATE;")
        ensure_user_row(con, int(from_id))
        ensure_user_row(con, int(to_id))
        row = con.execute("SELECT balance FROM users WHERE discord_id=?", (int(from_id),)).fetchone()
        bal = int(row[0] or 0) if row else 0
        if bal < amount:
            con.execute("ROLLBACK;")
            return False
        ts = int(time.time())
        con.execute("UPDATE users SET balance = balance - ?, updated_at=? WHERE discord_id=?", (amount, ts, int(from_id)))
        con.execute("UPDATE users SET balance = balance + ?, updated_at=? WHERE discord_id=?", (amount, ts, int(to_id)))
        # optional log table (if exists)
        try:
            con.execute(
                "INSERT INTO tx_log(ts, type, from_id, to_id, amount, note, status) VALUES(?,?,?,?,?,?,?)",
                (ts, "game", int(from_id), int(to_id), amount, note[:200], "ok")
            )
        except Exception:
            pass
        con.execute("COMMIT;")
        return True
    except Exception:
        try:
            con.execute("ROLLBACK;")
        except Exception:
            pass
        raise
    finally:
        con.close()


def credit_internal(to_id: int, amount: int, note: str) -> None:
    """Credit Ħ inside TipBot ledger (admin grant style)."""
    amount = max(0, int(amount))
    if amount <= 0:
        return
    con = db()
    try:
        con.execute("BEGIN IMMEDIATE;")
        ensure_user_row(con, int(to_id))
        ts = int(time.time())
        con.execute("UPDATE users SET balance = balance + ?, updated_at=? WHERE discord_id=?", (amount, ts, int(to_id)))
        try:
            con.execute(
                "INSERT INTO tx_log(ts, type, from_id, to_id, amount, note, status) VALUES(?,?,?,?,?,?,?)",
                (ts, "grant", None, int(to_id), amount, note[:200], "ok")
            )
        except Exception:
            pass
        con.execute("COMMIT;")
    except Exception:
        try:
            con.execute("ROLLBACK;")
        except Exception:
            pass
        raise
    finally:
        con.close()


def compute_raw_power(rig: dict, now_ts: int = None) -> int:
    now_ts = int(now_ts or time.time())

    raw = 0.0
    rig_level = int(rig.get("rig_level", 1) or 1)
    raw += max(0, (rig_level - 1)) * 10

    for asic in rig.get("asics", []) or []:
        stars = int(asic.get("stars", 0) or 0)
        base = float(asic_raw_for_stars(stars))
        mult = overclock_multiplier_for_device(asic, now_ts, OVERCLOCK_BOOST_PCT_ASIC)
        raw += base * mult

    for gpu in rig.get("gpus", []) or []:
        stars = int(gpu.get("stars", 0) or 0)
        base = float(gpu_raw_for_stars(stars))
        mult = overclock_multiplier_for_device(gpu, now_ts, OVERCLOCK_BOOST_PCT_GPU)
        raw += base * mult

    # CPUs: simple, non-upgradeable 1 Gh/s each.
    # They can be overclocked as a *group*; each CPU stores its own
    # `overclock_until` but we always set them all together.
    for cpu in rig.get("cpus", []) or []:
        base = 1.0
        mult = overclock_multiplier_for_device(cpu, now_ts, OVERCLOCK_BOOST_PCT_CPU)
        raw += base * mult

    return max(1.0, float(raw))


# --- Explicit per-device raw hashrate helpers ---
ASIC_RAW_TABLE = {
    0: 0,
    1: 50,   # unchanged for existing rigs
    2: 80,
    3: 115,
    4: 155,
    5: 200,
}


def asic_raw_for_stars(stars: int) -> int:
    """
    Raw hashrate contribution for a single ASIC with given star level.
    Must stay in sync with compute_raw_power().

    Uses a non-linear lookup table so that higher-star ASICs scale
    more aggressively than GPUs, while keeping 1★ behavior unchanged.
    """
    stars = max(0, min(5, int(stars)))
    return ASIC_RAW_TABLE.get(stars, ASIC_RAW_TABLE[5])


def gpu_raw_for_stars(stars: int) -> int:
    """
    Raw hashrate contribution for a single GPU with given star level.
    Must stay in sync with compute_raw_power().
    """
    return stars * 10 + 10


def compute_effective_power(raw_power: int) -> float:
    if EFFECTIVE_POWER_MODE == "sqrt":
        return math.sqrt(max(0, raw_power))
    else:
        return float(raw_power)


def current_payout_slot(ts: int) -> int:
    ts = int(ts or time.time())
    interval = max(60, int(PAYOUT_INTERVAL_SECONDS))
    return ts // interval


# --- Helper: Format seconds as D:H:M:S (days only shown when >= 24h) ---
def fmt_duration_hms(seconds: int) -> str:
    """Format a duration as Dd Hh Mm Ss (days only shown when >= 24h)."""
    seconds = max(0, int(seconds or 0))
    d = seconds // 86400
    h = (seconds % 86400) // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    if d > 0:
        return f"{d}d {h}h {m}m {s}s"
    return f"{h}h {m}m {s}s"


def fmt_duration_days_only(seconds: int) -> str:
    """Format long, fixed durations in whole days (e.g. 1d, 2d).

    Intended for *static* durations like OC duration (24/48/72/96h), not countdowns.
    Rounds to the nearest whole day.
    """
    seconds = max(0, int(seconds or 0))
    if seconds < 86400:
        # For sub-day static durations, show hours only
        h = max(0, int(round(seconds / 3600.0)))
        return f"{h}h"
    d = max(1, int(round(seconds / 86400.0)))
    return f"{d}d"

# --- Helper: Next payout ETA in seconds ---
def next_payout_eta_seconds(now_ts: int) -> int:
    now_ts = int(now_ts or time.time())
    interval = max(60, int(PAYOUT_INTERVAL_SECONDS))
    next_boundary = (now_ts // interval + 1) * interval
    return max(0, int(next_boundary - now_ts))


def apply_completed_upgrades(rig: dict, now_ts: int) -> bool:
    """Apply any upgrades whose timers have completed. Returns True if rig changed."""
    changed = False

    # Rig level upgrade
    upgrade_time = rig.get("upgrade_ready_time")
    if upgrade_time is not None and int(upgrade_time) > 0 and now_ts >= int(upgrade_time):
        cur_lvl = int(rig.get("rig_level", 1) or 1)
        if cur_lvl < MAX_RIG_LEVEL:
            rig["rig_level"] = cur_lvl + 1
        rig["upgrade_ready_time"] = None
        changed = True

    # ASIC upgrades
    for a in rig.get("asics", []) or []:
        uet = a.get("upgrade_ready_time")
        if uet is not None and int(uet) > 0 and now_ts >= int(uet):
            cur = int(a.get("stars", 0) or 0)
            if cur < 5:
                a["stars"] = cur + 1
            a["upgrade_ready_time"] = None
            changed = True

    # GPU upgrades
    for g in rig.get("gpus", []) or []:
        uet = g.get("upgrade_ready_time")
        if uet is not None and int(uet) > 0 and now_ts >= int(uet):
            cur = int(g.get("stars", 0) or 0)
            if cur < 5:
                g["stars"] = cur + 1
            g["upgrade_ready_time"] = None
            changed = True

    return changed


def reduce_upgrade_timers(
    rig: dict,
    now_ts: int,
    step_seconds: int,
    include_rig: bool = True,
    include_asics: bool = True,
    include_gpus: bool = True,
) -> tuple[int, int]:
    """
    Reduces running upgrade timers by step_seconds.

    Parameters
    ----------
    rig : dict
        Rig state dictionary.
    now_ts : int
        Current UNIX timestamp.
    step_seconds : int
        How many seconds to subtract from each active timer (at most).
    include_rig : bool
        Whether to include the rig upgrade timer.
    include_asics : bool
        Whether to include ASIC upgrade timers.
    include_gpus : bool
        Whether to include GPU upgrade timers.

    Returns
    -------
    reduced_total : int
        Total number of seconds shaved off across all affected timers.
    affected_count : int
        Number of timers that were actually reduced.
    """
    if not rig or not isinstance(rig, dict):
        return 0, 0

    now_ts = int(now_ts or time.time())
    step_seconds = max(1, int(step_seconds))

    reduced_total = 0
    affected_count = 0

    # Rig-Upgrade
    if include_rig:
        u = rig.get("upgrade_ready_time")
        if u is not None and int(u) > now_ts:
            remaining = int(u) - now_ts
            reduce_by = min(step_seconds, remaining)
            rig["upgrade_ready_time"] = now_ts + max(0, remaining - reduce_by)
            if reduce_by > 0:
                reduced_total += reduce_by
                affected_count += 1

    # ASIC-Upgrades
    if include_asics:
        for a in rig.get("asics", []) or []:
            u = a.get("upgrade_ready_time")
            if u is not None and int(u) > now_ts:
                remaining = int(u) - now_ts
                reduce_by = min(step_seconds, remaining)
                a["upgrade_ready_time"] = now_ts + max(0, remaining - reduce_by)
                if reduce_by > 0:
                    reduced_total += reduce_by
                    affected_count += 1

    # GPU-Upgrades
    if include_gpus:
        for g in rig.get("gpus", []) or []:
            u = g.get("upgrade_ready_time")
            if u is not None and int(u) > now_ts:
                remaining = int(u) - now_ts
                reduce_by = min(step_seconds, remaining)
                g["upgrade_ready_time"] = now_ts + max(0, remaining - reduce_by)
                if reduce_by > 0:
                    reduced_total += reduce_by
                    affected_count += 1

    return int(reduced_total), int(affected_count)


class MinerView(View):

    class RefreshButton(discord.ui.Button):
        def __init__(self, user_id, factory_bot, row=0):
            super().__init__(label="🔄 Refresh", style=discord.ButtonStyle.blurple, custom_id="refresh", row=row)
            self.user_id = user_id
            self.miner_bot = factory_bot

        async def callback(self, interaction: discord.Interaction):
            rig = self.miner_bot.get_user_rig(self.user_id)
            if rig:
                now_ts = int(time.time())
                if apply_completed_upgrades(rig, now_ts):
                    self.miner_bot.update_user_rig(self.user_id, rig)
            embed = self.miner_bot.create_miner_embed(rig, self.user_id)
            await interaction.response.edit_message(embed=embed, view=self.view)

    class BuyAsicButton(Button):
        def __init__(self, row=None):
            extra_raw = asic_raw_for_stars(1)
            disabled = (int(GAME_TREASURY_DISCORD_ID) == 0)
            label = f"𒁈 Buy ASIC (+{extra_raw} Gh/s, {ASIC_COST} Ħ)"
            if disabled:
                label = "𒁈 Buy ASIC (treasury not set)"
            super().__init__(
                label=label,
                style=discord.ButtonStyle.gray if disabled else discord.ButtonStyle.primary,
                custom_id="buy_asic",
                row=row,
                disabled=disabled,
            )

        async def callback(self, interaction: discord.Interaction):
            view: MinerView = self.view
            if interaction.user.id != view.user_id:
                await interaction.response.send_message("This is not your miner!!", ephemeral=True)
                return
            rig = view.miner_bot.get_user_rig(view.user_id)
            max_asics = MAX_ASICS_PER_LEVEL.get(rig.get("rig_level", 1), 1)
            if len(rig.get("asics", [])) >= max_asics:
                await interaction.response.send_message(
                    f"❌ You already have the maximum number of ASICs ({max_asics}) for your rig level.",
                    ephemeral=True
                )
                return
            balance = get_user_balance(view.user_id)
            if balance < ASIC_COST:
                await interaction.response.send_message("❌ Not enough Ħ to buy an ASIC.", ephemeral=True)
                return
            if GAME_TREASURY_DISCORD_ID == 0:
                await interaction.response.send_message("⚠️ Treasury not configured. Cannot buy.", ephemeral=True)
                return
            ok = transfer_internal(view.user_id, GAME_TREASURY_DISCORD_ID, ASIC_COST, note="buy asic")
            if not ok:
                await interaction.response.send_message("❌ Still processing the previous action or insufficient funds.", ephemeral=True)
                return
            rig.setdefault("asics", []).append({"stars": 1, "upgrade_ready_time": None, "overclock_until": None})
            view.miner_bot.update_user_rig(view.user_id, rig)
            await interaction.response.send_message("𒁈 ASIC bought and added to your rig!", ephemeral=True)

    class BuyGpuButton(Button):
        def __init__(self, row=None):
            extra_raw = gpu_raw_for_stars(1)
            disabled = (int(GAME_TREASURY_DISCORD_ID) == 0)
            label = f"🎮 Buy GPU (+{extra_raw} Gh/s, {GPU_COST} Ħ)"
            if disabled:
                label = "🎮 Buy GPU (treasury not set)"
            super().__init__(
                label=label,
                style=discord.ButtonStyle.gray if disabled else discord.ButtonStyle.primary,
                custom_id="buy_gpu",
                row=row,
                disabled=disabled,
            )

        async def callback(self, interaction: discord.Interaction):
            view: MinerView = self.view
            if interaction.user.id != view.user_id:
                await interaction.response.send_message("This is not your miner!", ephemeral=True)
                return
            rig = view.miner_bot.get_user_rig(view.user_id)
            max_gpus = MAX_GPUS_PER_LEVEL.get(rig.get("rig_level", 1), 1)
            if len(rig.get("gpus", [])) >= max_gpus:
                await interaction.response.send_message(
                    f"❌ You already have the maximum number of GPUs ({max_gpus}) for your rig level.",
                    ephemeral=True
                )
                return
            balance = get_user_balance(view.user_id)
            if balance < GPU_COST:
                await interaction.response.send_message("❌ Not enough Ħ to buy a GPU.", ephemeral=True)
                return
            if GAME_TREASURY_DISCORD_ID == 0:
                await interaction.response.send_message("⚠️ Treasury not configured. Cannot buy.", ephemeral=True)
                return
            ok = transfer_internal(view.user_id, GAME_TREASURY_DISCORD_ID, GPU_COST, note="buy gpu")
            if not ok:
                await interaction.response.send_message("❌ Still processing the previous action or insufficient funds.", ephemeral=True)
                return
            rig.setdefault("gpus", []).append({"stars": 1, "upgrade_ready_time": None, "overclock_until": None})
            view.miner_bot.update_user_rig(view.user_id, rig)
            await interaction.response.send_message("🎮 GPU bought and added to your rig!", ephemeral=True)

    class BuyCpuButton(Button):
        def __init__(self, row=None):
            disabled = (int(GAME_TREASURY_DISCORD_ID) == 0)
            label = f"🔲 Buy CPU (+1 Gh/s, {CPU_COST} Ħ)"
            if disabled:
                label = "🔲 Buy CPU (treasury not set)"
            super().__init__(
                label=label,
                style=discord.ButtonStyle.gray if disabled else discord.ButtonStyle.primary,
                custom_id="buy_cpu",
                row=row,
                disabled=disabled,
            )

        async def callback(self, interaction: discord.Interaction):
            view: MinerView = self.view
            if interaction.user.id != view.user_id:
                await interaction.response.send_message("This is not your miner!", ephemeral=True)
                return

            rig = view.miner_bot.get_user_rig(view.user_id)
            if not rig or not isinstance(rig, dict):
                await interaction.response.send_message("❌ Rig not found.", ephemeral=True)
                return

            max_cpus = MAX_CPUS_PER_LEVEL.get(rig.get("rig_level", 1), 8)
            if len(rig.get("cpus", []) or []) >= max_cpus:
                await interaction.response.send_message(
                    f"❌ You already have the maximum number of CPUs ({max_cpus}) for your rig level.",
                    ephemeral=True,
                )
                return

            balance = get_user_balance(view.user_id)
            if balance < CPU_COST:
                await interaction.response.send_message("❌ Not enough Ħ to buy a CPU.", ephemeral=True)
                return

            if GAME_TREASURY_DISCORD_ID == 0:
                await interaction.response.send_message("⚠️ Treasury not configured. Cannot buy.", ephemeral=True)
                return

            ok = transfer_internal(view.user_id, GAME_TREASURY_DISCORD_ID, CPU_COST, note="buy cpu")
            if not ok:
                await interaction.response.send_message(
                    "❌ Still processing the previous action or insufficient funds.",
                    ephemeral=True,
                )
                return

            # Store CPUs as dicts so we can support overclock later if desired
            rig.setdefault("cpus", []).append({"overclock_until": None})
            view.miner_bot.update_user_rig(view.user_id, rig)
            await interaction.response.send_message("🔲 CPU bought and added to your rig!", ephemeral=True)

    class UpgradeAsicButton(Button):
        def __init__(self, row=None):
            disabled = (int(GAME_TREASURY_DISCORD_ID) == 0)
            label = "⭐ Upgrade ASIC" if not disabled else "⭐ Upgrade ASIC (treasury not set)"
            super().__init__(
                label=label,
                style=discord.ButtonStyle.blurple if not disabled else discord.ButtonStyle.gray,
                custom_id="upgrade_asic",
                row=row,
                disabled=disabled,
            )

        async def callback(self, interaction: discord.Interaction):
            view: MinerView = self.view
            if interaction.user.id != view.user_id:
                await interaction.response.send_message("This is not your miner!", ephemeral=True)
                return
            upgrade_view = SelectAsicView(view.user_id, view.miner_bot)
            await interaction.response.send_message(
                "Select an ASIC to upgrade:",
                view=upgrade_view,
                ephemeral=True
            )

    class UpgradeGpuButton(Button):
        def __init__(self, row=None):
            disabled = (int(GAME_TREASURY_DISCORD_ID) == 0)
            label = "⚙️ Upgrade GPU" if not disabled else "⚙️ Upgrade GPU (treasury not set)"
            super().__init__(
                label=label,
                style=discord.ButtonStyle.blurple if not disabled else discord.ButtonStyle.gray,
                custom_id="upgrade_gpu",
                row=row,
                disabled=disabled,
            )

        async def callback(self, interaction: discord.Interaction):
            view: MinerView = self.view
            if interaction.user.id != view.user_id:
                await interaction.response.send_message("This is not your miner!", ephemeral=True)
                return
            upgrade_view = SelectGpuView(view.user_id, view.miner_bot)
            await interaction.response.send_message(
                "Select a GPU to upgrade:",
                view=upgrade_view,
                ephemeral=True
            )

    class OverclockAsicButton(Button):
        def __init__(self, row=None):
            disabled = (int(GAME_TREASURY_DISCORD_ID) == 0)
            label = f"⚡ Overclock ASIC (+{OVERCLOCK_BOOST_PCT_ASIC:.0f}%, {OVERCLOCK_COST_ASIC} Ħ)"
            if disabled:
                label = "⚡ Overclock ASIC (treasury not set)"
            super().__init__(
                label=label,
                style=discord.ButtonStyle.gray if disabled else discord.ButtonStyle.green,
                custom_id="overclock_asic",
                row=row,
                disabled=disabled,
            )

        async def callback(self, interaction: discord.Interaction):
            view: MinerView = self.view
            if interaction.user.id != view.user_id:
                await interaction.response.send_message("This is not your miner!", ephemeral=True)
                return
            rig = view.miner_bot.get_user_rig(view.user_id)
            if not rig or not (rig.get("asics") or []):
                await interaction.response.send_message("❌ You have no ASICs to overclock.", ephemeral=True)
                return
            oc_view = SelectAsicOCView(view.user_id, view.miner_bot)
            dur = int(overclock_duration_seconds_for_rig(rig))
            await interaction.response.send_message(f"Select an ASIC to overclock ({fmt_duration_days_only(dur)} boost):", view=oc_view, ephemeral=True)

    class OverclockGpuButton(Button):
        def __init__(self, row=None):
            disabled = (int(GAME_TREASURY_DISCORD_ID) == 0)
            label = f"⚡ Overclock GPU (+{OVERCLOCK_BOOST_PCT_GPU:.0f}%, {OVERCLOCK_COST_GPU} Ħ)"
            if disabled:
                label = "⚡ Overclock GPU (treasury not set)"
            super().__init__(
                label=label,
                style=discord.ButtonStyle.gray if disabled else discord.ButtonStyle.green,
                custom_id="overclock_gpu",
                row=row,
                disabled=disabled,
            )

        async def callback(self, interaction: discord.Interaction):
            view: MinerView = self.view
            if interaction.user.id != view.user_id:
                await interaction.response.send_message("This is not your miner!", ephemeral=True)
                return
            rig = view.miner_bot.get_user_rig(view.user_id)
            if not rig or not (rig.get("gpus") or []):
                await interaction.response.send_message("❌ You have no GPUs to overclock.", ephemeral=True)
                return
            oc_view = SelectGpuOCView(view.user_id, view.miner_bot)
            dur = int(overclock_duration_seconds_for_rig(rig))
            await interaction.response.send_message(f"Select a GPU to overclock ({fmt_duration_days_only(dur)} boost):", view=oc_view, ephemeral=True)

    class OverclockAllAsicsButton(Button):
        def __init__(self, row=None):
            disabled = (int(GAME_TREASURY_DISCORD_ID) == 0)
            # Cost is per ASIC; total cost is calculated at click time.
            label = f"⚡ OC all ASICs (+{OVERCLOCK_BOOST_PCT_ASIC:.0f}%, {OVERCLOCK_COST_ASIC} Ħ each)"
            if disabled:
                label = "⚡ OC all ASICs (treasury not set)"
            super().__init__(
                label=label,
                style=discord.ButtonStyle.gray if disabled else discord.ButtonStyle.green,
                custom_id="overclock_all_asics",
                row=row,
                disabled=disabled,
            )

        async def callback(self, interaction: discord.Interaction):
            view: MinerView = self.view
            if interaction.user.id != view.user_id:
                await interaction.response.send_message("This is not your miner!", ephemeral=True)
                return

            rig = view.miner_bot.get_user_rig(view.user_id)
            asics = (rig.get("asics", []) or []) if rig else []
            if not asics:
                await interaction.response.send_message("❌ You have no ASICs to overclock.", ephemeral=True)
                return

            if GAME_TREASURY_DISCORD_ID == 0:
                await interaction.response.send_message("⚠️ Treasury not configured. Cannot overclock.", ephemeral=True)
                return

            now = int(time.time())
            # Partition ASICs into active and inactive for overclock
            active_asics = [a for a in asics if is_overclock_active(a, now)]
            inactive_asics = [a for a in asics if not is_overclock_active(a, now)]

            if not inactive_asics:
                if active_asics:
                    rem = max(overclock_remaining_seconds(a, now) for a in active_asics)
                    await interaction.response.send_message(
                        f"⚡ All ASICs are already overclocked. Remaining: {fmt_duration_hms(rem)}.",
                        ephemeral=True,
                    )
                    return
                else:
                    await interaction.response.send_message("❌ No ASICs available to overclock.", ephemeral=True)
                    return

            total_asics = len(asics)
            newly_overclocked = len(inactive_asics)
            total_cost = int(newly_overclocked * OVERCLOCK_COST_ASIC)
            bal = get_user_balance(view.user_id)
            if bal < total_cost:
                await interaction.response.send_message(
                    f"❌ Not enough Ħ to overclock {newly_overclocked} ASIC(s) (cost {total_cost} Ħ).",
                    ephemeral=True,
                )
                return

            ok = transfer_internal(view.user_id, GAME_TREASURY_DISCORD_ID, total_cost, note="overclock all asics")
            if not ok:
                await interaction.response.send_message(
                    "❌ Still processing the previous action or insufficient funds.",
                    ephemeral=True,
                )
                return

            dur = int(overclock_duration_seconds_for_rig(rig))
            until_ts = now + dur
            for a in inactive_asics:
                if isinstance(a, dict):
                    a["overclock_until"] = until_ts

            view.miner_bot.update_user_rig(view.user_id, rig)

            previously_active = len(active_asics)
            msg = (
                f"⚡ Overclocked {newly_overclocked} ASIC(s)! Boost +{OVERCLOCK_BOOST_PCT_ASIC:.0f}% "
                f"for {fmt_duration_hms(dur)} (Power Plant Lvl {power_plant_level(rig)}). "
                f"Total cost: {total_cost} Ħ."
            )
            if previously_active > 0:
                msg += f" ({previously_active} ASIC(s) were already overclocked and remain boosted.)"
            await interaction.response.send_message(msg, ephemeral=True)

    class OverclockAllGpusButton(Button):
        def __init__(self, row=None):
            disabled = (int(GAME_TREASURY_DISCORD_ID) == 0)
            # Cost is per GPU; total cost is calculated at click time.
            label = f"⚡ OC all GPUs (+{OVERCLOCK_BOOST_PCT_GPU:.0f}%, {OVERCLOCK_COST_GPU} Ħ each)"
            if disabled:
                label = "⚡ OC all GPUs (treasury not set)"
            super().__init__(
                label=label,
                style=discord.ButtonStyle.gray if disabled else discord.ButtonStyle.green,
                custom_id="overclock_all_gpus",
                row=row,
                disabled=disabled,
            )

        async def callback(self, interaction: discord.Interaction):
            view: MinerView = self.view
            if interaction.user.id != view.user_id:
                await interaction.response.send_message("This is not your miner!", ephemeral=True)
                return

            rig = view.miner_bot.get_user_rig(view.user_id)
            gpus = (rig.get("gpus", []) or []) if rig else []
            if not gpus:
                await interaction.response.send_message("❌ You have no GPUs to overclock.", ephemeral=True)
                return

            if GAME_TREASURY_DISCORD_ID == 0:
                await interaction.response.send_message("⚠️ Treasury not configured. Cannot overclock.", ephemeral=True)
                return

            now = int(time.time())
            # Partition GPUs into active and inactive for overclock
            active_gpus = [g for g in gpus if is_overclock_active(g, now)]
            inactive_gpus = [g for g in gpus if not is_overclock_active(g, now)]

            if not inactive_gpus:
                if active_gpus:
                    rem = max(overclock_remaining_seconds(g, now) for g in active_gpus)
                    await interaction.response.send_message(
                        f"⚡ All GPUs are already overclocked. Remaining: {fmt_duration_hms(rem)}.",
                        ephemeral=True,
                    )
                    return
                else:
                    await interaction.response.send_message("❌ No GPUs available to overclock.", ephemeral=True)
                    return

            newly_overclocked = len(inactive_gpus)
            total_cost = int(newly_overclocked * OVERCLOCK_COST_GPU)
            bal = get_user_balance(view.user_id)
            if bal < total_cost:
                await interaction.response.send_message(
                    f"❌ Not enough Ħ to overclock {newly_overclocked} GPU(s) (cost {total_cost} Ħ).",
                    ephemeral=True,
                )
                return

            ok = transfer_internal(view.user_id, GAME_TREASURY_DISCORD_ID, total_cost, note="overclock all gpus")
            if not ok:
                await interaction.response.send_message(
                    "❌ Still processing the previous action or insufficient funds.",
                    ephemeral=True,
                )
                return

            dur = int(overclock_duration_seconds_for_rig(rig))
            until_ts = now + dur
            for g in inactive_gpus:
                if isinstance(g, dict):
                    g["overclock_until"] = until_ts

            view.miner_bot.update_user_rig(view.user_id, rig)

            previously_active = len(active_gpus)
            msg = (
                f"⚡ Overclocked {newly_overclocked} GPU(s)! Boost +{OVERCLOCK_BOOST_PCT_GPU:.0f}% "
                f"for {fmt_duration_hms(dur)} (Power Plant Lvl {power_plant_level(rig)}). "
                f"Total cost: {total_cost} Ħ."
            )
            if previously_active > 0:
                msg += f" ({previously_active} GPU(s) were already overclocked and remain boosted.)"
            await interaction.response.send_message(msg, ephemeral=True)

    class OverclockCpuButton(Button):
        def __init__(self, row=None):
            disabled = (int(GAME_TREASURY_DISCORD_ID) == 0)
            label = f"🔲 Overclock CPUs (+{OVERCLOCK_BOOST_PCT_CPU:.0f}%, 1 Ħ per CPU)"
            if disabled:
                label = "🔲 Overclock CPUs (treasury not set)"
            super().__init__(
                label=label,
                style=discord.ButtonStyle.gray if disabled else discord.ButtonStyle.green,
                custom_id="overclock_cpu",
                row=row,
                disabled=disabled,
            )

        async def callback(self, interaction: discord.Interaction):
            view: MinerView = self.view
            if interaction.user.id != view.user_id:
                await interaction.response.send_message("This is not your miner!", ephemeral=True)
                return

            rig = view.miner_bot.get_user_rig(view.user_id)
            cpus = (rig.get("cpus", []) or []) if rig else []
            if not cpus:
                await interaction.response.send_message("❌ You have no CPUs to overclock.", ephemeral=True)
                return

            if GAME_TREASURY_DISCORD_ID == 0:
                await interaction.response.send_message("⚠️ Treasury not configured. Cannot overclock.", ephemeral=True)
                return

            now = int(time.time())
            # If any CPU already has an active OC, treat the whole group as active.
            if any(is_overclock_active(cpu, now) for cpu in cpus):
                rem = max(overclock_remaining_seconds(cpu, now) for cpu in cpus)
                await interaction.response.send_message(
                    f"⚡ CPU overclock already active. Remaining: {fmt_duration_hms(rem)}.",
                    ephemeral=True,
                )
                return

            total_cpus = len(cpus)
            total_cost = int(total_cpus * OVERCLOCK_COST_CPU_PER_DEVICE)
            bal = get_user_balance(view.user_id)
            if bal < total_cost:
                await interaction.response.send_message(
                    f"❌ Not enough Ħ to overclock {total_cpus} CPUs (cost {total_cost} Ħ).",
                    ephemeral=True,
                )
                return

            ok = transfer_internal(view.user_id, GAME_TREASURY_DISCORD_ID, total_cost, note="overclock cpu group")
            if not ok:
                await interaction.response.send_message(
                    "❌ Still processing the previous action or insufficient funds.",
                    ephemeral=True,
                )
                return

            dur = int(overclock_duration_seconds_for_rig(rig))
            until_ts = now + dur
            for cpu in cpus:
                if isinstance(cpu, dict):
                    cpu["overclock_until"] = until_ts

            view.miner_bot.update_user_rig(view.user_id, rig)

            await interaction.response.send_message(
                f"🔲 All CPUs overclocked! Boost +{OVERCLOCK_BOOST_PCT_CPU:.0f}% for {fmt_duration_hms(dur)} (Power Plant Lvl {power_plant_level(rig)}).",
                ephemeral=True,
            )

    class AcceleratorRigButton(Button):
        def __init__(self, row=None):
            disabled = (int(GAME_TREASURY_DISCORD_ID) == 0)
            label = f"⏩ Speed up Rig (-1h, {ACCELERATOR_COST_PER_HOUR} Ħ)"
            if disabled:
                label = "⏩ Speed up Rig (treasury not set)"
            super().__init__(
                label=label,
                style=discord.ButtonStyle.gray if disabled else discord.ButtonStyle.blurple,
                custom_id="accelerate_rig",
                row=row,
                disabled=disabled,
            )

        async def callback(self, interaction: discord.Interaction):
            view: MinerView = self.view
            if interaction.user.id != view.user_id:
                await interaction.response.send_message("This is not your miner!", ephemeral=True)
                return

            if GAME_TREASURY_DISCORD_ID == 0:
                await interaction.response.send_message(
                    "⚠️ Treasury not configured. Cannot accelerate rig upgrade.",
                    ephemeral=True,
                )
                return

            rig = view.miner_bot.get_user_rig(view.user_id)
            if not rig or not isinstance(rig, dict):
                await interaction.response.send_message("❌ Rig not found.", ephemeral=True)
                return

            now = int(time.time())
            # Dry-run: check if the rig upgrade timer is active at all
            test_rig = json.loads(json.dumps(rig))
            reduced_test, affected_test = reduce_upgrade_timers(
                test_rig,
                now,
                ACCELERATOR_STEP_SECONDS,
                include_rig=True,
                include_asics=False,
                include_gpus=False,
            )
            if affected_test <= 0 or reduced_test <= 0:
                await interaction.response.send_message(
                    "⏩ No active rig upgrade to speed up right now.",
                    ephemeral=True,
                )
                return

            cost = int(ACCELERATOR_COST_PER_HOUR * affected_test)
            bal = get_user_balance(view.user_id)
            if bal < cost:
                await interaction.response.send_message(
                    f"❌ Not enough Ħ to accelerate the rig upgrade (need {cost} Ħ).",
                    ephemeral=True,
                )
                return

            ok = transfer_internal(
                view.user_id,
                GAME_TREASURY_DISCORD_ID,
                cost,
                note="accelerate rig upgrade",
            )
            if not ok:
                await interaction.response.send_message(
                    "❌ Still processing the previous action or insufficient funds.",
                    ephemeral=True,
                )
                return

            # Apply to real rig
            reduced_real, _ = reduce_upgrade_timers(
                rig,
                now,
                ACCELERATOR_STEP_SECONDS,
                include_rig=True,
                include_asics=False,
                include_gpus=False,
            )
            if apply_completed_upgrades(rig, now):
                # rig has been modified (level increased)
                pass

            view.miner_bot.update_user_rig(view.user_id, rig)

            hrs = max(1, int(round(reduced_real / 3600.0)))
            await interaction.response.send_message(
                f"⏩ Accelerated your rig upgrade by ~{hrs}h. Cost: {cost} Ħ.",
                ephemeral=True,
            )

    class AcceleratorAsicButton(Button):
        def __init__(self, row=None):
            disabled = (int(GAME_TREASURY_DISCORD_ID) == 0)
            label = f"⏩ Speed up ASIC upgrades (-1h, {ACCELERATOR_COST_PER_HOUR} Ħ per running upgrade)"
            if disabled:
                label = "⏩ Speed up ASIC upgrades (treasury not set)"
            super().__init__(
                label=label,
                style=discord.ButtonStyle.gray if disabled else discord.ButtonStyle.blurple,
                custom_id="accelerate_asics",
                row=row,
                disabled=disabled,
            )

        async def callback(self, interaction: discord.Interaction):
            view: MinerView = self.view
            if interaction.user.id != view.user_id:
                await interaction.response.send_message("This is not your miner!", ephemeral=True)
                return

            if GAME_TREASURY_DISCORD_ID == 0:
                await interaction.response.send_message(
                    "⚠️ Treasury not configured. Cannot accelerate ASIC upgrades.",
                    ephemeral=True,
                )
                return

            rig = view.miner_bot.get_user_rig(view.user_id)
            if not rig or not isinstance(rig, dict):
                await interaction.response.send_message("❌ Rig not found.", ephemeral=True)
                return

            now = int(time.time())
            test_rig = json.loads(json.dumps(rig))
            reduced_test, affected_test = reduce_upgrade_timers(
                test_rig,
                now,
                ACCELERATOR_STEP_SECONDS,
                include_rig=False,
                include_asics=True,
                include_gpus=False,
            )
            if affected_test <= 0 or reduced_test <= 0:
                await interaction.response.send_message(
                    "⏩ No active ASIC upgrades to speed up right now.",
                    ephemeral=True,
                )
                return

            cost = int(ACCELERATOR_COST_PER_HOUR * affected_test)
            bal = get_user_balance(view.user_id)
            if bal < cost:
                await interaction.response.send_message(
                    f"❌ Not enough Ħ to accelerate ASIC upgrades (need {cost} Ħ).",
                    ephemeral=True,
                )
                return

            ok = transfer_internal(
                view.user_id,
                GAME_TREASURY_DISCORD_ID,
                cost,
                note="accelerate asic upgrades",
            )
            if not ok:
                await interaction.response.send_message(
                    "❌ Still processing the previous action or insufficient funds.",
                    ephemeral=True,
                )
                return

            reduced_real, _ = reduce_upgrade_timers(
                rig,
                now,
                ACCELERATOR_STEP_SECONDS,
                include_rig=False,
                include_asics=True,
                include_gpus=False,
            )
            if apply_completed_upgrades(rig, now):
                pass

            view.miner_bot.update_user_rig(view.user_id, rig)

            hrs = max(1, int(round(reduced_real / 3600.0)))
            await interaction.response.send_message(
                f"⏩ Accelerated your ASIC upgrades by ~{hrs}h across {affected_test} running upgrade(s). "
                f"Cost: {cost} Ħ.",
                ephemeral=True,
            )

    class AcceleratorGpuButton(Button):
        def __init__(self, row=None):
            disabled = (int(GAME_TREASURY_DISCORD_ID) == 0)
            label = f"⏩ Speed up GPU upgrades (-1h, {ACCELERATOR_COST_PER_HOUR} Ħ per running upgrade)"
            if disabled:
                label = "⏩ Speed up GPU upgrades (treasury not set)"
            super().__init__(
                label=label,
                style=discord.ButtonStyle.gray if disabled else discord.ButtonStyle.blurple,
                custom_id="accelerate_gpus",
                row=row,
                disabled=disabled,
            )

        async def callback(self, interaction: discord.Interaction):
            view: MinerView = self.view
            if interaction.user.id != view.user_id:
                await interaction.response.send_message("This is not your miner!", ephemeral=True)
                return

            if GAME_TREASURY_DISCORD_ID == 0:
                await interaction.response.send_message(
                    "⚠️ Treasury not configured. Cannot accelerate GPU upgrades.",
                    ephemeral=True,
                )
                return

            rig = view.miner_bot.get_user_rig(view.user_id)
            if not rig or not isinstance(rig, dict):
                await interaction.response.send_message("❌ Rig not found.", ephemeral=True)
                return

            now = int(time.time())
            test_rig = json.loads(json.dumps(rig))
            reduced_test, affected_test = reduce_upgrade_timers(
                test_rig,
                now,
                ACCELERATOR_STEP_SECONDS,
                include_rig=False,
                include_asics=False,
                include_gpus=True,
            )
            if affected_test <= 0 or reduced_test <= 0:
                await interaction.response.send_message(
                    "⏩ No active GPU upgrades to speed up right now.",
                    ephemeral=True,
                )
                return

            cost = int(ACCELERATOR_COST_PER_HOUR * affected_test)
            bal = get_user_balance(view.user_id)
            if bal < cost:
                await interaction.response.send_message(
                    f"❌ Not enough Ħ to accelerate GPU upgrades (need {cost} Ħ).",
                    ephemeral=True,
                )
                return

            ok = transfer_internal(
                view.user_id,
                GAME_TREASURY_DISCORD_ID,
                cost,
                note="accelerate gpu upgrades",
            )
            if not ok:
                await interaction.response.send_message(
                    "❌ Still processing the previous action or insufficient funds.",
                    ephemeral=True,
                )
                return

            reduced_real, _ = reduce_upgrade_timers(
                rig,
                now,
                ACCELERATOR_STEP_SECONDS,
                include_rig=False,
                include_asics=False,
                include_gpus=True,
            )
            if apply_completed_upgrades(rig, now):
                pass

            view.miner_bot.update_user_rig(view.user_id, rig)

            hrs = max(1, int(round(reduced_real / 3600.0)))
            await interaction.response.send_message(
                f"⏩ Accelerated your GPU upgrades by ~{hrs}h across {affected_test} running upgrade(s). "
                f"Cost: {cost} Ħ.",
                ephemeral=True,
            )

    def __init__(self, user_id, miner_bot, requester=None):
        super().__init__(timeout=None)
        self.user_id = int(user_id)
        self.miner_bot = miner_bot
        self.miner_bot.data = self.miner_bot.load_data()

        import discord.utils
        if discord.utils.get(self.children, custom_id="refresh") is None:
            # Only the owner (or the original requester) should get interactive controls
            print(f"[MinerView] building view for user={self.user_id} children_before={len(self.children)}")  # Debug
            if requester is None or requester.id == self.user_id:
                rig = self.miner_bot.get_user_rig(self.user_id)
                if not rig or not isinstance(rig, dict):
                    rig = {"rig_level": 1, "asics": [], "gpus": [], "upgrade_ready_time": None, "power_plant_level": 0}

                # Core actions
                now = int(time.time())
                asics = rig.get("asics", []) or []
                gpus = rig.get("gpus", []) or []
                cpus = rig.get("cpus", []) or []

                self.add_item(self.RefreshButton(self.user_id, self.miner_bot, row=0))
                self.add_item(self.BuyAsicButton(row=1))
                self.add_item(self.BuyGpuButton(row=2))
                self.add_item(self.BuyCpuButton(row=3))
                self.add_item(self.UpgradeAsicButton(row=1))
                self.add_item(self.UpgradeGpuButton(row=2))
                self.add_item(self.OverclockAsicButton(row=1))

                # Accelerators: separate buttons for Rig, ASIC and GPU upgrades.
                self.add_item(self.AcceleratorRigButton(row=4))
                self.add_item(self.AcceleratorAsicButton(row=4))
                self.add_item(self.AcceleratorGpuButton(row=4))

                # Overclock all ASICs button: disable if all ASICs already have active OC
                oc_all_asics_btn = self.OverclockAllAsicsButton(row=1)
                if asics and all(is_overclock_active(a, now) for a in asics):
                    oc_all_asics_btn.disabled = True
                self.add_item(oc_all_asics_btn)

                self.add_item(self.OverclockGpuButton(row=2))

                # Overclock all GPUs button: disable if all GPUs already have active OC
                oc_all_gpus_btn = self.OverclockAllGpusButton(row=2)
                if gpus and all(is_overclock_active(g, now) for g in gpus):
                    oc_all_gpus_btn.disabled = True
                self.add_item(oc_all_gpus_btn)

                # CPU overclock button: disable if any CPU already has active OC
                oc_cpu_btn = self.OverclockCpuButton(row=3)
                if cpus and any(is_overclock_active(c, now) for c in cpus):
                    oc_cpu_btn.disabled = True
                self.add_item(oc_cpu_btn)

                # Rig upgrade button
                current_level = int(rig.get("rig_level", 1) or 1)
                upgrade_cost = int(RIG_BASE_BUILD_COST * (1.5 ** (current_level - 1)))
                if current_level < MAX_RIG_LEVEL:
                    self.add_item(self.UpgradeRigButton(current_level, upgrade_cost, row=4))

                # Power Plant upgrade button
                ppl = power_plant_level(rig)
                if ppl < int(POWER_PLANT_MAX_LEVEL):
                    nxt = ppl + 1
                    cost_pp = power_plant_upgrade_cost(nxt)
                    # Show the *new* OC duration after upgrading to next level
                    oc_dur_next = int(overclock_duration_seconds_for_rig({"power_plant_level": nxt}))
                    self.add_item(self.UpgradePowerPlantButton(ppl, nxt, cost_pp, oc_dur_next, row=4))
                print(f"[MinerView] built view for user={self.user_id} total_children={len(self.children)}")  # Debug

    class UpgradeRigButton(Button):
        def __init__(self, current_level, upgrade_cost, row=None):
            disabled = (int(GAME_TREASURY_DISCORD_ID) == 0)
            if disabled:
                label = "🏗️ Upgrade Rig (treasury not set)"
            else:
                label = f"🏗️ Upgrade Rig (Lvl {current_level} → {current_level + 1}, +10 Gh/s, {upgrade_cost} Ħ)"
            super().__init__(
                label=label,
                style=discord.ButtonStyle.gray if disabled else discord.ButtonStyle.blurple,
                custom_id="upgrade_rig",
                row=row,
                disabled=disabled,
            )
            self.upgrade_cost = upgrade_cost

        async def callback(self, interaction: discord.Interaction):
            try:
                view: MinerView = self.view
                if interaction.user.id != view.user_id:
                    await interaction.response.send_message("This is not your miner!", ephemeral=True)
                    return
                rig = view.miner_bot.get_user_rig(view.user_id)
                now = int(time.time())
                upgrade_time = rig.get("upgrade_ready_time") or 0
                if upgrade_time > now:
                    remaining = int(upgrade_time - now)
                    await interaction.response.send_message(
                        f"⏳ Rig upgrade already in progress. Ready in {fmt_duration_hms(remaining)}.",
                        ephemeral=True
                    )
                    return
                if rig["rig_level"] >= MAX_RIG_LEVEL:
                    await interaction.response.send_message("🏗️ Your rig is already max level!", ephemeral=True)
                    return
                balance = get_user_balance(view.user_id)
                if balance < self.upgrade_cost:
                    await interaction.response.send_message("❌ Not enough Ħ to upgrade your rig.", ephemeral=True)
                    return
                if GAME_TREASURY_DISCORD_ID == 0:
                    await interaction.response.send_message("⚠️ Treasury not configured. Cannot upgrade.", ephemeral=True)
                    return
                ok = transfer_internal(view.user_id, GAME_TREASURY_DISCORD_ID, self.upgrade_cost, note="upgrade rig")
                if not ok:
                    await interaction.response.send_message("❌ Still processing the previous action or insufficient funds.", ephemeral=True)
                    return
                level = rig["rig_level"]
                duration_minutes = BASE_UPGRADE_TIME_MINUTES * (2 ** level)
                rig["upgrade_ready_time"] = now + duration_minutes * 60
                view.miner_bot.update_user_rig(view.user_id, rig)
                await interaction.response.send_message(
                    f"🏗️ Rig upgrade started! It will complete in {fmt_duration_hms(int(duration_minutes * 60))}.",
                    ephemeral=True
                )
            except Exception as e:
                import traceback
                print("❌ Error in UpgradeRigButton.callback:", e)
                traceback.print_exc()
                try:
                    await interaction.response.send_message("❌ Internal error during rig upgrade.", ephemeral=True)
                except discord.errors.InteractionResponded:
                    pass

    class UpgradePowerPlantButton(Button):
        def __init__(self, current_level: int, next_level: int, cost: int, oc_dur_seconds: int, row=None):
            disabled = (int(GAME_TREASURY_DISCORD_ID) == 0)
            if disabled:
                label = "🏭 Upgrade Power Plant (treasury not set)"
            else:
                label = f"🏭 Power Plant (Lvl {current_level} → {next_level}, OC {fmt_duration_days_only(oc_dur_seconds)}) – {cost} Ħ"
            super().__init__(
                label=label,
                style=discord.ButtonStyle.gray if disabled else discord.ButtonStyle.blurple,
                custom_id="upgrade_power_plant",
                row=row,
                disabled=disabled,
            )

        async def callback(self, interaction: discord.Interaction):
            view: MinerView = self.view
            if interaction.user.id != view.user_id:
                await interaction.response.send_message("This is not your miner!", ephemeral=True)
                return

            if GAME_TREASURY_DISCORD_ID == 0:
                await interaction.response.send_message("⚠️ Treasury not configured. Cannot upgrade.", ephemeral=True)
                return

            rig = view.miner_bot.get_user_rig(view.user_id)
            if not rig or not isinstance(rig, dict):
                await interaction.response.send_message("❌ Rig not found.", ephemeral=True)
                return

            cur = power_plant_level(rig)
            if cur >= int(POWER_PLANT_MAX_LEVEL):
                await interaction.response.send_message("🏭 Power Plant already at max level!", ephemeral=True)
                return

            nxt = cur + 1
            cost = power_plant_upgrade_cost(nxt)
            bal = get_user_balance(view.user_id)
            if bal < cost:
                await interaction.response.send_message("❌ Not enough Ħ to upgrade the Power Plant.", ephemeral=True)
                return

            ok = transfer_internal(view.user_id, GAME_TREASURY_DISCORD_ID, cost, note="upgrade power plant")
            if not ok:
                await interaction.response.send_message("❌ Still processing the previous action or insufficient funds.",
                                                        ephemeral=True)
                return

            rig["power_plant_level"] = nxt
            view.miner_bot.update_user_rig(view.user_id, rig)

            dur = int(overclock_duration_seconds_for_rig(rig))
            await interaction.response.send_message(
                f"🏭 Power Plant upgraded to Level {nxt}! New OC duration: {fmt_duration_hms(dur)}.",
                ephemeral=True,
            )


# --- ASIC Selection View ---
class SelectAsicView(discord.ui.View):
    def __init__(self, user_id, miner_bot):
        super().__init__(timeout=60)
        self.user_id = int(user_id)
        self.miner_bot = miner_bot
        self.miner_bot.data = self.miner_bot.load_data()
        self.rig = miner_bot.get_user_rig(self.user_id)
        self.balance = get_user_balance(self.user_id)
        rig_level = self.rig.get("rig_level", 1)
        for idx, asic in enumerate(self.rig.get("asics", [])):
            stars = asic.get("stars", 0)
            if stars >= 5:
                label = f"𒁈 ASIC {idx+1}: Fully Upgraded ({stars}⭐)"
                self.add_item(SelectAsicButton(idx, label, discord.ButtonStyle.gray, 0, disabled=True))
                continue
            max_stars = min(rig_level + 1, 5)
            if stars >= max_stars:
                label = f"𒁈 ASIC {idx+1}: Increase Rig Level to upgrade"
                self.add_item(SelectAsicButton(idx, label, discord.ButtonStyle.gray, 0, disabled=True))
                continue
            cost = int(ASIC_COST * (1.3 ** stars))
            upgrade_ready_time = asic.get("upgrade_ready_time")
            disabled = upgrade_ready_time is not None and time.time() < upgrade_ready_time
            if disabled:
                label = f"𒁈 ASIC {idx+1}: Upgrading… ({stars}⭐ → {stars+1}⭐)"
            else:
                delta_raw = asic_raw_for_stars(stars + 1) - asic_raw_for_stars(stars)
                label = f"𒁈 Upgrade ASIC to {stars + 1}⭐ (+{delta_raw} Gh/s) – {cost} Ħ"
            style = discord.ButtonStyle.gray if disabled else (
                discord.ButtonStyle.green if self.balance >= cost else discord.ButtonStyle.gray
            )
            self.add_item(SelectAsicButton(idx, label, style, cost, disabled=disabled))


class SelectAsicButton(discord.ui.Button):
    def __init__(self, asic_index, label, style, cost, disabled=False):
        super().__init__(label=label, style=style, custom_id=f"upgrade_asic_{asic_index}", disabled=disabled)
        self.asic_index = asic_index
        self.cost = cost

    async def callback(self, interaction: discord.Interaction):
        try:
            view: SelectAsicView = self.view
            if interaction.user.id != view.user_id:
                await interaction.response.send_message("This is not your miner!", ephemeral=True)
                return
            rig = view.miner_bot.get_user_rig(view.user_id)
            now = int(time.time())
            try:
                asic = rig["asics"][self.asic_index]
            except IndexError:
                await interaction.response.send_message("❌ ASIC not found.", ephemeral=True)
                return
            upgrade_ready_time = asic.get("upgrade_ready_time")
            if upgrade_ready_time is not None and now < upgrade_ready_time:
                remaining = int(upgrade_ready_time - now)
                await interaction.response.send_message(
                    f"Upgrade in progress. Ready in {fmt_duration_hms(remaining)}.", ephemeral=True
                )
                return
            rig_level = rig.get("rig_level", 1)
            max_stars = min(rig_level + 1, 5)
            current_stars = asic.get("stars", 0)
            if current_stars + 1 > max_stars:
                await interaction.response.send_message(
                    f"❌ ASICs can only be upgraded up to {max_stars}⭐ with your current rig level.",
                    ephemeral=True
                )
                return
            cost = int(ASIC_COST * (1.3 ** current_stars))
            balance = get_user_balance(view.user_id)
            if balance < cost:
                await interaction.response.send_message("❌ Not enough Ħ to upgrade this ASIC.", ephemeral=True)
                return
            if GAME_TREASURY_DISCORD_ID == 0:
                await interaction.response.send_message("⚠️ Treasury not configured. Cannot upgrade.", ephemeral=True)
                return
            ok = transfer_internal(view.user_id, GAME_TREASURY_DISCORD_ID, cost, note="upgrade asic")
            if not ok:
                await interaction.response.send_message("❌ Still processing the previous action or insufficient funds.", ephemeral=True)
                return
            duration_minutes = BASE_UPGRADE_TIME_MINUTES * (1.5 ** (current_stars + 1))
            asic["upgrade_ready_time"] = now + duration_minutes * 60
            view.miner_bot.update_user_rig(view.user_id, rig)
            msg = f"⏳ Upgrade started: This ASIC will reach {current_stars + 1}⭐ in {fmt_duration_hms(int(duration_minutes * 60))}."
            if interaction.response.is_done():
                await interaction.followup.send(message=msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except Exception as e:
            import traceback
            print("❌ Error in SelectAsicButton.callback:", e)
            traceback.print_exc()
            if not interaction.response.is_done():
                await interaction.response.send_message("❌ Internal error during ASIC upgrade.", ephemeral=True)
            else:
                await interaction.followup.send("❌ Internal error during ASIC upgrade.", ephemeral=True)


# --- ASIC Overclock Selection View ---
class SelectAsicOCView(discord.ui.View):
    def __init__(self, user_id, miner_bot):
        super().__init__(timeout=60)
        self.user_id = int(user_id)
        self.miner_bot = miner_bot
        self.miner_bot.data = self.miner_bot.load_data()
        self.rig = miner_bot.get_user_rig(self.user_id)
        self.balance = get_user_balance(self.user_id)
        now = int(time.time())
        dur = int(overclock_duration_seconds_for_rig(self.rig))

        for idx, asic in enumerate((self.rig.get("asics", []) or [])):
            stars = int(asic.get("stars", 0) or 0)

            if is_overclock_active(asic, now):
                rem = overclock_remaining_seconds(asic, now)
                label = f"𒁈 ASIC {idx+1}: OC active ({fmt_duration_hms(rem)} left)"
                self.add_item(SelectAsicOCButton(idx, label, discord.ButtonStyle.gray, disabled=True))
                continue

            base = asic_raw_for_stars(stars)
            boosted = int(round(float(base) * (1.0 + (OVERCLOCK_BOOST_PCT_ASIC / 100.0))))
            label = f"⚡ Overclock ASIC {idx + 1} ({stars}⭐): {base} → {boosted} Gh/s · {fmt_duration_days_only(dur)} · {OVERCLOCK_COST_ASIC} Ħ"

            style = discord.ButtonStyle.green if self.balance >= OVERCLOCK_COST_ASIC else discord.ButtonStyle.gray
            self.add_item(SelectAsicOCButton(idx, label, style, disabled=(self.balance < OVERCLOCK_COST_ASIC)))


class SelectAsicOCButton(discord.ui.Button):
    def __init__(self, asic_index: int, label: str, style: discord.ButtonStyle, disabled: bool = False):
        super().__init__(label=label, style=style, custom_id=f"oc_asic_{asic_index}", disabled=disabled)
        self.asic_index = int(asic_index)

    async def callback(self, interaction: discord.Interaction):
        view: SelectAsicOCView = self.view
        if interaction.user.id != view.user_id:
            await interaction.response.send_message("This is not your miner!", ephemeral=True)
            return

        if GAME_TREASURY_DISCORD_ID == 0:
            await interaction.response.send_message("⚠️ Treasury not configured. Cannot overclock.", ephemeral=True)
            return

        rig = view.miner_bot.get_user_rig(view.user_id)
        now = int(time.time())
        try:
            asic = rig["asics"][self.asic_index]
        except Exception:
            await interaction.response.send_message("❌ ASIC not found.", ephemeral=True)
            return

        if is_overclock_active(asic, now):
            rem = overclock_remaining_seconds(asic, now)
            await interaction.response.send_message(
                f"⚡ Overclock already active. Remaining: {fmt_duration_hms(rem)}.",
                ephemeral=True
            )
            return

        bal = get_user_balance(view.user_id)
        if bal < OVERCLOCK_COST_ASIC:
            await interaction.response.send_message("❌ Not enough Ħ to overclock this ASIC.", ephemeral=True)
            return

        ok = transfer_internal(view.user_id, GAME_TREASURY_DISCORD_ID, OVERCLOCK_COST_ASIC, note="overclock asic")
        if not ok:
            await interaction.response.send_message("❌ Still processing the previous action or insufficient funds.", ephemeral=True)
            return

        dur = int(overclock_duration_seconds_for_rig(rig))
        asic["overclock_until"] = now + dur
        view.miner_bot.update_user_rig(view.user_id, rig)
        await interaction.response.send_message(
            f"⚡ ASIC overclocked! Boost +{OVERCLOCK_BOOST_PCT_ASIC:.0f}% for {fmt_duration_hms(dur)} (Power Plant Lvl {power_plant_level(rig)}).",
            ephemeral=True,
        )


# --- GPU Selection View ---
class SelectGpuView(discord.ui.View):
    def __init__(self, user_id, miner_bot):
        super().__init__(timeout=60)
        self.user_id = int(user_id)
        self.miner_bot = miner_bot
        self.miner_bot.data = self.miner_bot.load_data()
        self.rig = miner_bot.get_user_rig(self.user_id)
        self.balance = get_user_balance(self.user_id)
        rig_level = self.rig.get("rig_level", 1)
        for idx, gpu in enumerate(self.rig.get("gpus", [])):
            stars = gpu.get("stars", 0)
            if stars >= 5:
                label = f"🎮 GPU {idx+1}: Fully Upgraded ({stars}⭐)"
                self.add_item(SelectGpuButton(idx, label, discord.ButtonStyle.gray, 0, disabled=True))
                continue
            max_stars = min(rig_level + 1, 5)
            if stars >= max_stars:
                label = f"🎮 GPU {idx+1}: Increase Rig Level to upgrade"
                self.add_item(SelectGpuButton(idx, label, discord.ButtonStyle.gray, 0, disabled=True))
                continue
            cost = int(GPU_COST * (1.3 ** stars))
            upgrade_ready_time = gpu.get("upgrade_ready_time")
            disabled = upgrade_ready_time is not None and time.time() < upgrade_ready_time
            if disabled:
                label = f"🎮 GPU {idx+1}: Upgrading… ({stars}⭐ → {stars+1}⭐)"
            else:
                delta_raw = gpu_raw_for_stars(stars + 1) - gpu_raw_for_stars(stars)
                label = f"🎮 Upgrade GPU to {stars + 1}⭐ (+{delta_raw} Gh/s) – {cost} Ħ"
            style = discord.ButtonStyle.gray if disabled else (
                discord.ButtonStyle.green if self.balance >= cost else discord.ButtonStyle.gray
            )
            self.add_item(SelectGpuButton(idx, label, style, cost, disabled=disabled))


class SelectGpuButton(discord.ui.Button):
    def __init__(self, gpu_index, label, style, cost, disabled=False):
        super().__init__(label=label, style=style, custom_id=f"upgrade_gpu_{gpu_index}", disabled=disabled)
        self.gpu_index = gpu_index
        self.cost = cost

    async def callback(self, interaction: discord.Interaction):
        view: SelectGpuView = self.view
        if interaction.user.id != view.user_id:
            await interaction.response.send_message("This is not your miner!", ephemeral=True)
            return
        rig = view.miner_bot.get_user_rig(view.user_id)
        now = int(time.time())
        try:
            gpu = rig["gpus"][self.gpu_index]
        except IndexError:
            await interaction.response.send_message("❌ GPU not found.", ephemeral=True)
            return
        upgrade_ready_time = gpu.get("upgrade_ready_time")
        if upgrade_ready_time is not None and now < upgrade_ready_time:
            remaining = int(upgrade_ready_time - now)
            await interaction.response.send_message(
                f"Upgrade in progress. Ready in {fmt_duration_hms(remaining)}.", ephemeral=True
            )
            return
        rig_level = rig.get("rig_level", 1)
        max_stars = min(rig_level + 1, 5)
        current_stars = gpu.get("stars", 0)
        if current_stars + 1 > max_stars:
            await interaction.response.send_message(
                f"❌ GPUs can only be upgraded up to {max_stars}⭐ with your current rig level.",
                ephemeral=True
            )
            return
        cost = int(GPU_COST * (1.3 ** current_stars))
        balance = get_user_balance(view.user_id)
        if balance < cost:
            await interaction.response.send_message("❌ Not enough Ħ to upgrade this GPU.", ephemeral=True)
            return
        if GAME_TREASURY_DISCORD_ID == 0:
            await interaction.response.send_message("⚠️ Treasury not configured. Cannot upgrade.", ephemeral=True)
            return
        ok = transfer_internal(view.user_id, GAME_TREASURY_DISCORD_ID, cost, note="upgrade gpu")
        if not ok:
            await interaction.response.send_message("❌ Still processing the previous action or insufficient funds.", ephemeral=True)
            return
        duration_minutes = BASE_UPGRADE_TIME_MINUTES * (1.5 ** (current_stars + 1))
        gpu["upgrade_ready_time"] = now + duration_minutes * 60
        view.miner_bot.update_user_rig(view.user_id, rig)
        await interaction.response.send_message(
            f"⏳ GPU upgrade to {current_stars + 1}⭐ started! Time until completion: {fmt_duration_hms(int(duration_minutes * 60))}",
            ephemeral=True
        )


# --- GPU Overclock Selection View ---
class SelectGpuOCView(discord.ui.View):
    def __init__(self, user_id, miner_bot):
        super().__init__(timeout=60)
        self.user_id = int(user_id)
        self.miner_bot = miner_bot
        self.miner_bot.data = self.miner_bot.load_data()
        self.rig = miner_bot.get_user_rig(self.user_id)
        self.balance = get_user_balance(self.user_id)
        now = int(time.time())
        dur = int(overclock_duration_seconds_for_rig(self.rig))

        for idx, gpu in enumerate((self.rig.get("gpus", []) or [])):
            stars = int(gpu.get("stars", 0) or 0)

            if is_overclock_active(gpu, now):
                rem = overclock_remaining_seconds(gpu, now)
                label = f"🎮 GPU {idx+1}: OC active ({fmt_duration_hms(rem)} left)"
                self.add_item(SelectGpuOCButton(idx, label, discord.ButtonStyle.gray, disabled=True))
                continue

            base = gpu_raw_for_stars(stars)
            boosted = int(round(float(base) * (1.0 + (OVERCLOCK_BOOST_PCT_GPU / 100.0))))
            label = f"⚡ Overclock GPU {idx+1} ({stars}⭐): {base} → {boosted} Gh/s · {fmt_duration_days_only(dur)} · {OVERCLOCK_COST_GPU} Ħ"

            style = discord.ButtonStyle.green if self.balance >= OVERCLOCK_COST_GPU else discord.ButtonStyle.gray
            self.add_item(SelectGpuOCButton(idx, label, style, disabled=(self.balance < OVERCLOCK_COST_GPU)))


class SelectGpuOCButton(discord.ui.Button):
    def __init__(self, gpu_index: int, label: str, style: discord.ButtonStyle, disabled: bool = False):
        super().__init__(label=label, style=style, custom_id=f"oc_gpu_{gpu_index}", disabled=disabled)
        self.gpu_index = int(gpu_index)

    async def callback(self, interaction: discord.Interaction):
        view: SelectGpuOCView = self.view
        if interaction.user.id != view.user_id:
            await interaction.response.send_message("This is not your miner!", ephemeral=True)
            return

        if GAME_TREASURY_DISCORD_ID == 0:
            await interaction.response.send_message("⚠️ Treasury not configured. Cannot overclock.", ephemeral=True)
            return

        rig = view.miner_bot.get_user_rig(view.user_id)
        now = int(time.time())
        try:
            gpu = rig["gpus"][self.gpu_index]
        except Exception:
            await interaction.response.send_message("❌ GPU not found.", ephemeral=True)
            return

        if is_overclock_active(gpu, now):
            rem = overclock_remaining_seconds(gpu, now)
            await interaction.response.send_message(
                f"⚡ Overclock already active. Remaining: {fmt_duration_hms(rem)}.",
                ephemeral=True
            )
            return

        bal = get_user_balance(view.user_id)
        if bal < OVERCLOCK_COST_GPU:
            await interaction.response.send_message("❌ Not enough Ħ to overclock this GPU.", ephemeral=True)
            return

        ok = transfer_internal(view.user_id, GAME_TREASURY_DISCORD_ID, OVERCLOCK_COST_GPU, note="overclock gpu")
        if not ok:
            await interaction.response.send_message("❌ Still processing the previous action or insufficient funds.", ephemeral=True)
            return

        dur = int(overclock_duration_seconds_for_rig(rig))
        gpu["overclock_until"] = now + dur
        view.miner_bot.update_user_rig(view.user_id, rig)
        await interaction.response.send_message(
            f"⚡ GPU overclocked! Boost +{OVERCLOCK_BOOST_PCT_GPU:.0f}% for {fmt_duration_hms(dur)} (Power Plant Lvl {power_plant_level(rig)}).",
            ephemeral=True,
        )

class MinerGameBot:
    async def payout_daemon(self) -> None:
        """Boundary-synced payout loop: waits for next interval boundary and pays once."""
        await self.bot.wait_until_ready()
        await self._run_interval_payout_once(int(time.time()))

        while not self.bot.is_closed():
            try:
                now_ts = int(time.time())
                eta = int(next_payout_eta_seconds(now_ts))

                # sleep until the next boundary (+small buffer)
                await asyncio.sleep(max(0.25, float(eta) + 0.75))

                await self._run_interval_payout_once(int(time.time()))

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"❌ payout_daemon error: {e}")
                import traceback
                traceback.print_exc()
                await asyncio.sleep(5)

    def build_help_embed(self, user: 'discord.abc.User | None' = None) -> 'discord.Embed':
        # Local import to avoid circular issues
        import discord
        # Compute interval hours for payout pool
        interval_hours = max(1, int(PAYOUT_INTERVAL_SECONDS) // 3600)
        payout_total = payout_total_for_state(self.data)
        base_pool_line = f"Payout pool: up to {payout_total} Ħ every {interval_hours}h (funded from the game treasury)."
        # Power Plant OC duration table
        pp_lines = []
        for lvl in range(0, POWER_PLANT_MAX_LEVEL + 1):
            dummy_rig = {"power_plant_level": lvl}
            dur = fmt_duration_days_only(overclock_duration_seconds_for_rig(dummy_rig))
            pp_lines.append(f"Level {lvl}: OC duration {dur}")
        pp_text = "\n".join(pp_lines)
        embed = discord.Embed(
            title="⛏️ Hashcash Miner Game – Help",
            description=(
                "Mini manual for the Hashcash Miner Game.\n"
                "• Use `/miner` to open your rig.\n"
                "• All balances and payments use your Ħ TipBot account."
            )
        )
        # 1. Goal
        embed.add_field(
            name="Goal",
            value=(
                "Build and upgrade your virtual mining rig to increase your share of the payout pool. "
                "Payouts are automatic and based on your effective hashrate."
            ),
            inline=False
        )
        # 2. Getting started
        embed.add_field(
            name="Getting started",
            value=(
                "• Earn some Ħ either via the faucet or tips in the Hashcash TipBot or by IRL mining.\n"
                f"• Run `/miner` and build your first rig (costs {RIG_BASE_BUILD_COST} Ħ).\n"
                "• Use the buttons to buy ASICs, GPUs, or CPUs and start upgrading."
            ),
            inline=False
        )
        # 3. Rig & devices
        embed.add_field(
            name="Rig & devices",
            value=(
                "- Rig level adds base hashrate and increases max devices.\n"
                "- ASICs: strong, more expensive, upgrades (1–5⭐) make them much stronger.\n"
                "- GPUs: cheaper, more numerous, upgrades (1–5⭐) make them stronger.\n"
                "- CPUs: 1 Gh/s units, very cheap, many allowed per rig."
            ),
            inline=False
        )
        # 4. Upgrades
        embed.add_field(
            name="Upgrades",
            value=(
                "ASIC/GPU upgrades (⭐) take time—upgrading keeps the old power until the timer finishes. "
                "Rig level upgrades also use a timer; remaining time is shown in the rig view."
            ),
            inline=False
        )
        # 5. Overclock & Power Plant
        embed.add_field(
            name="Overclock & Power Plant",
            value=(
                "- Overclock temporarily boosts a single ASIC or GPU, or all CPUs at once.\n"
                "- Each overclock costs Ħ and lasts for a fixed duration depending on Power Plant level.\n"
                f"- The Power Plant increases OC duration and can be upgraded up to level {POWER_PLANT_MAX_LEVEL}.\n"
                "\n"
                "**Current OC durations:**\n"
                f"{pp_text}"
            ),
            inline=False
        )
        # 6. Effective hashrate & payouts
        embed.add_field(
            name="Effective hashrate & payouts",
            value=(
                f"{base_pool_line}\n"
                "Payouts are proportional to your raw in-game hashrate (Gh/s). "
                "You can see your estimated share and next payout time in the `/miner` view."
            ),
            inline=False
        )
        # 7. Leaderboard
        embed.add_field(
            name="Leaderboard",
            value=(
                "You can see the current mining leaderboard in <#1470034003028611303>. "
                "It is updated automatically every few minutes."
            ),
            inline=False
        )
        # 8. TipBot integration
        embed.add_field(
            name="TipBot integration",
            value=(
                "- Costs (buying/upgrading/overclocking) are paid from your TipBot balance.\n"
                "- Payouts are credited to your TipBot balance from the game treasury.\n"
                "- Use TipBot commands like `/balances`, `/tip` or `/withdraw` to manage your Ħ."
                "- Use the TipBot command `/help` for more information or read the <#1460421512690536560>."
            ),
            inline=False
        )
        embed.set_footer(text=f"Hashcash Miner Game {MINER_GAME_VERSION} – use /miner to open your rig.")
        if user is not None:
            embed.set_author(name=str(getattr(user, "display_name", None) or getattr(user, "name", None) or getattr(user, "id", "")))
        return embed
    DATA_FILE = FACTORY_FILE

    def __init__(self):
        load_dotenv()
        self.MINERGAME_CHANNELS = [int(x.strip()) for x in os.getenv("MINERGAME_CHANNEL_IDS", "").split(",") if x.strip()]
        # Use non-privileged intents only (no Members / Presence / Message Content).
        # Slash commands do not require privileged intents.
        intents = discord.Intents.default()
        intents.guilds = True
        self.bot = commands.Bot(command_prefix="!", intents=intents)
        self.data = self.load_data()
        self.daily_payout_task = None
        self.register_commands()
        self._stop_event = None
        self._names_cache = None

    def load_data(self):
        if not os.path.exists(self.DATA_FILE):
            return {"users": {}, "last_payout_slot": -1}
        with open(self.DATA_FILE, "r") as f:
            return json.load(f)

    def save_data(self):
        with open(self.DATA_FILE, "w") as f:
            json.dump(self.data, f, indent=2)

    def get_user_rig(self, user_id):
        """
        Always reload state from disk and normalize the rig structure
        (cpus + power_plant_level guaranteed to exist).
        """
        self.data = self.load_data()  # <- always fresh data
        uid = str(user_id)
        rig = (self.data.get("users", {}) or {}).get(uid)

        if rig is None:
            return None

        if not isinstance(rig, dict):
            rig = {}

        # Safety: ensure keys exist on older rigs
        if "asics" not in rig or rig["asics"] is None:
            rig["asics"] = []
        if "gpus" not in rig or rig["gpus"] is None:
            rig["gpus"] = []
        if "cpus" not in rig or rig["cpus"] is None:
            rig["cpus"] = []
        if "power_plant_level" not in rig or rig["power_plant_level"] is None:
            rig["power_plant_level"] = 0

        self.data["users"][uid] = rig
        self.save_data()

        return rig

    def update_user_rig(self, user_id, rig):
        uid = str(user_id)
        if "users" not in self.data:
            self.data["users"] = {}
        self.data["users"][uid] = rig
        self.save_data()

    def register_commands(self):
        @self.bot.tree.command(name="miner", description="Show your Hashcash Miner Rig")
        async def miner_command(interaction: discord.Interaction):
            update_names_cache_for_user(interaction.user)
            await interaction.response.defer(ephemeral=True)
            self.data = self.load_data()
            await self.show_miner_overview(interaction)

        @self.bot.tree.command(name="miner_help", description="Show help for the Hashcash Miner Game")
        async def miner_help_command(interaction: discord.Interaction):
            update_names_cache_for_user(interaction.user)
            embed = self.build_help_embed(interaction.user)
            await interaction.response.send_message(embed=embed, ephemeral=True)

    async def show_miner_overview(self, interaction: discord.Interaction, rig=None):
        try:
            user_id = interaction.user.id
            if not rig:
                self.data = self.load_data()
                rig = self.get_user_rig(user_id)
            if not rig:
                view = discord.ui.View()

                class BuildRigButton(discord.ui.Button):
                    def __init__(self, miner_bot):
                        disabled = (int(GAME_TREASURY_DISCORD_ID) == 0)
                        label = f"⛏️ Build Miner Rig ({RIG_BASE_BUILD_COST} Ħ)"
                        if disabled:
                            label = "⛏️ Build Miner Rig (treasury not set)"
                        super().__init__(label=label, style=discord.ButtonStyle.gray if disabled else discord.ButtonStyle.success, disabled=disabled)
                        self.miner_bot = miner_bot

                    async def callback(self, interaction: discord.Interaction):
                        balance = get_user_balance(interaction.user.id)
                        if balance < RIG_BASE_BUILD_COST:
                            await interaction.response.send_message("❌ Not enough Ħ to build a miner rig.", ephemeral=True)
                            return
                        if GAME_TREASURY_DISCORD_ID == 0:
                            await interaction.response.send_message("⚠️ Treasury not configured. Cannot build rig.", ephemeral=True)
                            return
                        ok = transfer_internal(interaction.user.id, GAME_TREASURY_DISCORD_ID, RIG_BASE_BUILD_COST, note="Initial Miner Rig Build")
                        if not ok:
                            await interaction.response.send_message("⚠️ Transaction already pending or insufficient funds. Please wait.", ephemeral=True)
                            return
                        now = int(time.time())
                        new_rig = {
                            "rig_level": 1,
                            "power_plant_level": 0,
                            "asics": [],
                            "gpus": [],
                            "cpus": [],
                            "upgrade_ready_time": None,
                            "created_at": now,
                        }
                        new_rig_user_id = str(interaction.user.id)
                        self.miner_bot.update_user_rig(new_rig_user_id, new_rig)
                        new_rig_obj = self.miner_bot.get_user_rig(new_rig_user_id)
                        await self.miner_bot.show_miner_overview(interaction, rig=new_rig_obj)
                view.add_item(BuildRigButton(self))
                await interaction.followup.send("You don't own a miner rig yet. Would you like to build one?", view=view, ephemeral=True)
                return
            self.data = self.load_data()
            if isinstance(rig, dict) and "power_plant_level" not in rig:
                rig["power_plant_level"] = 0
                self.update_user_rig(user_id, rig)
            embed = self.create_miner_embed(rig, interaction.user.id)
            user_id = int(user_id)
            view = MinerView(user_id, self, requester=interaction.user)
            if interaction.response.is_done():
                await interaction.followup.send(embed=embed, view=view, ephemeral=True)
            else:
                await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        except Exception as e:
            print(f"❌ Error in show_miner_overview: {e}")
            import traceback
            traceback.print_exc()
            try:
                await interaction.followup.send("❌ Failed to display your miner.", ephemeral=True)
            except discord.errors.InteractionResponded:
                pass

    @staticmethod
    def create_miner_embed(rig, user_id):
        import discord
        from collections import Counter
        embed = discord.Embed(title=f"⛏️ Your Hashcash Miner Rig")
        now = int(time.time())
        # --- Rig Upgrade Status ---
        upgrade_time = rig.get("upgrade_ready_time")
        if upgrade_time is not None and now < upgrade_time:
            remaining = int(upgrade_time - now)
            upgrade_status = f"⏳ Upgrade ready in {fmt_duration_hms(remaining)}"
        elif upgrade_time and now >= upgrade_time:
            upgrade_status = "✅ Upgrade complete! (Applied)"
        else:
            upgrade_status = "✅ No upgrade in progress"
        embed.add_field(name="Rig Upgrade Status", value=upgrade_status, inline=False)
        # --- Level ---
        embed.add_field(name="Rig Level", value=rig.get("rig_level", 1), inline=True)
        # --- ASICs ---
        asics = rig.get("asics", [])
        max_asics = MAX_ASICS_PER_LEVEL.get(rig.get("rig_level", 1), 1)
        embed.add_field(name="ASICs", value=f"{len(asics)} / {max_asics}", inline=True)
        # --- GPUs ---
        gpus = rig.get("gpus", [])
        max_gpus = MAX_GPUS_PER_LEVEL.get(rig.get("rig_level", 1), 1)
        embed.add_field(name="GPUs", value=f"{len(gpus)} / {max_gpus}", inline=True)
        # --- CPUs ---
        cpus = rig.get("cpus", []) or []
        max_cpus = MAX_CPUS_PER_LEVEL.get(rig.get("rig_level", 1), 8)
        embed.add_field(name="CPUs", value=f"{len(cpus)} / {max_cpus}", inline=True)
        # --- Power Plant ---
        ppl = power_plant_level(rig)
        oc_dur = int(overclock_duration_seconds_for_rig(rig))
        embed.add_field(
            name="Power Plant",
            value=f"Lvl {ppl} / {POWER_PLANT_MAX_LEVEL}\nOC duration: {fmt_duration_days_only(oc_dur)}",
            inline=True
        )
        # --- Hash power (shown as effective share weight) ---
        raw = compute_raw_power(rig, now_ts=now)
        eff = compute_effective_power(raw)
        embed.add_field(name="Your in-game hashrate (Gh/s)", value=f"{eff:.2f}", inline=True)

        # --- Pool Payout ---
        interval_hours = max(1, int(PAYOUT_INTERVAL_SECONDS) // 3600)
        eta = next_payout_eta_seconds(now)

        # Estimate payout for this user (best-effort snapshot)
        est_user = 0
        est_total_to_pay = 0
        est_share_pct = 0.0
        try:
            # Load all rigs to compute weights
            if os.path.exists(FACTORY_FILE):
                with open(FACTORY_FILE, "r", encoding="utf-8") as f:
                    snap = json.load(f)
            else:
                snap = {"users": {}}
            snap_users = (snap.get("users", {}) or {})

            weights = {}
            for uid, r in snap_users.items():
                if not r or not isinstance(r, dict):
                    continue
                # Apply completed upgrades for fairer snapshot
                rr = r
                if apply_completed_upgrades(rr, now):
                    snap_users[uid] = rr
                raw_i = compute_raw_power(rr, now_ts=now)
                eff_i = compute_effective_power(raw_i)
                if eff_i > 0:
                    weights[str(uid)] = float(eff_i)

            total_weight = float(sum(weights.values()))
            if total_weight > 0:
                treasury_bal = int(get_user_balance(GAME_TREASURY_DISCORD_ID)) if int(GAME_TREASURY_DISCORD_ID) != 0 else 0
                payout_total = payout_total_for_state({"users": snap_users})
                est_total_to_pay = int(min(int(payout_total), int(treasury_bal)))

                my_w = float(weights.get(str(user_id), 0.0))
                if est_total_to_pay > 0 and my_w > 0:
                    # Floor estimate (actual may differ by +/- 1 due to remainder distribution)
                    est_user = int(est_total_to_pay * my_w / total_weight)
            # --- Pool share percent ---
            if est_total_to_pay > 0:
                try:
                    if 'total_weight' in locals() and total_weight > 0 and 'my_w' in locals() and my_w > 0:
                        est_share_pct = 100.0 * float(my_w) / float(total_weight)
                except Exception:
                    est_share_pct = 0.0
        except Exception:
            est_user = 0
            est_total_to_pay = 0
            est_share_pct = 0.0

        payout_lines = [
            f"Every {interval_hours}h · Total up to {payout_total_for_state({'users': snap_users})} Ħ / interval",
            f"Next payout in ~{fmt_duration_hms(eta)}",
            f"Your pool share (snapshot): {est_share_pct:.2f}%",
        ]
        if est_total_to_pay > 0:
            payout_lines.append(f"Treasury-funded this interval: {est_total_to_pay} Ħ")
        if est_user > 0:
            payout_lines.append(f"Your estimate: ~{est_user} Ħ (snapshot)")

        embed.add_field(
            name="Pool Payout",
            value="\n".join(payout_lines),
            inline=False
        )

        embed.add_field(
            name="Cash out",
            value="To cash out to your Ħ address: use `/withdraw` in the TipBot.",
            inline=False
        )
        # --- TipBot Balance ---
        balance = get_user_balance(user_id)
        embed.add_field(name="Your TipBot balance", value=f"{balance} Ħ", inline=True)

        # --- Detailed per-device status (includes upgrade remaining time) ---
        def _device_lines(devs, label_emoji: str, raw_func, boost_pct: float) -> str:
            if not devs:
                return "None"
            lines = []
            for i, d in enumerate(devs, start=1):
                stars = int(d.get("stars", 0) or 0)
                base_raw = float(raw_func(stars))
                mult = overclock_multiplier_for_device(d, now, boost_pct)
                shown_raw = base_raw * mult

                oc_suffix = ""
                if is_overclock_active(d, now):
                    oc_rem = overclock_remaining_seconds(d, now)
                    oc_suffix = f" · ⚡ OC +{boost_pct:.0f}% ({fmt_duration_hms(oc_rem)})"

                uet = d.get("upgrade_ready_time")
                if uet and int(uet) > now:
                    rem = int(uet - now)
                    raw_next = float(raw_func(stars + 1))
                    raw_next_shown = raw_next * mult
                    lines.append(
                        f"{label_emoji} #{i}: {stars}⭐ ({shown_raw:.1f} Gh/s){oc_suffix} → {stars + 1}⭐ ({raw_next_shown:.1f} Gh/s) "
                        f"(ready in {fmt_duration_hms(rem)})"
                    )
                else:
                    lines.append(f"{label_emoji} #{i}: {stars}⭐ ({shown_raw:.1f} Gh/s){oc_suffix}")
            return "\n".join(lines)

        embed.add_field(
            name="ASIC status",
            value=_device_lines(asics, "𒁈 ASIC", asic_raw_for_stars, OVERCLOCK_BOOST_PCT_ASIC),
            inline=False,
        )
        embed.add_field(
            name="GPU status",
            value=_device_lines(gpus, "🎮 GPU", gpu_raw_for_stars, OVERCLOCK_BOOST_PCT_GPU),
            inline=False,
        )

        # CPUs: summarize count and total Gh/s (1 Gh/s each, +OC if active)
        total_cpus = len(cpus)
        if total_cpus > 0:
            base_total = float(total_cpus)
            boosted_total = base_total
            oc_suffix = ""
            # If any CPU has OC active we treat the whole group as OC'd and
            # show the longest remaining duration.
            active_cpus = [c for c in cpus if is_overclock_active(c, now)]
            if active_cpus:
                mult = overclock_multiplier_for_device(active_cpus[0], now, OVERCLOCK_BOOST_PCT_CPU)
                boosted_total = base_total * mult
                rem = max(overclock_remaining_seconds(c, now) for c in active_cpus)
                oc_suffix = f" · ⚡ OC +{OVERCLOCK_BOOST_PCT_CPU:.0f}% ({fmt_duration_hms(rem)})"

            if active_cpus:
                cpu_status = f"{total_cpus}× CPU · {boosted_total:.1f} Gh/s total{oc_suffix}"
            else:
                cpu_status = f"{total_cpus}× CPU · {base_total:.1f} Gh/s total"
        else:
            cpu_status = "None"
        embed.add_field(name="CPU status", value=cpu_status, inline=False)

        embed.set_footer(text=f"Hashcash Miner Game {MINER_GAME_VERSION}")
        return embed

    async def _run_interval_payout_once(self, now_ts: int) -> None:
        """Execute at most one payout for the current slot (idempotent via last_payout_slot)."""
        if GAME_TREASURY_DISCORD_ID == 0:
            return

        now_ts = int(now_ts or time.time())
        slot = current_payout_slot(now_ts)

        self.data = self.load_data()
        last_slot = int(self.data.get("last_payout_slot", -1) or -1)
        if last_slot == slot:
            return

        users = self.data.get("users", {}) or {}

        # Build weights (effective power)
        weights = {}
        for uid, rig in users.items():
            if not rig or not isinstance(rig, dict):
                continue

            # Apply completed upgrades
            if apply_completed_upgrades(rig, now_ts):
                users[uid] = rig

            raw = compute_raw_power(rig, now_ts=now_ts)
            eff = compute_effective_power(raw)
            if eff > 0:
                weights[uid] = float(eff)

        self.data["users"] = users

        if not weights:
            self.data["last_payout_slot"] = slot
            self.save_data()
            return

        total_weight = float(sum(weights.values()))
        if total_weight <= 0:
            self.data["last_payout_slot"] = slot
            self.save_data()
            return

        # Treasury balance gates the actual paid total
        treasury_bal = get_user_balance(GAME_TREASURY_DISCORD_ID)
        payout_total = payout_total_for_state(self.data)
        total_to_pay = min(int(payout_total), int(treasury_bal))
        if total_to_pay <= 0:
            self.data["last_payout_slot"] = slot
            self.save_data()
            return

        # Floor distribution
        payouts = {uid: int(total_to_pay * w / total_weight) for uid, w in weights.items()}
        total_paid = int(sum(payouts.values()))
        remainder = int(total_to_pay - total_paid)

        if remainder > 0:
            sorted_uids = sorted(weights.items(), key=lambda x: -x[1])
            for i in range(remainder):
                uid = sorted_uids[i % len(sorted_uids)][0]
                payouts[uid] += 1

        # Execute transfers (REAL internal ledger tx: treasury -> user)
        note = f"mining payout slot {slot}"
        for uid, amt in payouts.items():
            if amt <= 0:
                continue
            ok = transfer_internal(GAME_TREASURY_DISCORD_ID, int(uid), int(amt), note=note)
            if not ok:
                # Stop early if treasury depleted mid-loop
                break

        self.data["last_payout_slot"] = slot
        self.save_data()


def create_miner_game_bot():
    return MinerGameBot()


def run_bot(stop_event=None):
    import asyncio
    import os
    BotClass = create_miner_game_bot()
    bot = BotClass
    @bot.bot.event
    async def on_ready():
        await bot.bot.tree.sync()
        print(f"✅ Slash commands synced as {bot.bot.user}")
        # Start the boundary-synced payout daemon once.
        if not hasattr(bot, "_payout_task") or bot._payout_task is None or bot._payout_task.done():
            bot._payout_task = asyncio.create_task(bot.payout_daemon())

    @bot.bot.event
    async def on_interaction(interaction: discord.Interaction):
        # Best-effort: keep names cache updated for leaderboard scripts.
        try:
            if getattr(interaction, "user", None) is not None:
                update_names_cache_for_user(interaction.user)
        except Exception:
            pass
        # Let discord.py continue processing the interaction
        await bot.bot.process_application_commands(interaction)
    async def runner():
        async def shutdown_watcher():
            while not stop_event.is_set():
                await asyncio.sleep(1)
            print("🔻 Shutdown signal received. Closing Miner Game Bot...")
            await bot.bot.close()
        try:
            if stop_event:
                asyncio.create_task(shutdown_watcher())
            await bot.bot.start(os.getenv("DISCORD_TOKEN_MINERGAME"))
        except Exception as e:
            print(f"❌ Miner Game Bot runner error: {e}")
        finally:
            try:
                # Cancel payout daemon if running
                if hasattr(bot, "_payout_task") and bot._payout_task is not None:
                    try:
                        bot._payout_task.cancel()
                    except Exception:
                        pass
                await bot.bot.close()
            finally:
                await asyncio.sleep(0.1)
                print("🔻 Miner Game Bot has shut down.")
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(runner())
    finally:
        loop.close()


def main():
    # .env is already loaded at import-time with override=True.

    token = os.getenv("DISCORD_TOKEN_MINERGAME", "").strip()
    if not token:
        raise SystemExit(
            "Missing DISCORD_TOKEN_MINERGAME. Put it in your environment or .env file."
        )

    print("✅ Starting Miner Game Bot...")
    print(f"- State file: {FACTORY_FILE}")
    print(f"- TipBot DB:  {TIPBOT_DB_PATH}")
    print(f"- Treasury Discord ID: {GAME_TREASURY_DISCORD_ID}")
    if GAME_TREASURY_DISCORD_ID == 0:
        print("⚠️  GAME_TREASURY_DISCORD_ID is 0. Buying/upgrades/payouts will be disabled until you set it in .env")
    try:
        with open(FACTORY_FILE, "r", encoding="utf-8") as f:
            cur_state = json.load(f)
    except Exception:
        cur_state = {"users": {}}

    print(f"- Payout: {payout_total_for_state(cur_state)} Ħ every {PAYOUT_INTERVAL_SECONDS}s")

    run_bot(stop_event=None)


if __name__ == "__main__":
    main()