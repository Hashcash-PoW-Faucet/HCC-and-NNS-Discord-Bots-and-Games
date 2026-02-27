#!/usr/bin/env python3
import os
import json
import math
import time
from typing import Dict, Any, List, Tuple
from dotenv import load_dotenv
import requests

load_dotenv()

WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL_MINING", "").strip().strip('"').strip("'")
STATE_FILE = os.getenv("FACTORY_FILE", "mining_game_state.json").strip()
TOP_N = int(os.getenv("LEADERBOARD_TOP_N", "20"))
EFFECTIVE_POWER_MODE = os.getenv("EFFECTIVE_POWER_MODE", "sqrt").strip().lower()  # sqrt|linear

TITLE = os.getenv("LEADERBOARD_TITLE", "⛏️ Discord Mining Game Leaderboard").strip()

# --- Overclock (temporary boost) ---
# Duration is fixed in the game; leaderboard only needs the boost percentages.
OVERCLOCK_BOOST_PCT_ASIC = float(os.getenv("OVERCLOCK_BOOST_PCT_ASIC", "25"))  # e.g. 25 => +25%
OVERCLOCK_BOOST_PCT_GPU = float(os.getenv("OVERCLOCK_BOOST_PCT_GPU", "20"))    # e.g. 20 => +20%

# --- Power Plant (extends Overclock duration) ---
POWER_PLANT_MAX_LEVEL = int(os.getenv("POWER_PLANT_MAX_LEVEL", "3"))

# If 1: users may get pinged. If 0: no pings, but mentions still render as names.
PING_USERS = os.getenv("LEADERBOARD_PING_USERS", "0").strip() == "1"

# Names cache config

NAMES_CACHE_FILE = os.getenv("NAMES_CACHE_FILE", "names_cache.json").strip()
MAX_NAME_LEN = int(os.getenv("LEADERBOARD_MAX_NAME_LEN", "22"))

# Store the last leaderboard webhook message id so we can edit it instead of reposting
MESSAGE_ID_FILE = os.getenv("LEADERBOARD_MESSAGE_ID_FILE", "leaderboard_message_id.txt").strip()



# --- Overclock helpers ---

def _clamp_float(x: float, lo: float, hi: float) -> float:
    try:
        v = float(x)
    except Exception:
        v = float(lo)
    return max(float(lo), min(float(hi), v))


def is_overclock_active(dev: Dict[str, Any], now_ts: int) -> bool:
    try:
        until = int((dev or {}).get("overclock_until") or 0)
        return until > int(now_ts)
    except Exception:
        return False


def overclock_multiplier_for_device(dev: Dict[str, Any], now_ts: int, boost_pct: float) -> float:
    if not dev or not isinstance(dev, dict):
        return 1.0
    if not is_overclock_active(dev, now_ts):
        return 1.0
    pct = _clamp_float(boost_pct, 0.0, 500.0)
    return 1.0 + (pct / 100.0)


def power_plant_level(rig: Dict[str, Any]) -> int:
    try:
        lvl = int((rig or {}).get("power_plant_level", 0) or 0)
    except Exception:
        lvl = 0
    return max(0, min(int(POWER_PLANT_MAX_LEVEL), lvl))


def overclock_duration_seconds_for_rig(rig: Dict[str, Any]) -> int:
    # Base 24h, +24h per Power Plant level
    return int(24 * 3600 * (1 + power_plant_level(rig)))


def _fmt_hours(seconds: int) -> str:
    h = max(0, int(seconds) // 3600)
    return f"{h}h"


def count_active_overclocks(rig: Dict[str, Any], now_ts: int) -> int:
    n = 0
    for dev in (rig.get("asics", []) or []):
        if isinstance(dev, dict) and is_overclock_active(dev, now_ts):
            n += 1
    for dev in (rig.get("gpus", []) or []):
        if isinstance(dev, dict) and is_overclock_active(dev, now_ts):
            n += 1
    return n


# --- Raw hashrate helpers (must match mining_game_bot.py) ---
# We try to stay in sync with the game by:
# 1) importing ASIC_RAW_TABLE from mining_game_bot.py (preferred),
# 2) otherwise reading ASIC_RAW_TABLE_JSON from .env,
# 3) otherwise using a safe default.

DEFAULT_ASIC_RAW_TABLE = {
    0: 0,
    1: 50,
    2: 80,
    3: 115,
    4: 155,
    5: 200,
}


def _load_asic_raw_table_from_env() -> Dict[int, int]:
    s = (os.getenv("ASIC_RAW_TABLE_JSON", "") or "").strip()
    if not s:
        return {}
    try:
        data = json.loads(s)
        if not isinstance(data, dict):
            return {}
        out: Dict[int, int] = {}
        for k, v in data.items():
            try:
                out[int(k)] = int(v)
            except Exception:
                continue
        return out
    except Exception:
        return {}


def _load_asic_raw_table() -> Dict[int, int]:
    # 1) Try import from the game bot (keeps it in sync if both scripts share a codebase)
    try:
        from mining_game_bot import ASIC_RAW_TABLE as GAME_ASIC_RAW_TABLE  # type: ignore
        if isinstance(GAME_ASIC_RAW_TABLE, dict) and GAME_ASIC_RAW_TABLE:
            out: Dict[int, int] = {}
            for k, v in GAME_ASIC_RAW_TABLE.items():
                try:
                    out[int(k)] = int(v)
                except Exception:
                    continue
            if out:
                return out
    except Exception:
        pass

    # 2) Try env JSON
    env_tbl = _load_asic_raw_table_from_env()
    if env_tbl:
        return env_tbl

    # 3) Fallback default
    return dict(DEFAULT_ASIC_RAW_TABLE)


ASIC_RAW_TABLE = _load_asic_raw_table()


def asic_raw_for_stars(stars: int) -> int:
    stars = max(0, min(5, int(stars)))
    return ASIC_RAW_TABLE.get(stars, ASIC_RAW_TABLE[5])


def gpu_raw_for_stars(stars: int) -> int:
    return int(stars) * 10 + 10


def compute_raw_power(rig: Dict[str, Any], now_ts: int = None) -> int:
    now_ts = int(now_ts or time.time())

    raw = 0.0
    rig_level = int(rig.get("rig_level", 1) or 1)
    raw += (rig_level - 1) * 10

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

    return max(1, int(round(raw)))


def compute_effective_power(raw_power: int) -> float:
    if EFFECTIVE_POWER_MODE == "sqrt":
        return math.sqrt(max(0, raw_power))
    return float(raw_power)


def fmt_num(x: float) -> str:
    s = f"{x:.2f}"
    if s.endswith("00"):
        return s[:-3]
    if s.endswith("0"):
        return s[:-1]
    return s


def load_names_cache(path: str) -> Dict[str, str]:
    """Load a Discord-ID -> display-name cache from JSON.

    Expected format: {"123": "Name", ...}
    If the file is missing or invalid, returns an empty dict.
    """
    if not path:
        return {}
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return {}
        out: Dict[str, str] = {}
        for k, v in data.items():
            if k is None or v is None:
                continue
            out[str(k)] = str(v)
        return out
    except Exception:
        # Keep it resilient: if the cache is malformed, just ignore it.
        return {}


def build_table(rows: List[Tuple[str, float, float, str]]) -> str:
    """Return a nice monospace leaderboard table inside a code block.

    rows: [(name, hashrate, pct, oc_info)] where name is already a plain string.
    """
    # Prepare rows
    prepared: List[Tuple[str, str, str, str, str]] = []
    for i, (name, hr, pct, oc_info) in enumerate(rows, start=1):
        rank = str(i)
        nm = (name or "").strip()
        if len(nm) > MAX_NAME_LEN:
            nm = nm[: MAX_NAME_LEN - 1] + "…"
        hr_s = fmt_num(hr)
        pct_s = f"{pct:.2f}%"
        prepared.append((rank, nm, hr_s, pct_s, (oc_info or "").strip()))

    # Column widths
    w_rank = max(2, max(len(r[0]) for r in prepared) if prepared else 2)
    w_name = max(4, max(len(r[1]) for r in prepared) if prepared else 4)
    w_hr = max(13, max(len(r[2]) for r in prepared) if prepared else 13)
    w_pct = max(8, max(len(r[3]) for r in prepared) if prepared else 8)
    w_oc = max(9, max(len(r[4]) for r in prepared) if prepared else 9)

    header = (
        f"{'#':>{w_rank}}  "
        f"{'Name':<{w_name}}  "
        f"{'Total Hashrate':>{w_hr}}  "
        f"{'% Shares':>{w_pct}}  "
        f"{'OC':<{w_oc}}"
    )
    sep = (
        f"{'-':>{w_rank}}  "
        f"{'-' * w_name}  "
        f"{'-' * w_hr}  "
        f"{'-' * w_pct}  "
        f"{'-' * w_oc}"
    )

    lines = [header, sep]
    for rank, nm, hr_s, pct_s, oc_s in prepared:
        lines.append(
            f"{rank:>{w_rank}}  {nm:<{w_name}}  {hr_s:>{w_hr}}  {pct_s:>{w_pct}}  {oc_s:<{w_oc}}"
        )

    return "```\n" + "\n".join(lines) + "\n```"


def load_state(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)



def compute_leaderboard(state: Dict[str, Any], top_n: int, names_cache: Dict[str, str]) -> List[Tuple[str, float, float, str]]:
    users = (state.get("users", {}) or {})
    weights: List[Tuple[str, float]] = []
    now_ts = int(time.time())

    for uid, rig in users.items():
        if not isinstance(rig, dict):
            continue
        raw = compute_raw_power(rig, now_ts=now_ts)
        eff = compute_effective_power(raw)
        if eff > 0:
            weights.append((str(uid), float(eff)))

    if not weights:
        return []

    total = sum(w for _, w in weights)
    if total <= 0:
        return []

    weights.sort(key=lambda x: x[1], reverse=True)
    weights = weights[: max(1, int(top_n))]

    rows: List[Tuple[str, float, float, str]] = []
    for uid, w in weights:
        rig = users.get(uid, {}) if isinstance(users.get(uid, {}), dict) else {}
        name = names_cache.get(str(uid), f"user-{str(uid)[-6:]}")
        pct = 100.0 * w / total
        ppl = power_plant_level(rig)
        oc_dur = _fmt_hours(overclock_duration_seconds_for_rig(rig))
        oc_active = count_active_overclocks(rig, now_ts)
        oc_info = f"PP{ppl} · {oc_dur} · {oc_active}⚡"
        rows.append((name, w, pct, oc_info))
    return rows



def _webhook_base_url(url: str) -> str:
    # strip any query params, keep the base webhook URL
    return (url or "").strip().split("?")[0]


def _load_message_id(path: str) -> str:
    if not path:
        return ""
    try:
        if not os.path.exists(path):
            return ""
        with open(path, "r", encoding="utf-8") as f:
            return (f.read() or "").strip()
    except Exception:
        return ""


def _save_message_id(path: str, message_id: str) -> None:
    if not path:
        return
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(str(message_id).strip())
    except Exception:
        # non-fatal
        pass


def _build_webhook_payload(content: str) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"content": content}

    # Mentions:
    # - If you allow parsing, users may get pinged.
    # - If you disable parsing, Discord will NOT ping; mentions in text are treated as plain text.
    if PING_USERS:
        payload["allowed_mentions"] = {"parse": ["users"]}
    else:
        payload["allowed_mentions"] = {"parse": []}

    return payload


def post_or_edit_webhook_message(webhook_url: str, content: str, message_id_file: str) -> None:
    """Edit the previous leaderboard message if possible; otherwise create a new one.

    Uses a local message-id file to remember which webhook message to edit next time.
    """
    base_url = _webhook_base_url(webhook_url)
    payload = _build_webhook_payload(content)

    # Try to edit existing message
    msg_id = _load_message_id(message_id_file)
    if msg_id:
        edit_url = f"{base_url}/messages/{msg_id}"
        r = requests.patch(edit_url, json=payload, timeout=20)
        if r.status_code < 300:
            return
        # If it fails (deleted / invalid), we fall back to creating a new message.

    # Create new message (wait=true returns message JSON incl. id)
    create_url = f"{base_url}?wait=true"
    r2 = requests.post(create_url, json=payload, timeout=20)
    if r2.status_code >= 300:
        raise RuntimeError(f"Webhook failed: {r2.status_code} {r2.text}")

    # Save message id for next run
    try:
        data = r2.json()
        new_id = str(data.get("id", "") or "").strip()
        if new_id:
            _save_message_id(message_id_file, new_id)
    except Exception:
        pass


def main():
    if not WEBHOOK_URL:
        raise SystemExit("Missing DISCORD_WEBHOOK_URL_MINING in env.")
    if not os.path.exists(STATE_FILE):
        raise SystemExit(f"State file not found: {STATE_FILE}")

    state = load_state(STATE_FILE)
    names_cache = load_names_cache(NAMES_CACHE_FILE)
    rows = compute_leaderboard(state, TOP_N, names_cache)

    ts = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime())
    if not rows:
        msg = f"**{TITLE}**\n_No active miners yet._\nUpdated: {ts}"
        post_or_edit_webhook_message(WEBHOOK_URL, msg, MESSAGE_ID_FILE)
        return

    table = build_table(rows)
    msg = f"**{TITLE}**\n{table}\nUpdated: {ts}"
    post_or_edit_webhook_message(WEBHOOK_URL, msg, MESSAGE_ID_FILE)


if __name__ == "__main__":
    main()