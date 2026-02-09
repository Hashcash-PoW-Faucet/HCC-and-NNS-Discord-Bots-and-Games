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

# If 1: users may get pinged. If 0: no pings, but mentions still render as names.
PING_USERS = os.getenv("LEADERBOARD_PING_USERS", "0").strip() == "1"

# Names cache config

NAMES_CACHE_FILE = os.getenv("NAMES_CACHE_FILE", "names_cache.json").strip()
MAX_NAME_LEN = int(os.getenv("LEADERBOARD_MAX_NAME_LEN", "22"))

# Store the last leaderboard webhook message id so we can edit it instead of reposting
MESSAGE_ID_FILE = os.getenv("LEADERBOARD_MESSAGE_ID_FILE", "leaderboard_message_id.txt").strip()


def compute_raw_power(rig: Dict[str, Any]) -> int:
    raw = 0
    rig_level = int(rig.get("rig_level", 1) or 1)
    raw += (rig_level - 1) * 10

    for asic in rig.get("asics", []) or []:
        stars = int(asic.get("stars", 0) or 0)
        raw += stars * 20 + 30

    for gpu in rig.get("gpus", []) or []:
        stars = int(gpu.get("stars", 0) or 0)
        raw += stars * 10 + 10

    return max(1, raw)


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


def build_table(rows: List[Tuple[str, float, float]]) -> str:
    """Return a nice monospace leaderboard table inside a code block.

    rows: [(name, hashrate, pct)] where name is already a plain string.
    """
    # Prepare rows
    prepared: List[Tuple[str, str, str, str]] = []
    for i, (name, hr, pct) in enumerate(rows, start=1):
        rank = str(i)
        nm = (name or "").strip()
        if len(nm) > MAX_NAME_LEN:
            nm = nm[: MAX_NAME_LEN - 1] + "…"
        hr_s = fmt_num(hr)
        pct_s = f"{pct:.2f}%"
        prepared.append((rank, nm, hr_s, pct_s))

    # Column widths
    w_rank = max(2, max(len(r[0]) for r in prepared) if prepared else 2)
    w_name = max(4, max(len(r[1]) for r in prepared) if prepared else 4)
    w_hr = max(13, max(len(r[2]) for r in prepared) if prepared else 13)
    w_pct = max(8, max(len(r[3]) for r in prepared) if prepared else 8)

    header = (
        f"{'#':>{w_rank}}  "
        f"{'Name':<{w_name}}  "
        f"{'Total Hashrate':>{w_hr}}  "
        f"{'% Shares':>{w_pct}}"
    )
    sep = (
        f"{'-':>{w_rank}}  "
        f"{'-' * w_name}  "
        f"{'-' * w_hr}  "
        f"{'-' * w_pct}"
    )

    lines = [header, sep]
    for rank, nm, hr_s, pct_s in prepared:
        lines.append(
            f"{rank:>{w_rank}}  {nm:<{w_name}}  {hr_s:>{w_hr}}  {pct_s:>{w_pct}}"
        )

    return "```\n" + "\n".join(lines) + "\n```"


def load_state(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def compute_leaderboard(state: Dict[str, Any], top_n: int, names_cache: Dict[str, str]) -> List[Tuple[str, float, float]]:
    users = (state.get("users", {}) or {})
    weights: List[Tuple[str, float]] = []

    for uid, rig in users.items():
        if not isinstance(rig, dict):
            continue
        raw = compute_raw_power(rig)
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

    rows: List[Tuple[str, float, float]] = []
    for uid, w in weights:
        name = names_cache.get(str(uid), f"user-{str(uid)[-6:]}")
        pct = 100.0 * w / total
        rows.append((name, w, pct))
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