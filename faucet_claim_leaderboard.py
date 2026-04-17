#!/usr/bin/env python3
import os
import json
import time
import sqlite3
from typing import Dict, Any, List, Tuple, Optional
from urllib.parse import urlparse, parse_qsl, urlencode
from dotenv import load_dotenv
import requests

load_dotenv()

# ----------------------------
# Config (env)
# ----------------------------
WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL_FAUCET", "").strip().strip('"').strip("'")
DB_PATH = os.getenv("TIPBOT_DB", "tipbot.db").strip()

TOP_N = int(os.getenv("LEADERBOARD_TOP_N", "20"))
TITLE = os.getenv("LEADERBOARD_TITLE", "🚰 HCC Faucet Leaderboard (All-Time)").strip()

# If 1: allow real pings. If 0: no pings, but mentions may still render as names depending on Discord.
PING_USERS = os.getenv("LEADERBOARD_PING_USERS", "0").strip() == "1"

# Cache Discord-ID -> display name (so we can show usernames without live API calls)
NAMES_CACHE_FILE = os.getenv("NAMES_CACHE_FILE", "names_cache.json").strip()
MAX_NAME_LEN = int(os.getenv("LEADERBOARD_MAX_NAME_LEN", "22"))

# Store last webhook message id so we can edit instead of reposting
MESSAGE_ID_FILE = os.getenv("LEADERBOARD_MESSAGE_ID_FILE", "faucet_leaderboard_message_id.txt").strip()

BLACKLIST_IDS = {
    int(x.strip())
    for x in os.getenv("LEADERBOARD_BLACKLIST_IDS", "").split(",")
    if x.strip().isdigit()
}

# Optional: resolve usernames via Discord API (fills/updates cache)
DISCORD_BOT_TOKEN = os.getenv("DISCORD_TOKEN", "").strip()  # reuse your bot token if you want
GUILD_ID = os.getenv("GUILD_ID", "").strip()  # optional: enables guild nickname lookup


# ----------------------------
# Helpers
# ----------------------------
def db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con


def load_names_cache(path: str) -> Dict[str, str]:
    if not path or not os.path.exists(path):
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
        return {}


def save_names_cache(path: str, cache: Dict[str, str]) -> None:
    if not path:
        return
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def shorten_name(name: str) -> str:
    nm = (name or "").strip()
    if not nm:
        return nm
    if len(nm) > MAX_NAME_LEN:
        return nm[: MAX_NAME_LEN - 1] + "…"
    return nm


def fmt_int(n: int) -> str:
    # simple thousands separator
    return f"{int(n):,}".replace(",", ".")


def _webhook_base_and_query(url: str) -> Tuple[str, Dict[str, str]]:
    parsed = urlparse((url or "").strip())
    base = f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    return base, query


def _load_message_id(path: str) -> str:
    if not path or not os.path.exists(path):
        return ""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return (f.read() or "").strip()
    except Exception:
        return ""


def _save_message_id(path: str, message_id: str) -> None:
    if not path:
        return
    tmp = f"{path}.tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(str(message_id).strip())
        os.replace(tmp, path)
    except Exception:
        pass


def _build_webhook_payload(content: str) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"content": content}
    if PING_USERS:
        payload["allowed_mentions"] = {"parse": ["users"]}
    else:
        payload["allowed_mentions"] = {"parse": []}
    return payload


def _is_retryable_webhook_error(message: str) -> bool:
    msg = (message or "").lower()
    return (
        " 429 " in f" {msg} "
        or "rate limit" in msg
        or "timeout" in msg
        or "timed out" in msg
        or "connection aborted" in msg
        or "connection reset" in msg
        or "temporarily unavailable" in msg
        or " 500 " in f" {msg} "
        or " 502 " in f" {msg} "
        or " 503 " in f" {msg} "
        or " 504 " in f" {msg} "
    )


def _request_with_rate_limit_retry(method: str, url: str, payload: Dict[str, Any], timeout: int = 20) -> requests.Response:
    last_response: Optional[requests.Response] = None
    for attempt in range(2):
        response = requests.request(method, url, json=payload, timeout=timeout)
        last_response = response
        if response.status_code != 429:
            return response

        retry_after = response.headers.get("Retry-After", "").strip()
        sleep_seconds = 1.5
        try:
            if retry_after:
                sleep_seconds = max(0.5, float(retry_after))
        except Exception:
            sleep_seconds = 1.5

        if attempt == 0:
            time.sleep(min(sleep_seconds, 10.0))
            continue
        return response

    if last_response is None:
        raise RuntimeError("webhook request failed without response")
    return last_response


def post_or_edit_webhook_message(webhook_url: str, content: str, message_id_file: str) -> None:
    base_url, query = _webhook_base_and_query(webhook_url)
    payload = _build_webhook_payload(content)

    msg_id = _load_message_id(message_id_file)
    if msg_id:
        edit_url = f"{base_url}/messages/{msg_id}"
        if query:
            edit_url += "?" + urlencode(query)

        print(f"Trying to edit faucet leaderboard message_id={msg_id}")
        r = _request_with_rate_limit_retry("PATCH", edit_url, payload, timeout=20)
        if r.status_code < 300:
            return

        err_msg = f"webhook edit failed: {r.status_code} {r.text[:500]}"
        print(err_msg)

        # For temporary Discord/network problems, do not create duplicate messages.
        if _is_retryable_webhook_error(err_msg):
            raise RuntimeError(err_msg)

    create_query = dict(query)
    create_query["wait"] = "true"
    create_url = base_url + "?" + urlencode(create_query)
    r2 = _request_with_rate_limit_retry("POST", create_url, payload, timeout=20)
    if r2.status_code >= 300:
        raise RuntimeError(f"Webhook failed: {r2.status_code} {r2.text[:500]}")

    try:
        data = r2.json()
        new_id = str(data.get("id", "") or "").strip()
        if new_id:
            _save_message_id(message_id_file, new_id)
    except Exception:
        pass


# ----------------------------
# Discord name resolving (optional)
# ----------------------------
def _discord_api_get(url: str) -> Optional[dict]:
    if not DISCORD_BOT_TOKEN:
        return None
    headers = {"Authorization": f"Bot {DISCORD_BOT_TOKEN}"}
    try:
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None


def resolve_display_name(discord_id: str) -> Optional[str]:
    """Try to resolve a friendly display name via Discord API (guild nick > global username)."""
    did = str(discord_id).strip()
    if not did.isdigit():
        return None

    # 1) Guild member (preferred) -> nick or user.global_name/username
    if GUILD_ID and GUILD_ID.isdigit():
        mem = _discord_api_get(f"https://discord.com/api/v10/guilds/{GUILD_ID}/members/{did}")
        if isinstance(mem, dict):
            nick = (mem.get("nick") or "").strip()
            if nick:
                return nick
            u = mem.get("user") or {}
            if isinstance(u, dict):
                gn = (u.get("global_name") or "").strip()
                if gn:
                    return gn
                un = (u.get("username") or "").strip()
                if un:
                    return un

    # 2) User object (no guild nick)
    u2 = _discord_api_get(f"https://discord.com/api/v10/users/{did}")
    if isinstance(u2, dict):
        gn = (u2.get("global_name") or "").strip()
        if gn:
            return gn
        un = (u2.get("username") or "").strip()
        if un:
            return un

    return None


# ----------------------------
# Leaderboard logic
# ----------------------------
def fetch_faucet_leaderboard(con: sqlite3.Connection, top_n: int) -> List[Tuple[int, int, int]]:
    """
    Returns rows: [(discord_id, total_claimed, claim_count), ...] sorted by total_claimed desc
    Uses tx_log entries created by /claim:
      type='faucet_claim', to_id=<discord_id>, amount=<FAUCET_AMOUNT>, status='ok'
    """
    q = """
    SELECT
      to_id AS discord_id,
      COALESCE(SUM(amount), 0) AS total_claimed,
      COUNT(*) AS claim_count
    FROM tx_log
    WHERE type='faucet_claim' AND status='ok' AND to_id IS NOT NULL
    GROUP BY to_id
    ORDER BY total_claimed DESC
    LIMIT ?
    """
    out: List[Tuple[int, int, int]] = []
    for row in con.execute(q, (int(top_n) + max(0, len(BLACKLIST_IDS)),)).fetchall():
        try:
            did = int(row[0])
            total = int(row[1] or 0)
            cnt = int(row[2] or 0)
        except Exception:
            continue
        if did in BLACKLIST_IDS:
            continue
        out.append((did, total, cnt))
        if len(out) >= int(top_n):
            break
    return out


def build_table(rows: List[Tuple[str, int, int]]) -> str:
    """
    rows: [(name, total_claimed, claim_count)]
    """
    prepared: List[Tuple[str, str, str, str]] = []
    for i, (name, total, cnt) in enumerate(rows, start=1):
        rank = str(i)
        nm = shorten_name(name)
        total_s = fmt_int(total)
        cnt_s = fmt_int(cnt)
        prepared.append((rank, nm, total_s, cnt_s))

    w_rank = max(2, max(len(r[0]) for r in prepared) if prepared else 2)
    w_name = max(4, max(len(r[1]) for r in prepared) if prepared else 4)
    w_total = max(10, max(len(r[2]) for r in prepared) if prepared else 10)
    w_cnt = max(6, max(len(r[3]) for r in prepared) if prepared else 6)

    header = (
        f"{'#':>{w_rank}}  "
        f"{'User':<{w_name}}  "
        f"{'Claimed':>{w_total}}  "
        f"{'Claims':>{w_cnt}}"
    )
    sep = (
        f"{'-':>{w_rank}}  "
        f"{'-' * w_name}  "
        f"{'-' * w_total}  "
        f"{'-' * w_cnt}"
    )

    lines = [header, sep]
    for rank, nm, total_s, cnt_s in prepared:
        lines.append(
            f"{rank:>{w_rank}}  {nm:<{w_name}}  {total_s:>{w_total}}  {cnt_s:>{w_cnt}}"
        )

    return "```\n" + "\n".join(lines) + "\n```"


def main() -> None:
    if not WEBHOOK_URL:
        raise SystemExit("Missing DISCORD_WEBHOOK_URL_FAUCET in env.")
    if not os.path.exists(DB_PATH):
        raise SystemExit(f"DB not found: {DB_PATH}")

    names_cache = load_names_cache(NAMES_CACHE_FILE)

    con = db()
    try:
        top = fetch_faucet_leaderboard(con, TOP_N)
    except sqlite3.Error as e:
        raise SystemExit(f"Faucet leaderboard DB error: {e}")
    finally:
        try:
            con.close()
        except Exception:
            pass

    # Resolve names (optional) and update cache
    rows_for_table: List[Tuple[str, int, int]] = []
    cache_changed = False

    for did, total, cnt in top:
        key = str(did)

        # Prefer cached name
        name = names_cache.get(key)

        # Try to resolve via Discord API if token set
        if (not name) and DISCORD_BOT_TOKEN:
            resolved = resolve_display_name(key)
            if resolved:
                names_cache[key] = resolved
                name = resolved
                cache_changed = True

        # Fallback: mention
        if not name:
            # Mention may display username in Discord clients; pinging controlled by allowed_mentions
            name = f"<@{key}>"

        rows_for_table.append((name, total, cnt))

    if cache_changed:
        save_names_cache(NAMES_CACHE_FILE, names_cache)

    ts = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime())

    if not rows_for_table:
        msg = f"**{TITLE}**\n_No faucet claims yet._\nUpdated: {ts}"
        post_or_edit_webhook_message(WEBHOOK_URL, msg, MESSAGE_ID_FILE)
        return

    table = build_table(rows_for_table)
    msg = f"**{TITLE}**\n{table}\nUpdated: {ts}"
    post_or_edit_webhook_message(WEBHOOK_URL, msg, MESSAGE_ID_FILE)


if __name__ == "__main__":
    main()