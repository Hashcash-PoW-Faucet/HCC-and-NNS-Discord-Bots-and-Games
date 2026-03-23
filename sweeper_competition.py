#!/usr/bin/env python3
import os
import json
import time
import sqlite3
from typing import Dict, Any, List, Tuple, Optional

from dotenv import load_dotenv
import requests

load_dotenv()

# ----------------------------
# Config (env)
# ----------------------------

DB_PATH = os.getenv("TIPBOT_DB", "tipbot.db").strip()

# Output JSON file
OUTPUT_FILE = os.getenv("SWEEPER_EVENT_OUTPUT_FILE", "sweeper_event_ranking.json").strip()

# Optional Discord webhook output
WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL_SWEEPER_EVENT", "").strip().strip('"').strip("'")
MESSAGE_ID_FILE = os.getenv("SWEEPER_EVENT_MESSAGE_ID_FILE", "sweeper_event_message_id.txt").strip()
PING_USERS = os.getenv("SWEEPER_EVENT_PING_USERS", "0").strip() == "1"

# Time window (inclusive)
# Expected as UNIX timestamps, e.g. 1773000000
EVENT_START_TS = int(os.getenv("SWEEPER_EVENT_START_TS", "0").strip() or "0")
EVENT_END_TS = int(os.getenv("SWEEPER_EVENT_END_TS", "0").strip() or "0")

# Ranking size
TOP_N = int(os.getenv("SWEEPER_EVENT_TOP_N", "100").strip() or "100")

# Scoring
POINTS_PER_GAME = int(os.getenv("SWEEPER_EVENT_POINTS_PER_GAME", "1").strip() or "1")
POINTS_PER_WIN = int(os.getenv("SWEEPER_EVENT_POINTS_PER_WIN", "5").strip() or "5")

# Optional metadata
EVENT_NAME = os.getenv("SWEEPER_EVENT_NAME", "Sweeper Event").strip()

# Name cache / Discord lookup
NAMES_CACHE_FILE = os.getenv("SWEEPER_NAMES_CACHE_FILE", "names_cache_sweeper.json").strip()
MAX_NAME_LEN = int(os.getenv("SWEEPER_LEADERBOARD_MAX_NAME_LEN", "22"))

# Optional: resolve usernames via Discord API (guild nick > global name)
DISCORD_BOT_TOKEN = os.getenv("DISCORD_TOKEN_SWEEPER", "").strip()
GUILD_ID = os.getenv("SWEEPER_GUILD_ID", "").strip()


# ----------------------------
# DB helper
# ----------------------------

def db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.row_factory = sqlite3.Row
    return con


# ----------------------------
# Names cache helpers
# ----------------------------

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
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
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
    return f"{int(n):,}".replace(",", ".")


def _webhook_base_url(url: str) -> str:
    return (url or "").strip().split("?")[0]


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
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(str(message_id).strip())
    except Exception:
        pass


def _build_webhook_payload(content: str) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"content": content}
    if PING_USERS:
        payload["allowed_mentions"] = {"parse": ["users"]}
    else:
        payload["allowed_mentions"] = {"parse": []}
    return payload


def post_or_edit_webhook_message(webhook_url: str, content: str, message_id_file: str) -> None:
    base_url = _webhook_base_url(webhook_url)
    payload = _build_webhook_payload(content)

    msg_id = _load_message_id(message_id_file)
    if msg_id:
        edit_url = f"{base_url}/messages/{msg_id}"
        r = requests.patch(edit_url, json=payload, timeout=20)
        if r.status_code < 300:
            return

    create_url = f"{base_url}?wait=true"
    r2 = requests.post(create_url, json=payload, timeout=20)
    if r2.status_code >= 300:
        raise RuntimeError(f"Webhook failed: {r2.status_code} {r2.text}")

    try:
        data = r2.json()
        new_id = str(data.get("id", "") or "").strip()
        if new_id:
            _save_message_id(message_id_file, new_id)
    except Exception:
        pass


def build_competition_table(entries: List[Dict[str, Any]]) -> str:
    """
    Build a compact text table for Discord from ranking JSON entries.
    """
    prepared: List[Tuple[str, str, str, str, str]] = []
    for item in entries:
        rank = str(int(item.get("rank", 0) or 0))
        name = shorten_name(str(item.get("name") or ""))
        score = fmt_int(int(item.get("score", 0) or 0))
        games = fmt_int(int(item.get("games_played", 0) or 0))
        wins = fmt_int(int(item.get("wins", 0) or 0))
        prepared.append((rank, name, score, games, wins))

    w_rank = max(2, max((len(r[0]) for r in prepared), default=2))
    w_name = max(4, max((len(r[1]) for r in prepared), default=4))
    w_score = max(5, max((len(r[2]) for r in prepared), default=5))
    w_games = max(5, max((len(r[3]) for r in prepared), default=5))
    w_wins = max(4, max((len(r[4]) for r in prepared), default=4))

    header = (
        f"{'#':>{w_rank}}  "
        f"{'User':<{w_name}}  "
        f"{'Score':>{w_score}}  "
        f"{'Games':>{w_games}}  "
        f"{'Wins':>{w_wins}}"
    )
    sep = (
        f"{'-':>{w_rank}}  "
        f"{'-' * w_name}  "
        f"{'-' * w_score}  "
        f"{'-' * w_games}  "
        f"{'-' * w_wins}"
    )

    lines = [header, sep]
    for rank, name, score, games, wins in prepared:
        lines.append(
            f"{rank:>{w_rank}}  {name:<{w_name}}  {score:>{w_score}}  {games:>{w_games}}  {wins:>{w_wins}}"
        )

    return "```\n" + "\n".join(lines) + "\n```"


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
    """Resolve a friendly display name via Discord API (guild nick > global username)."""
    did = str(discord_id).strip()
    if not did.isdigit():
        return None

    # 1) Guild member (preferred)
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

    # 2) Fallback: user object
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
# Ranking logic
# ----------------------------

def _validate_window(start_ts: int, end_ts: int) -> Tuple[int, int]:
    start_ts = int(start_ts or 0)
    end_ts = int(end_ts or 0)

    if start_ts <= 0:
        raise SystemExit("SWEEPER_EVENT_START_TS must be set to a UNIX timestamp > 0")
    if end_ts <= 0:
        raise SystemExit("SWEEPER_EVENT_END_TS must be set to a UNIX timestamp > 0")
    if end_ts < start_ts:
        raise SystemExit("SWEEPER_EVENT_END_TS must be >= SWEEPER_EVENT_START_TS")

    return start_ts, end_ts


def fetch_event_stats(
    con: sqlite3.Connection,
    start_ts: int,
    end_ts: int,
) -> Dict[int, Dict[str, int]]:
    """
    Return per-user event stats within [start_ts, end_ts].

    Games are detected via tx_log entries:
      type='game', status='ok', note LIKE 'Sweeper entry fee%'

    Wins are detected via:
      type='game', status='ok', note LIKE 'Sweeper win%'

    Important:
    - A played game is attributed using from_id on entry fee rows.
    - A win is attributed using to_id on win rows.
    """
    stats: Dict[int, Dict[str, int]] = {}

    # Played games
    q_games = """
    SELECT
      from_id AS discord_id,
      COUNT(*) AS games_played
    FROM tx_log
    WHERE
      type='game'
      AND status='ok'
      AND from_id IS NOT NULL
      AND note LIKE 'Sweeper entry fee%%'
      AND ts >= ?
      AND ts <= ?
    GROUP BY from_id
    """

    # Wins
    q_wins = """
    SELECT
      to_id AS discord_id,
      COUNT(*) AS wins
    FROM tx_log
    WHERE
      type='game'
      AND status='ok'
      AND to_id IS NOT NULL
      AND note LIKE 'Sweeper win%%'
      AND ts >= ?
      AND ts <= ?
    GROUP BY to_id
    """

    for row in con.execute(q_games, (int(start_ts), int(end_ts))).fetchall():
        try:
            did = int(row["discord_id"])
            games = int(row["games_played"] or 0)
        except Exception:
            continue
        if did not in stats:
            stats[did] = {"games_played": 0, "wins": 0}
        stats[did]["games_played"] += games

    for row in con.execute(q_wins, (int(start_ts), int(end_ts))).fetchall():
        try:
            did = int(row["discord_id"])
            wins = int(row["wins"] or 0)
        except Exception:
            continue
        if did not in stats:
            stats[did] = {"games_played": 0, "wins": 0}
        stats[did]["wins"] += wins

    return stats


def build_ranking_rows(
    raw_stats: Dict[int, Dict[str, int]],
    points_per_game: int,
    points_per_win: int,
) -> List[Tuple[int, int, int, int]]:
    """
    Convert raw stats into sortable ranking rows:
      [(discord_id, score, games_played, wins), ...]
    """
    rows: List[Tuple[int, int, int, int]] = []

    for did, s in raw_stats.items():
        games_played = int(s.get("games_played", 0) or 0)
        wins = int(s.get("wins", 0) or 0)
        score = games_played * int(points_per_game) + wins * int(points_per_win)

        # Only include users with at least one relevant event
        if games_played > 0 or wins > 0:
            rows.append((int(did), int(score), int(games_played), int(wins)))

    # Sort by:
    # 1) score desc
    # 2) wins desc
    # 3) games_played desc
    # 4) discord_id asc (stable fallback)
    rows.sort(key=lambda x: (-x[1], -x[3], -x[2], x[0]))
    return rows


# ----------------------------
# Main
# ----------------------------

def main() -> None:
    if not os.path.exists(DB_PATH):
        raise SystemExit(f"DB not found: {DB_PATH}")

    start_ts, end_ts = _validate_window(EVENT_START_TS, EVENT_END_TS)

    names_cache = load_names_cache(NAMES_CACHE_FILE)

    con = db()
    try:
        raw_stats = fetch_event_stats(con, start_ts, end_ts)
    except sqlite3.Error as e:
        raise SystemExit(f"Sweeper event DB error: {e}")
    finally:
        try:
            con.close()
        except Exception:
            pass

    ranking = build_ranking_rows(raw_stats, POINTS_PER_GAME, POINTS_PER_WIN)

    cache_changed = False
    entries: List[Dict[str, Any]] = []

    for rank, (did, score, games_played, wins) in enumerate(ranking[:TOP_N], start=1):
        key = str(did)

        # Prefer cached name
        name = names_cache.get(key)

        # Try live Discord resolution if token available
        if (not name) and DISCORD_BOT_TOKEN:
            resolved = resolve_display_name(key)
            if resolved:
                names_cache[key] = resolved
                name = resolved
                cache_changed = True

        if not name:
            name = f"<@{key}>"

        entries.append({
            "rank": rank,
            "discord_id": key,
            "name": shorten_name(name),
            "score": int(score),
            "games_played": int(games_played),
            "wins": int(wins),
        })

    if cache_changed:
        save_names_cache(NAMES_CACHE_FILE, names_cache)

    total_games = sum(int(s.get("games_played", 0) or 0) for s in raw_stats.values())
    total_wins = sum(int(s.get("wins", 0) or 0) for s in raw_stats.values())
    total_players = len([1 for s in raw_stats.values() if (s.get("games_played", 0) or 0) > 0 or (s.get("wins", 0) or 0) > 0])

    output: Dict[str, Any] = {
        "event_name": EVENT_NAME,
        "generated_at": int(time.time()),
        "window": {
            "start_ts": int(start_ts),
            "end_ts": int(end_ts),
        },
        "scoring": {
            "points_per_game": int(POINTS_PER_GAME),
            "points_per_win": int(POINTS_PER_WIN),
        },
        "summary": {
            "total_players": int(total_players),
            "total_games": int(total_games),
            "total_wins": int(total_wins),
        },
        "ranking": entries,
    }

    try:
        tmp = f"{OUTPUT_FILE}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        os.replace(tmp, OUTPUT_FILE)
    except Exception as e:
        raise SystemExit(f"Could not write output JSON: {e}")

    if WEBHOOK_URL:
        entries_for_post = entries[: min(len(entries), 20)]
        table = build_competition_table(entries_for_post) if entries_for_post else "_No ranked players in this event window yet._"

        summary_lines = [
            f"**{EVENT_NAME}**",
            f"Window: <t:{int(start_ts)}:f> → <t:{int(end_ts)}:f>",
            f"Scoring: **{POINTS_PER_GAME}** point per game, **{POINTS_PER_WIN}** points per win",
            "",
            f"Players: **{fmt_int(total_players)}** | Games: **{fmt_int(total_games)}** | Wins: **{fmt_int(total_wins)}**",
            "",
            table,
            f"Updated: {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())}",
        ]
        webhook_msg = "\n".join(summary_lines)
        try:
            post_or_edit_webhook_message(WEBHOOK_URL, webhook_msg, MESSAGE_ID_FILE)
        except Exception as e:
            print(f"Webhook update failed: {e}")

    print(f"Wrote event ranking to: {OUTPUT_FILE}")
    print(f"Players: {total_players} | Games: {total_games} | Wins: {total_wins} | Ranked entries: {len(entries)}")
    if WEBHOOK_URL:
        print(f"Webhook updated via: {WEBHOOK_URL}")


if __name__ == "__main__":
    main()