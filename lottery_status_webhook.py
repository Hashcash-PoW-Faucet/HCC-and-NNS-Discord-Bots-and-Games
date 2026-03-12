#!/usr/bin/env python3
import os
import json
import time
import sqlite3
import requests
from urllib.parse import urlparse, parse_qsl, urlencode
from typing import Optional, Dict, Any, Tuple

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


DB_PATH = os.environ.get("LOTTERY_DB_PATH", "tipbot.db").strip()

WEBHOOK_URL = os.environ.get("DISCORD_LOTTERY_WEBHOOK_URL", "").strip()
STATE_FILE = os.environ.get("LOTTERY_WEBHOOK_STATE_FILE", "lottery_webhook_state.json").strip()

USERNAME = os.environ.get("LOTTERY_WEBHOOK_USERNAME", "").strip() or None
AVATAR_URL = os.environ.get("LOTTERY_WEBHOOK_AVATAR_URL", "").strip() or None
PING_ROLE_ID = os.environ.get("LOTTERY_WEBHOOK_PING_ROLE_ID", "").strip() or None
EDIT_WINDOW_MIN = int(os.environ.get("LOTTERY_WEBHOOK_EDIT_WINDOW_MIN", "0") or "0")

# Optional: show prize estimates (must match lottery bot settings)
REGULAR_POOL_BPS = int(os.environ.get("LOTTERY_REGULAR_POOL_BPS", "500") or "500")  # default 5%
JACKPOT_CHANCE_BPS = int(os.environ.get("LOTTERY_JACKPOT_CHANCE_BPS", "300") or "300")  # default 3% chance
JACKPOT_PCT_BPS = int(os.environ.get("LOTTERY_JACKPOT_PCT_BPS", "1000") or "1000")  # default 10% of pot
JACKPOT_CAP_HCC = int(os.environ.get("LOTTERY_JACKPOT_CAP_HCC", "0") or "0")  # default no cap

# clamp like the bot
REGULAR_POOL_BPS = max(0, min(REGULAR_POOL_BPS, 5000))
JACKPOT_CHANCE_BPS = max(0, min(JACKPOT_CHANCE_BPS, 10000))
JACKPOT_PCT_BPS = max(0, min(JACKPOT_PCT_BPS, 5000))
JACKPOT_CAP_HCC = max(0, min(JACKPOT_CAP_HCC, 1_000_000_000))


def now_unix() -> int:
    return int(time.time())


def db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute("PRAGMA busy_timeout=30000;")
    return con


def load_state() -> Dict[str, Any]:
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}


def save_state(st: Dict[str, Any]) -> None:
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATE_FILE)


def build_webhook_base_and_query() -> Tuple[str, Dict[str, str]]:
    """Split webhook URL into base path and query parameters."""
    parsed = urlparse(WEBHOOK_URL)
    base = f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    return base, query


def is_retryable_webhook_error(message: str) -> bool:
    """Return True for temporary webhook failures where we should not repost immediately."""
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


def webhook_post(content: str) -> Optional[str]:
    base, query = build_webhook_base_and_query()
    query["wait"] = "true"
    url = base + "?" + urlencode(query)

    payload: Dict[str, Any] = {"content": content}
    if USERNAME:
        payload["username"] = USERNAME
    if AVATAR_URL:
        payload["avatar_url"] = AVATAR_URL

    r = requests.post(url, json=payload, timeout=20)
    if r.status_code // 100 != 2:
        raise RuntimeError(f"webhook post failed: {r.status_code} {r.text[:500]}")
    try:
        msg = r.json()
        mid = str(msg.get("id") or "")
        return mid or None
    except Exception:
        return None


def webhook_edit(message_id: str, content: str) -> None:
    # webhook edit endpoint: {webhook_url}/messages/{message_id}
    base, query = build_webhook_base_and_query()
    url = base + f"/messages/{message_id}"
    if query:
        url += "?" + urlencode(query)

    payload: Dict[str, Any] = {"content": content}
    if USERNAME:
        payload["username"] = USERNAME
    if AVATAR_URL:
        payload["avatar_url"] = AVATAR_URL

    r = requests.patch(url, json=payload, timeout=20)
    if r.status_code // 100 != 2:
        raise RuntimeError(f"webhook edit failed: {r.status_code} {r.text[:500]}")


def fmt_ts_rel(ts: int) -> str:
    # Discord relative timestamp
    return f"<t:{int(ts)}:R>"


def fmt_ts_abs(ts: int) -> str:
    return f"<t:{int(ts)}:f>"


def fetch_lottery_status(con: sqlite3.Connection) -> Dict[str, Any]:
    st = con.execute("SELECT active_round_id, COALESCE(enabled,1) AS enabled FROM lottery_state WHERE id=1").fetchone()
    active_id = int(st["active_round_id"] or 0) if st else 0
    enabled = int(st["enabled"] or 1) if st else 1

    out: Dict[str, Any] = {
        "enabled": bool(enabled == 1),
        "active_round_id": active_id if active_id > 0 else None,
    }

    if active_id <= 0:
        return out

    r = con.execute("SELECT * FROM lottery_rounds WHERE id=?", (active_id,)).fetchone()
    if not r:
        return out

    # Tickets sum
    trow = con.execute("SELECT COALESCE(SUM(tickets),0) AS n FROM lottery_tickets WHERE round_id=?", (active_id,)).fetchone()
    total_tickets = int(trow["n"] or 0) if trow else 0

    seed_hcc = int(r["seed_hcc"] or 0)
    pot_tickets_hcc = int(r["pot_from_tickets_hcc"] or 0)
    pot_total = seed_hcc + pot_tickets_hcc

    out.update({
        "round_id": int(r["id"]),
        "status": str(r["status"] or ""),
        "starts_at": int(r["starts_at"] or 0),
        "ends_at": int(r["ends_at"] or 0),
        "ticket_price_hcc": int(r["ticket_price_hcc"] or 0),
        "house_fee_bps": int(r["house_fee_bps"] or 0),
        "split1_bps": int(r["split1_bps"] or 0),
        "split2_bps": int(r["split2_bps"] or 0),
        "split3_bps": int(r["split3_bps"] or 0),
        "ticket_cap": int(r["ticket_cap"] or 0),
        "seed_hcc": seed_hcc,
        "pot_tickets_hcc": pot_tickets_hcc,
        "pot_total_hcc": pot_total,
        "total_tickets": total_tickets,
        "commit_hash": str(r["commit_hash"] or ""),
        "created_at": int(r["created_at"] or 0),
    })
    return out


def compute_prizes(pot_total: int, pot_tickets: int, fee_bps: int, split1_bps: int, split2_bps: int, split3_bps: int) -> Tuple[int, int, int, int, int]:
    """Return (house_fee, net_pot, p1, p2, p3) for the regular pool."""
    house_fee = (int(pot_tickets) * int(fee_bps)) // 10000
    net_pot = max(0, int(pot_total) - int(house_fee))

    # Regular pool is a fraction of total pot, capped by net pot
    rp = 0
    if REGULAR_POOL_BPS > 0:
        rp = (int(pot_total) * int(REGULAR_POOL_BPS)) // 10000
        if rp < 0:
            rp = 0
        if rp > net_pot:
            rp = net_pot

    # Normalize splits if needed
    s1, s2, s3 = int(split1_bps), int(split2_bps), int(split3_bps)
    ssum = s1 + s2 + s3
    if ssum <= 0:
        s1, s2, s3 = 6000, 2500, 1000
        ssum = s1 + s2 + s3
    if ssum != 10000 and ssum > 0:
        # scale to 10000 bps
        s1 = int(round(s1 * 10000 / ssum))
        s2 = int(round(s2 * 10000 / ssum))
        s3 = max(0, 10000 - s1 - s2)

    p1 = (rp * s1) // 10000
    p2 = (rp * s2) // 10000
    p3 = (rp * s3) // 10000
    used = p1 + p2 + p3
    if used < rp:
        p1 += (rp - used)

    return house_fee, net_pot, p1, p2, p3


def compute_jackpot(pot_total: int, net_pot: int) -> int:
    if JACKPOT_PCT_BPS <= 0:
        return 0
    j = (int(pot_total) * int(JACKPOT_PCT_BPS)) // 10000
    if JACKPOT_CAP_HCC > 0:
        j = min(j, int(JACKPOT_CAP_HCC))
    if j > net_pot:
        j = net_pot
    if j < 0:
        j = 0
    return int(j)


def render_message(s: Dict[str, Any]) -> str:
    enabled = bool(s.get("enabled"))
    rid = s.get("active_round_id")

    head = "🎟️ **HCC Lottery — Status**"
    if not enabled:
        head += "  🛑 *(paused)*"

    if not rid:
        return (
            f"{head}\n"
            "No active round right now.\n"
            "Use `/start_lottery` to enable / start rounds."
        )

    # Round details
    round_id = s.get("round_id")
    status = s.get("status", "")
    starts_at = int(s.get("starts_at") or 0)
    ends_at = int(s.get("ends_at") or 0)

    pot_total = int(s.get("pot_total_hcc") or 0)
    seed = int(s.get("seed_hcc") or 0)
    pot_tickets = int(s.get("pot_tickets_hcc") or 0)
    total_tickets = int(s.get("total_tickets") or 0)
    price = int(s.get("ticket_price_hcc") or 0)

    fee_bps = int(s.get("house_fee_bps") or 0)
    s1 = int(s.get("split1_bps") or 0)
    s2 = int(s.get("split2_bps") or 0)
    s3 = int(s.get("split3_bps") or 0)
    cap = int(s.get("ticket_cap") or 0)
    commit = str(s.get("commit_hash") or "")
    commit_short = (commit[:16] + "…") if commit else "—"

    # Small derived
    time_left = max(0, ends_at - now_unix())
    hours = time_left // 3600
    mins = (time_left % 3600) // 60
    secs = time_left % 60
    if hours > 0:
        left_txt = f"{hours}h {mins}m {secs}s"
    else:
        left_txt = f"{mins}m {secs}s"

    cap_txt = "∞" if cap <= 0 else str(cap)

    prize_block = ""
    if REGULAR_POOL_BPS > 0:
        house_fee_hcc, net_pot, p1, p2, p3 = compute_prizes(pot_total, pot_tickets, fee_bps, s1, s2, s3)
        jackpot_amt = compute_jackpot(pot_total, net_pot)

        prize_block = (
            f"Prizes (regular {REGULAR_POOL_BPS/100:.2f}%): 🥇 **{p1}**  🥈 **{p2}**  🥉 **{p3}** HCC\n"
        )
        if JACKPOT_CHANCE_BPS > 0 and JACKPOT_PCT_BPS > 0:
            prize_block += (
                f"Jackpot: {JACKPOT_CHANCE_BPS/100:.2f}% chance of {JACKPOT_PCT_BPS/100:.2f}% pot"
                + (f" (cap {JACKPOT_CAP_HCC} HCC)" if JACKPOT_CAP_HCC > 0 else "")
                + f" → ~**{jackpot_amt} HCC**\n"
            )

    # Optional ping
    ping = ""
    if PING_ROLE_ID:
        ping = f"<@&{PING_ROLE_ID}>\n"

    return (
        f"{ping}{head}\n"
        f"**Round #{round_id}**  _(status: {status})_\n"
        f"Pot: **{pot_total} HCC**  *(Seed {seed} + Tickets {pot_tickets})*\n"
        f"Tickets sold: **{total_tickets}**  | Ticket price: **{price} HCC**  | Cap: **{cap_txt}**\n"
        f"{prize_block}"
        f"Ends: {fmt_ts_rel(ends_at)}  ({fmt_ts_abs(ends_at)})  •  remaining ~ **{left_txt}**\n"
        f"Fee: **{fee_bps/100:.2f}%** (ticket pot)  | Split: **{s1/100:.0f}/{s2/100:.0f}/{s3/100:.0f}**\n"
        f"Commit: `{commit_short}`\n"
        f"_Updated: {fmt_ts_rel(now_unix())}_"
    )


def main() -> None:
    if not WEBHOOK_URL:
        raise SystemExit("DISCORD_LOTTERY_WEBHOOK_URL missing")

    con = db()
    try:
        s = fetch_lottery_status(con)
    finally:
        con.close()

    msg = render_message(s)

    st = load_state()
    mid = str(st.get("message_id") or "").strip() or None
    last_post_ts = int(st.get("posted_at") or 0)

    # optional: if edit window set, post a new message after it
    if mid and EDIT_WINDOW_MIN > 0:
        if (now_unix() - last_post_ts) > (EDIT_WINDOW_MIN * 60):
            mid = None

    try:
        if mid:
            print(f"Trying to edit lottery webhook message_id={mid}")
            webhook_edit(mid, msg)
            st["last_error"] = ""
            save_state(st)
        else:
            new_id = webhook_post(msg)
            if new_id:
                st["message_id"] = new_id
                st["posted_at"] = now_unix()
                st["last_error"] = ""
                save_state(st)
    except Exception as e:
        err_msg = str(e)
        print(f"Lottery webhook update failed for message_id={mid}: {err_msg}")
        st["last_error"] = err_msg

        # For temporary Discord/network problems, do not create duplicate messages.
        if is_retryable_webhook_error(err_msg):
            save_state(st)
            raise

        # If edit failed because the message is gone or invalid, post a new one and overwrite state.
        if mid:
            try:
                new_id = webhook_post(msg)
                if new_id:
                    st["message_id"] = new_id
                    st["posted_at"] = now_unix()
                    st["last_error"] = ""
                    save_state(st)
                return
            except Exception as post_err:
                st["last_error"] = f"{err_msg} | fallback post failed: {post_err}"
                save_state(st)
                raise

        save_state(st)
        raise


if __name__ == "__main__":
    main()