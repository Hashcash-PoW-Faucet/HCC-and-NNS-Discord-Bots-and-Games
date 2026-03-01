#!/usr/bin/env python3
"""
HCC Lottery Reminder Webhook

Posts a reminder about the current lottery round to a Discord webhook.
"""

import os
import time
import sqlite3
import secrets
from typing import List, Tuple, Dict, Any, Optional

import requests
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.environ.get("LOTTERY_DB_PATH", "tipbot.db").strip()
WEBHOOK_URL = os.environ.get("LOTTERY_REMINDER_WEBHOOK_URL", "").strip()

if not WEBHOOK_URL:
    raise SystemExit("LOTTERY_REMINDER_WEBHOOK_URL missing in .env")


# ----------------------------
# Helpers
# ----------------------------

def now_unix() -> int:
    return int(time.time())


def db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con


def ensure_tables(con: sqlite3.Connection) -> None:
    """
    Minimal subset of the lottery tables, identical to the main bot.
    If everything already exists, this is a no-op.
    """
    con.execute("""
    CREATE TABLE IF NOT EXISTS lottery_state (
        id INTEGER PRIMARY KEY CHECK (id=1),
        active_round_id INTEGER,
        enabled INTEGER NOT NULL DEFAULT 1,
        updated_at INTEGER
    );
    """)
    con.execute("""
    INSERT OR IGNORE INTO lottery_state(id, active_round_id, updated_at)
    VALUES(1, NULL, strftime('%s','now'));
    """)
    cols = [r[1] for r in con.execute("PRAGMA table_info(lottery_state)").fetchall()]
    if "enabled" not in cols:
        con.execute("ALTER TABLE lottery_state ADD COLUMN enabled INTEGER NOT NULL DEFAULT 1;")
        con.execute("UPDATE lottery_state SET enabled=COALESCE(enabled,1) WHERE id=1;")

    con.execute("""
    CREATE TABLE IF NOT EXISTS lottery_rounds (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        starts_at INTEGER NOT NULL,
        ends_at INTEGER NOT NULL,
        status TEXT NOT NULL,
        ticket_price_hcc INTEGER NOT NULL,
        house_fee_bps INTEGER NOT NULL,
        split1_bps INTEGER NOT NULL,
        split2_bps INTEGER NOT NULL,
        split3_bps INTEGER NOT NULL,
        ticket_cap INTEGER NOT NULL,
        seed_hcc INTEGER NOT NULL,
        commit_hash TEXT NOT NULL,
        reveal_secret TEXT,
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


def compute_pot_total(seed_hcc: int, pot_tickets_hcc: int) -> int:
    return int(seed_hcc + pot_tickets_hcc)


def weighted_draw_without_replacement(entries: List[Tuple[int, int]], k: int) -> List[int]:
    """
    entries: [(discord_id, tickets), ...]
    returns up to k winners (distinct discord_ids), weighted by tickets.
    (Copied from HCC_lottery_bot for consistent odds.)
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


def format_pct(x: float) -> str:
    return f"{max(0.0, min(1.0, x)) * 100.0:.2f}%"


def compute_odds_top3(entries: List[Tuple[int, int]], my_id: int) -> Dict[str, float]:
    """
    Approximate odds for my_id to get 1st/2nd/3rd place and any prize.
    Logic copied from HCC_lottery_bot for consistency.
    """
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

    # If too many participants, Monte Carlo
    if len(others) > 500:
        iters = 3000
        win1 = win2 = win3 = 0
        pool = [(uid, int(t)) for (uid, t) in entries if int(t or 0) > 0]
        for _ in range(iters):
            winners = weighted_draw_without_replacement(pool, 3)
            if not winners:
                continue
            if len(winners) > 0 and winners[0] == my_id:
                win1 += 1
            if len(winners) > 1 and winners[1] == my_id:
                win2 += 1
            if len(winners) > 2 and winners[2] == my_id:
                win3 += 1
        p1 = win1 / iters
        p2 = win2 / iters
        p3 = win3 / iters
        pany = min(1.0, p1 + p2 + p3)
        return {"p1": p1, "p2": p2, "p3": p3, "pany": pany}

    # Analytic approx (O(n^2), ok for <=~500)
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
# Core reminder logic
# ----------------------------

def build_reminder_payload() -> Optional[Dict[str, Any]]:
    con = db()
    try:
        ensure_tables(con)

        # Find active round id
        st = con.execute("SELECT active_round_id FROM lottery_state WHERE id=1").fetchone()
        rid = int(st["active_round_id"] or 0) if st else 0
        if rid <= 0:
            return None

        row = con.execute("SELECT * FROM lottery_rounds WHERE id=?", (rid,)).fetchone()
        if not row:
            return None

        status = str(row["status"] or "")
        if status != "active":
            return None

        seed_hcc = int(row["seed_hcc"] or 0)
        pot_tickets_hcc = int(row["pot_from_tickets_hcc"] or 0)
        pot_total = compute_pot_total(seed_hcc, pot_tickets_hcc)
        ends_at = int(row["ends_at"] or 0)

        # Total tickets sold so far
        trows = con.execute(
            "SELECT discord_id, tickets FROM lottery_tickets WHERE round_id=?",
            (rid,)
        ).fetchall()
        entries = [(int(r["discord_id"]), int(r["tickets"] or 0)) for r in trows]
        total_tickets = sum(t for _, t in entries)

        # Simulate a hypothetical player with 10 tickets on top of current pool
        MY_FAKE_ID = -1_000_000_000
        entries_with_me = entries + [(MY_FAKE_ID, 10)]
        odds = compute_odds_top3(entries_with_me, MY_FAKE_ID)

        return {
            "round_id": rid,
            "pot_total": pot_total,
            "seed_hcc": seed_hcc,
            "tickets_hcc": pot_tickets_hcc,
            "total_tickets": total_tickets,
            "ends_at": ends_at,
            "odds": odds,
        }
    finally:
        con.close()


def send_reminder():
    data = build_reminder_payload()
    if not data:
        # No active round → silently do nothing
        return

    rid = data["round_id"]
    pot = data["pot_total"]
    seed_hcc = data["seed_hcc"]
    tickets_hcc = data["tickets_hcc"]
    total_tickets = data["total_tickets"]
    ends_at = data["ends_at"]
    odds = data["odds"]

    p1 = format_pct(odds["p1"])
    p2 = format_pct(odds["p2"])
    p3 = format_pct(odds["p3"])
    pany = format_pct(odds["pany"])

    # Discord timestamp markup (<t:unix:R>) also works for webhooks.
    content = (
        f"⏰ **HCC Lottery Reminder**\n"
        f"Current round: **#{rid}** — ends <t:{ends_at}:R>\n"
        f"Pot so far: **{pot} HCC** (Seed {seed_hcc}, +Tickets {tickets_hcc})\n"
        f"Tickets sold: **{total_tickets}**\n\n"
        f"If you bought **10 tickets now**, your approximate chances this round would be:\n"
        f"🥇 1st: **{p1}**\n"
        f"🥈 2nd: **{p2}**\n"
        f"🥉 3rd: **{p3}**\n"
        f"🎯 Any prize: **{pany}**\n\n"
        f"Use `/lottery`, `/buytickets` and `/lottery_odds` for details."
    )

    resp = requests.post(WEBHOOK_URL, json={"content": content})
    if resp.status_code >= 300:
        print(f"[lottery_reminder] webhook error {resp.status_code}: {resp.text}")


if __name__ == "__main__":
    send_reminder()