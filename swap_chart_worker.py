#!/usr/bin/env python3
"""
swap_chart_webhook.py

Render a small price chart (VECO per HCC) from tipbot.db:swap_log and post/update it via Discord webhook.

- Reads last N swaps (default 20) with status='ok'
- Computes HCC/VECO price per swap, regardless of direction:
    HCC->VECO: price = amount_in_hcc / (amount_out_veco)
    VECO->HCC: price = amount_out_hcc / (amount_in_veco)
- Auto X-axis: uses timestamps of swaps; matplotlib chooses sensible ticks.
- Updates an existing webhook message if DISCORD_WEBHOOK_MESSAGE_ID is set, otherwise posts a new one.

Env:
  TIPBOT_DB=/pathto/tipbot.db
  SWAP_CHART_LAST_N=20
  DISCORD_WEBHOOK_URL_CHART=https://discord.com/api/webhooks/...
  DISCORD_WEBHOOK_MESSAGE_ID=...   (optional; overrides state file if set)
  DISCORD_WEBHOOK_STATE_FILE=/path/to/swap_chart_state.json (optional; default: scriptdir/.swap_chart_state.json)
  DISCORD_WEBHOOK_USERNAME= (optional)
  DISCORD_WEBHOOK_AVATAR_URL=...   (optional)
"""

import os
import io
import json
import time
import sqlite3
from dataclasses import dataclass
from typing import List, Optional, Tuple
import requests
from dotenv import load_dotenv
from pathlib import Path
import matplotlib
matplotlib.use("Agg")  # headless
import matplotlib.pyplot as plt  # noqa

load_dotenv(Path(__file__).resolve().parent / ".env")

VECO_SATS = 100_000_000


def fmt_utc(ts: int) -> str:
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(int(ts)))
    except Exception:
        return ""


def clamp_int(v: int, lo: int, hi: int) -> int:
    try:
        v = int(v)
    except Exception:
        v = lo
    if v < lo:
        v = lo
    if v > hi:
        v = hi
    return v


@dataclass
class SwapPoint:
    ts: int
    price_veco_per_hcc: float
    direction: str  # "HCC→VECO" or "VECO→HCC"


def db_connect(path: str) -> sqlite3.Connection:
    con = sqlite3.connect(path, timeout=30)
    con.row_factory = sqlite3.Row
    return con


def load_message_id_from_state(state_path: Path) -> Optional[str]:
    try:
        if not state_path.exists():
            return None
        raw = state_path.read_text(encoding="utf-8").strip()
        if not raw:
            return None
        obj = json.loads(raw)
        if isinstance(obj, dict):
            mid = str(obj.get("message_id") or "").strip()
            return mid or None
    except Exception:
        return None
    return None


def save_message_id_to_state(state_path: Path, message_id: str) -> None:
    try:
        state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = state_path.with_suffix(state_path.suffix + ".tmp")
        tmp.write_text(json.dumps({"message_id": str(message_id).strip()}, ensure_ascii=False), encoding="utf-8")
        tmp.replace(state_path)
    except Exception:
        # Best-effort only
        pass


def fetch_last_swaps(con: sqlite3.Connection, limit: int) -> List[sqlite3.Row]:
    # Using id DESC to get last N, then reverse to chronological for nicer plot
    rows = con.execute(
        """
        SELECT id, ts, from_asset, to_asset, amount_in, amount_out, fee_amount, status
        FROM swap_log
        WHERE status='ok'
        ORDER BY id DESC
        LIMIT ?
        """,
        (int(limit),),
    ).fetchall()
    rows = list(rows)
    rows.reverse()
    return rows


def compute_points(rows: List[sqlite3.Row]) -> List[SwapPoint]:
    pts: List[SwapPoint] = []
    for r in rows:
        ts = int(r["ts"] or 0)
        fa = str(r["from_asset"] or "").upper()
        ta = str(r["to_asset"] or "").upper()
        ain = int(r["amount_in"] or 0)
        aout = int(r["amount_out"] or 0)

        if ts <= 0 or ain <= 0 or aout <= 0:
            continue

        # We want price = HCC per 1 VECO
        # Case 1: HCC -> VECO
        if fa == "HCC" and ta == "VECO":
            out_veco = float(aout) / float(VECO_SATS)
            if out_veco <= 0:
                continue
            price = out_veco / float(ain)          # VECO / HCC
            pts.append(SwapPoint(ts=ts, price_veco_per_hcc=price, direction="HCC→VECO"))
            continue

        # Case 2: VECO -> HCC
        if fa == "VECO" and ta == "HCC":
            in_veco = float(ain) / float(VECO_SATS)
            if in_veco <= 0:
                continue
            price = in_veco / float(aout)          # VECO / HCC
            pts.append(SwapPoint(ts=ts, price_veco_per_hcc=price, direction="VECO→HCC"))
            continue

        # Unknown pair (ignore)
    return pts


def render_chart(points: List[SwapPoint], title: str) -> bytes:
    ys = [p.price_veco_per_hcc for p in points]
    xs = list(range(1, len(ys) + 1))  # 1..N

    # --- Discord-ish dark styling, more square-ish + larger fonts ---
    fig = plt.figure(figsize=(7.2, 5.2), dpi=170, facecolor="#0f1117")
    ax = fig.add_subplot(111, facecolor="#0f1117")

    # Step plot (discrete ticks)
    ax.step(xs, ys, where="post", linewidth=3.0, color="#4aa3ff", alpha=0.95)

    # Marker on last point
    ax.scatter([xs[-1]], [ys[-1]], s=85, color="#4aa3ff", zorder=6)

    # Fill under the step curve (to baseline)
    baseline = min(ys)
    ax.fill_between(xs, ys, baseline, step="post", color="#4aa3ff", alpha=0.10)

    # Title + labels
    ax.set_title(title, color="#eaeaea", pad=12, fontsize=16, fontweight="bold")
    ax.set_ylabel("VECO per HCC", color="#d6d6d6", fontsize=13)
    ax.set_xlabel("Swap #", color="#d6d6d6", fontsize=13)

    # Ticks: readable
    ax.tick_params(axis="x", colors="#cfcfcf", labelsize=12)
    ax.tick_params(axis="y", colors="#cfcfcf", labelsize=12)

    # X ticks: show every swap index (1..N)
    n = len(xs)
    ax.set_xticks(xs)
    ax.set_xlim(0.8, n + 0.6)

    # Subtle grid
    ax.grid(True, axis="y", linestyle="-", linewidth=1.0, alpha=0.18)
    ax.grid(True, axis="x", linestyle="-", linewidth=0.8, alpha=0.10)

    # Spines
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#2a2f3a")
    ax.spines["bottom"].set_color("#2a2f3a")

    # Last price badge + faint line
    last_y = ys[-1]
    ax.axhline(last_y, color="#4aa3ff", alpha=0.16, linewidth=1.2)
    ax.annotate(
        f"{last_y:.4f}",
        xy=(xs[-1], last_y),
        xytext=(10, 0),
        textcoords="offset points",
        va="center",
        color="#0f1117",
        fontsize=12,
        bbox=dict(boxstyle="round,pad=0.30", fc="#4aa3ff", ec="none", alpha=0.95),
        zorder=10,
    )

    # Y-limits with modest padding
    y_min, y_max = min(ys), max(ys)
    span = (y_max - y_min)
    pad = span * 0.35 if span > 0 else (max(0.02, y_max * 0.01))
    ax.set_ylim(y_min - pad, y_max + pad)

    fig.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    return buf.getvalue()


def discord_webhook_post_or_patch(
    webhook_url: str,
    message_id: Optional[str],
    png_bytes: bytes,
    content: str,
    username: Optional[str] = None,
    avatar_url: Optional[str] = None,
) -> Tuple[bool, str]:
    """
    If message_id is set: PATCH /messages/{id} to update.
    Else: POST to create.

    Returns (ok, info). PATCH replaces the chart attachment; POST may return the created message id.
    """
    if not webhook_url:
        return False, "DISCORD_WEBHOOK_URL_CHART is empty"

    params = {"wait": "true"}  # return JSON with id on POST

    # Build payload. When PATCHing an existing message, we must explicitly specify
    # the attachments we want to keep; otherwise Discord may keep old attachments
    # and add the new one, resulting in multiple images.
    payload: dict = {"content": content}

    # Only POST supports overriding username/avatar. PATCH ignores these fields.
    if not message_id:
        if username:
            payload["username"] = username
        if avatar_url:
            payload["avatar_url"] = avatar_url

    # If we have image bytes, attach as file0 and map it in payload_json.
    files = None
    if png_bytes:
        payload["attachments"] = [{"id": 0, "filename": "swap_chart.png"}]
        files = {
            "files[0]": ("swap_chart.png", png_bytes, "image/png"),
        }

    data = {"payload_json": json.dumps(payload)}

    try:
        if message_id:
            url = webhook_url.rstrip("/") + f"/messages/{message_id}"
            r = requests.patch(url, params=params, data=data, files=files, timeout=30)
        else:
            url = webhook_url.rstrip("/")
            r = requests.post(url, params=params, data=data, files=files, timeout=30)

        txt = r.text or ""
        if r.status_code < 200 or r.status_code >= 300:
            return False, f"Discord HTTP {r.status_code}: {txt[:500]}"

        # On POST with wait=true we get a JSON with id
        if not message_id:
            try:
                j = r.json()
                mid = str(j.get("id") or "").strip()
                if mid:
                    return True, f"posted_ok:{mid}"
            except Exception:
                pass
            return True, "posted ok"
        return True, "patched ok"
    except Exception as e:
        return False, f"Discord request failed: {e}"


def main() -> int:
    db_path = os.environ.get("TIPBOT_DB", "tipbot.db").strip()
    last_n = clamp_int(int(os.environ.get("SWAP_CHART_LAST_N", "20")), 2, 200)

    webhook_url = os.environ.get("DISCORD_WEBHOOK_URL_CHART", "").strip()
    message_id = os.environ.get("DISCORD_WEBHOOK_MESSAGE_ID", "").strip() or None

    state_file = os.environ.get(
        "DISCORD_WEBHOOK_STATE_FILE",
        str(Path(__file__).resolve().parent / ".swap_chart_state.json"),
    ).strip()
    state_path = Path(state_file)

    # If message id not explicitly provided, try state file.
    if not message_id:
        message_id = load_message_id_from_state(state_path)

    username = os.environ.get("DISCORD_WEBHOOK_USERNAME", "").strip() or None
    avatar_url = os.environ.get("DISCORD_WEBHOOK_AVATAR_URL", "").strip() or None

    # Load swaps
    try:
        con = db_connect(db_path)
    except Exception as e:
        print(f"ERROR: cannot open db: {db_path} ({e})")
        return 2

    try:
        rows = fetch_last_swaps(con, last_n)
    except Exception as e:
        con.close()
        print(f"ERROR: query failed: {e}")
        return 3
    finally:
        try:
            con.close()
        except Exception:
            pass

    points = compute_points(rows)
    if len(points) < 2:
        msg = f"Not enough swap data to chart (need >=2 points). Found: {len(points)}"
        print(msg)
        # still post a small message (optional)
        if webhook_url:
            ok, info = discord_webhook_post_or_patch(
                webhook_url=webhook_url,
                message_id=message_id,
                png_bytes=b"",  # no file
                content=msg,
                username=username,
                avatar_url=avatar_url,
            )
            print(("OK: " if ok else "ERROR: ") + info)
        return 0

    # Prepare title + content
    last_ts = points[-1].ts
    last_price = points[-1].price_veco_per_hcc
    title = f"VECO/HCC (last {len(points)} swaps)"
    content = (
        f"**VECO/HCC price chart** (last {len(points)} swaps)\n"
        f"Last: **{last_price:.4f} VECO/HCC** • {fmt_utc(last_ts)}"
    )

    # Render image
    png = render_chart(points, title=title)

    # Post/patch to Discord
    if webhook_url:
        ok, info = discord_webhook_post_or_patch(
            webhook_url=webhook_url,
            message_id=message_id,
            png_bytes=png,
            content=content,
            username=username,
            avatar_url=avatar_url,
        )

        # If we tried to patch but the message is gone (404), create a new one.
        if (not ok) and message_id and ("HTTP 404" in info or "Unknown Message" in info):
            print("WARN: previous webhook message missing; posting a new one.")
            message_id = None
            ok, info = discord_webhook_post_or_patch(
                webhook_url=webhook_url,
                message_id=None,
                png_bytes=png,
                content=content,
                username=username,
                avatar_url=avatar_url,
            )

        print(("OK: " if ok else "ERROR: ") + info)

        # Persist message id if we created a new message
        if ok and info.startswith("posted_ok:"):
            new_mid = info.split("posted_ok:", 1)[1].strip()
            if new_mid:
                save_message_id_to_state(state_path, new_mid)
                print(f"Saved webhook message id to state: {state_path}")
    else:
        print("DISCORD_WEBHOOK_URL_CHART not set; wrote chart bytes only (not posted).")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())