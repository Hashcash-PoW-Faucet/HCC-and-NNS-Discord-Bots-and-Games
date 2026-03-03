#!/usr/bin/env python3
"""
pool_spot_chart_webhook.py

Shows N recent spot price changes of the AMM pool (VECO/HCC).
The points are based ONLY on the pool spot price after each swap.
Intermediate states with identical spot prices (except for Epsilon) are filtered out.

Env:
  TIPBOT_DB=/path/to/tipbot.db
  SPOT_CHART_LAST_N_SWAPS=40       # how many last swaps for reconstruction
  MIN_SPOT_DELTA=0.000001          # minimal spot change to create a new point

  DISCORD_WEBHOOK_URL_CHART=...
  DISCORD_WEBHOOK_MESSAGE_ID=...   (optional)
  DISCORD_WEBHOOK_STATE_FILE=/path/.spot_chart_state.json (optional)
  DISCORD_WEBHOOK_USERNAME=...     (optional, nur bei POST)
  DISCORD_WEBHOOK_AVATAR_URL=...   (optional, nur bei POST)
"""

import os
import io
import json
import time
import sqlite3
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import List, Optional, Tuple

import requests
from dotenv import load_dotenv

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa

VECO_SATS = 100_000_000


# ------- helpers -------

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
    return max(lo, min(hi, v))


@dataclass
class SpotPoint:
    swap_id: int
    ts: int
    price_veco_per_hcc: float


def db_connect(path: str) -> sqlite3.Connection:
    con = sqlite3.connect(path, timeout=30)
    con.row_factory = sqlite3.Row
    return con


def get_current_pool(con: sqlite3.Connection) -> Tuple[int, int]:
    row = con.execute(
        "SELECT hcc_reserve, veco_reserve_sat FROM amm_pool WHERE id=1"
    ).fetchone()
    if not row:
        raise RuntimeError("AMM pool not initialized (amm_pool.id=1 missing)")
    return int(row[0]), int(row[1])


def fetch_last_swaps(con: sqlite3.Connection, limit: int) -> List[sqlite3.Row]:
    rows = con.execute(
        """
        SELECT id, ts, from_asset, to_asset, amount_in, amount_out, fee_amount
        FROM swap_log
        WHERE status='ok'
        ORDER BY id DESC
        LIMIT ?
        """,
        (int(limit),),
    ).fetchall()
    return list(rows)


def spot_from_reserves(hcc_reserve: int, veco_reserve_sat: int) -> Optional[float]:
    if hcc_reserve <= 0 or veco_reserve_sat <= 0:
        return None
    return (veco_reserve_sat / VECO_SATS) / float(hcc_reserve)


def invert_swap(
    hcc_reserve_after: int,
    veco_reserve_after: int,
    row: sqlite3.Row,
) -> Tuple[int, int]:
    """
    Reconstructs the pool reserves BEFORE this swap from the state AFTER the swap.
    Uses the same formulas as the bot:

    HCC -> VECO:
      new_hcc = old_hcc + (amount_in - fee_amount)
      new_veco = old_veco - amount_out

    VECO -> HCC:
      new_veco = old_veco + (amount_in - fee_amount)
      new_hcc = old_hcc - amount_out
    """
    fa = str(row["from_asset"] or "").upper()
    ta = str(row["to_asset"] or "").upper()
    ain = int(row["amount_in"] or 0)
    aout = int(row["amount_out"] or 0)
    fee = int(row["fee_amount"] or 0)

    h_after = int(hcc_reserve_after)
    v_after = int(veco_reserve_after)

    if ain <= 0 or aout <= 0:
        return h_after, v_after

    if fa == "HCC" and ta == "VECO":
        # new_h = old_h + (ain - fee)  => old_h = new_h - (ain - fee)
        # new_v = old_v - aout         => old_v = new_v + aout
        old_h = h_after - (ain - fee)
        old_v = v_after + aout
        return old_h, old_v

    if fa == "VECO" and ta == "HCC":
        # new_v = old_v + (ain - fee)  => old_v = new_v - (ain - fee)
        # new_h = old_h - aout         => old_h = new_h + aout
        old_v = v_after - (ain - fee)
        old_h = h_after + aout
        return old_h, old_v

    # Unknown pairs: do not change anything
    return h_after, v_after


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
        tmp.write_text(
            json.dumps({"message_id": str(message_id).strip()}, ensure_ascii=False),
            encoding="utf-8",
        )
        tmp.replace(state_path)
    except Exception:
        pass


def build_spot_series(
    con: sqlite3.Connection,
    last_n_swaps: int,
    min_delta: float,
) -> List[SpotPoint]:
    # 1) Current pool status
    h_now, v_now = get_current_pool(con)

    # 2) last N swaps (DESC = new -> old)
    rows_desc = fetch_last_swaps(con, last_n_swaps)
    if not rows_desc:
        return []

    spots_desc: List[SpotPoint] = []
    h_after, v_after = h_now, v_now

    for r in rows_desc:
        # Spot after this swap (current state h_after, v_after)
        spot = spot_from_reserves(h_after, v_after)
        if spot is not None:
            spots_desc.append(
                SpotPoint(
                    swap_id=int(r["id"]),
                    ts=int(r["ts"] or 0),
                    price_veco_per_hcc=float(spot),
                )
            )

        # Reconstruct pool state before this swap for the next iteration
        h_prev, v_prev = invert_swap(h_after, v_after, r)
        h_after, v_after = h_prev, v_prev

    # now we have S_K, S_{K-1}, ... DESC – for the chart, better chronologically
    spots_desc.sort(key=lambda p: p.swap_id)  # ascending by swap ID

    # 3) compress: only if Spot actually changes
    filtered: List[SpotPoint] = []
    last_spot: Optional[Decimal] = None
    delta = Decimal(str(min_delta))

    for p in spots_desc:
        cur = Decimal(str(p.price_veco_per_hcc))
        if last_spot is None or (cur - last_spot).copy_abs() > delta:
            filtered.append(p)
            last_spot = cur

    return filtered


def render_chart(points: List[SpotPoint], title: str) -> bytes:
    ys = [p.price_veco_per_hcc for p in points]
    xs = list(range(1, len(ys) + 1))

    fig = plt.figure(figsize=(7.2, 5.2), dpi=170, facecolor="#0f1117")
    ax = fig.add_subplot(111, facecolor="#0f1117")

    ax.step(xs, ys, where="post", linewidth=3.0, color="#4aa3ff", alpha=0.95)
    ax.scatter([xs[-1]], [ys[-1]], s=85, color="#4aa3ff", zorder=6)

    baseline = min(ys)
    ax.fill_between(xs, ys, baseline, step="post", color="#4aa3ff", alpha=0.10)

    ax.set_title(title, color="#eaeaea", pad=12, fontsize=16, fontweight="bold")
    ax.set_ylabel("VECO per HCC (pool spot)", color="#d6d6d6", fontsize=13)
    ax.set_xlabel("Spot change index", color="#d6d6d6", fontsize=13)

    ax.tick_params(axis="x", colors="#cfcfcf", labelsize=12)
    ax.tick_params(axis="y", colors="#cfcfcf", labelsize=12)

    ax.set_xticks(xs)
    ax.set_xlim(0.8, len(xs) + 0.6)

    ax.grid(True, axis="y", linestyle="-", linewidth=1.0, alpha=0.18)
    ax.grid(True, axis="x", linestyle="-", linewidth=0.8, alpha=0.10)

    # last price badge
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
    if not webhook_url:
        return False, "DISCORD_WEBHOOK_URL_CHART is empty"

    params = {"wait": "true"}
    payload: dict = {"content": content}

    if not message_id:
        if username:
            payload["username"] = username
        if avatar_url:
            payload["avatar_url"] = avatar_url

    files = None
    if png_bytes:
        payload["attachments"] = [{"id": 0, "filename": "spot_chart.png"}]
        files = {
            "files[0]": ("spot_chart.png", png_bytes, "image/png"),
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
    load_dotenv(Path(__file__).resolve().parent / ".env")

    db_path = os.environ.get("TIPBOT_DB", "tipbot.db").strip()
    last_n = clamp_int(
        int(os.environ.get("SPOT_CHART_LAST_N_SWAPS", "40")), 2, 500
    )
    min_delta = float(os.environ.get("MIN_SPOT_DELTA", "0.000001"))

    webhook_url = os.environ.get("DISCORD_WEBHOOK_URL_CHART", "").strip()
    message_id = os.environ.get("DISCORD_WEBHOOK_MESSAGE_ID", "").strip() or None

    state_file = os.environ.get(
        "DISCORD_WEBHOOK_STATE_FILE",
        str(Path(__file__).resolve().parent / ".spot_chart_state.json"),
    ).strip()
    state_path = Path(state_file)

    if not message_id:
        message_id = load_message_id_from_state(state_path)

    username = os.environ.get("DISCORD_WEBHOOK_USERNAME", "").strip() or None
    avatar_url = os.environ.get("DISCORD_WEBHOOK_AVATAR_URL", "").strip() or None

    try:
        con = db_connect(db_path)
    except Exception as e:
        print(f"ERROR: cannot open db: {db_path} ({e})")
        return 2

    try:
        points = build_spot_series(con, last_n_swaps=last_n, min_delta=min_delta)
    except Exception as e:
        con.close()
        print(f"ERROR: failed to build spot series: {e}")
        return 3
    finally:
        try:
            con.close()
        except Exception:
            pass

    if len(points) < 2:
        msg = f"Not enough spot data (need >=2 points). Found: {len(points)}"
        print(msg)
        if webhook_url:
            ok, info = discord_webhook_post_or_patch(
                webhook_url=webhook_url,
                message_id=message_id,
                png_bytes=b"",
                content=msg,
                username=username,
                avatar_url=avatar_url,
            )
            print(("OK: " if ok else "ERROR: ") + info)
        return 0

    last = points[-1]
    title = f"VECO/HCC pool spot (last {len(points)} changes)"
    content = (
        f"**Pool spot price** (VECO/HCC)\n"
        f"Points: **{len(points)}** (filtered from last {last_n} swaps)\n"
        f"Last: **{last.price_veco_per_hcc:.4f} VECO/HCC** • {fmt_utc(last.ts)}"
    )

    png = render_chart(points, title=title)

    if webhook_url:
        ok, info = discord_webhook_post_or_patch(
            webhook_url=webhook_url,
            message_id=message_id,
            png_bytes=png,
            content=content,
            username=username,
            avatar_url=avatar_url,
        )

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
        if ok and info.startswith("posted_ok:"):
            new_mid = info.split("posted_ok:", 1)[1].strip()
            if new_mid:
                save_message_id_to_state(state_path, new_mid)
                print(f"Saved webhook message id to state: {state_path}")
    else:
        print("DISCORD_WEBHOOK_URL_CHART not set; generated chart only (no post).")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())