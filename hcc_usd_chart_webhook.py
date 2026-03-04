#!/usr/bin/env python3
"""
hcc_usd_chart_webhook.py

Displays the HCC/USD price for the last 24 hours as a time series.

Price calculation:
  spot = (veco_reserve_sat / 1e8) / hcc_reserve   # VECO per HCC
  veco_usd = VECO price from CoinPaprika (veco-veco)
  hcc_usd = spot * veco_usd

This script:
  - reads amm_pool.id=1 (current pool state),
  - fetches the VECO price from CoinPaprika,
  - stores each sample in a JSON state file,
  - drops all samples older than WINDOW_SECONDS (default 24h),
  - renders a chart and sends it to a Discord webhook.

Environment variables:

  TIPBOT_DB=/path/to/tipbot.db

  # Window & sampling
  HCC_USD_WINDOW_SECONDS=86400        # 24h
  HCC_USD_MIN_SAMPLES=2               # min. points to plot

  # CoinPaprika
  COINPAPRIKA_URL=https://api.coinpaprika.com/v1/tickers/veco-veco
  # optional: timeout in seconds
  COINPAPRIKA_TIMEOUT=10

  # Discord webhook (separate webhook for HCC/USD)
  DISCORD_WEBHOOK_URL_HCCUSD=...
  DISCORD_WEBHOOK_MESSAGE_ID_HCCUSD=...       # optional (for PATCH)
  DISCORD_WEBHOOK_STATE_FILE_HCCUSD=/path/.hcc_usd_chart_state.json  # optional

  DISCORD_WEBHOOK_USERNAME_HCCUSD=HCC/USD     # optional (POST only)
  DISCORD_WEBHOOK_AVATAR_URL_HCCUSD=...       # optional (POST only)
"""

import io
import json
import os
import sqlite3
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

import requests
from dotenv import load_dotenv

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates  # noqa

VECO_SATS = 100_000_000


# ---------- helpers ----------

def clamp_int(v: int, lo: int, hi: int) -> int:
    try:
        v = int(v)
    except Exception:
        v = lo
    return max(lo, min(hi, v))


def fmt_utc(ts: int) -> str:
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(int(ts)))
    except Exception:
        return ""


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


def spot_from_reserves(hcc_reserve: int, veco_reserve_sat: int) -> Optional[float]:
    if hcc_reserve <= 0 or veco_reserve_sat <= 0:
        return None
    return (veco_reserve_sat / VECO_SATS) / float(hcc_reserve)


def fetch_veco_usd(url: str, timeout: int = 10) -> float:
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    # CoinPaprika: .quotes.USD.price
    try:
        return float(data["quotes"]["USD"]["price"])
    except Exception:
        raise RuntimeError("Unexpected CoinPaprika JSON format")


@dataclass
class Sample:
    ts: int               # Unix timestamp
    spot: float           # VECO/HCC
    veco_usd: float       # USD per VECO
    hcc_usd: float        # USD per HCC


def load_samples(path: Path) -> List[Sample]:
    if not path.exists():
        return []
    try:
        raw = path.read_text(encoding="utf-8")
        if not raw.strip():
            return []
        obj = json.loads(raw)
        arr = obj.get("samples", [])
        samples: List[Sample] = []
        for item in arr:
            try:
                samples.append(
                    Sample(
                        ts=int(item["ts"]),
                        spot=float(item["spot"]),
                        veco_usd=float(item["veco_usd"]),
                        hcc_usd=float(item["hcc_usd"]),
                    )
                )
            except Exception:
                continue
        return samples
    except Exception:
        return []


def save_samples(path: Path, samples: List[Sample]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)

        # Merge with existing JSON (if any) so we do not clobber message_id.
        existing: dict = {}
        if path.exists():
            try:
                raw = path.read_text(encoding="utf-8")
                if raw.strip():
                    existing = json.loads(raw)
            except Exception:
                existing = {}

        existing["samples"] = [asdict(s) for s in samples]

        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(existing, ensure_ascii=False), encoding="utf-8")
        tmp.replace(path)
    except Exception:
        # Best-effort only: silently ignore failures.
        pass


def render_chart(samples: List[Sample]) -> bytes:
    # sort by time
    samples = sorted(samples, key=lambda s: s.ts)
    xs = [datetime.fromtimestamp(s.ts, tz=timezone.utc) for s in samples]
    ys = [s.hcc_usd for s in samples]

    fig = plt.figure(figsize=(7.6, 4.8), dpi=170, facecolor="#0f1117")
    ax = fig.add_subplot(111, facecolor="#0f1117")

    ax.plot(xs, ys, "-", linewidth=2.5, color="#4aa3ff", alpha=0.95)
    ax.scatter([xs[-1]], [ys[-1]], s=80, color="#4aa3ff", zorder=6)

    baseline = min(ys)
    ax.fill_between(xs, ys, baseline, step="pre", color="#4aa3ff", alpha=0.12)

    ax.set_title("HCC/USD",
                 color="#eaeaea", fontsize=15, fontweight="bold", pad=10)
    ax.set_ylabel("HCC price in USD", color="#d6d6d6", fontsize=12)
    ax.set_xlabel("Time (UTC)", color="#d6d6d6", fontsize=12)

    # X axis: 24h window, reasonable ticks
    locator = mdates.AutoDateLocator(minticks=4, maxticks=8)
    formatter = mdates.DateFormatter("%H:%M")
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(formatter)
    fig.autofmt_xdate(rotation=30, ha="right")

    ax.tick_params(axis="x", colors="#cfcfcf", labelsize=10)
    ax.tick_params(axis="y", colors="#cfcfcf", labelsize=10)

    ax.grid(True, axis="y", linestyle="-", linewidth=1.0, alpha=0.18)
    ax.grid(True, axis="x", linestyle=":", linewidth=0.8, alpha=0.10)

    # Range with some padding
    y_min, y_max = min(ys), max(ys)
    span = y_max - y_min
    pad = span * 0.25 if span > 0 else max(1e-6, y_max * 0.10)
    ax.set_ylim(y_min - pad, y_max + pad)

    # Badge with last price
    last_y = ys[-1]
    ax.axhline(last_y, color="#4aa3ff", alpha=0.20, linewidth=1.0)
    ax.annotate(
        f"{last_y:.8f} USD",
        xy=(xs[-1], last_y),
        xytext=(12, 0),
        textcoords="offset points",
        va="center",
        color="#0f1117",
        fontsize=10,
        bbox=dict(boxstyle="round,pad=0.3", fc="#4aa3ff", ec="none", alpha=0.96),
        zorder=10,
    )

    fig.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    return buf.getvalue()


def load_message_id_from_state(state_path: Path) -> Optional[str]:
    try:
        if not state_path.exists():
            return None
        raw = state_path.read_text(encoding="utf-8").strip()
        if not raw:
            return None
        obj = json.loads(raw)
        mid = str(obj.get("message_id") or "").strip()
        return mid or None
    except Exception:
        return None


def save_message_id_to_state(state_path: Path, message_id: str) -> None:
    try:
        state_path.parent.mkdir(parents=True, exist_ok=True)

        # Merge with existing JSON (if any) so we do not clobber samples.
        existing: dict = {}
        if state_path.exists():
            try:
                raw = state_path.read_text(encoding="utf-8")
                if raw.strip():
                    existing = json.loads(raw)
            except Exception:
                existing = {}

        existing["message_id"] = str(message_id).strip()

        tmp = state_path.with_suffix(state_path.suffix + ".tmp")
        tmp.write_text(
            json.dumps(existing, ensure_ascii=False),
            encoding="utf-8",
        )
        tmp.replace(state_path)
    except Exception:
        # Best-effort only.
        pass


def discord_webhook_post_or_patch(
    webhook_url: str,
    message_id: Optional[str],
    png_bytes: bytes,
    content: str,
    username: Optional[str] = None,
    avatar_url: Optional[str] = None,
) -> Tuple[bool, str]:
    if not webhook_url:
        return False, "DISCORD_WEBHOOK_URL_HCCUSD is empty"

    params = {"wait": "true"}
    payload: dict = {"content": content}

    # only on POST we can set username/avatar
    if not message_id:
        if username:
            payload["username"] = username
        if avatar_url:
            payload["avatar_url"] = avatar_url

    files = None
    if png_bytes:
        payload["attachments"] = [{"id": 0, "filename": "hcc_usd_chart.png"}]
        files = {
            "files[0]": ("hcc_usd_chart.png", png_bytes, "image/png"),
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
            return False, f"Discord HTTP {r.status_code}: {txt[:400]}"

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
    # load .env from project directory
    load_dotenv(Path(__file__).resolve().parent / ".env")

    db_path = os.environ.get("TIPBOT_DB", "tipbot.db").strip()
    window_seconds = int(os.environ.get("HCC_USD_WINDOW_SECONDS", "86400"))
    min_samples = clamp_int(os.environ.get("HCC_USD_MIN_SAMPLES", "2"), 2, 1000)

    paprika_url = os.environ.get(
        "COINPAPRIKA_URL", "https://api.coinpaprika.com/v1/tickers/veco-veco"
    ).strip()
    paprika_timeout = int(os.environ.get("COINPAPRIKA_TIMEOUT", "10"))

    webhook_url = os.environ.get("DISCORD_WEBHOOK_URL_HCCUSD", "").strip()
    message_id = os.environ.get("DISCORD_WEBHOOK_MESSAGE_ID_HCCUSD", "").strip() or None

    state_file = os.environ.get(
        "DISCORD_WEBHOOK_STATE_FILE_HCCUSD",
        str(Path(__file__).resolve().parent / ".hcc_usd_chart_state.json"),
    ).strip()
    state_path = Path(state_file)

    if not message_id:
        message_id = load_message_id_from_state(state_path)

    username = os.environ.get("DISCORD_WEBHOOK_USERNAME_HCCUSD", "").strip() or None
    avatar_url = os.environ.get("DISCORD_WEBHOOK_AVATAR_URL_HCCUSD", "").strip() or None

    # --- load current spot from DB ---
    try:
        con = db_connect(db_path)
    except Exception as e:
        print(f"ERROR: cannot open db: {db_path} ({e})")
        return 2

    try:
        hcc_res, veco_res_sat = get_current_pool(con)
    except Exception as e:
        con.close()
        print(f"ERROR: {e}")
        return 3
    finally:
        try:
            con.close()
        except Exception:
            pass

    spot = spot_from_reserves(hcc_res, veco_res_sat)
    if spot is None:
        print("ERROR: invalid pool reserves for spot computation.")
        return 4

    # --- fetch VECO price ---
    try:
        veco_usd = fetch_veco_usd(paprika_url, timeout=paprika_timeout)
    except Exception as e:
        print(f"ERROR: could not fetch VECO price: {e}")
        return 5

    hcc_usd = spot * veco_usd
    now_ts = int(time.time())

    print(f"Spot: {spot:.8f} VECO/HCC | VECO: {veco_usd:.8f} USD | HCC: {hcc_usd:.10f} USD")

    # --- load samples, append new one, prune old ones ---
    samples = load_samples(state_path)
    samples.append(Sample(ts=now_ts, spot=spot, veco_usd=veco_usd, hcc_usd=hcc_usd))

    cutoff = now_ts - window_seconds
    samples = [s for s in samples if s.ts >= cutoff]

    save_samples(state_path, samples)

    if len(samples) < min_samples:
        msg = f"Not enough HCC/USD samples yet (have {len(samples)}, need {min_samples})."
        print(msg)
        if webhook_url:
            ok, info = discord_webhook_post_or_patch(
                webhook_url, message_id, b"", msg, username=username, avatar_url=avatar_url
            )
            print(("OK: " if ok else "ERROR: ") + info)
        return 0

    # --- render chart ---
    png = render_chart(samples)

    # Discord text
    last = max(samples, key=lambda s: s.ts)
    window_hours = window_seconds / 3600.0
    content = (
        f"**HCC/USD price** (via pool spot × VECO/USDT from CoinPaprika)\n"
        f"Window: last **{window_hours:.1f} h** • Samples: **{len(samples)}**\n"
        f"Spot: **{last.spot:.6f} VECO/HCC**\n"
        f"VECO: **{last.veco_usd:.6f} USD** • HCC: **{last.hcc_usd:.8f} USD**\n"
        f"Last update: {fmt_utc(last.ts)}"
    )

    if webhook_url:
        ok, info = discord_webhook_post_or_patch(
            webhook_url,
            message_id,
            png,
            content,
            username=username,
            avatar_url=avatar_url,
        )

        # If the previous message was deleted → post a new one
        if (not ok) and message_id and ("HTTP 404" in info or "Unknown Message" in info):
            print("WARN: previous webhook message missing; posting a new one.")
            message_id = None
            ok, info = discord_webhook_post_or_patch(
                webhook_url,
                None,
                png,
                content,
                username=username,
                avatar_url=avatar_url,
            )

        print(("OK: " if ok else "ERROR: ") + info)
        if ok and info.startswith("posted_ok:"):
            new_mid = info.split("posted_ok:", 1)[1].strip()
            if new_mid:
                save_message_id_to_state(state_path, new_mid)
                print(f"Saved webhook message id: {state_path}")
    else:
        print("DISCORD_WEBHOOK_URL_HCCUSD not set; generated chart only (no post).")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())