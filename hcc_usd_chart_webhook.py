#!/usr/bin/env python3
"""
hcc_usd_chart_webhook.py

Displays the HCC/USD price for the last 24 hours as a time series.

Price calculation:
  spot_veco = (veco_reserve_sat / 1e8) / hcc_reserve_veco   # VECO per HCC
  spot_nns  = (nns_reserve_sat  / 1e8) / hcc_reserve_nns    # NNS per HCC
  veco_usd = VECO price from CoinPaprika (veco-veco)
  hcc_usd = spot_veco * veco_usd

This script:
  - reads amm_pool.id=1 (current pool state for HCC/VECO and HCC/NNS),
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
from typing import List, Optional, Tuple, Dict, Any
from urllib.parse import urlparse, parse_qsl, urlencode

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


def get_current_pool(con: sqlite3.Connection) -> Dict[str, int]:
    row = con.execute(
        "SELECT hcc_reserve_veco, veco_reserve_sat, hcc_reserve_nns, nns_reserve_sat FROM amm_pool WHERE id=1"
    ).fetchone()
    if not row:
        raise RuntimeError("AMM pool not initialized (amm_pool.id=1 missing)")
    return {
        "hcc_reserve_veco": int(row[0]),
        "veco_reserve_sat": int(row[1]),
        "hcc_reserve_nns": int(row[2]),
        "nns_reserve_sat": int(row[3]),
    }



def spot_from_veco_reserves(hcc_reserve_veco: int, veco_reserve_sat: int) -> Optional[float]:
    if hcc_reserve_veco <= 0 or veco_reserve_sat <= 0:
        return None
    return (veco_reserve_sat / VECO_SATS) / float(hcc_reserve_veco)


def spot_from_nns_reserves(hcc_reserve_nns: int, nns_reserve_sat: int) -> Optional[float]:
    if hcc_reserve_nns <= 0 or nns_reserve_sat <= 0:
        return None
    return (nns_reserve_sat / VECO_SATS) / float(hcc_reserve_nns)


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
    spot_veco: float      # VECO/HCC
    spot_nns: float       # NNS/HCC
    veco_usd: float       # USD per VECO
    hcc_usd: float        # USD per HCC (derived from VECO pool)


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
                        spot_veco=float(item.get("spot_veco", item.get("spot", 0.0))),
                        spot_nns=float(item.get("spot_nns", 0.0)),
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



def render_hcc_usd_chart(samples: List[Sample]) -> bytes:
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


def build_webhook_base_and_query(webhook_url: str) -> Tuple[str, Dict[str, str]]:
    parsed = urlparse((webhook_url or "").strip())
    base = f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    return base, query



def is_retryable_webhook_error(message: str) -> bool:
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



def request_with_rate_limit_retry(
    method: str,
    url: str,
    *,
    params: Optional[Dict[str, str]] = None,
    data: Optional[Dict[str, str]] = None,
    files: Optional[Dict[str, Tuple[str, bytes, str]]] = None,
    timeout: int = 30,
) -> requests.Response:
    last_response: Optional[requests.Response] = None
    for attempt in range(2):
        response = requests.request(method, url, params=params, data=data, files=files, timeout=timeout)
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
        raise RuntimeError("Discord request failed without response")
    return last_response


def discord_webhook_post_or_patch(
    webhook_url: str,
    message_id: Optional[str],
    files_payload: List[Tuple[str, bytes, str]],
    content: str,
    username: Optional[str] = None,
    avatar_url: Optional[str] = None,
) -> Tuple[bool, str]:
    if not webhook_url:
        return False, "DISCORD_WEBHOOK_URL_HCCUSD is empty"

    base_url, query = build_webhook_base_and_query(webhook_url)
    params = dict(query)
    params["wait"] = "true"
    payload: dict = {"content": content}

    # only on POST we can set username/avatar
    if not message_id:
        if username:
            payload["username"] = username
        if avatar_url:
            payload["avatar_url"] = avatar_url

    files = None
    if files_payload:
        payload["attachments"] = [
            {"id": idx, "filename": filename}
            for idx, (filename, _blob, _mime) in enumerate(files_payload)
        ]
        files = {
            f"files[{idx}]": (filename, blob, mime)
            for idx, (filename, blob, mime) in enumerate(files_payload)
        }

    data = {"payload_json": json.dumps(payload)}

    try:
        if message_id:
            url = base_url + f"/messages/{message_id}"
            print(f"Trying to edit HCC/USD webhook message_id={message_id}")
            r = request_with_rate_limit_retry("PATCH", url, params=params, data=data, files=files, timeout=30)
        else:
            url = base_url
            r = request_with_rate_limit_retry("POST", url, params=params, data=data, files=files, timeout=30)

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
        pool = get_current_pool(con)
    except Exception as e:
        con.close()
        print(f"ERROR: {e}")
        return 3
    finally:
        try:
            con.close()
        except Exception:
            pass

    spot_veco = spot_from_veco_reserves(pool["hcc_reserve_veco"], pool["veco_reserve_sat"])
    spot_nns = spot_from_nns_reserves(pool["hcc_reserve_nns"], pool["nns_reserve_sat"])
    if spot_veco is None:
        print("ERROR: invalid HCC/VECO pool reserves for spot computation.")
        return 4
    if spot_nns is None:
        print("ERROR: invalid HCC/NNS pool reserves for spot computation.")
        return 4

    # --- fetch VECO price ---
    try:
        veco_usd = fetch_veco_usd(paprika_url, timeout=paprika_timeout)
    except Exception as e:
        print(f"ERROR: could not fetch VECO price: {e}")
        return 5

    hcc_usd = spot_veco * veco_usd
    now_ts = int(time.time())

    print(
        f"Spot VECO: {spot_veco:.8f} VECO/HCC | "
        f"Spot NNS: {spot_nns:.8f} NNS/HCC | "
        f"VECO: {veco_usd:.8f} USD | HCC: {hcc_usd:.10f} USD"
    )

    # --- load samples, append new one, prune old ones ---
    samples = load_samples(state_path)
    samples.append(
        Sample(
            ts=now_ts,
            spot_veco=spot_veco,
            spot_nns=spot_nns,
            veco_usd=veco_usd,
            hcc_usd=hcc_usd,
        )
    )

    cutoff = now_ts - window_seconds
    samples = [s for s in samples if s.ts >= cutoff]

    save_samples(state_path, samples)

    if len(samples) < min_samples:
        msg = f"Not enough HCC/USD samples yet (have {len(samples)}, need {min_samples})."
        print(msg)
        if webhook_url:
            ok, info = discord_webhook_post_or_patch(
                webhook_url, message_id, [], msg, username=username, avatar_url=avatar_url
            )
            print(("OK: " if ok else "ERROR: ") + info)
        return 0

    # --- render chart ---
    png_usd = render_hcc_usd_chart(samples)

    # Discord text
    last = max(samples, key=lambda s: s.ts)
    window_hours = window_seconds / 3600.0
    content = (
        f"**HCC/USD price chart**\n"
        f"Window: last **{window_hours:.1f} h** • Samples: **{len(samples)}**\n"
        f"HCC/VECO: **{last.spot_veco:.6f} VECO per HCC**\n"
        f"VECO: **{last.veco_usd:.6f} USD**\n"
        f"HCC/USD: **{last.hcc_usd:.8f} USD**\n"
        f"Last update: {fmt_utc(last.ts)}"
    )

    if webhook_url:
        ok, info = discord_webhook_post_or_patch(
            webhook_url,
            message_id,
            [("hcc_usd_chart.png", png_usd, "image/png")],
            content,
            username=username,
            avatar_url=avatar_url,
        )

        # If the previous message was deleted → post a new one.
        # For temporary Discord/network failures, do not create duplicates.
        if (not ok) and message_id:
            if "HTTP 404" in info or "Unknown Message" in info:
                print("WARN: previous webhook message missing; posting a new one.")
                message_id = None
                ok, info = discord_webhook_post_or_patch(
                    webhook_url,
                    None,
                    [("hcc_usd_chart.png", png_usd, "image/png")],
                    content,
                    username=username,
                    avatar_url=avatar_url,
                )
            elif is_retryable_webhook_error(info):
                print("WARN: temporary Discord/webhook failure; skipping repost to avoid duplicates.")

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