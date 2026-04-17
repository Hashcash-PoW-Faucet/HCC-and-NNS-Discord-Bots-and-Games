#!/usr/bin/env python3
import json
import os
import sys
import tempfile
from decimal import Decimal, ROUND_DOWN
from pathlib import Path

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


RPC_USER = os.environ.get("NNS_RPC_USER", "").strip()
RPC_PASSWORD = os.environ.get("NNS_RPC_PASSWORD", "").strip()
RPC_HOST = os.environ.get("NNS_RPC_URL", "127.0.0.1").strip()
#RPC_PORT = os.environ.get("NNS_RPC_PORT", "48931").strip()

NNS_STAKING_BLOCK_TIME_SECONDS = int(os.environ.get("NNS_STAKING_BLOCK_TIME_SECONDS", "180"))
NNS_STAKING_APR_FACTOR = Decimal(os.environ.get("NNS_STAKING_APR_FACTOR", "0.75").strip() or "0.75")
NNS_STAKING_APR_CACHE_FILE = Path(
    os.environ.get("NNS_STAKING_APR_CACHE_FILE", "nns_staking_apr.json").strip()
)

TIMEOUT = int(os.environ.get("NNS_HTTP_TIMEOUT", "20"))


def rpc_request(method: str, params=None):
    url = f"{RPC_HOST}"
    payload = {
        "jsonrpc": "1.0",
        "id": "nns-apr-cache-updater",
        "method": method,
        "params": params or [],
    }
    r = requests.post(
        url,
        json=payload,
        auth=(RPC_USER, RPC_PASSWORD),
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    data = r.json()
    if data.get("error"):
        raise RuntimeError(f"RPC error in {method}: {data['error']}")
    return data["result"]


def clamp_apr(apr: Decimal) -> Decimal:
    if apr < Decimal("0"):
        return Decimal("0")
    if apr > Decimal("1000"):
        return Decimal("1000")
    return apr


def main() -> int:
    if not RPC_USER or not RPC_PASSWORD:
        print("Missing NNS_RPC_USER or NNS_RPC_PASSWORD", file=sys.stderr)
        return 1

    try:
        mining = rpc_request("getmininginfo")
        blockchain = rpc_request("getblockchaininfo")

        block_value_sat = Decimal(str(mining.get("blockvalue", 0) or 0))
        money_supply = Decimal(str(blockchain.get("moneysupply", 0) or 0))
        block_time_seconds = max(1, int(NNS_STAKING_BLOCK_TIME_SECONDS))
        apr_factor = NNS_STAKING_APR_FACTOR

        if apr_factor < Decimal("0"):
            apr_factor = Decimal("0")
        elif apr_factor > Decimal("1"):
            apr_factor = Decimal("1")

        if block_value_sat <= 0:
            raise RuntimeError("blockvalue is zero or invalid")
        if money_supply <= 0:
            raise RuntimeError("moneysupply is zero or invalid")

        # getmininginfo.blockvalue is in satoshis
        block_reward_coins = block_value_sat / Decimal("100000000")
        blocks_per_year = Decimal("31536000") / Decimal(block_time_seconds)

        calculated_apr = (block_reward_coins * blocks_per_year / money_supply) * Decimal("100")
        effective_apr = clamp_apr(calculated_apr * apr_factor)

        payload = {
            "apr_percent": float(effective_apr),
            "updated_at": int(__import__("time").time()),
            "apr_factor": float(apr_factor),
            "block_time_seconds": block_time_seconds,
            "blockvalue_sat": int(block_value_sat),
            "block_reward_coins": float(block_reward_coins),
            "moneysupply": float(money_supply),
            "calculated_apr_percent": float(calculated_apr),
        }

        NNS_STAKING_APR_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)

        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=str(NNS_STAKING_APR_CACHE_FILE.parent),
            delete=False,
        ) as tmp:
            json.dump(payload, tmp, ensure_ascii=False, indent=2)
            tmp.write("\n")
            tmp_path = Path(tmp.name)

        os.replace(tmp_path, NNS_STAKING_APR_CACHE_FILE)

        print(
            f"APR cache updated: {NNS_STAKING_APR_CACHE_FILE} | "
            f"base APR={calculated_apr:.6f}% | effective APR={effective_apr:.6f}%"
        )
        return 0

    except Exception as e:
        print(f"APR cache update failed: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())