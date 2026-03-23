#!/usr/bin/env python3
import os
import json
import time
import asyncio
from typing import Dict, Any, Optional, List

import aiohttp
import discord
from discord import app_commands
from dotenv import load_dotenv

load_dotenv()

DISCORD_TOKEN = os.environ.get("AIRDROP_DISCORD_TOKEN", "").strip()

NNS_RPC_URL = os.environ.get("NNS_RPC_URL", "http://127.0.0.1:48931/").strip()
NNS_RPC_USER = os.environ.get("NNS_RPC_USER", "").strip()
NNS_RPC_PASSWORD = os.environ.get("NNS_RPC_PASSWORD", "").strip()

AIRDROP_JSON = os.environ.get("AIRDROP_JSON", "airdrop_registrations.json").strip()
AIRDROP_DURATION_SECONDS = int(os.environ.get("AIRDROP_DURATION_SECONDS", "129600"))

# Optional: fixed start timestamp.
# If empty, the bot creates the window on first start.
AIRDROP_START_TS = os.environ.get("AIRDROP_START_TS", "").strip()


GUILD_ID = os.environ.get("GUILD_ID", "").strip()
ANNOUNCE_CHANNEL_ID = os.environ.get("ANNOUNCE_CHANNEL_ID", "").strip()
AIRDROP_BATCH_SIZE = int(os.environ.get("AIRDROP_BATCH_SIZE", "996"))


def now_ts() -> int:
    return int(time.time())


def load_state() -> Dict[str, Any]:
    if not os.path.exists(AIRDROP_JSON):
        start_ts = int(AIRDROP_START_TS) if AIRDROP_START_TS else now_ts()
        end_ts = start_ts + AIRDROP_DURATION_SECONDS
        state = {
            "airdrop_start_ts": start_ts,
            "airdrop_end_ts": end_ts,
            "registrations": {}
        }
        save_state(state)
        return state

    try:
        with open(AIRDROP_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
        if "registrations" not in data or not isinstance(data["registrations"], dict):
            data["registrations"] = {}
        if "airdrop_end_ts" not in data:
            start_ts = int(AIRDROP_START_TS) if AIRDROP_START_TS else now_ts()
            data["airdrop_start_ts"] = start_ts
            data["airdrop_end_ts"] = start_ts + AIRDROP_DURATION_SECONDS
        return data
    except Exception:
        start_ts = int(AIRDROP_START_TS) if AIRDROP_START_TS else now_ts()
        state = {
            "airdrop_start_ts": start_ts,
            "airdrop_end_ts": start_ts + AIRDROP_DURATION_SECONDS,
            "registrations": {}
        }
        save_state(state)
        return state


def save_state(state: Dict[str, Any]) -> None:
    tmp = AIRDROP_JSON + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, AIRDROP_JSON)


def is_admin(interaction: discord.Interaction) -> bool:
    perms = getattr(interaction.user, "guild_permissions", None)
    if not perms:
        return False
    return bool(getattr(perms, "administrator", False) or getattr(perms, "manage_guild", False))


def get_announce_channel(bot_client: discord.Client) -> Optional[discord.abc.Messageable]:
    if not ANNOUNCE_CHANNEL_ID:
        return None
    try:
        ch = bot_client.get_channel(int(ANNOUNCE_CHANNEL_ID))
        return ch
    except Exception:
        return None


async def nns_rpc_call(method: str, params: Optional[list] = None) -> Any:
    if not NNS_RPC_URL or not NNS_RPC_USER or not NNS_RPC_PASSWORD:
        raise RuntimeError("NNS RPC not configured")

    payload = {
        "jsonrpc": "1.0",
        "id": "airdropbot",
        "method": method,
        "params": params or [],
    }

    auth = aiohttp.BasicAuth(NNS_RPC_USER, NNS_RPC_PASSWORD)

    async with aiohttp.ClientSession(auth=auth) as session:
        async with session.post(NNS_RPC_URL, json=payload, timeout=20) as r:
            txt = await r.text()
            if r.status != 200:
                raise RuntimeError(f"RPC HTTP {r.status}: {txt}")
            data = json.loads(txt)
            if data.get("error"):
                raise RuntimeError(f"RPC error: {data['error']}")
            return data.get("result")


async def is_valid_nns_address(address: str) -> bool:
    address = (address or "").strip()
    if not address:
        print("[validateaddress] empty address", flush=True)
        return False

    try:
        result = await nns_rpc_call("validateaddress", [address])
        print(f"[validateaddress] address={address} result={result}", flush=True)
        if isinstance(result, dict):
            return bool(result.get("isvalid", False))
        print(f"[validateaddress] unexpected result type for address={address}: {type(result).__name__}", flush=True)
    except Exception as e:
        print(f"[validateaddress] address={address} error={e}", flush=True)

    return False


class AirdropBot(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)
        self.file_lock = asyncio.Lock()

    async def setup_hook(self):
        load_state()
        if GUILD_ID:
            guild = discord.Object(id=int(GUILD_ID))
            self.tree.copy_global_to(guild=guild)
            await self.tree.sync(guild=guild)
        else:
            await self.tree.sync()


bot = AirdropBot()


@bot.tree.command(name="register_airdrop", description="Register your NNS address for the airdrop.")
@app_commands.describe(address="Your 996-Coin address")
async def register_airdrop(interaction: discord.Interaction, address: str):
    await interaction.response.defer(ephemeral=True, thinking=True)

    address = (address or "").strip()

    async with bot.file_lock:
        state = load_state()
        end_ts = int(state["airdrop_end_ts"])
        regs = state["registrations"]

        if now_ts() > end_ts:
            await interaction.followup.send(
                f"The registration window is closed.\nEnded: <t:{end_ts}:F>",
                ephemeral=True,
            )
            return

        user_id = str(interaction.user.id)

    try:
        valid = await is_valid_nns_address(address)
    except Exception as e:
        await interaction.followup.send(
            f"Could not verify the address: `{e}`",
            ephemeral=True,
        )
        return

    if not valid:
        await interaction.followup.send(
            "This is not a valid 996-Coin address.",
            ephemeral=True,
        )
        return

    async with bot.file_lock:
        state = load_state()
        end_ts = int(state["airdrop_end_ts"])
        regs = state["registrations"]
        user_id = str(interaction.user.id)

        if now_ts() > end_ts:
            await interaction.followup.send(
                f"The registration window is closed.\nEnded: <t:{end_ts}:F>",
                ephemeral=True,
            )
            return

        old = regs.get(user_id)
        old_address = None
        old_registered_at = None
        if isinstance(old, dict):
            old_address = str(old.get("address") or "").strip() or None
            if old.get("registered_at") is not None:
                try:
                    old_registered_at = int(old.get("registered_at"))
                except Exception:
                    old_registered_at = None

        regs[user_id] = {
            "username": str(interaction.user),
            "address": address,
            "registered_at": now_ts(),
        }
        save_state(state)

    if old_address:
        old_line = f"Previous address: `{old_address}`"
        if old_registered_at:
            old_line += f" (registered <t:{old_registered_at}:F>)"
        await interaction.followup.send(
            f"Registration updated ✅\n"
            f"Stored address: `{address}`\n"
            f"{old_line}\n"
            f"Registration closes: <t:{end_ts}:R>",
            ephemeral=True,
        )
    else:
        await interaction.followup.send(
            f"Registration successful ✅\n"
            f"Stored address: `{address}`\n"
            f"Registration closes: <t:{end_ts}:R>",
            ephemeral=True,
        )


@bot.tree.command(name="airdrop_status", description="Show airdrop registration status.")
async def airdrop_status(interaction: discord.Interaction):
    state = load_state()
    end_ts = int(state["airdrop_end_ts"])
    count = len(state["registrations"])

    if now_ts() <= end_ts:
        msg = (
            f"Airdrop registration is **open**.\n"
            f"Ends: <t:{end_ts}:F>\n"
            f"Time left: <t:{end_ts}:R>\n"
            f"Registrations: **{count}**"
        )
    else:
        msg = (
            f"Airdrop registration is **closed**.\n"
            f"Ended: <t:{end_ts}:F>\n"
            f"Registrations: **{count}**"
        )

    await interaction.response.send_message(msg, ephemeral=True)


# ---- New airdrop finalization helpers and command ----

async def send_airdrop_multisend(addresses: List[str], amount: str) -> str:
    if not addresses:
        raise RuntimeError("No addresses to send to")

    outputs = {addr: float(amount) for addr in addresses}
    txid = await nns_rpc_call("sendmany", ["", outputs])
    if not isinstance(txid, str) or not txid.strip():
        raise RuntimeError("sendmany returned invalid txid")
    return txid.strip()


@bot.tree.command(name="finalize_airdrop", description="(Admin) Send the airdrop to all registered NNS addresses and announce it.")
@app_commands.describe(amount="Amount of NNS to send to EACH registered address (e.g. 996)")
async def finalize_airdrop(interaction: discord.Interaction, amount: str):
    await interaction.response.defer(ephemeral=True, thinking=True)

    if not is_admin(interaction):
        await interaction.followup.send("Admin only.", ephemeral=True)
        return

    try:
        amt = float((amount or "").strip())
        if amt <= 0:
            raise ValueError("amount must be > 0")
    except Exception as e:
        await interaction.followup.send(f"Invalid amount: `{e}`", ephemeral=True)
        return

    async with bot.file_lock:
        state = load_state()
        regs = state.get("registrations", {})
        if not isinstance(regs, dict) or not regs:
            await interaction.followup.send("No registrations found.", ephemeral=True)
            return

        if state.get("airdrop_sent"):
            old_txids = state.get("airdrop_txids") or []
            await interaction.followup.send(
                f"Airdrop was already marked as sent. TXIDs: `{', '.join(old_txids)}`",
                ephemeral=True,
            )
            return

        addresses = []
        seen = set()
        for item in regs.values():
            if not isinstance(item, dict):
                continue
            addr = str(item.get("address") or "").strip()
            if addr and addr not in seen:
                seen.add(addr)
                addresses.append(addr)

    if not addresses:
        await interaction.followup.send("No valid addresses found in registrations.", ephemeral=True)
        return

    txids: List[str] = []
    try:
        for i in range(0, len(addresses), AIRDROP_BATCH_SIZE):
            batch = addresses[i:i + AIRDROP_BATCH_SIZE]
            txid = await send_airdrop_multisend(batch, amount)
            txids.append(txid)
    except Exception as e:
        await interaction.followup.send(f"Airdrop send failed ❌ `{e}`", ephemeral=True)
        return

    async with bot.file_lock:
        state = load_state()
        state["airdrop_sent"] = True
        state["airdrop_sent_at"] = now_ts()
        state["airdrop_amount_each"] = str(amount)
        state["airdrop_txids"] = txids
        save_state(state)

    announce_text = (
        f"🎉 **NNS airdrop completed**\n"
        f"Recipients: **{len(addresses)}**\n"
        f"Amount per address: **{amount} NNS**\n"
        f"TXID(s):\n" + "\n".join(f"`{t}`" for t in txids)
    )

    ch = get_announce_channel(bot)
    if ch is not None:
        try:
            await ch.send(announce_text)
        except Exception:
            pass

    await interaction.followup.send(
        f"Airdrop sent ✅\nRecipients: **{len(addresses)}**\nAmount each: **{amount} NNS**\n"
        f"TXID(s):\n" + "\n".join(f"`{t}`" for t in txids),
        ephemeral=True,
    )


def main():
    if not DISCORD_TOKEN:
        raise SystemExit("Missing AIRDROP_DISCORD_TOKEN")
    bot.run(DISCORD_TOKEN)


if __name__ == "__main__":
    main()