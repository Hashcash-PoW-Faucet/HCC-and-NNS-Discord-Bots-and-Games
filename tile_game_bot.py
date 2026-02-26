#!/usr/bin/env python3
import os
import json
import time
import random
import asyncio
from typing import Dict, Any, Optional, List

import discord
from discord.ext import commands
from dotenv import load_dotenv

# Reuse TipBot ledger + treasury from mining_game_bot
from mining_game_bot import transfer_internal, GAME_TREASURY_DISCORD_ID

# Load .env so TILE_* variables are available
load_dotenv(override=True)


# ==========================
# Config / constants
# ==========================

TILE_GAME_CHANNEL_ID = int((os.getenv("TILE_GAME_CHANNEL_ID", "0") or "0").strip())
STATE_FILE = os.getenv("TILE_GAME_STATE_FILE", "tile_game_state.json").strip() or "tile_game_state.json"

GAME_DURATION_SECONDS = int(os.getenv("TILE_GAME_DURATION_SECONDS", "86400"))  # default 24h
PLAYER_COOLDOWN_SECONDS = int(os.getenv("TILE_GAME_PLAYER_COOLDOWN_SECONDS", "1800"))  # default 30min

# Pause after full clear before spawning a new board
COMPLETION_COOLDOWN_SECONDS = int(os.getenv("TILE_GAME_COMPLETION_COOLDOWN_SECONDS", "7200"))  # default 2h pause after full clear
# ==========================
# State helpers
# ==========================


def can_start_new_game(state: Dict[str, Any], now_ts: Optional[int] = None) -> bool:
    """Return True if we are allowed to spawn a new board (no active completion cooldown)."""
    now_ts = int(now_ts or time.time())
    pause_until = int(state.get("completion_cooldown_until", 0) or 0)
    return pause_until <= now_ts


# Reward distribution:
# payout (HCC) : number of tiles
REWARD_DISTRIBUTION = {
    1: 6,
    2: 4,
    4: 3,
    8: 2,
    16: 1,
}

TOTAL_TILES = sum(REWARD_DISTRIBUTION.values())  # should be 16 for a 4x4 grid


# ==========================
# State helpers
# ==========================

def load_state() -> Dict[str, Any]:
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_state(state: Dict[str, Any]) -> None:
    try:
        tmp = f"{STATE_FILE}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
        os.replace(tmp, STATE_FILE)
    except Exception:
        # Do not crash the bot because of state IO issues
        pass


def build_new_game_state(channel_id: int) -> Dict[str, Any]:
    """
    Create a new game state with randomized tile rewards.
    Structure:
      current_game: {
        game_id, channel_id, message_id (set after send),
        created_at, tiles: [{index, amount, claimed_by, claimed_at}, ...]
      }
      user_cooldowns: {discord_id: ts}
      last_game_id
    """
    # Build reward list according to REWARD_DISTRIBUTION
    rewards: List[int] = []
    for amt, count in REWARD_DISTRIBUTION.items():
        rewards.extend([amt] * count)
    # Safety: ensure correct length
    rewards = rewards[:TOTAL_TILES]
    random.shuffle(rewards)

    now = int(time.time())
    state = load_state()
    last_id = int(state.get("last_game_id", 0) or 0) + 1

    tiles = []
    for idx, amt in enumerate(rewards):
        tiles.append({
            "index": idx,
            "amount": int(amt),
            "claimed_by": None,
            "claimed_at": None,
        })

    state["current_game"] = {
        "game_id": last_id,
        "channel_id": int(channel_id),
        "message_id": None,  # to be filled after sending the message
        "created_at": now,
        "tiles": tiles,
    }
    # Keep existing cooldowns; they apply across games
    if "user_cooldowns" not in state or not isinstance(state["user_cooldowns"], dict):
        state["user_cooldowns"] = {}
    state["last_game_id"] = last_id
    save_state(state)
    return state


def is_game_expired(game: Dict[str, Any], now_ts: Optional[int] = None) -> bool:
    """Return True if the board has exceeded the configured lifetime.
    If GAME_DURATION_SECONDS <= 0, time-based expiration is disabled.
    """
    if GAME_DURATION_SECONDS <= 0:
        return False
    now_ts = int(now_ts or time.time())
    start = int(game.get("created_at", now_ts))
    return (now_ts - start) >= int(GAME_DURATION_SECONDS)


def all_tiles_claimed(game: Dict[str, Any]) -> bool:
    tiles = game.get("tiles") or []
    for t in tiles:
        if t.get("claimed_by") is None:
            return False
    return True


def get_remaining_tiles(game: Dict[str, Any]) -> int:
    tiles = game.get("tiles") or []
    return sum(1 for t in tiles if t.get("claimed_by") is None)


def user_on_cooldown(state: Dict[str, Any], user_id: int, now_ts: Optional[int] = None) -> int:
    """
    Return remaining cooldown in seconds for this user. 0 if not on cooldown.
    """
    now_ts = int(now_ts or time.time())
    cds = state.get("user_cooldowns") or {}
    until = int(cds.get(str(user_id), 0) or 0)
    if until <= now_ts:
        return 0
    return until - now_ts


def set_user_cooldown(state: Dict[str, Any], user_id: int) -> None:
    now_ts = int(time.time())
    cds = state.get("user_cooldowns")
    if not isinstance(cds, dict):
        cds = {}
    cds[str(user_id)] = now_ts + int(PLAYER_COOLDOWN_SECONDS)
    state["user_cooldowns"] = cds
    save_state(state)


# ==========================
# Discord view / buttons
# ==========================

class TileButton(discord.ui.Button):
    def __init__(self, tile_index: int, label: str, disabled: bool, game_bot: "TileGameBot", row: int):
        # Use a neutral style; the "golden" look is conveyed via the label emoji.
        super().__init__(
            label=label,
            style=discord.ButtonStyle.secondary,
            custom_id=f"tile_{tile_index}",
            disabled=disabled,
            row=row,
        )
        self.tile_index = tile_index
        self.game_bot = game_bot

    async def callback(self, interaction: discord.Interaction):
        await self.game_bot.handle_tile_click(interaction, self.tile_index)


class TileGameView(discord.ui.View):
    def __init__(self, game_bot: "TileGameBot", game_state: Dict[str, Any]):
        super().__init__(timeout=None)
        self.game_bot = game_bot
        self.game_state = game_state
        self._build_buttons()

    def _build_buttons(self):
        """
        Build a 4x4 grid of buttons.
        Claimed tiles are disabled and show the revealed amount.
        Unclaimed tiles show a golden square-style emoji.
        """
        game = self.game_state.get("current_game") or {}
        tiles = game.get("tiles") or []
        # Ensure correct length
        tiles = tiles[:TOTAL_TILES]

        for idx, tile in enumerate(tiles):
            claimed_by = tile.get("claimed_by")
            amount = int(tile.get("amount", 0))
            row = idx // 4  # 0..3

            if claimed_by is None:
                # Not yet clicked – show a golden square-style emoji
                label = "🟨"
                disabled = False
            else:
                # Already revealed – keep the golden look and show the amount
                label = f"{amount} Ħ"
                disabled = True

            self.add_item(TileButton(idx, label, disabled, self.game_bot, row=row))


# ==========================
# Bot implementation
# ==========================

class TileGameBot:
    def __init__(self):
        intents = discord.Intents.default()
        intents.guilds = True
        # No message content / member intents needed

        self.bot = commands.Bot(command_prefix="!", intents=intents)
        self._watchdog_task: Optional[asyncio.Task] = None

        @self.bot.event
        async def on_ready():
            print(f"✅ TileGameBot logged in as {self.bot.user}")
            if TILE_GAME_CHANNEL_ID == 0:
                print("⚠️ TILE_GAME_CHANNEL_ID is 0 – tile game will not start.")
                return
            if GAME_TREASURY_DISCORD_ID == 0:
                print("⚠️ GAME_TREASURY_DISCORD_ID is 0 – payouts will be disabled / fail silently.")
            # Start watchdog loop once
            if self._watchdog_task is None or self._watchdog_task.done():
                self._watchdog_task = asyncio.create_task(self.watchdog_loop())

    async def create_new_game_message(self) -> None:
        """
        Create a new game (new tiles), post the message, and update state with the message ID.
        """
        if TILE_GAME_CHANNEL_ID == 0:
            print("⚠️ No TILE_GAME_CHANNEL_ID configured – cannot create tile game.")
            return

        channel = self.bot.get_channel(TILE_GAME_CHANNEL_ID)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(TILE_GAME_CHANNEL_ID)
            except Exception as e:
                print(f"❌ Failed to fetch tile game channel: {e}")
                return

        # Build fresh state and tiles
        state = build_new_game_state(channel.id)
        game = state.get("current_game") or {}
        game_id = game.get("game_id")

        title = f"🎰 HashCash Tile Game – Round #{game_id}"
        desc = (
            "Click a tile to reveal a random HCC reward.\n\n"
            "- 6 tiles: **1 Ħ**\n"
            "- 4 tiles: **2 Ħ**\n"
            "- 3 tiles: **4 Ħ**\n"
            "- 2 tiles: **8 Ħ**\n"
            "- 1 tile: **16 Ħ**\n\n"
            f"Each player has a **{PLAYER_COOLDOWN_SECONDS // 60} min** cooldown between clicks.\n"
            # f"This board expires after **{GAME_DURATION_SECONDS // 3600} hours** or when all tiles are gone."
        )

        embed = discord.Embed(title=title, description=desc)
        remaining = get_remaining_tiles(game)
        embed.add_field(name="Tiles remaining", value=f"{remaining} / {TOTAL_TILES}", inline=False)

        view = TileGameView(self, state)

        msg = await channel.send(embed=embed, view=view)

        # Update state with message ID
        state = load_state()
        if "current_game" not in state:
            # It is possible that in between someone reset state; recreate game wrapper
            state["current_game"] = game
        state["current_game"]["message_id"] = int(msg.id)
        state["current_game"]["channel_id"] = int(channel.id)
        save_state(state)

        print(f"✅ Created new tile game #{game_id} in channel {channel.id} (message {msg.id})")

    async def reset_game_if_needed(self) -> None:
        """\
        Periodically check whether the current game is finished and, if so, run a
        cooldown _without_ deleting the board message. After the cooldown, reuse
        the same message for the next round.
        """
        state = load_state()
        game = state.get("current_game")
        now = int(time.time())

        # No game stored yet: only start a new one if we are past any cooldown.
        if not game or not isinstance(game, dict):
            if not can_start_new_game(state, now_ts=now):
                return
            await self.create_new_game_message()
            return

        expired = is_game_expired(game, now_ts=now)
        completed = all_tiles_claimed(game)

        # If the board is still active (not expired, not completed), do nothing.
        if not expired and not completed:
            return

        channel_id = int(game.get("channel_id") or 0)
        message_id = int(game.get("message_id") or 0)
        if not channel_id or not message_id:
            return

        try:
            channel = self.bot.get_channel(channel_id) or await self.bot.fetch_channel(channel_id)
            msg = await channel.fetch_message(message_id)
        except Exception:
            # If we cannot fetch the message, bail out silently.
            return

        # If the board was completed, we want to keep it visible and show a cooldown.
        if completed:
            cooldown_until = int(state.get("completion_cooldown_until", 0) or 0)

            # First time we notice completion: start cooldown and update the embed
            if cooldown_until == 0 and COMPLETION_COOLDOWN_SECONDS > 0:
                cooldown_until = now + int(COMPLETION_COOLDOWN_SECONDS)
                state["completion_cooldown_until"] = cooldown_until
                save_state(state)

            # If cooldown is still running, update the message to show the next-round time.
            if cooldown_until > now:
                # Build an embed similar to the normal board, but with extra info.
                game_id = game.get("game_id")
                remaining = get_remaining_tiles(game)

                title = f"🎰 HashCash Tile Game – Round #{game_id}"
                desc = (
                    "Click a tile to reveal a random HCC reward.\n\n"
                    "- 6 tiles: **1 Ħ**\n"
                    "- 4 tiles: **2 Ħ**\n"
                    "- 3 tiles: **4 Ħ**\n"
                    "- 2 tiles: **8 Ħ**\n"
                    "- 1 tile: **16 Ħ**\n\n"
                    f"Each player has a **{PLAYER_COOLDOWN_SECONDS // 60} min** cooldown between clicks.\n"
                    f"This board has been fully cleared.\n"
                )

                # Discord timestamp helper: show relative and absolute time.
                ts_rel = f"<t:{cooldown_until}:R>"
                ts_abs = f"<t:{cooldown_until}:f>"
                desc += f"\n⏳ Next round starts {ts_rel} ({ts_abs})."

                embed = discord.Embed(title=title, description=desc)
                embed.add_field(
                    name="Tiles remaining",
                    value=f"{remaining} / {TOTAL_TILES}",
                    inline=False,
                )

                # Rebuild the view from state – all tiles are now disabled with amounts.
                state = load_state()
                view = TileGameView(self, state)
                try:
                    await msg.edit(embed=embed, view=view)
                except Exception:
                    pass
                return

            # Cooldown over: reset the board on the SAME message.
            if 0 < cooldown_until <= now and can_start_new_game(state, now_ts=now):
                # Build fresh state (new tiles, new game_id) but reuse channel/message IDs.
                new_state = build_new_game_state(channel_id)
                new_game = new_state.get("current_game") or {}
                new_game["message_id"] = message_id
                new_game["channel_id"] = channel_id
                new_state["current_game"] = new_game
                new_state["completion_cooldown_until"] = 0
                save_state(new_state)

                new_game_id = new_game.get("game_id")
                remaining = get_remaining_tiles(new_game)

                title = f"🎰 HashCash Tile Game – Round #{new_game_id}"
                desc = (
                    "Click a tile to reveal a random HCC reward.\n\n"
                    "- 6 tiles: **1 Ħ**\n"
                    "- 4 tiles: **2 Ħ**\n"
                    "- 3 tiles: **4 Ħ**\n"
                    "- 2 tiles: **8 Ħ**\n"
                    "- 1 tile: **16 Ħ**\n\n"
                    f"Each player has a **{PLAYER_COOLDOWN_SECONDS // 60} min** cooldown between clicks.\n"
                    # f"This board expires after **{GAME_DURATION_SECONDS // 3600} hours** or when all tiles are gone."
                )

                embed = discord.Embed(title=title, description=desc)
                embed.add_field(
                    name="Tiles remaining",
                    value=f"{remaining} / {TOTAL_TILES}",
                    inline=False,
                )

                view = TileGameView(self, new_state)
                try:
                    await msg.edit(embed=embed, view=view)
                except Exception as e:
                    print(f"❌ Failed to start new tile game on existing message: {e}")
                return

            # If we get here, there is no active cooldown configured; do nothing.
            return

        # If the board simply expired (time-based) without being completed, we do not
        # delete it anymore. We just leave it as-is and, if desired, a manual reset
        # can clear the state file.
        if expired and not completed:
            return

    async def watchdog_loop(self) -> None:
        """
        Periodically checks if the game has expired or is completed and rotates the board.
        """
        await self.bot.wait_until_ready()
        while not self.bot.is_closed():
            try:
                await self.reset_game_if_needed()
            except Exception as e:
                print(f"❌ TileGame watchdog error: {e}")
            # Check roughly once per minute
            await asyncio.sleep(60)

    async def handle_tile_click(self, interaction: discord.Interaction, tile_index: int):
        """
        Handle a click on a specific tile index.
        Enforces:
          - game is still active
          - tile not already claimed
          - user cooldown
          - treasury-based payout via transfer_internal
        """
        user = interaction.user
        user_id = int(user.id)
        now_ts = int(time.time())

        state = load_state()
        game = state.get("current_game")

        # Basic sanity checks
        if not game or not isinstance(game, dict):
            await interaction.response.send_message("⚠️ This game has expired. Please wait for the next round.", ephemeral=True)
            return

        # Check that this interaction is for the current board message
        msg_id = int(game.get("message_id") or 0)
        if interaction.message.id != msg_id:
            await interaction.response.send_message("⚠️ This board is no longer active.", ephemeral=True)
            return

        # Expired?
        if is_game_expired(game, now_ts):
            await interaction.response.send_message("⏳ This board has expired. A new one will start soon!", ephemeral=True)
            # Let watchdog handle rotation
            return

        # Cooldown check
        remaining_cd = user_on_cooldown(state, user_id, now_ts=now_ts)
        if remaining_cd > 0:
            mins = remaining_cd // 60
            secs = remaining_cd % 60
            await interaction.response.send_message(
                f"⏳ You are on cooldown. Please wait **{mins}m {secs}s** before clicking another tile.",
                ephemeral=True,
            )
            return

        tiles = game.get("tiles") or []
        if tile_index < 0 or tile_index >= len(tiles):
            await interaction.response.send_message("❌ Invalid tile.", ephemeral=True)
            return

        tile = tiles[tile_index]
        if tile.get("claimed_by") is not None:
            await interaction.response.send_message("⚠️ This tile was already claimed.", ephemeral=True)
            return

        amount = int(tile.get("amount", 0))
        if amount <= 0:
            # Safety, but should never happen
            await interaction.response.send_message("❌ This tile is empty. Please contact an admin.", ephemeral=True)
            return

        # Treasury payout via TipBot ledger
        if GAME_TREASURY_DISCORD_ID == 0:
            await interaction.response.send_message(
                "⚠️ Treasury is not configured. This game is currently not paying out.",
                ephemeral=True,
            )
            return

        ok = transfer_internal(GAME_TREASURY_DISCORD_ID, user_id, amount, note="tile game reward")
        if not ok:
            await interaction.response.send_message(
                "⚠️ Payout failed (treasury might be empty or busy). Please try again later.",
                ephemeral=True,
            )
            return

        # Mark tile as claimed + set cooldown
        tile["claimed_by"] = user_id
        tile["claimed_at"] = now_ts
        # Update back into state
        tiles[tile_index] = tile
        game["tiles"] = tiles
        state["current_game"] = game
        set_user_cooldown(state, user_id)  # also saves state

        # Rebuild embed + view and use the interaction response to edit the message in place
        await self.update_game_message(interaction, game, user, amount)

    async def update_game_message(
        self,
        interaction: discord.Interaction,
        game: Dict[str, Any],
        last_user: Optional[discord.User] = None,
        last_amount: Optional[int] = None,
    ) -> None:
        """
        Edit the current game message with updated tiles and remaining count, in-place using the interaction.
        Optionally show the last user/amount who flipped a tile.
        """
        game_id = game.get("game_id")
        remaining = get_remaining_tiles(game)

        title = f"🎰 HashCash Tile Game – Round #{game_id}"
        desc = (
            "Click a tile to reveal a random HCC reward.\n\n"
            "- 6 tiles: **1 Ħ**\n"
            "- 4 tiles: **2 Ħ**\n"
            "- 3 tiles: **4 Ħ**\n"
            "- 2 tiles: **8 Ħ**\n"
            "- 1 tile: **16 Ħ**\n\n"
            f"Each player has a **{PLAYER_COOLDOWN_SECONDS // 60} min** cooldown between clicks.\n"
            # f"This board expires after **{GAME_DURATION_SECONDS // 3600} hours** or when all tiles are gone."
        )
        embed = discord.Embed(title=title, description=desc)
        embed.add_field(name="Tiles remaining", value=f"{remaining} / {TOTAL_TILES}", inline=False)

        if last_user is not None and last_amount is not None:
            embed.add_field(
                name="Last flip",
                value=f"{last_user.mention} revealed **{last_amount} Ħ**!",
                inline=False,
            )

        state = load_state()
        view = TileGameView(self, state)

        try:
            await interaction.response.edit_message(embed=embed, view=view)
        except Exception as e:
            print(f"❌ Failed to edit tile game message: {e}")


def main():
    token = os.getenv("DISCORD_TOKEN_TILEGAME", "").strip()
    if not token:
        raise SystemExit("Missing DISCORD_TOKEN_TILEGAME in environment or .env")

    if TILE_GAME_CHANNEL_ID == 0:
        print("⚠️ TILE_GAME_CHANNEL_ID is 0 – no channel configured for the tile game.")
    if GAME_TREASURY_DISCORD_ID == 0:
        print("⚠️ GAME_TREASURY_DISCORD_ID is 0 – payouts will fail / be disabled.")

    bot = TileGameBot()
    print("✅ Starting Tile Game Bot...")
    bot.bot.run(token)


if __name__ == "__main__":
    main()