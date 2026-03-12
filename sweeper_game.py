# sweeper_game.py
import os
import random
import time
from dataclasses import dataclass, asdict
from typing import Dict, Tuple, List, Set

import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv

from mining_game_bot import transfer_internal, get_user_balance, GAME_TREASURY_DISCORD_ID

# Load environment variables so DISCORD_TOKEN_SWEEPER, SWEEPER_* etc. are available
load_dotenv(override=True)

# ---- Game config ----
# Desktop preset: 5x5 board
DESKTOP_ROWS = 5
DESKTOP_COLS = 5
DESKTOP_MINES = 6

# Mobile-friendly preset: 5 rows x 4 columns
MOBILE_ROWS = 5
MOBILE_COLS = 4
MOBILE_MINES = 5


ENTRY_FEE_HCC = int(os.getenv("SWEEPER_ENTRY_FEE", "2"))

# Max active games per user (avoid spam)
MAX_ACTIVE_GAMES_PER_USER = 1

# Max total payout per game (for UX text + safety)

MAX_REWARD_HCC = int(os.getenv("SWEEPER_MAX_REWARD", "16"))

# Optional: restrict fast slash-command sync to a single guild (recommended for dev)
DEV_GUILD_ID = int(os.getenv("SWEEPER_DEV_GUILD_ID", "0") or "0")


@dataclass
class SweeperGame:
    user_id: int
    created_at: int
    status: str  # "running", "cashed_out", "lost"
    rows: int
    cols: int
    mine_positions: Set[Tuple[int, int]]  # (row, col)
    revealed: Set[Tuple[int, int]]
    safe_reveals: int  # number of non-mine cells successfully opened

    #@property
    #def potential_reward(self) -> int:
    #    n = int(self.safe_reveals)
        # base = n * (n + 1) // 2
        # base = n + (n - 1)
        # base = n
        # return min(MAX_REWARD_HCC, base)

    def is_mine(self, r: int, c: int) -> bool:
        return (r, c) in self.mine_positions

    @staticmethod
    def in_bounds_static(r: int, c: int, rows: int, cols: int) -> bool:
        return 0 <= r < rows and 0 <= c < cols

    def in_bounds(self, r: int, c: int) -> bool:
        return self.in_bounds_static(r, c, self.rows, self.cols)

    def adjacent_mines(self, r: int, c: int) -> int:
        cnt = 0
        for dr in (-1, 0, 1):
            for dc in (-1, 0, 1):
                if dr == 0 and dc == 0:
                    continue
                nr, nc = r + dr, c + dc
                if self.in_bounds(nr, nc) and self.is_mine(nr, nc):
                    cnt += 1
        return cnt


# In-memory game storage: key = (guild_id, user_id)
# For a single guild we just use key by user_id.
ACTIVE_GAMES: Dict[Tuple[int, int], SweeperGame] = {}


def new_game_for_user(user_id: int, rows: int, cols: int, num_mines: int) -> SweeperGame:
    # Randomly place num_mines unique mine positions in the given board
    cells = [(r, c) for r in range(rows) for c in range(cols)]
    mines = set(random.sample(cells, num_mines))

    return SweeperGame(
        user_id=int(user_id),
        created_at=int(time.time()),
        status="running",
        rows=int(rows),
        cols=int(cols),
        mine_positions=mines,
        revealed=set(),
        safe_reveals=0,
    )


class SweeperView(discord.ui.View):
    """Discord view with a 5x5 grid of buttons."""

    def __init__(self, game: SweeperGame, guild_id: int):
        super().__init__(timeout=600)  # 10 minutes
        self.game = game
        self.guild_id = guild_id
        self.build_buttons()

    def build_buttons(self):
        """Rebuild all buttons based on current game state."""
        self.clear_items()

        # Board based on per-game rows/cols (can be 5x5 or 5x4)
        for r in range(self.game.rows):
            for c in range(self.game.cols):
                custom_id = f"sweeper_{self.game.user_id}_{r}_{c}"
                if self.game.status != "running":
                    disabled = True
                else:
                    disabled = (r, c) in self.game.revealed

                label, style = self._cell_label_style(r, c)
                btn = discord.ui.Button(
                    label=label,
                    style=style,
                    custom_id=custom_id,
                    disabled=disabled,
                    row=r,
                )
                btn.callback = self._make_cell_callback(r, c)
                self.add_item(btn)

    def _cell_label_style(self, r: int, c: int) -> Tuple[str, discord.ButtonStyle]:
        """Return (label, style) for this cell based on game state."""
        if (r, c) not in self.game.revealed:
            # hidden cell
            if self.game.status == "running":
                return "⬜", discord.ButtonStyle.secondary
            else:
                # game ended, show bombs / numbers
                if self.game.is_mine(r, c):
                    return "💣", discord.ButtonStyle.danger
                else:
                    n = self.game.adjacent_mines(r, c)
                    return (str(n) if n > 0 else "·"), discord.ButtonStyle.secondary

        # revealed cell
        if self.game.is_mine(r, c):
            return "💥", discord.ButtonStyle.danger

        n = self.game.adjacent_mines(r, c)
        label = str(n) if n > 0 else "·"
        return label, discord.ButtonStyle.primary

    def _make_cell_callback(self, r: int, c: int):
        async def callback(interaction: discord.Interaction):
            # Only owner may interact
            if interaction.user.id != self.game.user_id:
                await interaction.response.send_message(
                    "This is not your Sweeper game!", ephemeral=True
                )
                return

            key = (interaction.guild_id or 0, self.game.user_id)
            game = ACTIVE_GAMES.get(key)
            if not game or game.status != "running":
                await interaction.response.send_message(
                    "This game is no longer active.", ephemeral=True
                )
                return

            # Reveal logic
            if (r, c) in game.revealed:
                await interaction.response.defer()
                return

            # --- First-click safety: ensure the very first click is never a mine ---
            if len(game.revealed) == 0 and game.safe_reveals == 0 and game.is_mine(r, c):
                # Move the mine at (r, c) to a different, non-mine cell
                all_cells = [(rr, cc) for rr in range(game.rows) for cc in range(game.cols)]
                candidates = [
                    cell
                    for cell in all_cells
                    if cell != (r, c) and cell not in game.mine_positions
                ]
                if candidates:
                    game.mine_positions.remove((r, c))
                    new_pos = random.choice(candidates)
                    game.mine_positions.add(new_pos)

            # After potential relocation, re-check if the clicked cell is a mine
            if game.is_mine(r, c):
                # Hit a mine: game over, no reward
                game.status = "lost"
                game.revealed.add((r, c))
                # Optionally reveal all cells so the board is visible
                for rr in range(game.rows):
                    for cc in range(game.cols):
                        game.revealed.add((rr, cc))
                self.game = game
                self.build_buttons()
                await interaction.response.edit_message(
                    content="💥 Boom! You hit a mine and lost all pending rewards.",
                    view=self
                )
                return

            # Safe cell
            game.revealed.add((r, c))
            game.safe_reveals += 1

            # Check if all safe cells have been revealed
            total_cells = game.rows * game.cols
            total_safe = total_cells - len(game.mine_positions)

            if game.safe_reveals >= total_safe:
                # Player has cleared all safe cells – they win and receive the final reward
                game.status = "won"
                reward = MAX_REWARD_HCC

                if reward > 0 and GAME_TREASURY_DISCORD_ID != 0:
                    ok = transfer_internal(
                        GAME_TREASURY_DISCORD_ID,
                        game.user_id,
                        reward,
                        note="Sweeper win",
                    )
                    if not ok:
                        # If payout fails, leave the game state as won but inform the player
                        self.game = game
                        # Reveal full board for clarity
                        for rr in range(game.rows):
                            for cc in range(game.cols):
                                game.revealed.add((rr, cc))
                        self.build_buttons()
                        await interaction.response.edit_message(
                            content=(
                                "✅ You cleared the board, but the payout failed "
                                "(treasury might be empty or busy). Please contact an admin."
                            ),
                            view=self,
                        )
                        return

                # Reveal full board and remove from active games
                for rr in range(game.rows):
                    for cc in range(game.cols):
                        game.revealed.add((rr, cc))
                self.game = game
                self.build_buttons()

                # Remove from ACTIVE_GAMES so the user can start a new game
                ACTIVE_GAMES.pop(key, None)

                await interaction.response.edit_message(
                    content=f"🏆 Perfect run! You cleared all safe cells and won **{reward} Ħ**. GG!",
                    view=self,
                )
                return

            # Otherwise: game continues
            self.game = game
            self.build_buttons()

            await interaction.response.edit_message(
                content=(
                    f"Safe! Current streak: {game.safe_reveals} safe reveals.\n"
                    "Clear all safe cells without hitting a mine to win the final reward."
                ),
                view=self,
            )

        return callback



class SweeperConfirmView(discord.ui.View):
    """Confirmation view before charging entry fee and starting a Sweeper game."""

    def __init__(self, interaction: discord.Interaction, rows: int, cols: int, num_mines: int):
        super().__init__(timeout=60)
        self.interaction = interaction
        self.user_id = interaction.user.id
        self.guild_id = interaction.guild_id or 0
        self.rows = int(rows)
        self.cols = int(cols)
        self.num_mines = int(num_mines)

        # Confirm button
        confirm_btn = discord.ui.Button(
            label=f"✅ Start Sweeper (pay {ENTRY_FEE_HCC} Ħ)",
            style=discord.ButtonStyle.success,
            custom_id="sweeper_confirm_start",
        )
        confirm_btn.callback = self._confirm_callback
        self.add_item(confirm_btn)

        # Cancel button
        cancel_btn = discord.ui.Button(
            label="❌ Cancel",
            style=discord.ButtonStyle.danger,
            custom_id="sweeper_confirm_cancel",
        )
        cancel_btn.callback = self._cancel_callback
        self.add_item(cancel_btn)

    async def _confirm_callback(self, interaction: discord.Interaction):
        # Only the original user may confirm
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(
                "This confirmation is not for you.", ephemeral=True
            )
            return

        # Check if treasury is configured
        if GAME_TREASURY_DISCORD_ID == 0:
            await interaction.response.edit_message(
                content="Game treasury is not configured. Sweeper is temporarily disabled.",
                view=None,
            )
            return

        key = (self.guild_id, self.user_id)
        existing = ACTIVE_GAMES.get(key)
        if existing and existing.status == "running":
            # User somehow already has an active game; show it instead
            view = SweeperView(existing, self.guild_id)
            await interaction.response.edit_message(
                content=(
                    "You already have a running Sweeper game.\n"
                    "Continue playing below."
                ),
                view=view,
            )
            return

        # Balance check and entry fee transfer
        try:
            bal = get_user_balance(self.user_id)
        except NotImplementedError:
            await interaction.response.edit_message(
                content="TipBot integration is not wired yet (get_user_balance missing).",
                view=None,
            )
            return

        if bal < ENTRY_FEE_HCC:
            await interaction.response.edit_message(
                content=(
                    f"Not enough Ħ to start Sweeper. "
                    f"Entry fee is {ENTRY_FEE_HCC} Ħ, your balance is {bal} Ħ."
                ),
                view=None,
            )
            return

        ok = transfer_internal(
            self.user_id,
            GAME_TREASURY_DISCORD_ID,
            ENTRY_FEE_HCC,
            note="Sweeper entry fee",
        )
        if not ok:
            await interaction.response.edit_message(
                content="Could not charge entry fee (maybe another transaction is pending).",
                view=None,
            )
            return

        # Create and store new game
        game = new_game_for_user(self.user_id, self.rows, self.cols, self.num_mines)
        ACTIVE_GAMES[key] = game
        view = SweeperView(game, self.guild_id)

        await interaction.response.edit_message(
            content=(
                "🎮 **HCC Sweeper started!**\n"
                f"- Board size: {self.rows}×{self.cols}\n"
                f"- Mines: {self.num_mines}\n"
                f"- Entry fee: {ENTRY_FEE_HCC} Ħ (paid)\n"
                f"- Maximum possible reward: {MAX_REWARD_HCC} Ħ\n\n"
                "Reveal safe cells one by one. If you clear **all** safe cells without hitting a mine, "
                "you win the final reward. Hitting a mine at any time ends the game with **no payout**."
            ),
            view=view,
        )

    async def _cancel_callback(self, interaction: discord.Interaction):
        # Only original user may cancel
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(
                "This confirmation is not for you.", ephemeral=True
            )
            return

        await interaction.response.edit_message(
            content="Sweeper game cancelled. No Ħ has been charged.",
            view=None,
        )
# ---- Standalone SweeperBot and main() entrypoint ----


# Register the sweeper command as a standalone app command
@app_commands.command(name="sweeper_mobile", description="Play HCC Minesweeper (mobile 5x4)")
async def sweeper_command(interaction: discord.Interaction):
    if interaction.guild_id is None:
        await interaction.response.send_message(
            "Sweeper can only be played in a server, not in DMs.",
            ephemeral=True,
        )
        return

    if GAME_TREASURY_DISCORD_ID == 0:
        await interaction.response.send_message(
            "Game treasury is not configured. Sweeper is temporarily disabled.",
            ephemeral=True,
        )
        return

    user_id = interaction.user.id
    key = (interaction.guild_id, user_id)

    existing = ACTIVE_GAMES.get(key)
    if existing and existing.status == "running":
        # User already has an active game; show current board instead
        view = SweeperView(existing, interaction.guild_id)
        await interaction.response.send_message(
            content=(
                "You already have a running Sweeper game.\n"
            ),
            view=view,
            ephemeral=True,
        )
        return

    # Ask for confirmation before charging the entry fee
    view = SweeperConfirmView(
        interaction,
        rows=MOBILE_ROWS,
        cols=MOBILE_COLS,
        num_mines=MOBILE_MINES,
    )
    await interaction.response.send_message(
        content=(
            "🎮 **Start HCC Sweeper?**\n"
            f"- Board size: {MOBILE_ROWS}×{MOBILE_COLS}\n"
            f"- Mines: {MOBILE_MINES}\n"
            f"- Entry fee: **{ENTRY_FEE_HCC} Ħ**\n"
            f"- Reward if you win: **{MAX_REWARD_HCC}**!\n\n"
            "Reveal safe cells one by one. If you clear **all** safe cells without hitting a mine, "
            "you receive the final reward. If you hit a mine, the game ends immediately with **no payout**."
        ),
        view=view,
        ephemeral=True,
    )


# Desktop sweeper command
@app_commands.command(name="sweeper_desktop", description="Play HCC Minesweeper (desktop 5x5)")
async def sweeper_desktop_command(interaction: discord.Interaction):
    if interaction.guild_id is None:
        await interaction.response.send_message(
            "Sweeper can only be played in a server, not in DMs.",
            ephemeral=True,
        )
        return

    if GAME_TREASURY_DISCORD_ID == 0:
        await interaction.response.send_message(
            "Game treasury is not configured. Sweeper is temporarily disabled.",
            ephemeral=True,
        )
        return

    user_id = interaction.user.id
    key = (interaction.guild_id, user_id)

    existing = ACTIVE_GAMES.get(key)
    if existing and existing.status == "running":
        view = SweeperView(existing, interaction.guild_id)
        await interaction.response.send_message(
            content=(
                "You already have a running Sweeper game.\n"
            ),
            view=view,
            ephemeral=True,
        )
        return

    view = SweeperConfirmView(
        interaction,
        rows=DESKTOP_ROWS,
        cols=DESKTOP_COLS,
        num_mines=DESKTOP_MINES,
    )
    await interaction.response.send_message(
        content=(
            "🎮 **Start HCC Sweeper (desktop mode)?**\n"
            f"- Board size: {DESKTOP_ROWS}×{DESKTOP_COLS}\n"
            f"- Mines: {DESKTOP_MINES}\n"
            f"- Entry fee: **{ENTRY_FEE_HCC} Ħ**\n"
            f"- Reward if you win: **{MAX_REWARD_HCC}**!\n\n"
            "Reveal safe cells one by one. If you clear **all** safe cells without hitting a mine, "
            "you receive the final reward. If you hit a mine, the game ends immediately with **no payout**."
        ),
        view=view,
        ephemeral=True,
    )


# ---- Standalone SweeperBot and main() entrypoint ----
class SweeperBot:
    """
    Standalone bot wrapper for HCC Sweeper.

    This creates its own discord.Bot instance, registers the Sweeper slash command
    and syncs the slash command tree on startup.
    """
    def __init__(self):
        intents = discord.Intents.default()
        intents.guilds = True

        self.bot = commands.Bot(command_prefix="!", intents=intents)

        @self.bot.event
        async def on_ready():
            # Debug: print DEV_GUILD_ID and command names at start of on_ready
            cmds = self.bot.tree.get_commands()
            print(f"[DEBUG] DEV_GUILD_ID: {DEV_GUILD_ID}")
            print(f"[DEBUG] App commands at on_ready: {[cmd.name for cmd in cmds]}")
            print(f"✅ SweeperBot logged in as {self.bot.user}")
            try:
                if DEV_GUILD_ID:
                    # Fast sync for a specific guild so /sweeper appears immediately there
                    guild = discord.Object(id=DEV_GUILD_ID)
                    # Copy all global commands to that guild and sync
                    self.bot.tree.copy_global_to(guild=guild)
                    await self.bot.tree.sync(guild=guild)
                    print(f"✅ Sweeper slash commands synced for guild {DEV_GUILD_ID}.")
                else:
                    # Fallback: global sync (may take up to an hour to propagate)
                    await self.bot.tree.sync()
                    print("✅ Sweeper global slash commands synced (may take a while to appear).")
            except Exception as e:
                print(f"⚠️ Failed to sync sweeper app commands: {e}")

        # Attach the sweeper slash commands to this bot's tree
        self.bot.tree.add_command(sweeper_command)
        self.bot.tree.add_command(sweeper_desktop_command)
        cmds = self.bot.tree.get_commands()
        print(f"[DEBUG] After add_command: {len(cmds)} app commands registered: {[cmd.name for cmd in cmds]}")


def main():
    """
    Entry point for running the Sweeper bot as a standalone process.
    """
    token = os.getenv("DISCORD_TOKEN_SWEEPER", "").strip()
    if not token:
        raise SystemExit("Missing DISCORD_TOKEN_SWEEPER in environment or .env")

    if GAME_TREASURY_DISCORD_ID == 0:
        print("⚠️ GAME_TREASURY_DISCORD_ID is 0 – sweeper payouts will fail or be disabled.")

    sweeper_bot = SweeperBot()
    print("✅ Starting Sweeper Bot...")
    sweeper_bot.bot.run(token)


if __name__ == "__main__":
    main()
