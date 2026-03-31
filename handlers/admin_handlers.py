# handlers/admin_handlers.py - Admin panel for Harvest Kingdom

import os
import json
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from telegram.constants import ParseMode

from database.db import (
    get_db, get_user, update_user, parse_json_field,
    dump_json_field, log_admin_action, get_setting, set_setting
)
from game.engine import (
    add_to_inventory, remove_from_inventory, get_user_full,
    get_item_count
)
from game.data import (
    CROPS, ANIMALS, BUILDINGS, UPGRADE_TOOLS, EXPANSION_TOOLS,
    CLEARING_TOOLS, get_item_emoji, get_item_name
)

logger = logging.getLogger(__name__)

def get_admin_ids() -> list[int]:
    raw = os.getenv("ADMIN_IDS", "")
    return [int(x.strip()) for x in raw.split(",") if x.strip().isdigit()]

def is_admin(user_id: int) -> bool:
    return user_id in get_admin_ids()

def admin_only(func):
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        if not is_admin(user.id):
            if update.message:
                await update.message.reply_text("🚫 Admin only.")
            elif update.callback_query:
                await update.callback_query.answer("🚫 Admin only.", show_alert=True)
            return
        return await func(update, ctx)
    wrapper.__name__ = func.__name__
    return wrapper


def admin_main_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("👥 User Management", callback_data="adm_users"),
            InlineKeyboardButton("💰 Give Items", callback_data="adm_give"),
        ],
        [
            InlineKeyboardButton("⚙️ Game Settings", callback_data="adm_settings"),
            InlineKeyboardButton("📊 Stats", callback_data="adm_stats"),
        ],
        [
            InlineKeyboardButton("📢 Broadcast", callback_data="adm_broadcast"),
            InlineKeyboardButton("🗃️ Admin Logs", callback_data="adm_logs"),
        ],
        [
            InlineKeyboardButton("🌾 Manage Items DB", callback_data="adm_items"),
            InlineKeyboardButton("🏠 Close", callback_data="menu"),
        ],
    ])

def admin_settings_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔧 Toggle Maintenance", callback_data="adm_set_maintenance")],
        [InlineKeyboardButton("2x XP Event", callback_data="adm_set_double_xp")],
        [InlineKeyboardButton("2x Coins Event", callback_data="adm_set_double_coins")],
        [InlineKeyboardButton("✏️ Set Welcome Message", callback_data="adm_set_welcome")],
        [InlineKeyboardButton("📈 Set Drop Rate", callback_data="adm_set_droprate")],
        [InlineKeyboardButton("🏪 Set Max Market Price", callback_data="adm_set_maxprice")],
        [InlineKeyboardButton("⬅️ Back", callback_data="adm_panel")],
    ])


@admin_only
async def admin_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👑 **Admin Panel — Harvest Kingdom**\n\nSelect an option:",
        reply_markup=admin_main_keyboard(),
        parse_mode=ParseMode.MARKDOWN
    )

@admin_only
async def adm_panel_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text(
        "👑 **Admin Panel — Harvest Kingdom**\n\nSelect an option:",
        reply_markup=admin_main_keyboard(),
        parse_mode=ParseMode.MARKDOWN
    )


# ─── STATS ───────────────────────────────────────────────────────────────────

@admin_only
async def adm_stats_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    async with await get_db() as db:
        total_users = (await db.execute_fetchone("SELECT COUNT(*) as c FROM users"))["c"]
        total_harvests = (await db.execute_fetchone("SELECT SUM(total_harvests) as s FROM users"))["s"] or 0
        total_sales = (await db.execute_fetchone("SELECT SUM(total_sales) as s FROM users"))["s"] or 0
        total_market = (await db.execute_fetchone("SELECT COUNT(*) as c FROM market_listings"))["c"]
        max_level_user = await db.execute_fetchone("SELECT first_name, level, xp FROM users ORDER BY level DESC, xp DESC LIMIT 1")
        total_coins = (await db.execute_fetchone("SELECT SUM(coins) as s FROM users"))["s"] or 0
        active_orders = (await db.execute_fetchone("SELECT COUNT(*) as c FROM orders WHERE status='active'"))["c"]

    top = f"{max_level_user['first_name']} (Lv {max_level_user['level']})" if max_level_user else "N/A"
    maintenance = await get_setting("maintenance_mode", "0")
    double_xp = await get_setting("double_xp", "0")
    double_coins = await get_setting("double_coins", "0")
    drop_rate = await get_setting("bonus_drop_rate", "0.05")

    text = (
        f"📊 **Game Statistics**\n\n"
        f"👥 Total players: **{total_users}**\n"
        f"🌾 Total harvests: **{total_harvests:,}**\n"
        f"🚚 Total sales: **{total_sales:,}**\n"
        f"🏪 Market listings: **{total_market}**\n"
        f"📋 Active orders: **{active_orders}**\n"
        f"💰 Total coins in game: **{total_coins:,}**\n"
        f"🏆 Top player: **{top}**\n\n"
        f"**Active Events:**\n"
        f"🔧 Maintenance: {'ON' if maintenance=='1' else 'OFF'}\n"
        f"⭐ Double XP: {'ON' if double_xp=='1' else 'OFF'}\n"
        f"💰 Double Coins: {'ON' if double_coins=='1' else 'OFF'}\n"
        f"🎁 Drop Rate: {float(drop_rate)*100:.1f}%"
    )
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup([[
        InlineKeyboardButton("⬅️ Back", callback_data="adm_panel")
    ]]), parse_mode=ParseMode.MARKDOWN)


# ─── SETTINGS ────────────────────────────────────────────────────────────────

@admin_only
async def adm_settings_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    maintenance = await get_setting("maintenance_mode", "0")
    double_xp = await get_setting("double_xp", "0")
    double_coins = await get_setting("double_coins", "0")
    drop_rate = await get_setting("bonus_drop_rate", "0.05")

    text = (
        f"⚙️ **Game Settings**\n\n"
        f"🔧 Maintenance: {'🟢 ON' if maintenance=='1' else '🔴 OFF'}\n"
        f"⭐ Double XP: {'🟢 ON' if double_xp=='1' else '🔴 OFF'}\n"
        f"💰 Double Coins: {'🟢 ON' if double_coins=='1' else '🔴 OFF'}\n"
        f"🎁 Drop Rate: {float(drop_rate)*100:.1f}%\n"
    )
    await query.edit_message_text(text, reply_markup=admin_settings_keyboard(), parse_mode=ParseMode.MARKDOWN)

@admin_only
async def adm_toggle_setting(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    action = query.data

    if action == "adm_set_maintenance":
        cur = await get_setting("maintenance_mode", "0")
        new = "0" if cur == "1" else "1"
        await set_setting("maintenance_mode", new)
        status = "enabled" if new == "1" else "disabled"
        await query.answer(f"Maintenance {status}!", show_alert=True)

    elif action == "adm_set_double_xp":
        cur = await get_setting("double_xp", "0")
        new = "0" if cur == "1" else "1"
        await set_setting("double_xp", new)
        await query.answer(f"2x XP {'ON' if new=='1' else 'OFF'}!", show_alert=True)

    elif action == "adm_set_double_coins":
        cur = await get_setting("double_coins", "0")
        new = "0" if cur == "1" else "1"
        await set_setting("double_coins", new)
        await query.answer(f"2x Coins {'ON' if new=='1' else 'OFF'}!", show_alert=True)

    elif action == "adm_set_welcome":
        ctx.user_data["adm_action"] = "set_welcome"
        await query.edit_message_text(
            "✏️ Send the new welcome message text:\n(Send /cancel to abort)",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Cancel", callback_data="adm_settings")]]),
            parse_mode=ParseMode.MARKDOWN
        )
        return

    elif action == "adm_set_droprate":
        ctx.user_data["adm_action"] = "set_droprate"
        cur = await get_setting("bonus_drop_rate", "0.05")
        await query.edit_message_text(
            f"📈 Current drop rate: {float(cur)*100:.1f}%\n\nSend new rate as decimal (e.g. 0.05 = 5%):",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Cancel", callback_data="adm_settings")]]),
        )
        return

    elif action == "adm_set_maxprice":
        ctx.user_data["adm_action"] = "set_maxprice"
        cur = await get_setting("max_market_price", "9999")
        await query.edit_message_text(
            f"🏪 Current max price: {cur} coins\n\nSend new max price:",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Cancel", callback_data="adm_settings")]]),
        )
        return

    # Refresh settings page
    await adm_settings_callback(update, ctx)

@admin_only
async def adm_text_input(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    action = ctx.user_data.get("adm_action")
    if not action:
        return
    text = update.message.text.strip()

    if action == "set_welcome":
        await set_setting("welcome_message", text)
        await update.message.reply_text(f"✅ Welcome message updated!")
        ctx.user_data.pop("adm_action", None)

    elif action == "set_droprate":
        try:
            rate = float(text)
            if not 0 <= rate <= 1:
                raise ValueError
            await set_setting("bonus_drop_rate", str(rate))
            await update.message.reply_text(f"✅ Drop rate set to {rate*100:.1f}%")
        except ValueError:
            await update.message.reply_text("❌ Invalid. Send a decimal 0.0 to 1.0")
        ctx.user_data.pop("adm_action", None)

    elif action == "set_maxprice":
        try:
            price = int(text)
            await set_setting("max_market_price", str(price))
            await update.message.reply_text(f"✅ Max market price set to {price} coins")
        except ValueError:
            await update.message.reply_text("❌ Invalid number.")
        ctx.user_data.pop("adm_action", None)

    elif action == "give_item_qty":
        try:
            parts = text.split()
            target_id = int(ctx.user_data.get("adm_target_id"))
            item_key = ctx.user_data.get("adm_give_item")
            qty = int(parts[0])
            ok, msg = await add_to_inventory(target_id, item_key, qty)
            if ok:
                await log_admin_action(update.effective_user.id, "give_item", target_id, f"{item_key} x{qty}")
                await update.message.reply_text(f"✅ Gave {qty}x {get_item_name(item_key)} to user {target_id}")
            else:
                await update.message.reply_text(f"❌ Failed: {msg}")
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {e}")
        ctx.user_data.pop("adm_action", None)

    elif action == "set_coins":
        try:
            amount = int(text)
            target_id = int(ctx.user_data.get("adm_target_id"))
            await update_user(target_id, coins=amount)
            await log_admin_action(update.effective_user.id, "set_coins", target_id, str(amount))
            await update.message.reply_text(f"✅ Set coins to {amount} for user {target_id}")
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {e}")
        ctx.user_data.pop("adm_action", None)

    elif action == "set_level":
        try:
            level = int(text)
            target_id = int(ctx.user_data.get("adm_target_id"))
            from game.data import LEVEL_THRESHOLDS
            xp = LEVEL_THRESHOLDS[min(level-1, len(LEVEL_THRESHOLDS)-1)]
            await update_user(target_id, level=level, xp=xp)
            await log_admin_action(update.effective_user.id, "set_level", target_id, str(level))
            await update.message.reply_text(f"✅ Set level to {level} for user {target_id}")
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {e}")
        ctx.user_data.pop("adm_action", None)

    elif action == "set_gems":
        try:
            gems = int(text)
            target_id = int(ctx.user_data.get("adm_target_id"))
            await update_user(target_id, gems=gems)
            await log_admin_action(update.effective_user.id, "set_gems", target_id, str(gems))
            await update.message.reply_text(f"✅ Set gems to {gems} for user {target_id}")
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {e}")
        ctx.user_data.pop("adm_action", None)

    elif action == "broadcast_msg":
        msg_text = text
        async with await get_db() as db:
            rows = await db.execute_fetchall("SELECT user_id FROM users")
            user_ids = [r["user_id"] for r in rows]

        sent = 0
        failed = 0
        for uid in user_ids:
            try:
                await ctx.bot.send_message(uid, f"📢 **Admin Announcement**\n\n{msg_text}", parse_mode=ParseMode.MARKDOWN)
                sent += 1
            except Exception:
                failed += 1

        await log_admin_action(update.effective_user.id, "broadcast", None, msg_text[:100])
        await update.message.reply_text(f"📢 Broadcast sent to {sent} users. Failed: {failed}")
        ctx.user_data.pop("adm_action", None)

    elif action == "add_item_db":
        # Format: key,name,emoji,grow_time,sell_price,xp,level_req,seed_cost
        try:
            parts = text.split(",")
            if len(parts) != 8:
                raise ValueError("Need 8 comma-separated values")
            key, name, emoji, grow_time, sell_price, xp, level_req, seed_cost = [p.strip() for p in parts]
            CROPS[key] = {
                "name": name, "emoji": emoji,
                "grow_time": int(grow_time), "sell_price": int(sell_price),
                "xp": int(xp), "level_req": int(level_req), "seed_cost": int(seed_cost)
            }
            await update.message.reply_text(f"✅ Added crop: {emoji} {name} to database (runtime only - add to data.py to persist)")
        except Exception as e:
            await update.message.reply_text(f"❌ Format: key,name,emoji,grow_time,sell_price,xp,level_req,seed_cost\nError: {e}")
        ctx.user_data.pop("adm_action", None)


# ─── USER MANAGEMENT ─────────────────────────────────────────────────────────

@admin_only
async def adm_users_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    async with await get_db() as db:
        rows = await db.execute_fetchall(
            "SELECT user_id, first_name, username, level, coins, xp FROM users ORDER BY level DESC, xp DESC LIMIT 15"
        )
        users = [dict(r) for r in rows]

    buttons = []
    for u in users:
        uname = f"@{u['username']}" if u["username"] else f"ID:{u['user_id']}"
        buttons.append([InlineKeyboardButton(
            f"[Lv{u['level']}] {u['first_name']} {uname} — {u['coins']}💰",
            callback_data=f"adm_user_{u['user_id']}"
        )])
    buttons.append([InlineKeyboardButton("⬅️ Back", callback_data="adm_panel")])
    await query.edit_message_text("👥 **Top Players** (tap to manage):", reply_markup=InlineKeyboardMarkup(buttons), parse_mode=ParseMode.MARKDOWN)

@admin_only
async def adm_user_detail_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    target_id = int(query.data.split("_")[2])
    user = await get_user(target_id)
    if not user:
        await query.answer("User not found!", show_alert=True)
        return

    silo = parse_json_field(user["silo_items"])
    barn = parse_json_field(user["barn_items"])
    text = (
        f"👤 **User: {user['first_name']}**\n"
        f"🪪 ID: `{user['user_id']}`\n"
        f"👑 Level: {user['level']} | XP: {user['xp']}\n"
        f"💰 Coins: {user['coins']} | 💎 Gems: {user['gems']}\n"
        f"🌾 Harvests: {user['total_harvests']}\n"
        f"📦 Silo: {sum(silo.values())}/{user['silo_cap']}\n"
        f"🏚 Barn: {sum(barn.values())}/{user['barn_cap']}\n"
    )
    buttons = [
        [
            InlineKeyboardButton("💰 Set Coins", callback_data=f"adm_setcoins_{target_id}"),
            InlineKeyboardButton("💎 Set Gems", callback_data=f"adm_setgems_{target_id}"),
        ],
        [
            InlineKeyboardButton("👑 Set Level", callback_data=f"adm_setlevel_{target_id}"),
            InlineKeyboardButton("🎁 Give Item", callback_data=f"adm_giveitem_{target_id}"),
        ],
        [
            InlineKeyboardButton("🗑️ Reset User", callback_data=f"adm_resetuser_{target_id}"),
            InlineKeyboardButton("🚫 Ban/Unban", callback_data=f"adm_ban_{target_id}"),
        ],
        [InlineKeyboardButton("⬅️ Back", callback_data="adm_users")],
    ]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(buttons), parse_mode=ParseMode.MARKDOWN)

@admin_only
async def adm_setcoins_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    target_id = int(query.data.split("_")[2])
    ctx.user_data["adm_action"] = "set_coins"
    ctx.user_data["adm_target_id"] = target_id
    await query.edit_message_text(f"💰 Send new coin amount for user {target_id}:")

@admin_only
async def adm_setlevel_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    target_id = int(query.data.split("_")[2])
    ctx.user_data["adm_action"] = "set_level"
    ctx.user_data["adm_target_id"] = target_id
    await query.edit_message_text(f"👑 Send new level for user {target_id}:")

@admin_only
async def adm_setgems_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    target_id = int(query.data.split("_")[2])
    ctx.user_data["adm_action"] = "set_gems"
    ctx.user_data["adm_target_id"] = target_id
    await query.edit_message_text(f"💎 Send gem amount for user {target_id}:")

@admin_only
async def adm_giveitem_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    target_id = int(query.data.split("_")[2])
    ctx.user_data["adm_target_id"] = target_id

    all_keys = (
        list(CROPS.keys()) + list(UPGRADE_TOOLS.keys()) +
        list(EXPANSION_TOOLS.keys()) + list(CLEARING_TOOLS.keys())
    )
    buttons = []
    row = []
    for key in all_keys[:24]:
        emoji = get_item_emoji(key)
        row.append(InlineKeyboardButton(f"{emoji}{get_item_name(key)}", callback_data=f"adm_give2_{target_id}_{key}"))
        if len(row) == 3:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton("⬅️ Cancel", callback_data=f"adm_user_{target_id}")])
    await query.edit_message_text(f"🎁 Give item to user {target_id} — choose item:", reply_markup=InlineKeyboardMarkup(buttons))

@admin_only
async def adm_give2_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    parts = query.data.split("_")
    target_id = int(parts[2])
    item_key = "_".join(parts[3:])
    ctx.user_data["adm_action"] = "give_item_qty"
    ctx.user_data["adm_target_id"] = target_id
    ctx.user_data["adm_give_item"] = item_key
    emoji = get_item_emoji(item_key)
    name = get_item_name(item_key)
    await query.edit_message_text(f"🎁 Give {emoji} {name} to user {target_id}\nSend quantity:")

@admin_only
async def adm_resetuser_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    target_id = int(query.data.split("_")[2])
    await update_user(target_id, coins=500, gems=5, xp=0, level=1,
                      silo_items="{}", barn_items="{}", land_items="{}")
    await log_admin_action(query.from_user.id, "reset_user", target_id)
    await query.answer(f"✅ User {target_id} reset!", show_alert=True)


# ─── BROADCAST ────────────────────────────────────────────────────────────────

@admin_only
async def adm_broadcast_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    ctx.user_data["adm_action"] = "broadcast_msg"
    await query.edit_message_text(
        "📢 **Broadcast Message**\n\nSend the message to broadcast to ALL players:",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Cancel", callback_data="adm_panel")]]),
        parse_mode=ParseMode.MARKDOWN
    )


# ─── LOGS ────────────────────────────────────────────────────────────────────

@admin_only
async def adm_logs_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    async with await get_db() as db:
        rows = await db.execute_fetchall(
            "SELECT * FROM admin_logs ORDER BY created_at DESC LIMIT 20"
        )
        logs = [dict(r) for r in rows]

    if not logs:
        text = "📋 No admin actions logged yet."
    else:
        lines = ["📋 **Recent Admin Actions:**\n"]
        for log in logs:
            lines.append(f"• [{log['created_at'][:16]}] Admin {log['admin_id']} → {log['action']} on {log['target_id']}: {log['details']}")
        text = "\n".join(lines)

    await query.edit_message_text(
        text[:4000],
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="adm_panel")]]),
        parse_mode=ParseMode.MARKDOWN
    )


# ─── ITEMS DB ─────────────────────────────────────────────────────────────────

@admin_only
async def adm_items_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    lines = ["🌾 **Current Crops in DB:**\n"]
    for k, v in CROPS.items():
        lines.append(f"{v['emoji']} {v['name']} (key:`{k}`) Lv{v['level_req']} | {v['grow_time']}s | {v['sell_price']}💰")

    buttons = [
        [InlineKeyboardButton("➕ Add Crop (runtime)", callback_data="adm_addcrop")],
        [InlineKeyboardButton("⬅️ Back", callback_data="adm_panel")],
    ]
    await query.edit_message_text("\n".join(lines)[:4000], reply_markup=InlineKeyboardMarkup(buttons), parse_mode=ParseMode.MARKDOWN)

@admin_only
async def adm_addcrop_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    ctx.user_data["adm_action"] = "add_item_db"
    await query.edit_message_text(
        "➕ **Add Crop (runtime only)**\n\nSend in format:\n`key,name,emoji,grow_time_secs,sell_price,xp,level_req,seed_cost`\n\nExample:\n`mango,Mango,🥭,7200,200,12,14,160`",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Cancel", callback_data="adm_items")]]),
        parse_mode=ParseMode.MARKDOWN
    )


# ─── GIVE COMMANDS ────────────────────────────────────────────────────────────

@admin_only
async def adm_give_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text(
        "🎁 **Give Items to Player**\n\nUse command:\n`/give <user_id> <item_key> <qty>`\n\nExamples:\n"
        "`/give 123456789 wheat 50`\n`/give 123456789 bolt 10`\n`/give 123456789 axe 5`",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="adm_panel")]]),
        parse_mode=ParseMode.MARKDOWN
    )

@admin_only
async def give_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = ctx.args
    if len(args) < 3:
        await update.message.reply_text("Usage: `/give <user_id> <item_key> <qty>`", parse_mode=ParseMode.MARKDOWN)
        return
    try:
        target_id = int(args[0])
        item_key = args[1].lower()
        qty = int(args[2])
    except ValueError:
        await update.message.reply_text("❌ Invalid arguments.")
        return

    user = await get_user(target_id)
    if not user:
        await update.message.reply_text(f"❌ User {target_id} not found.")
        return

    ok, msg = await add_to_inventory(target_id, item_key, qty)
    if ok:
        await log_admin_action(update.effective_user.id, "give_item", target_id, f"{item_key}x{qty}")
        emoji = get_item_emoji(item_key)
        await update.message.reply_text(f"✅ Gave {qty}x {emoji} {get_item_name(item_key)} to {user['first_name']} (ID:{target_id})")
        try:
            await ctx.bot.send_message(target_id, f"🎁 Admin gave you {qty}x {emoji} {get_item_name(item_key)}!")
        except Exception:
            pass
    else:
        await update.message.reply_text(f"❌ {msg}")

@admin_only
async def givecoins_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = ctx.args
    if len(args) < 2:
        await update.message.reply_text("Usage: `/givecoins <user_id> <amount>`", parse_mode=ParseMode.MARKDOWN)
        return
    try:
        target_id = int(args[0])
        amount = int(args[1])
    except ValueError:
        await update.message.reply_text("❌ Invalid.")
        return

    user = await get_user(target_id)
    if not user:
        await update.message.reply_text(f"❌ User not found.")
        return

    await update_user(target_id, coins=user["coins"] + amount)
    await log_admin_action(update.effective_user.id, "give_coins", target_id, str(amount))
    await update.message.reply_text(f"✅ Gave {amount} coins to {user['first_name']}.")
    try:
        await ctx.bot.send_message(target_id, f"🎁 Admin gave you {amount} 💰 coins!")
    except Exception:
        pass
