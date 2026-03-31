# handlers/main_handlers.py - Core handlers for Harvest Kingdom

import json
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from telegram.constants import ParseMode

from database.db import get_or_create_user, get_setting
from game.engine import (
    get_plots, get_animal_pens, get_user_buildings, get_orders,
    plant_crop, harvest_crop, harvest_all,
    buy_animal, collect_animal,
    buy_building, start_production, collect_production,
    ensure_orders, fulfill_order,
    get_market_listings, buy_from_market, list_item_on_market, remove_market_listing,
    get_obstacles, clear_obstacle,
    upgrade_silo, upgrade_barn, expand_farm, expand_animal_pens,
    sell_item, claim_daily, get_user_full, get_item_count
)
from utils.keyboards import (
    main_menu_keyboard, farm_keyboard, plant_keyboard, animals_keyboard,
    buy_animal_keyboard, factories_keyboard, factory_detail_keyboard,
    storage_keyboard, storage_items_keyboard, sell_keyboard,
    orders_keyboard, market_keyboard, land_keyboard, back_to_menu
)
from utils.formatters import (
    fmt_farm, fmt_animals, fmt_storage, fmt_factories,
    fmt_orders, fmt_market, fmt_profile, fmt_help
)
from database.db import parse_json_field

logger = logging.getLogger(__name__)

# Safe edit/send helpers
async def safe_edit(query, text: str, keyboard=None, parse_mode=ParseMode.MARKDOWN):
    try:
        await query.edit_message_text(
            text, reply_markup=keyboard, parse_mode=parse_mode,
            disable_web_page_preview=True
        )
    except Exception:
        try:
            await query.message.reply_text(
                text, reply_markup=keyboard, parse_mode=parse_mode,
                disable_web_page_preview=True
            )
        except Exception as e:
            logger.error(f"safe_edit failed: {e}")

async def safe_send(update: Update, text: str, keyboard=None):
    try:
        await update.message.reply_text(
            text, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN,
            disable_web_page_preview=True
        )
    except Exception as e:
        logger.error(f"safe_send failed: {e}")


# ─── START / MENU ─────────────────────────────────────────────────────────────

async def start_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    maintenance = await get_setting("maintenance_mode", "0")
    
    # Check maintenance (skip for admin)
    import os
    admin_ids = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()]
    if maintenance == "1" and user.id not in admin_ids:
        await update.message.reply_text("🔧 Game is under maintenance. Check back soon!")
        return

    db_user = await get_or_create_user(user.id, user.username, user.first_name)
    welcome = await get_setting("welcome_message", "Welcome to Harvest Kingdom! 🌾👑")

    text = (
        f"{welcome}\n\n"
        f"👋 Hello, **{db_user['first_name']}**!\n"
        f"👑 Level {db_user['level']}  💰 {db_user['coins']:,} coins\n\n"
        f"What would you like to do?"
    )
    await safe_send(update, text, main_menu_keyboard())

async def menu_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    db_user = await get_or_create_user(user.id, user.username, user.first_name)

    text = (
        f"🏠 **Main Menu**\n"
        f"👑 Level {db_user['level']}  💰 {db_user['coins']:,} coins  💎 {db_user['gems']}\n\n"
        f"What would you like to do?"
    )
    await safe_edit(query, text, main_menu_keyboard())


# ─── FARM ─────────────────────────────────────────────────────────────────────

async def farm_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    db_user = await get_or_create_user(user.id, user.username, user.first_name)
    plots = await get_plots(user.id)
    text = fmt_farm(db_user, plots)
    await safe_edit(query, text, farm_keyboard(plots, db_user["level"]))

async def farm_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db_user = await get_or_create_user(user.id, user.username, user.first_name)
    plots = await get_plots(user.id)
    text = fmt_farm(db_user, plots)
    await safe_send(update, text, farm_keyboard(plots, db_user["level"]))

async def plot_plant_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    slot = int(query.data.split("_")[2])
    user = query.from_user
    db_user = await get_or_create_user(user.id, user.username, user.first_name)
    await safe_edit(query, f"🌱 **Choose a crop for Plot {slot+1}:**\n\n(Price shown is seed cost)", plant_keyboard(db_user["level"], slot))

async def plant_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    parts = query.data.split("_")
    slot = int(parts[1])
    crop_key = "_".join(parts[2:])
    user = query.from_user
    db_user = await get_or_create_user(user.id, user.username, user.first_name)
    ok, msg = await plant_crop(user.id, slot, crop_key)
    if ok:
        plots = await get_plots(user.id)
        db_user = await get_user_full(user.id)
        await safe_edit(query, msg + "\n\n" + fmt_farm(db_user, plots), farm_keyboard(plots, db_user["level"]))
    else:
        await query.answer(msg, show_alert=True)

async def plot_harvest_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    slot = int(query.data.split("_")[2])
    user = query.from_user
    await get_or_create_user(user.id, user.username, user.first_name)
    ok, msg = await harvest_crop(user.id, slot)
    if ok:
        db_user = await get_user_full(user.id)
        plots = await get_plots(user.id)
        await safe_edit(query, msg + "\n\n" + fmt_farm(db_user, plots), farm_keyboard(plots, db_user["level"]))
    else:
        await query.answer(msg, show_alert=True)

async def harvest_all_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    await get_or_create_user(user.id, user.username, user.first_name)
    count, failed, _ = await harvest_all(user.id)
    db_user = await get_user_full(user.id)
    plots = await get_plots(user.id)
    if count > 0:
        msg = f"✅ Harvested {count} crop(s)!"
        if failed:
            msg += f" ({failed} failed, storage may be full)"
    else:
        msg = "⏳ No crops ready to harvest yet."
    await safe_edit(query, msg + "\n\n" + fmt_farm(db_user, plots), farm_keyboard(plots, db_user["level"]))

async def expand_farm_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    ok, msg = await expand_farm(user.id)
    await query.answer(msg, show_alert=True)
    if ok:
        db_user = await get_user_full(user.id)
        plots = await get_plots(user.id)
        await safe_edit(query, fmt_farm(db_user, plots), farm_keyboard(plots, db_user["level"]))


# ─── ANIMALS ──────────────────────────────────────────────────────────────────

async def animals_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    db_user = await get_or_create_user(user.id, user.username, user.first_name)
    pens = await get_animal_pens(user.id)
    text = fmt_animals(db_user, pens)
    await safe_edit(query, text, animals_keyboard(pens, db_user["level"]))

async def pen_buy_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    slot = int(query.data.split("_")[2])
    user = query.from_user
    db_user = await get_or_create_user(user.id, user.username, user.first_name)
    await safe_edit(query, f"🐾 **Choose an animal for Pen {slot+1}:**", buy_animal_keyboard(db_user["level"], slot))

async def buyanimal_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    parts = query.data.split("_")
    slot = int(parts[1])
    animal_key = "_".join(parts[2:])
    user = query.from_user
    await get_or_create_user(user.id, user.username, user.first_name)
    ok, msg = await buy_animal(user.id, slot, animal_key)
    if ok:
        db_user = await get_user_full(user.id)
        pens = await get_animal_pens(user.id)
        await safe_edit(query, msg + "\n\n" + fmt_animals(db_user, pens), animals_keyboard(pens, db_user["level"]))
    else:
        await query.answer(msg, show_alert=True)

async def pen_collect_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    slot = int(query.data.split("_")[2])
    user = query.from_user
    ok, msg = await collect_animal(user.id, slot)
    if ok:
        db_user = await get_user_full(user.id)
        pens = await get_animal_pens(user.id)
        await safe_edit(query, msg + "\n\n" + fmt_animals(db_user, pens), animals_keyboard(pens, db_user["level"]))
    else:
        await query.answer(msg, show_alert=True)

async def expand_pens_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    ok, msg = await expand_animal_pens(user.id)
    await query.answer(msg, show_alert=True)
    if ok:
        db_user = await get_user_full(user.id)
        pens = await get_animal_pens(user.id)
        await safe_edit(query, fmt_animals(db_user, pens), animals_keyboard(pens, db_user["level"]))


# ─── FACTORIES ────────────────────────────────────────────────────────────────

async def factories_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    db_user = await get_or_create_user(user.id, user.username, user.first_name)
    buildings = await get_user_buildings(user.id)
    text = fmt_factories(db_user, buildings)
    await safe_edit(query, text, factories_keyboard(buildings, db_user["level"]))

async def buy_building_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    building_key = "_".join(query.data.split("_")[2:])
    user = query.from_user
    ok, msg = await buy_building(user.id, building_key)
    if ok:
        db_user = await get_user_full(user.id)
        buildings = await get_user_buildings(user.id)
        await safe_edit(query, msg + "\n\n" + fmt_factories(db_user, buildings), factories_keyboard(buildings, db_user["level"]))
    else:
        await query.answer(msg, show_alert=True)

async def factory_detail_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    building_key = "_".join(query.data.split("_")[1:])
    user = query.from_user
    db_user = await get_or_create_user(user.id, user.username, user.first_name)
    buildings = await get_user_buildings(user.id)
    slots = [b for b in buildings if b["building"] == building_key]

    from game.data import BUILDINGS
    bld = BUILDINGS.get(building_key, {})
    text = f"{bld.get('emoji','🏭')} **{bld.get('name','Factory')}**\n\nChoose a recipe to produce:"
    await safe_edit(query, text, factory_detail_keyboard(building_key, slots))

async def produce_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    parts = query.data.split("_")
    building_key = parts[1]
    recipe_key = "_".join(parts[2:])
    user = query.from_user
    ok, msg = await start_production(user.id, building_key, recipe_key)
    if ok:
        buildings = await get_user_buildings(user.id)
        slots = [b for b in buildings if b["building"] == building_key]
        await safe_edit(query, msg, factory_detail_keyboard(building_key, slots))
    else:
        await query.answer(msg, show_alert=True)

async def collect_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    parts = query.data.split("_")
    building_key = parts[1]
    slot = int(parts[2])
    user = query.from_user
    ok, msg = await collect_production(user.id, building_key, slot)
    if ok:
        buildings = await get_user_buildings(user.id)
        bld_slots = [b for b in buildings if b["building"] == building_key]
        await safe_edit(query, msg, factory_detail_keyboard(building_key, bld_slots))
    else:
        await query.answer(msg, show_alert=True)


# ─── STORAGE ──────────────────────────────────────────────────────────────────

async def storage_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    db_user = await get_or_create_user(user.id, user.username, user.first_name)
    silo = parse_json_field(db_user["silo_items"])
    barn = parse_json_field(db_user["barn_items"])
    text = (
        f"📦 **Storage Overview**\n\n"
        f"🌾 Silo (Lv{db_user['silo_level']}): {sum(silo.values())}/{db_user['silo_cap']}\n"
        f"🏚 Barn (Lv{db_user['barn_level']}): {sum(barn.values())}/{db_user['barn_cap']}\n\n"
        f"Select storage to view items:"
    )
    await safe_edit(query, text, storage_keyboard())

async def storage_silo_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    db_user = await get_user_full(user.id)
    text = fmt_storage(db_user, "silo")
    items = parse_json_field(db_user["silo_items"])
    await safe_edit(query, text, storage_items_keyboard(items, "silo"))

async def storage_barn_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    db_user = await get_user_full(user.id)
    text = fmt_storage(db_user, "barn")
    items = parse_json_field(db_user["barn_items"])
    await safe_edit(query, text, storage_items_keyboard(items, "barn"))

async def storage_page_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    parts = query.data.split("_")
    storage_type = parts[1]
    page = int(parts[3])
    user = query.from_user
    db_user = await get_user_full(user.id)
    if storage_type == "silo":
        items = parse_json_field(db_user["silo_items"])
        text = fmt_storage(db_user, "silo")
    else:
        items = parse_json_field(db_user["barn_items"])
        text = fmt_storage(db_user, "barn")
    await safe_edit(query, text, storage_items_keyboard(items, storage_type, page))

async def sell_menu_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    item_key = "_".join(query.data.split("_")[2:])
    user = query.from_user
    qty = await get_item_count(user.id, item_key)
    if qty == 0:
        await query.answer("You don't have this item!", show_alert=True)
        return
    from game.data import get_item_emoji, get_item_name, CROPS, BUILDINGS
    emoji = get_item_emoji(item_key)
    name = get_item_name(item_key)

    sell_price = 0
    if item_key in CROPS:
        sell_price = CROPS[item_key]["sell_price"]
    else:
        for bld in BUILDINGS.values():
            if item_key in bld["recipes"]:
                sell_price = bld["recipes"][item_key]["sell_price"]
                break

    price_line = f"💰 Sell price: {sell_price} coins each" if sell_price else "⚠️ Cannot sell directly (list on market instead)"
    text = f"{emoji} **{name}** (you have: {qty})\n{price_line}"
    await safe_edit(query, text, sell_keyboard(item_key, qty))

async def sell_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    parts = query.data.split("_")
    item_key = "_".join(parts[1:-1])
    qty = int(parts[-1])
    user = query.from_user
    ok, msg = await sell_item(user.id, item_key, qty)
    await query.answer(msg, show_alert=True)
    if ok:
        db_user = await get_user_full(user.id)
        silo = parse_json_field(db_user["silo_items"])
        barn = parse_json_field(db_user["barn_items"])
        text = (
            f"📦 **Storage Overview**\n\n"
            f"🌾 Silo (Lv{db_user['silo_level']}): {sum(silo.values())}/{db_user['silo_cap']}\n"
            f"🏚 Barn (Lv{db_user['barn_level']}): {sum(barn.values())}/{db_user['barn_cap']}\n\n"
            f"Select storage to view items:"
        )
        await safe_edit(query, text, storage_keyboard())

async def upgrade_silo_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    ok, msg = await upgrade_silo(user.id)
    await query.answer(msg, show_alert=True)
    if ok:
        db_user = await get_user_full(user.id)
        silo = parse_json_field(db_user["silo_items"])
        barn = parse_json_field(db_user["barn_items"])
        await safe_edit(query,
            f"📦 **Storage**\n🌾 Silo: {sum(silo.values())}/{db_user['silo_cap']}\n🏚 Barn: {sum(barn.values())}/{db_user['barn_cap']}",
            storage_keyboard()
        )

async def upgrade_barn_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    ok, msg = await upgrade_barn(user.id)
    await query.answer(msg, show_alert=True)
    if ok:
        db_user = await get_user_full(user.id)
        silo = parse_json_field(db_user["silo_items"])
        barn = parse_json_field(db_user["barn_items"])
        await safe_edit(query,
            f"📦 **Storage**\n🌾 Silo: {sum(silo.values())}/{db_user['silo_cap']}\n🏚 Barn: {sum(barn.values())}/{db_user['barn_cap']}",
            storage_keyboard()
        )


# ─── ORDERS ───────────────────────────────────────────────────────────────────

async def orders_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    db_user = await get_or_create_user(user.id, user.username, user.first_name)
    await ensure_orders(user.id, db_user["level"])
    orders = await get_orders(user.id)
    text = fmt_orders(orders)
    await safe_edit(query, text, orders_keyboard(orders))

async def orders_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db_user = await get_or_create_user(user.id, user.username, user.first_name)
    await ensure_orders(user.id, db_user["level"])
    orders = await get_orders(user.id)
    text = fmt_orders(orders)
    await safe_send(update, text, orders_keyboard(orders))

async def fulfill_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    order_id = int(query.data.split("_")[1])
    user = query.from_user
    ok, msg = await fulfill_order(user.id, order_id)
    if ok:
        db_user = await get_user_full(user.id)
        await ensure_orders(user.id, db_user["level"])
        orders = await get_orders(user.id)
        await safe_edit(query, msg + "\n\n" + fmt_orders(orders), orders_keyboard(orders))
    else:
        await query.answer(msg, show_alert=True)


# ─── MARKET ───────────────────────────────────────────────────────────────────

async def market_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE, page: int = 0):
    if hasattr(update, "callback_query") and update.callback_query:
        query = update.callback_query
        await query.answer()
        send_fn = lambda t, k: safe_edit(query, t, k)
    else:
        send_fn = lambda t, k: safe_send(update, t, k)

    per_page = 9
    listings = await get_market_listings(page, per_page)
    from database.db import get_db
    async with await get_db() as db:
        row = await db.execute_fetchone("SELECT COUNT(*) as c FROM market_listings")
        total = row["c"]

    text = fmt_market(listings, page, total)
    await send_fn(text, market_keyboard(listings, page, total, per_page))

async def market_page_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    page = int(query.data.split("_")[2])
    per_page = 9
    listings = await get_market_listings(page, per_page)
    from database.db import get_db
    async with await get_db() as db:
        row = await db.execute_fetchone("SELECT COUNT(*) as c FROM market_listings")
        total = row["c"]
    text = fmt_market(listings, page, total)
    await safe_edit(query, text, market_keyboard(listings, page, total, per_page))

async def market_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await get_or_create_user(user.id, user.username, user.first_name)
    per_page = 9
    listings = await get_market_listings(0, per_page)
    from database.db import get_db
    async with await get_db() as db:
        row = await db.execute_fetchone("SELECT COUNT(*) as c FROM market_listings")
        total = row["c"]
    text = fmt_market(listings, 0, total)
    await safe_send(update, text, market_keyboard(listings, 0, total, per_page))

async def mkt_buy_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    listing_id = int(query.data.split("_")[2])
    user = query.from_user
    await get_or_create_user(user.id, user.username, user.first_name)
    ok, msg = await buy_from_market(user.id, listing_id)
    await query.answer(msg, show_alert=True)
    if ok:
        listings = await get_market_listings(0, 9)
        from database.db import get_db
        async with await get_db() as db:
            row = await db.execute_fetchone("SELECT COUNT(*) as c FROM market_listings")
            total = row["c"]
        await safe_edit(query, fmt_market(listings, 0, total), market_keyboard(listings, 0, total))

async def my_listings_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    from database.db import get_db
    async with await get_db() as db:
        rows = await db.execute_fetchall(
            "SELECT * FROM market_listings WHERE seller_id = ?", (user.id,)
        )
        listings = [dict(r) for r in rows]

    if not listings:
        await safe_edit(query, "📭 You have no active listings.", back_to_menu())
        return

    from game.data import get_item_emoji, get_item_name
    buttons = []
    for l in listings:
        emoji = get_item_emoji(l["item"])
        name = get_item_name(l["item"])
        buttons.append([InlineKeyboardButton(
            f"❌ Remove: {emoji}{name} x{l['qty']} @ {l['price']}💰",
            callback_data=f"rmlist_{l['id']}"
        )])
    buttons.append([InlineKeyboardButton("⬅️ Back to Market", callback_data="market")])
    await safe_edit(query, "📋 **Your Listings** (tap to remove):", InlineKeyboardMarkup(buttons))

async def rmlist_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    listing_id = int(query.data.split("_")[1])
    user = query.from_user
    ok, msg = await remove_market_listing(user.id, listing_id)
    await query.answer(msg, show_alert=True)

async def market_list_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    item_key = "_".join(query.data.split("_")[2:])
    ctx.user_data["listing_item"] = item_key
    from game.data import get_item_emoji, get_item_name
    emoji = get_item_emoji(item_key)
    name = get_item_name(item_key)
    qty = await get_item_count(query.from_user.id, item_key)
    await safe_edit(
        query,
        f"📢 **List {emoji} {name} on Market**\nYou have: {qty}\n\nSend a message in format:\n`/listitem {item_key} <qty> <price>`\n\nExample: `/listitem {item_key} 5 50`",
        back_to_menu()
    )

async def listitem_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db_user = await get_or_create_user(user.id, user.username, user.first_name)
    args = ctx.args
    if len(args) < 3:
        await safe_send(update, "Usage: `/listitem <item> <qty> <price>`\nExample: `/listitem wheat 10 5`")
        return
    item_key = args[0].lower()
    try:
        qty = int(args[1])
        price = int(args[2])
    except ValueError:
        await safe_send(update, "❌ Qty and price must be numbers.")
        return

    seller_name = user.first_name or user.username or "Farmer"
    ok, msg = await list_item_on_market(user.id, seller_name, item_key, qty, price)
    await safe_send(update, msg)


# ─── LAND ─────────────────────────────────────────────────────────────────────

async def land_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    db_user = await get_or_create_user(user.id, user.username, user.first_name)
    obstacles = await get_obstacles(user.id)
    plots = await get_plots(user.id)
    text = (
        f"🗺️ **Land Management**\n"
        f"Farm plots: {db_user['plots']} | Animal pens: {db_user['animal_pens']}\n\n"
        f"Clear obstacles to unlock new plots!\n"
        f"Clearing tools drop when harvesting crops (5% chance).\n\n"
        f"🪓 Obstacles to clear: {len(obstacles)}"
    )
    await safe_edit(query, text, land_keyboard(obstacles, plots))

async def clear_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    slot = int(query.data.split("_")[1])
    user = query.from_user
    ok, msg = await clear_obstacle(user.id, slot)
    await query.answer(msg, show_alert=True)
    if ok:
        obstacles = await get_obstacles(user.id)
        plots = await get_plots(user.id)
        db_user = await get_user_full(user.id)
        text = (
            f"🗺️ **Land Management**\n"
            f"Farm plots: {db_user['plots']} | Obstacles: {len(obstacles)}"
        )
        await safe_edit(query, text, land_keyboard(obstacles, plots))


# ─── PROFILE / DAILY / HELP ───────────────────────────────────────────────────

async def profile_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    db_user = await get_or_create_user(user.id, user.username, user.first_name)
    await safe_edit(query, fmt_profile(db_user), back_to_menu())

async def profile_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db_user = await get_or_create_user(user.id, user.username, user.first_name)
    await safe_send(update, fmt_profile(db_user), back_to_menu())

async def daily_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    await get_or_create_user(user.id, user.username, user.first_name)
    ok, msg = await claim_daily(user.id)
    await query.answer(msg, show_alert=True)
    if ok:
        db_user = await get_user_full(user.id)
        await safe_edit(query, fmt_profile(db_user), back_to_menu())

async def daily_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await get_or_create_user(user.id, user.username, user.first_name)
    ok, msg = await claim_daily(user.id)
    await safe_send(update, msg, back_to_menu())

async def help_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await safe_edit(query, fmt_help(), back_to_menu())

async def help_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await safe_send(update, fmt_help(), back_to_menu())

async def noop_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer("Nothing to do here!", show_alert=False)

async def locked_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer("🔒 Level up to unlock this!", show_alert=True)
