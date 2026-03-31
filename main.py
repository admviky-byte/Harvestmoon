#!/usr/bin/env python3
# main.py - Harvest Kingdom Bot Entry Point

import os
import logging
import asyncio
from dotenv import load_dotenv

from telegram import Update
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes
)

from database.db import init_db
from handlers.main_handlers import (
    start_handler, menu_callback, farm_callback, farm_cmd,
    plot_plant_callback, plant_callback, plot_harvest_callback,
    harvest_all_callback, expand_farm_callback,
    animals_callback, pen_buy_callback, buyanimal_callback,
    pen_collect_callback, expand_pens_callback,
    factories_callback, buy_building_callback, factory_detail_callback,
    produce_callback, collect_callback,
    storage_callback, storage_silo_callback, storage_barn_callback,
    storage_page_callback, sell_menu_callback, sell_callback,
    upgrade_silo_callback, upgrade_barn_callback,
    orders_callback, orders_cmd, fulfill_callback,
    market_callback, market_page_callback, market_cmd,
    mkt_buy_callback, my_listings_callback, rmlist_callback,
    market_list_callback, listitem_cmd,
    land_callback, clear_callback,
    profile_callback, profile_cmd,
    daily_callback, daily_cmd,
    help_callback, help_cmd,
    noop_callback, locked_callback,
)
from handlers.admin_handlers import (
    admin_cmd, adm_panel_callback, adm_stats_callback,
    adm_settings_callback, adm_toggle_setting,
    adm_users_callback, adm_user_detail_callback,
    adm_setcoins_callback, adm_setlevel_callback, adm_setgems_callback,
    adm_giveitem_callback, adm_give2_callback, adm_resetuser_callback,
    adm_broadcast_callback, adm_logs_callback,
    adm_items_callback, adm_addcrop_callback,
    adm_give_callback, adm_text_input,
    give_cmd, givecoins_cmd,
    get_admin_ids,
)

load_dotenv()
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


async def error_handler(update: object, ctx: ContextTypes.DEFAULT_TYPE):
    logger.error("Exception while handling update:", exc_info=ctx.error)
    if isinstance(update, Update):
        if update.callback_query:
            try:
                await update.callback_query.answer("⚠️ Something went wrong. Please try again.", show_alert=True)
            except Exception:
                pass
        elif update.message:
            try:
                await update.message.reply_text("⚠️ An error occurred. Please try again.")
            except Exception:
                pass


def register_handlers(app: Application):
    # ─── COMMANDS ─────────────────────────────────────────────────────────────
    app.add_handler(CommandHandler("start", start_handler))
    app.add_handler(CommandHandler("farm", farm_cmd))
    app.add_handler(CommandHandler("storage", lambda u,c: storage_callback_cmd(u,c)))
    app.add_handler(CommandHandler("market", market_cmd))
    app.add_handler(CommandHandler("orders", orders_cmd))
    app.add_handler(CommandHandler("daily", daily_cmd))
    app.add_handler(CommandHandler("profile", profile_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("listitem", listitem_cmd))

    # Admin commands
    app.add_handler(CommandHandler("admin", admin_cmd))
    app.add_handler(CommandHandler("give", give_cmd))
    app.add_handler(CommandHandler("givecoins", givecoins_cmd))

    # ─── ADMIN TEXT INPUT (must be before generic message handler) ─────────────
    admin_ids = get_admin_ids()
    if admin_ids:
        app.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND & filters.User(user_id=admin_ids),
            adm_text_input
        ))

    # ─── CALLBACK QUERIES ─────────────────────────────────────────────────────
    # Menu
    app.add_handler(CallbackQueryHandler(menu_callback, pattern="^menu$"))

    # Farm
    app.add_handler(CallbackQueryHandler(farm_callback, pattern="^farm$"))
    app.add_handler(CallbackQueryHandler(plot_plant_callback, pattern=r"^plot_plant_\d+$"))
    app.add_handler(CallbackQueryHandler(plot_harvest_callback, pattern=r"^plot_harvest_\d+$"))
    app.add_handler(CallbackQueryHandler(plant_callback, pattern=r"^plant_\d+_.+$"))
    app.add_handler(CallbackQueryHandler(harvest_all_callback, pattern="^harvest_all$"))
    app.add_handler(CallbackQueryHandler(expand_farm_callback, pattern="^expand_farm$"))

    # Animals
    app.add_handler(CallbackQueryHandler(animals_callback, pattern="^animals$"))
    app.add_handler(CallbackQueryHandler(pen_buy_callback, pattern=r"^pen_buy_\d+$"))
    app.add_handler(CallbackQueryHandler(buyanimal_callback, pattern=r"^buyanimal_\d+_.+$"))
    app.add_handler(CallbackQueryHandler(pen_collect_callback, pattern=r"^pen_collect_\d+$"))
    app.add_handler(CallbackQueryHandler(expand_pens_callback, pattern="^expand_pens$"))

    # Factories
    app.add_handler(CallbackQueryHandler(factories_callback, pattern="^factories$"))
    app.add_handler(CallbackQueryHandler(buy_building_callback, pattern=r"^buy_building_.+$"))
    app.add_handler(CallbackQueryHandler(factory_detail_callback, pattern=r"^factory_.+$"))
    app.add_handler(CallbackQueryHandler(produce_callback, pattern=r"^produce_.+$"))
    app.add_handler(CallbackQueryHandler(collect_callback, pattern=r"^collect_.+$"))

    # Storage
    app.add_handler(CallbackQueryHandler(storage_callback, pattern="^storage$"))
    app.add_handler(CallbackQueryHandler(storage_silo_callback, pattern="^storage_silo$"))
    app.add_handler(CallbackQueryHandler(storage_barn_callback, pattern="^storage_barn$"))
    app.add_handler(CallbackQueryHandler(storage_page_callback, pattern=r"^storage_(silo|barn)_page_\d+$"))
    app.add_handler(CallbackQueryHandler(sell_menu_callback, pattern=r"^sell_menu_.+$"))
    app.add_handler(CallbackQueryHandler(sell_callback, pattern=r"^sell_.+_\d+$"))
    app.add_handler(CallbackQueryHandler(upgrade_silo_callback, pattern="^upgrade_silo$"))
    app.add_handler(CallbackQueryHandler(upgrade_barn_callback, pattern="^upgrade_barn$"))

    # Orders
    app.add_handler(CallbackQueryHandler(orders_callback, pattern="^orders$"))
    app.add_handler(CallbackQueryHandler(fulfill_callback, pattern=r"^fulfill_\d+$"))

    # Market
    app.add_handler(CallbackQueryHandler(market_callback, pattern="^market$"))
    app.add_handler(CallbackQueryHandler(market_page_callback, pattern=r"^market_page_\d+$"))
    app.add_handler(CallbackQueryHandler(mkt_buy_callback, pattern=r"^mkt_buy_\d+$"))
    app.add_handler(CallbackQueryHandler(my_listings_callback, pattern="^my_listings$"))
    app.add_handler(CallbackQueryHandler(rmlist_callback, pattern=r"^rmlist_\d+$"))
    app.add_handler(CallbackQueryHandler(market_list_callback, pattern=r"^market_list_.+$"))

    # Land
    app.add_handler(CallbackQueryHandler(land_callback, pattern="^land$"))
    app.add_handler(CallbackQueryHandler(clear_callback, pattern=r"^clear_\d+$"))

    # Profile / Daily / Help
    app.add_handler(CallbackQueryHandler(profile_callback, pattern="^profile$"))
    app.add_handler(CallbackQueryHandler(daily_callback, pattern="^daily$"))
    app.add_handler(CallbackQueryHandler(help_callback, pattern="^help$"))
    app.add_handler(CallbackQueryHandler(noop_callback, pattern="^noop$"))
    app.add_handler(CallbackQueryHandler(locked_callback, pattern="^locked$"))

    # Admin panel callbacks
    app.add_handler(CallbackQueryHandler(adm_panel_callback, pattern="^adm_panel$"))
    app.add_handler(CallbackQueryHandler(adm_stats_callback, pattern="^adm_stats$"))
    app.add_handler(CallbackQueryHandler(adm_settings_callback, pattern="^adm_settings$"))
    app.add_handler(CallbackQueryHandler(adm_toggle_setting, pattern=r"^adm_set_.+$"))
    app.add_handler(CallbackQueryHandler(adm_users_callback, pattern="^adm_users$"))
    app.add_handler(CallbackQueryHandler(adm_user_detail_callback, pattern=r"^adm_user_\d+$"))
    app.add_handler(CallbackQueryHandler(adm_setcoins_callback, pattern=r"^adm_setcoins_\d+$"))
    app.add_handler(CallbackQueryHandler(adm_setlevel_callback, pattern=r"^adm_setlevel_\d+$"))
    app.add_handler(CallbackQueryHandler(adm_setgems_callback, pattern=r"^adm_setgems_\d+$"))
    app.add_handler(CallbackQueryHandler(adm_giveitem_callback, pattern=r"^adm_giveitem_\d+$"))
    app.add_handler(CallbackQueryHandler(adm_give2_callback, pattern=r"^adm_give2_\d+_.+$"))
    app.add_handler(CallbackQueryHandler(adm_resetuser_callback, pattern=r"^adm_resetuser_\d+$"))
    app.add_handler(CallbackQueryHandler(adm_broadcast_callback, pattern="^adm_broadcast$"))
    app.add_handler(CallbackQueryHandler(adm_logs_callback, pattern="^adm_logs$"))
    app.add_handler(CallbackQueryHandler(adm_items_callback, pattern="^adm_items$"))
    app.add_handler(CallbackQueryHandler(adm_addcrop_callback, pattern="^adm_addcrop$"))
    app.add_handler(CallbackQueryHandler(adm_give_callback, pattern="^adm_give$"))

    app.add_error_handler(error_handler)


async def storage_callback_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    from handlers.main_handlers import safe_send
    from database.db import get_or_create_user
    from database.db import parse_json_field
    user = update.effective_user
    db_user = await get_or_create_user(user.id, user.username, user.first_name)
    from database.db import parse_json_field
    silo = parse_json_field(db_user["silo_items"])
    barn = parse_json_field(db_user["barn_items"])
    from utils.keyboards import storage_keyboard
    text = (
        f"📦 **Storage Overview**\n\n"
        f"🌾 Silo (Lv{db_user['silo_level']}): {sum(silo.values())}/{db_user['silo_cap']}\n"
        f"🏚 Barn (Lv{db_user['barn_level']}): {sum(barn.values())}/{db_user['barn_cap']}\n\n"
        f"Select storage to view items:"
    )
    await safe_send(update, text, storage_keyboard())


def main():
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise ValueError("❌ BOT_TOKEN not set! Add it to your environment variables.")

    async def post_init(app: Application):
        await init_db()
        logger.info("✅ Database initialized")
        admin_ids = get_admin_ids()
        if admin_ids:
            logger.info(f"✅ Admin IDs: {admin_ids}")
        else:
            logger.warning("⚠️ No ADMIN_IDS set. Admin panel will be inaccessible.")

    app = (
        Application.builder()
        .token(token)
        .post_init(post_init)
        .build()
    )

    register_handlers(app)
    logger.info("🌾 Harvest Kingdom Bot starting...")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
