# utils/keyboards.py - Keyboard builders for Harvest Kingdom

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from game.data import CROPS, ANIMALS, BUILDINGS, get_item_emoji, get_item_name, OBSTACLES


def main_menu_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🏠 My Farm", callback_data="farm"),
            InlineKeyboardButton("🐾 Animals", callback_data="animals"),
        ],
        [
            InlineKeyboardButton("🏭 Factories", callback_data="factories"),
            InlineKeyboardButton("📦 Storage", callback_data="storage"),
        ],
        [
            InlineKeyboardButton("🚚 Orders", callback_data="orders"),
            InlineKeyboardButton("🏪 Market", callback_data="market"),
        ],
        [
            InlineKeyboardButton("🗺️ Land", callback_data="land"),
            InlineKeyboardButton("📊 Profile", callback_data="profile"),
        ],
        [
            InlineKeyboardButton("🎁 Daily Reward", callback_data="daily"),
            InlineKeyboardButton("❓ Help", callback_data="help"),
        ],
    ])

def back_to_menu():
    return InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Main Menu", callback_data="menu")]])

def farm_keyboard(plots: list[dict], user_level: int):
    buttons = []
    row = []
    for i, plot in enumerate(plots):
        slot = plot["slot"]
        if plot["status"] == "empty":
            label = f"🟩 {slot+1}"
            cb = f"plot_plant_{slot}"
        elif plot["status"] == "growing":
            from datetime import datetime, timezone
            ready_at = datetime.fromisoformat(plot["ready_at"])
            if ready_at.tzinfo is None:
                ready_at = ready_at.replace(tzinfo=timezone.utc)
            from datetime import datetime, timezone
            now = datetime.now(timezone.utc)
            if now >= ready_at:
                label = f"✅ {slot+1}"
            else:
                label = f"🌱 {slot+1}"
            cb = f"plot_harvest_{slot}"
        else:
            label = f"❓ {slot+1}"
            cb = f"plot_{slot}"

        row.append(InlineKeyboardButton(label, callback_data=cb))
        if len(row) == 4:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    buttons.append([
        InlineKeyboardButton("🌾 Harvest All", callback_data="harvest_all"),
        InlineKeyboardButton("🔧 Expand", callback_data="expand_farm"),
    ])
    buttons.append([InlineKeyboardButton("🏠 Main Menu", callback_data="menu")])
    return InlineKeyboardMarkup(buttons)

def plant_keyboard(user_level: int, slot: int):
    buttons = []
    row = []
    for crop_key, crop in CROPS.items():
        if crop["level_req"] <= user_level:
            label = f"{crop['emoji']} {crop['name']}"
            row.append(InlineKeyboardButton(label, callback_data=f"plant_{slot}_{crop_key}"))
            if len(row) == 2:
                buttons.append(row)
                row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton("⬅️ Back", callback_data="farm")])
    return InlineKeyboardMarkup(buttons)

def animals_keyboard(pens: list[dict], user_level: int):
    buttons = []
    row = []
    for pen in pens:
        slot = pen["slot"]
        if pen["status"] == "empty":
            label = f"🟩 {slot+1}"
            cb = f"pen_buy_{slot}"
        elif pen["status"] == "producing":
            from datetime import datetime, timezone
            ready_at = datetime.fromisoformat(pen["ready_at"])
            if ready_at.tzinfo is None:
                ready_at = ready_at.replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            if now >= ready_at:
                label = f"✅ {ANIMALS[pen['animal']]['emoji']}{slot+1}"
            else:
                label = f"{ANIMALS[pen['animal']]['emoji']} {slot+1}"
            cb = f"pen_collect_{slot}"
        else:
            label = f"❓ {slot+1}"
            cb = f"pen_{slot}"
        row.append(InlineKeyboardButton(label, callback_data=cb))
        if len(row) == 3:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    buttons.append([InlineKeyboardButton("🐾 Expand Pens", callback_data="expand_pens")])
    buttons.append([InlineKeyboardButton("🏠 Main Menu", callback_data="menu")])
    return InlineKeyboardMarkup(buttons)

def buy_animal_keyboard(user_level: int, slot: int):
    buttons = []
    row = []
    for a_key, a in ANIMALS.items():
        if a["level_req"] <= user_level:
            label = f"{a['emoji']} {a['name']}"
            row.append(InlineKeyboardButton(label, callback_data=f"buyanimal_{slot}_{a_key}"))
            if len(row) == 2:
                buttons.append(row)
                row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton("⬅️ Back", callback_data="animals")])
    return InlineKeyboardMarkup(buttons)

def factories_keyboard(user_buildings: list[dict], user_level: int):
    from game.data import BUILDINGS
    buttons = []
    owned = {b["building"] for b in user_buildings}

    for bld_key, bld in BUILDINGS.items():
        if bld_key in owned:
            buttons.append([InlineKeyboardButton(f"{bld['emoji']} {bld['name']}", callback_data=f"factory_{bld_key}")])
        elif bld["level_req"] <= user_level:
            buttons.append([InlineKeyboardButton(f"🔓 Buy {bld['emoji']} {bld['name']} ({bld['buy_cost']}💰)", callback_data=f"buy_building_{bld_key}")])
        else:
            buttons.append([InlineKeyboardButton(f"🔒 {bld['name']} (Lv{bld['level_req']})", callback_data="locked")])

    buttons.append([InlineKeyboardButton("🏠 Main Menu", callback_data="menu")])
    return InlineKeyboardMarkup(buttons)

def factory_detail_keyboard(building_key: str, slots: list[dict]):
    from game.data import BUILDINGS, PROCESSED_EMOJI
    bld = BUILDINGS[building_key]
    buttons = []

    for rec_key, rec in bld["recipes"].items():
        emoji = PROCESSED_EMOJI.get(rec_key, "📦")
        ingredients = ", ".join(f"{qty}x {get_item_name(k)}" for k, qty in rec["inputs"].items())
        label = f"{emoji} {get_item_name(rec_key)} ({ingredients})"
        buttons.append([InlineKeyboardButton(label, callback_data=f"produce_{building_key}_{rec_key}")])

    for s in slots:
        if s["status"] == "producing":
            from datetime import datetime, timezone
            ready_at = datetime.fromisoformat(s["ready_at"])
            if ready_at.tzinfo is None:
                ready_at = ready_at.replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            if now >= ready_at:
                label = f"✅ Slot {s['slot']+1}: {get_item_name(s['item'])} READY"
            else:
                from game.engine import fmt_time
                remaining = int((ready_at - now).total_seconds())
                label = f"⏳ Slot {s['slot']+1}: {get_item_name(s['item'])} ({fmt_time(remaining)})"
            buttons.append([InlineKeyboardButton(label, callback_data=f"collect_{building_key}_{s['slot']}")])

    buttons.append([InlineKeyboardButton("⬅️ Back", callback_data="factories")])
    return InlineKeyboardMarkup(buttons)

def storage_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🌾 Silo (Crops)", callback_data="storage_silo"),
            InlineKeyboardButton("🏚 Barn (Goods)", callback_data="storage_barn"),
        ],
        [
            InlineKeyboardButton("⬆️ Upgrade Silo", callback_data="upgrade_silo"),
            InlineKeyboardButton("⬆️ Upgrade Barn", callback_data="upgrade_barn"),
        ],
        [InlineKeyboardButton("🏠 Main Menu", callback_data="menu")],
    ])

def storage_items_keyboard(items: dict, storage_type: str, page: int = 0):
    buttons = []
    item_list = list(items.items())
    per_page = 8
    start = page * per_page
    end = start + per_page
    page_items = item_list[start:end]

    for item_key, qty in page_items:
        emoji = get_item_emoji(item_key)
        name = get_item_name(item_key)
        buttons.append([
            InlineKeyboardButton(f"{emoji} {name} x{qty}", callback_data=f"sell_menu_{item_key}"),
        ])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"storage_{storage_type}_page_{page-1}"))
    if end < len(item_list):
        nav.append(InlineKeyboardButton("▶️ Next", callback_data=f"storage_{storage_type}_page_{page+1}"))
    if nav:
        buttons.append(nav)

    buttons.append([InlineKeyboardButton("⬅️ Back", callback_data="storage")])
    return InlineKeyboardMarkup(buttons)

def sell_keyboard(item_key: str, qty: int):
    buttons = []
    amounts = [1, 5, 10, qty]
    amounts = sorted(set(a for a in amounts if a <= qty and a > 0))
    row = []
    for amt in amounts:
        row.append(InlineKeyboardButton(f"Sell {amt}", callback_data=f"sell_{item_key}_{amt}"))
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton("📢 List on Market", callback_data=f"market_list_{item_key}")])
    buttons.append([InlineKeyboardButton("⬅️ Back", callback_data="storage")])
    return InlineKeyboardMarkup(buttons)

def orders_keyboard(orders: list[dict]):
    buttons = []
    for order in orders:
        import json
        items = json.loads(order["items"])
        preview = ", ".join(f"{qty}x{get_item_emoji(k)}{get_item_name(k)}" for k, qty in items.items())
        label = f"🚚 {preview[:30]}{'...' if len(preview)>30 else ''} → {order['reward_coins']}💰"
        buttons.append([InlineKeyboardButton(label, callback_data=f"fulfill_{order['id']}")])

    buttons.append([InlineKeyboardButton("🏠 Main Menu", callback_data="menu")])
    return InlineKeyboardMarkup(buttons)

def market_keyboard(listings: list[dict], page: int = 0, total: int = 0, per_page: int = 9):
    buttons = []
    for listing in listings:
        emoji = get_item_emoji(listing["item"])
        name = get_item_name(listing["item"])
        label = f"{emoji}{name} x{listing['qty']} @ {listing['price']}💰 [{listing['seller_name']}]"
        buttons.append([InlineKeyboardButton(label[:50], callback_data=f"mkt_buy_{listing['id']}")])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"market_page_{page-1}"))
    if (page + 1) * per_page < total:
        nav.append(InlineKeyboardButton("▶️ Next", callback_data=f"market_page_{page+1}"))
    if nav:
        buttons.append(nav)

    buttons.append([
        InlineKeyboardButton("📢 My Listings", callback_data="my_listings"),
        InlineKeyboardButton("🏠 Menu", callback_data="menu"),
    ])
    return InlineKeyboardMarkup(buttons)

def land_keyboard(obstacles: list[dict], plots: list[dict]):
    buttons = []
    obs_slots = {o["slot"]: o for o in obstacles}
    for obs_slot, obs_data in obs_slots.items():
        obs = OBSTACLES[obs_data["obstacle"]]
        buttons.append([InlineKeyboardButton(
            f"{obs['emoji']} Slot {obs_slot+1}: {obs['name']} (need {get_item_emoji(obs['tool'])} {get_item_name(obs['tool'])})",
            callback_data=f"clear_{obs_slot}"
        )])

    if not obs_slots:
        buttons.append([InlineKeyboardButton("✅ No obstacles!", callback_data="noop")])

    buttons.append([
        InlineKeyboardButton("🔧 Expand Farm", callback_data="expand_farm"),
        InlineKeyboardButton("🐾 Expand Pens", callback_data="expand_pens"),
    ])
    buttons.append([InlineKeyboardButton("🏠 Main Menu", callback_data="menu")])
    return InlineKeyboardMarkup(buttons)
