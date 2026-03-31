# utils/formatters.py - Message text formatters for Harvest Kingdom

from datetime import datetime, timezone
from game.data import (
    CROPS, ANIMALS, BUILDINGS, UPGRADE_TOOLS, EXPANSION_TOOLS, CLEARING_TOOLS,
    OBSTACLES, get_item_emoji, get_item_name, get_xp_for_next_level, PROCESSED_EMOJI
)
from database.db import parse_json_field
from game.engine import fmt_time


def fmt_farm(user: dict, plots: list[dict]) -> str:
    now = datetime.now(timezone.utc)
    level = user["level"]
    coins = user["coins"]
    xp = user["xp"]
    next_xp = get_xp_for_next_level(level)
    xp_bar = make_xp_bar(xp, next_xp, level)

    lines = [
        f"🏡 **{user['first_name']}'s Farm**",
        f"👑 Level {level}  💰 {coins:,} coins  💎 {user['gems']} gems",
        f"📈 XP: {xp:,} / {next_xp:,}  {xp_bar}",
        "",
        f"🌾 **Farm Plots** ({user['plots']} total):",
    ]

    for plot in plots:
        slot = plot["slot"]
        if plot["status"] == "empty":
            lines.append(f"  [{slot+1}] 🟩 Empty — tap to plant")
        elif plot["status"] == "growing":
            crop = CROPS.get(plot["crop"], {})
            ready_at = datetime.fromisoformat(plot["ready_at"])
            if ready_at.tzinfo is None:
                ready_at = ready_at.replace(tzinfo=timezone.utc)
            if now >= ready_at:
                lines.append(f"  [{slot+1}] ✅ {crop.get('emoji','🌱')} {crop.get('name', plot['crop'])} — **READY TO HARVEST!**")
            else:
                remaining = int((ready_at - now).total_seconds())
                lines.append(f"  [{slot+1}] 🌱 {crop.get('emoji','🌱')} {crop.get('name', plot['crop'])} — ⏳ {fmt_time(remaining)}")
        else:
            lines.append(f"  [{slot+1}] ❓ {plot['status']}")

    silo = parse_json_field(user["silo_items"])
    lines.append(f"\n📦 Silo: {sum(silo.values())}/{user['silo_cap']}  🏚 Barn: {sum(parse_json_field(user['barn_items']).values())}/{user['barn_cap']}")
    return "\n".join(lines)


def fmt_animals(user: dict, pens: list[dict]) -> str:
    now = datetime.now(timezone.utc)
    lines = [f"🐾 **Animal Pens** ({user['animal_pens']} pens):", ""]

    for pen in pens:
        slot = pen["slot"]
        if pen["status"] == "empty":
            lines.append(f"  [{slot+1}] 🟩 Empty pen — tap to buy animal")
        elif pen["status"] == "producing":
            animal = ANIMALS.get(pen["animal"], {})
            ready_at = datetime.fromisoformat(pen["ready_at"])
            if ready_at.tzinfo is None:
                ready_at = ready_at.replace(tzinfo=timezone.utc)
            if now >= ready_at:
                lines.append(f"  [{slot+1}] ✅ {animal.get('emoji','🐾')} {animal.get('name', pen['animal'])} → {animal.get('prod_emoji','📦')} **READY!**")
            else:
                remaining = int((ready_at - now).total_seconds())
                lines.append(f"  [{slot+1}] {animal.get('emoji','🐾')} {animal.get('name', pen['animal'])} → ⏳ {fmt_time(remaining)}")
        else:
            lines.append(f"  [{slot+1}] ❓ {pen['status']}")
    return "\n".join(lines)


def fmt_storage(user: dict, storage_type: str = "silo") -> str:
    if storage_type == "silo":
        items = parse_json_field(user["silo_items"])
        cap = user["silo_cap"]
        used = sum(items.values())
        level = user["silo_level"]
        title = f"🌾 **Silo** (Level {level}) — {used}/{cap}"
    else:
        items = parse_json_field(user["barn_items"])
        cap = user["barn_cap"]
        used = sum(items.values())
        level = user["barn_level"]
        title = f"🏚 **Barn** (Level {level}) — {used}/{cap}"

    bar = make_capacity_bar(used, cap)
    lines = [title, bar, ""]

    if not items:
        lines.append("  (empty)")
    else:
        for item_key, qty in sorted(items.items(), key=lambda x: -x[1]):
            emoji = get_item_emoji(item_key)
            name = get_item_name(item_key)
            lines.append(f"  {emoji} {name}: **{qty}**")
    return "\n".join(lines)


def fmt_factories(user: dict, buildings: list[dict]) -> str:
    now = datetime.now(timezone.utc)
    owned_keys = {b["building"] for b in buildings}

    if not owned_keys:
        lines = [
            "🏭 **Factories**",
            "",
            "You don't own any factories yet!",
            "Buy your first factory from the menu below.",
        ]
    else:
        lines = ["🏭 **Factories**", ""]
        for bld_key in owned_keys:
            bld = BUILDINGS[bld_key]
            bld_slots = [b for b in buildings if b["building"] == bld_key]
            lines.append(f"{bld['emoji']} **{bld['name']}**")
            for s in bld_slots:
                slot_num = s["slot"] + 1
                if s["status"] == "idle":
                    lines.append(f"  Slot {slot_num}: 💤 Idle")
                elif s["status"] == "producing":
                    ready_at = datetime.fromisoformat(s["ready_at"])
                    if ready_at.tzinfo is None:
                        ready_at = ready_at.replace(tzinfo=timezone.utc)
                    if now >= ready_at:
                        emoji = PROCESSED_EMOJI.get(s["item"], "📦")
                        lines.append(f"  Slot {slot_num}: ✅ {emoji} {get_item_name(s['item'])} READY!")
                    else:
                        remaining = int((ready_at - now).total_seconds())
                        emoji = PROCESSED_EMOJI.get(s["item"], "📦")
                        lines.append(f"  Slot {slot_num}: ⏳ {emoji} {get_item_name(s['item'])} — {fmt_time(remaining)}")
    return "\n".join(lines)


def fmt_orders(orders: list[dict]) -> str:
    import json
    lines = ["🚚 **Delivery Orders** (9 slots)", "Complete orders to earn coins & XP!", ""]
    if not orders:
        lines.append("No active orders. Check back soon!")
    for i, order in enumerate(orders, 1):
        items = json.loads(order["items"])
        item_parts = []
        for item_key, qty in items.items():
            emoji = get_item_emoji(item_key)
            name = get_item_name(item_key)
            item_parts.append(f"{qty}x {emoji}{name}")
        lines.append(f"**Order {i}** (Slot {order['slot']+1})")
        lines.append(f"  📋 {', '.join(item_parts)}")
        lines.append(f"  💰 {order['reward_coins']} coins  ⭐ {order['reward_xp']} XP")
        lines.append("")
    return "\n".join(lines)


def fmt_market(listings: list[dict], page: int, total: int) -> str:
    lines = [
        "🏪 **Global Market**",
        f"📰 {total} item(s) for sale | Page {page+1}",
        "",
    ]
    if not listings:
        lines.append("No listings available. Be the first to sell!")
    for listing in listings:
        emoji = get_item_emoji(listing["item"])
        name = get_item_name(listing["item"])
        total_price = listing["price"] * listing["qty"]
        lines.append(f"{emoji} **{name}** x{listing['qty']}")
        lines.append(f"  💰 {listing['price']}/each (Total: {total_price}) | 👤 {listing['seller_name']}")
        lines.append("")
    lines.append("Tap an item to purchase it.")
    return "\n".join(lines)


def fmt_profile(user: dict) -> str:
    level = user["level"]
    xp = user["xp"]
    next_xp = get_xp_for_next_level(level)
    xp_bar = make_xp_bar(xp, next_xp, level)
    lines = [
        f"📊 **{user['first_name']}'s Profile**",
        f"🪪 ID: `{user['user_id']}`",
        "",
        f"👑 **Level:** {level}",
        f"📈 **XP:** {xp:,} / {next_xp:,}",
        f"   {xp_bar}",
        "",
        f"💰 **Coins:** {user['coins']:,}",
        f"💎 **Gems:** {user['gems']}",
        f"🌾 **Total Harvests:** {user['total_harvests']}",
        f"🚚 **Total Sales:** {user['total_sales']}",
        "",
        f"🌱 **Farm:** {user['plots']} plots",
        f"🐾 **Animal Pens:** {user['animal_pens']}",
        f"📦 **Silo:** Lv{user['silo_level']} ({user['silo_cap']} cap)",
        f"🏚 **Barn:** Lv{user['barn_level']} ({user['barn_cap']} cap)",
        f"📅 **Member since:** {user['created_at'][:10]}",
    ]
    return "\n".join(lines)


def fmt_help() -> str:
    return """
❓ **HARVEST KINGDOM — HELP & TUTORIAL**

👋 Welcome, Farmer! Here's how to play:

━━━━━━━━━━━━━━━━━━━━
🌾 **FARMING (My Farm)**
━━━━━━━━━━━━━━━━━━━━
1. Tap **🏠 My Farm** to see your plots
2. Tap an empty 🟩 plot to choose a crop to plant
3. Crops cost coins for seeds and grow over time
4. When a crop is ✅ READY, tap it to harvest
5. Harvested crops go to your **Silo**
6. Use **🌾 Harvest All** to harvest everything at once!
7. 🎁 5% chance of bonus tools when harvesting!

━━━━━━━━━━━━━━━━━━━━
🐾 **ANIMALS**
━━━━━━━━━━━━━━━━━━━━
1. Tap **🐾 Animals** to see your pens
2. Tap an empty 🟩 pen to buy an animal
3. Animals automatically produce items over time
4. Tap ✅ pens to collect their products (Eggs, Milk, etc.)
5. Products go to your **Silo**
6. Expand your pens to hold more animals!

━━━━━━━━━━━━━━━━━━━━
🏭 **FACTORIES**
━━━━━━━━━━━━━━━━━━━━
1. Tap **🏭 Factories** to see/buy buildings
2. Use crops + animal products to make **Processed Goods**
3. Processed goods sell for much more coins!
4. Goods go to your **Barn**
5. Each factory has multiple production slots

Example chain:
🌾 Wheat → 🏭 Bakery → 🍞 Bread

━━━━━━━━━━━━━━━━━━━━
📦 **STORAGE**
━━━━━━━━━━━━━━━━━━━━
🌾 **Silo**: Stores crops and animal products
🏚 **Barn**: Stores processed goods and tools
- Upgrade each to increase capacity
- Upgrades need special tools (found as bonus drops!)

━━━━━━━━━━━━━━━━━━━━
🚚 **TRUCK ORDERS**
━━━━━━━━━━━━━━━━━━━━
- 9 random delivery orders are always available
- Each order needs specific items from your storage
- Completing orders gives **Coins + XP**
- Orders refresh automatically when completed

━━━━━━━━━━━━━━━━━━━━
🏪 **GLOBAL MARKET**
━━━━━━━━━━━━━━━━━━━━
- Buy items from other players
- Sell your items: go to Storage → tap item → List on Market
- Set your price and wait for buyers!
- Max 5 listings at a time

━━━━━━━━━━━━━━━━━━━━
🗺️ **LAND EXPANSION**
━━━━━━━━━━━━━━━━━━━━
- New land comes with obstacles (Trees, Rocks, Swamps)
- Use clearing tools (Axe, Dynamite, Shovel) to clear them
- Clearing tools drop as **bonus items** when harvesting
- After clearing, the land becomes a new farm plot!
- Expand your farm with **Land Deed + Mallet + Marker Stake**

━━━━━━━━━━━━━━━━━━━━
💰 **TIPS**
━━━━━━━━━━━━━━━━━━━━
✅ Claim your 🎁 **Daily Reward** every day!
✅ Process raw crops into goods — higher sell price!
✅ Complete truck orders for bonus coins & XP
✅ Higher level = harder crops & animals unlocked
✅ Check the market for cheap items!
✅ Save tools for storage upgrades!

━━━━━━━━━━━━━━━━━━━━
📋 **COMMANDS**
━━━━━━━━━━━━━━━━━━━━
/start — Main menu
/farm — Go to farm
/storage — Check storage
/market — Global market
/orders — Delivery orders
/daily — Claim daily reward
/profile — View your stats
/help — This help page

Happy farming! 🌾👑
"""


def make_xp_bar(xp: int, next_xp: int, level: int) -> str:
    if next_xp <= 0:
        return "[MAX LEVEL]"
    filled = int((xp / next_xp) * 10)
    filled = min(10, max(0, filled))
    return "[" + "█" * filled + "░" * (10 - filled) + "]"


def make_capacity_bar(used: int, cap: int) -> str:
    if cap <= 0:
        return "[███████████] MAX"
    pct = min(1.0, used / cap)
    filled = int(pct * 10)
    bar = "[" + "█" * filled + "░" * (10 - filled) + f"] {used}/{cap}"
    return bar
