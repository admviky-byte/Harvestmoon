# game/engine.py - Core game logic for Harvest Kingdom

import json
import random
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import aiosqlite

from database.db import get_db, parse_json_field, dump_json_field, get_setting
from game.data import (
    CROPS, ANIMALS, BUILDINGS, UPGRADE_TOOLS, EXPANSION_TOOLS,
    CLEARING_TOOLS, OBSTACLES, BONUS_DROP_RATE, BARN_UPGRADE, SILO_UPGRADE,
    PLOTS_PER_EXPANSION, get_level_from_xp, get_xp_for_next_level,
    get_item_emoji, get_item_name, PROCESSED_EMOJI
)

logger = logging.getLogger(__name__)

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def fmt_time(seconds: int) -> str:
    if seconds <= 0:
        return "Ready!"
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        m, s = divmod(seconds, 60)
        return f"{m}m {s}s"
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h}h {m}m"

async def get_silo_used(user_id: int, silo_items: dict) -> int:
    return sum(silo_items.values())

async def get_barn_used(user_id: int, barn_items: dict) -> int:
    return sum(barn_items.values())

def is_silo_item(item_key: str) -> bool:
    if item_key in CROPS:
        return True
    animal_raw = {"egg", "milk", "bacon", "wool", "goat_milk", "honey", "feather", "fish", "lobster", "mozzarella"}
    if item_key in animal_raw:
        return True
    return False

def is_barn_item(item_key: str) -> bool:
    if item_key in UPGRADE_TOOLS or item_key in EXPANSION_TOOLS or item_key in CLEARING_TOOLS:
        return True
    for b in BUILDINGS.values():
        if item_key in b["recipes"]:
            return True
    return False


# ─── INVENTORY ───────────────────────────────────────────────────────────────

async def add_to_inventory(user_id: int, item_key: str, qty: int = 1) -> tuple[bool, str]:
    async with await get_db() as db:
        row = await db.execute_fetchone("SELECT silo_items, barn_items, silo_cap, barn_cap FROM users WHERE user_id = ?", (user_id,))
        silo = parse_json_field(row["silo_items"])
        barn = parse_json_field(row["barn_items"])
        silo_cap = row["silo_cap"]
        barn_cap = row["barn_cap"]

        if is_silo_item(item_key):
            used = sum(silo.values())
            if used + qty > silo_cap:
                return False, f"🚫 Silo full! ({used}/{silo_cap}). Upgrade your silo first."
            silo[item_key] = silo.get(item_key, 0) + qty
            await db.execute("UPDATE users SET silo_items = ? WHERE user_id = ?", (dump_json_field(silo), user_id))
        elif is_barn_item(item_key):
            used = sum(barn.values())
            if used + qty > barn_cap:
                return False, f"🚫 Barn full! ({used}/{barn_cap}). Upgrade your barn first."
            barn[item_key] = barn.get(item_key, 0) + qty
            await db.execute("UPDATE users SET barn_items = ? WHERE user_id = ?", (dump_json_field(barn), user_id))
        else:
            return False, f"❓ Unknown item: {item_key}"

        await db.commit()
        return True, "ok"

async def remove_from_inventory(user_id: int, item_key: str, qty: int = 1) -> tuple[bool, str]:
    async with await get_db() as db:
        row = await db.execute_fetchone("SELECT silo_items, barn_items FROM users WHERE user_id = ?", (user_id,))
        silo = parse_json_field(row["silo_items"])
        barn = parse_json_field(row["barn_items"])

        if is_silo_item(item_key):
            have = silo.get(item_key, 0)
            if have < qty:
                return False, f"Not enough {get_item_name(item_key)} in silo (have {have}, need {qty})"
            silo[item_key] = have - qty
            if silo[item_key] == 0:
                del silo[item_key]
            await db.execute("UPDATE users SET silo_items = ? WHERE user_id = ?", (dump_json_field(silo), user_id))
        elif is_barn_item(item_key):
            have = barn.get(item_key, 0)
            if have < qty:
                return False, f"Not enough {get_item_name(item_key)} in barn (have {have}, need {qty})"
            barn[item_key] = have - qty
            if barn[item_key] == 0:
                del barn[item_key]
            await db.execute("UPDATE users SET barn_items = ? WHERE user_id = ?", (dump_json_field(barn), user_id))
        else:
            return False, f"Unknown item: {item_key}"

        await db.commit()
        return True, "ok"

async def get_item_count(user_id: int, item_key: str) -> int:
    async with await get_db() as db:
        row = await db.execute_fetchone("SELECT silo_items, barn_items FROM users WHERE user_id = ?", (user_id,))
        if is_silo_item(item_key):
            return parse_json_field(row["silo_items"]).get(item_key, 0)
        return parse_json_field(row["barn_items"]).get(item_key, 0)

async def add_xp_and_check_level(user_id: int, xp_gain: int) -> tuple[int, bool, int]:
    async with await get_db() as db:
        row = await db.execute_fetchone("SELECT xp, level FROM users WHERE user_id = ?", (user_id,))
        old_xp = row["xp"]
        old_level = row["level"]
        new_xp = old_xp + xp_gain
        new_level = get_level_from_xp(new_xp)
        leveled_up = new_level > old_level
        await db.execute("UPDATE users SET xp = ?, level = ? WHERE user_id = ?", (new_xp, new_level, user_id))
        await db.commit()
        return new_level, leveled_up, new_xp


# ─── CROPS ───────────────────────────────────────────────────────────────────

async def get_plots(user_id: int) -> list[dict]:
    async with await get_db() as db:
        rows = await db.execute_fetchall(
            "SELECT * FROM plots WHERE user_id = ? ORDER BY slot", (user_id,)
        )
        return [dict(r) for r in rows]

async def plant_crop(user_id: int, slot: int, crop_key: str) -> tuple[bool, str]:
    if crop_key not in CROPS:
        return False, "❓ Unknown crop."
    crop = CROPS[crop_key]

    async with await get_db() as db:
        user = dict(await db.execute_fetchone("SELECT * FROM users WHERE user_id = ?", (user_id,)))
        if crop["level_req"] > user["level"]:
            return False, f"🔒 Requires Level {crop['level_req']}."

        plot = await db.execute_fetchone("SELECT * FROM plots WHERE user_id = ? AND slot = ?", (user_id, slot))
        if not plot or plot["status"] not in ("empty",):
            return False, "🌱 This plot is not empty."

        seed_cost = crop["seed_cost"]
        if user["coins"] < seed_cost:
            return False, f"💰 Need {seed_cost} coins for seeds (you have {user['coins']})."

        now = utcnow()
        ready_at = now + timedelta(seconds=crop["grow_time"])

        await db.execute(
            "UPDATE plots SET crop=?, planted_at=?, ready_at=?, status='growing' WHERE user_id=? AND slot=?",
            (crop_key, now.isoformat(), ready_at.isoformat(), user_id, slot)
        )
        await db.execute("UPDATE users SET coins = coins - ? WHERE user_id = ?", (seed_cost, user_id))
        await db.commit()
        return True, f"✅ Planted {crop['emoji']} {crop['name']}! Ready in {fmt_time(crop['grow_time'])}."

async def harvest_crop(user_id: int, slot: int) -> tuple[bool, str]:
    async with await get_db() as db:
        plot = await db.execute_fetchone(
            "SELECT * FROM plots WHERE user_id = ? AND slot = ?", (user_id, slot)
        )
        if not plot or plot["status"] != "growing":
            return False, "Nothing to harvest here."

        ready_at = datetime.fromisoformat(plot["ready_at"])
        if ready_at.tzinfo is None:
            ready_at = ready_at.replace(tzinfo=timezone.utc)
        now = utcnow()

        if now < ready_at:
            remaining = int((ready_at - now).total_seconds())
            return False, f"⏳ {CROPS[plot['crop']]['emoji']} {CROPS[plot['crop']]['name']} not ready yet! ({fmt_time(remaining)} left)"

        crop_key = plot["crop"]
        crop = CROPS[crop_key]

        ok, msg = await add_to_inventory(user_id, crop_key, 1)
        if not ok:
            return False, msg

        await db.execute("UPDATE plots SET crop=NULL, planted_at=NULL, ready_at=NULL, status='empty' WHERE user_id=? AND slot=?",
                         (user_id, slot))

        bonus_drop = ""
        bonus_rate = float(await get_setting("bonus_drop_rate", BONUS_DROP_RATE))
        if random.random() < bonus_rate:
            all_tools = list(UPGRADE_TOOLS.keys()) + list(CLEARING_TOOLS.keys()) + list(EXPANSION_TOOLS.keys())
            bonus_item = random.choice(all_tools)
            ok2, _ = await add_to_inventory(user_id, bonus_item, 1)
            if ok2:
                b_emoji = get_item_emoji(bonus_item)
                b_name = get_item_name(bonus_item)
                bonus_drop = f"\n🎁 Bonus drop: {b_emoji} {b_name}!"

        await db.execute("UPDATE users SET total_harvests = total_harvests + 1 WHERE user_id = ?", (user_id,))
        await db.commit()

    new_level, leveled_up, _ = await add_xp_and_check_level(user_id, crop["xp"])
    level_msg = f"\n🎉 Level Up! You're now Level {new_level}!" if leveled_up else ""
    return True, f"✅ Harvested {crop['emoji']} {crop['name']}! +{crop['xp']} XP{bonus_drop}{level_msg}"

async def harvest_all(user_id: int) -> tuple[int, int, str]:
    plots = await get_plots(user_id)
    now = utcnow()
    harvested = 0
    failed = 0
    details = []
    for p in plots:
        if p["status"] == "growing":
            ready_at = datetime.fromisoformat(p["ready_at"])
            if ready_at.tzinfo is None:
                ready_at = ready_at.replace(tzinfo=timezone.utc)
            if now >= ready_at:
                ok, msg = await harvest_crop(user_id, p["slot"])
                if ok:
                    harvested += 1
                else:
                    failed += 1
    return harvested, failed, ""


# ─── ANIMALS ─────────────────────────────────────────────────────────────────

async def get_animal_pens(user_id: int) -> list[dict]:
    async with await get_db() as db:
        rows = await db.execute_fetchall(
            "SELECT * FROM animal_pens WHERE user_id = ? ORDER BY slot", (user_id,)
        )
        return [dict(r) for r in rows]

async def buy_animal(user_id: int, slot: int, animal_key: str) -> tuple[bool, str]:
    if animal_key not in ANIMALS:
        return False, "❓ Unknown animal."
    animal = ANIMALS[animal_key]
    async with await get_db() as db:
        user = dict(await db.execute_fetchone("SELECT * FROM users WHERE user_id = ?", (user_id,)))
        if animal["level_req"] > user["level"]:
            return False, f"🔒 Requires Level {animal['level_req']}."
        if user["coins"] < animal["buy_cost"]:
            return False, f"💰 Need {animal['buy_cost']} coins (you have {user['coins']})."

        pen = await db.execute_fetchone("SELECT * FROM animal_pens WHERE user_id = ? AND slot = ?", (user_id, slot))
        if not pen:
            return False, "❌ Invalid pen slot."
        if pen["status"] != "empty":
            return False, f"🐾 This pen already has a {pen['animal']}."

        now = utcnow()
        ready_at = now + timedelta(seconds=animal["feed_time"])
        await db.execute(
            "UPDATE animal_pens SET animal=?, fed_at=?, ready_at=?, status='producing' WHERE user_id=? AND slot=?",
            (animal_key, now.isoformat(), ready_at.isoformat(), user_id, slot)
        )
        await db.execute("UPDATE users SET coins = coins - ? WHERE user_id = ?", (animal["buy_cost"], user_id))
        await db.commit()
        return True, f"✅ {animal['emoji']} {animal['name']} moved in! First product ready in {fmt_time(animal['feed_time'])}."

async def collect_animal(user_id: int, slot: int) -> tuple[bool, str]:
    async with await get_db() as db:
        pen = await db.execute_fetchone("SELECT * FROM animal_pens WHERE user_id = ? AND slot = ?", (user_id, slot))
        if not pen or pen["status"] == "empty":
            return False, "No animal here."
        if pen["status"] != "producing":
            return False, "Animal is not ready yet."

        ready_at = datetime.fromisoformat(pen["ready_at"])
        if ready_at.tzinfo is None:
            ready_at = ready_at.replace(tzinfo=timezone.utc)
        now = utcnow()

        if now < ready_at:
            remaining = int((ready_at - now).total_seconds())
            animal = ANIMALS[pen["animal"]]
            return False, f"⏳ {animal['emoji']} {animal['name']} needs {fmt_time(remaining)} more."

        animal_key = pen["animal"]
        animal = ANIMALS[animal_key]
        product = animal["product"]

        ok, msg = await add_to_inventory(user_id, product, 1)
        if not ok:
            return False, msg

        next_ready = now + timedelta(seconds=animal["feed_time"])
        await db.execute(
            "UPDATE animal_pens SET fed_at=?, ready_at=?, status='producing' WHERE user_id=? AND slot=?",
            (now.isoformat(), next_ready.isoformat(), user_id, slot)
        )
        await db.commit()

    new_level, leveled_up, _ = await add_xp_and_check_level(user_id, 3)
    level_msg = f"\n🎉 Level Up! You're now Level {new_level}!" if leveled_up else ""
    return True, f"✅ Collected {animal['prod_emoji']} {get_item_name(product)} from {animal['emoji']} {animal['name']}!{level_msg}"


# ─── BUILDINGS ───────────────────────────────────────────────────────────────

async def get_user_buildings(user_id: int) -> list[dict]:
    async with await get_db() as db:
        rows = await db.execute_fetchall(
            "SELECT * FROM buildings WHERE user_id = ? ORDER BY building, slot", (user_id,)
        )
        return [dict(r) for r in rows]

async def buy_building(user_id: int, building_key: str) -> tuple[bool, str]:
    if building_key not in BUILDINGS:
        return False, "❓ Unknown building."
    bld = BUILDINGS[building_key]
    async with await get_db() as db:
        user = dict(await db.execute_fetchone("SELECT * FROM users WHERE user_id = ?", (user_id,)))
        if bld["level_req"] > user["level"]:
            return False, f"🔒 Requires Level {bld['level_req']}."
        if user["coins"] < bld["buy_cost"]:
            return False, f"💰 Need {bld['buy_cost']} coins."

        existing = await db.execute_fetchone(
            "SELECT id FROM buildings WHERE user_id = ? AND building = ? AND slot = 0", (user_id, building_key)
        )
        if existing:
            return False, f"🏭 You already own a {bld['name']}."

        for slot in range(bld["slots"]):
            await db.execute("""
                INSERT OR IGNORE INTO buildings (user_id, building, slot, status)
                VALUES (?, ?, ?, 'idle')
            """, (user_id, building_key, slot))

        await db.execute("UPDATE users SET coins = coins - ? WHERE user_id = ?", (bld["buy_cost"], user_id))
        await db.commit()
        return True, f"✅ {bld['emoji']} {bld['name']} built! You have {bld['slots']} production slot(s)."

async def start_production(user_id: int, building_key: str, recipe_key: str) -> tuple[bool, str]:
    if building_key not in BUILDINGS:
        return False, "❓ Unknown building."
    bld = BUILDINGS[building_key]
    if recipe_key not in bld["recipes"]:
        return False, "❓ Unknown recipe."
    recipe = bld["recipes"][recipe_key]

    async with await get_db() as db:
        slots = await db.execute_fetchall(
            "SELECT * FROM buildings WHERE user_id = ? AND building = ? ORDER BY slot", (user_id, building_key)
        )
        slots = [dict(s) for s in slots]
        if not slots:
            return False, f"🏭 You don't own a {bld['name']}. Buy it first!"

        free_slot = next((s for s in slots if s["status"] == "idle"), None)
        if not free_slot:
            return False, f"⚙️ All {bld['name']} slots are busy!"

        # Check and consume ingredients
        for ing, qty in recipe["inputs"].items():
            count = await get_item_count(user_id, ing)
            if count < qty:
                ing_emoji = get_item_emoji(ing)
                return False, f"❌ Need {qty}x {ing_emoji} {get_item_name(ing)} (you have {count})."

        for ing, qty in recipe["inputs"].items():
            await remove_from_inventory(user_id, ing, qty)

        now = utcnow()
        ready_at = now + timedelta(seconds=recipe["time"])
        await db.execute("""
            UPDATE buildings SET item=?, started_at=?, ready_at=?, status='producing'
            WHERE user_id=? AND building=? AND slot=?
        """, (recipe_key, now.isoformat(), ready_at.isoformat(), user_id, building_key, free_slot["slot"]))
        await db.commit()

        out_emoji = PROCESSED_EMOJI.get(recipe_key, "📦")
        return True, f"✅ {out_emoji} {get_item_name(recipe_key)} production started! Ready in {fmt_time(recipe['time'])}."

async def collect_production(user_id: int, building_key: str, slot: int) -> tuple[bool, str]:
    async with await get_db() as db:
        bld_slot = await db.execute_fetchone(
            "SELECT * FROM buildings WHERE user_id=? AND building=? AND slot=?", (user_id, building_key, slot)
        )
        if not bld_slot:
            return False, "❓ Building slot not found."
        bld_slot = dict(bld_slot)
        if bld_slot["status"] != "producing":
            return False, "Nothing to collect here."

        ready_at = datetime.fromisoformat(bld_slot["ready_at"])
        if ready_at.tzinfo is None:
            ready_at = ready_at.replace(tzinfo=timezone.utc)
        if utcnow() < ready_at:
            remaining = int((ready_at - utcnow()).total_seconds())
            item_emoji = PROCESSED_EMOJI.get(bld_slot["item"], "📦")
            return False, f"⏳ {item_emoji} {get_item_name(bld_slot['item'])} ready in {fmt_time(remaining)}."

        recipe_key = bld_slot["item"]
        bld = BUILDINGS[building_key]
        recipe = bld["recipes"].get(recipe_key, {})

        ok, msg = await add_to_inventory(user_id, recipe_key, 1)
        if not ok:
            return False, msg

        await db.execute("UPDATE buildings SET item=NULL, started_at=NULL, ready_at=NULL, status='idle' WHERE user_id=? AND building=? AND slot=?",
                         (user_id, building_key, slot))
        await db.commit()

    new_level, leveled_up, _ = await add_xp_and_check_level(user_id, recipe.get("xp", 5))
    level_msg = f"\n🎉 Level Up! You're now Level {new_level}!" if leveled_up else ""
    out_emoji = PROCESSED_EMOJI.get(recipe_key, "📦")
    return True, f"✅ Collected {out_emoji} {get_item_name(recipe_key)}! +{recipe.get('xp',5)} XP{level_msg}"


# ─── ORDERS ──────────────────────────────────────────────────────────────────

import random as _random

def _generate_order(user_level: int) -> dict:
    all_items = []
    for crop_k, crop_v in CROPS.items():
        if crop_v["level_req"] <= user_level:
            all_items.append((crop_k, crop_v["sell_price"]))
    for bld in BUILDINGS.values():
        for rec_k, rec_v in bld["recipes"].items():
            all_items.append((rec_k, rec_v["sell_price"]))

    if not all_items:
        all_items = [("wheat", 5)]

    num_items = _random.randint(1, min(3, len(all_items)))
    selected = _random.sample(all_items, num_items)
    items = {}
    total_value = 0
    for item_key, base_price in selected:
        qty = _random.randint(1, 4)
        items[item_key] = qty
        total_value += base_price * qty

    reward_coins = int(total_value * 1.4)
    reward_xp = max(5, int(total_value // 10))
    return {"items": items, "reward_coins": reward_coins, "reward_xp": reward_xp}

async def ensure_orders(user_id: int, user_level: int):
    async with await get_db() as db:
        existing = await db.execute_fetchall(
            "SELECT slot FROM orders WHERE user_id = ? AND status = 'active'", (user_id,)
        )
        used_slots = {r["slot"] for r in existing}
        for slot in range(9):
            if slot not in used_slots:
                order = _generate_order(user_level)
                await db.execute("""
                    INSERT INTO orders (user_id, slot, items, reward_coins, reward_xp, status)
                    VALUES (?, ?, ?, ?, ?, 'active')
                """, (user_id, slot, json.dumps(order["items"]), order["reward_coins"], order["reward_xp"]))
        await db.commit()

async def get_orders(user_id: int) -> list[dict]:
    async with await get_db() as db:
        rows = await db.execute_fetchall(
            "SELECT * FROM orders WHERE user_id = ? AND status = 'active' ORDER BY slot", (user_id,)
        )
        return [dict(r) for r in rows]

async def fulfill_order(user_id: int, order_id: int) -> tuple[bool, str]:
    async with await get_db() as db:
        order = await db.execute_fetchone(
            "SELECT * FROM orders WHERE id = ? AND user_id = ? AND status = 'active'", (order_id, user_id)
        )
        if not order:
            return False, "❌ Order not found."
        order = dict(order)
        items_needed = json.loads(order["items"])

        # Check all items available
        for item_key, qty in items_needed.items():
            have = await get_item_count(user_id, item_key)
            if have < qty:
                emoji = get_item_emoji(item_key)
                return False, f"❌ Need {qty}x {emoji} {get_item_name(item_key)} (have {have})."

        # Remove items
        for item_key, qty in items_needed.items():
            await remove_from_inventory(user_id, item_key, qty)

        # Give rewards
        coins = order["reward_coins"]
        double = await get_setting("double_coins", "0")
        if double == "1":
            coins *= 2

        await db.execute("UPDATE users SET coins = coins + ?, total_sales = total_sales + 1 WHERE user_id = ?", (coins, user_id))
        await db.execute("UPDATE orders SET status = 'completed' WHERE id = ?", (order_id,))
        await db.commit()

    # Generate replacement
    user_row = await get_user_full(user_id)
    new_order = _generate_order(user_row["level"])
    async with await get_db() as db:
        await db.execute("""
            INSERT INTO orders (user_id, slot, items, reward_coins, reward_xp, status)
            VALUES (?, ?, ?, ?, ?, 'active')
        """, (user_id, order["slot"], json.dumps(new_order["items"]), new_order["reward_coins"], new_order["reward_xp"]))
        await db.commit()

    new_level, leveled_up, _ = await add_xp_and_check_level(user_id, order["reward_xp"])
    level_msg = f"\n🎉 Level Up! You're now Level {new_level}!" if leveled_up else ""
    return True, f"✅ Order fulfilled! +{coins} 💰 +{order['reward_xp']} XP{level_msg}"


# ─── MARKET ──────────────────────────────────────────────────────────────────

async def list_item_on_market(user_id: int, seller_name: str, item_key: str, qty: int, price: int) -> tuple[bool, str]:
    max_price = int(await get_setting("max_market_price", "9999"))
    max_listings = int(await get_setting("max_market_listings", "5"))

    if price > max_price:
        return False, f"💰 Max price is {max_price} coins per item."
    if qty < 1 or price < 1:
        return False, "❌ Qty and price must be positive."

    async with await get_db() as db:
        count = await db.execute_fetchone(
            "SELECT COUNT(*) as c FROM market_listings WHERE seller_id = ?", (user_id,)
        )
        if count["c"] >= max_listings:
            return False, f"🏪 You can only have {max_listings} listings. Remove one first."

    ok, msg = await remove_from_inventory(user_id, item_key, qty)
    if not ok:
        return False, msg

    async with await get_db() as db:
        await db.execute("""
            INSERT INTO market_listings (seller_id, seller_name, item, qty, price)
            VALUES (?, ?, ?, ?, ?)
        """, (user_id, seller_name, item_key, qty, price))
        await db.commit()

    emoji = get_item_emoji(item_key)
    return True, f"✅ Listed {qty}x {emoji} {get_item_name(item_key)} at {price} coins each."

async def get_market_listings(page: int = 0, per_page: int = 9) -> list[dict]:
    async with await get_db() as db:
        rows = await db.execute_fetchall(
            "SELECT * FROM market_listings ORDER BY listed_at DESC LIMIT ? OFFSET ?",
            (per_page, page * per_page)
        )
        return [dict(r) for r in rows]

async def buy_from_market(buyer_id: int, listing_id: int) -> tuple[bool, str]:
    async with await get_db() as db:
        listing = await db.execute_fetchone("SELECT * FROM market_listings WHERE id = ?", (listing_id,))
        if not listing:
            return False, "❌ Listing not found."
        listing = dict(listing)

        if listing["seller_id"] == buyer_id:
            return False, "❌ You can't buy your own listing!"

        buyer = dict(await db.execute_fetchone("SELECT * FROM users WHERE user_id = ?", (buyer_id,)))
        total_cost = listing["price"] * listing["qty"]
        if buyer["coins"] < total_cost:
            return False, f"💰 Not enough coins! Need {total_cost}, have {buyer['coins']}."

        ok, msg = await add_to_inventory(buyer_id, listing["item"], listing["qty"])
        if not ok:
            return False, msg

        await db.execute("UPDATE users SET coins = coins - ? WHERE user_id = ?", (total_cost, buyer_id))
        await db.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (total_cost, listing["seller_id"]))
        await db.execute("DELETE FROM market_listings WHERE id = ?", (listing_id,))
        await db.commit()

        emoji = get_item_emoji(listing["item"])
        return True, f"✅ Bought {listing['qty']}x {emoji} {get_item_name(listing['item'])} for {total_cost} 💰!"

async def remove_market_listing(user_id: int, listing_id: int) -> tuple[bool, str]:
    async with await get_db() as db:
        listing = await db.execute_fetchone(
            "SELECT * FROM market_listings WHERE id = ? AND seller_id = ?", (listing_id, user_id)
        )
        if not listing:
            return False, "❌ Listing not found."
        listing = dict(listing)

        ok, msg = await add_to_inventory(user_id, listing["item"], listing["qty"])
        if not ok:
            return False, f"❌ Could not return items: {msg}"

        await db.execute("DELETE FROM market_listings WHERE id = ?", (listing_id,))
        await db.commit()
        return True, f"✅ Listing removed. Items returned to your storage."


# ─── LAND CLEARING ───────────────────────────────────────────────────────────

async def get_obstacles(user_id: int) -> list[dict]:
    async with await get_db() as db:
        rows = await db.execute_fetchall(
            "SELECT * FROM obstacles WHERE user_id = ? ORDER BY slot", (user_id,)
        )
        return [dict(r) for r in rows]

async def generate_obstacles_for_expansion(user_id: int, new_slots: list[int]):
    obstacle_types = list(OBSTACLES.keys())
    async with await get_db() as db:
        for slot in new_slots:
            obs = _random.choice(obstacle_types)
            await db.execute(
                "INSERT OR IGNORE INTO obstacles (user_id, slot, obstacle) VALUES (?, ?, ?)",
                (user_id, slot, obs)
            )
        await db.commit()

async def clear_obstacle(user_id: int, slot: int) -> tuple[bool, str]:
    async with await get_db() as db:
        obs_row = await db.execute_fetchone("SELECT * FROM obstacles WHERE user_id=? AND slot=?", (user_id, slot))
        if not obs_row:
            return False, "No obstacle here."
        obs_row = dict(obs_row)
        obs = OBSTACLES[obs_row["obstacle"]]

        tool = obs["tool"]
        have = await get_item_count(user_id, tool)
        if have < 1:
            tool_emoji = get_item_emoji(tool)
            return False, f"❌ You need a {tool_emoji} {get_item_name(tool)} to clear this {obs['emoji']} {obs['name']}."

        await remove_from_inventory(user_id, tool, 1)
        await db.execute("DELETE FROM obstacles WHERE user_id=? AND slot=?", (user_id, slot))

        # Now create a new empty plot at this slot
        await db.execute("INSERT OR IGNORE INTO plots (user_id, slot, status) VALUES (?, ?, 'empty')", (user_id, slot))
        await db.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (obs["coins"], user_id))
        await db.commit()

    new_level, leveled_up, _ = await add_xp_and_check_level(user_id, obs["xp"])
    level_msg = f"\n🎉 Level Up! You're now Level {new_level}!" if leveled_up else ""
    return True, f"✅ Cleared {obs['emoji']} {obs['name']}! +{obs['coins']} 💰 +{obs['xp']} XP{level_msg}"


# ─── UPGRADES ────────────────────────────────────────────────────────────────

async def upgrade_silo(user_id: int) -> tuple[bool, str]:
    async with await get_db() as db:
        user = dict(await db.execute_fetchone("SELECT * FROM users WHERE user_id = ?", (user_id,)))
        barn = parse_json_field(user["barn_items"])
        cost = SILO_UPGRADE["cost_per_upgrade"]
        tools = SILO_UPGRADE["tools_needed"]

        if user["coins"] < cost:
            return False, f"💰 Need {cost} coins (have {user['coins']})."

        missing = []
        for tool, qty in tools.items():
            have = barn.get(tool, 0)
            if have < qty:
                emoji = get_item_emoji(tool)
                missing.append(f"{qty}x {emoji} {get_item_name(tool)} (have {have})")
        if missing:
            return False, "❌ Missing: " + ", ".join(missing)

        for tool, qty in tools.items():
            barn[tool] = barn.get(tool, 0) - qty
            if barn[tool] <= 0:
                del barn[tool]

        new_cap = user["silo_cap"] + SILO_UPGRADE["upgrade_amount"]
        new_lv = user["silo_level"] + 1
        await db.execute("UPDATE users SET silo_cap=?, silo_level=?, barn_items=?, coins=coins-? WHERE user_id=?",
                         (new_cap, new_lv, dump_json_field(barn), cost, user_id))
        await db.commit()
        return True, f"✅ Silo upgraded to Level {new_lv}! Capacity: {new_cap} 📦"

async def upgrade_barn(user_id: int) -> tuple[bool, str]:
    async with await get_db() as db:
        user = dict(await db.execute_fetchone("SELECT * FROM users WHERE user_id = ?", (user_id,)))
        barn = parse_json_field(user["barn_items"])
        cost = BARN_UPGRADE["cost_per_upgrade"]
        tools = BARN_UPGRADE["tools_needed"]

        if user["coins"] < cost:
            return False, f"💰 Need {cost} coins (have {user['coins']})."

        missing = []
        for tool, qty in tools.items():
            have = barn.get(tool, 0)
            if have < qty:
                emoji = get_item_emoji(tool)
                missing.append(f"{qty}x {emoji} {get_item_name(tool)} (have {have})")
        if missing:
            return False, "❌ Missing: " + ", ".join(missing)

        for tool, qty in tools.items():
            barn[tool] = barn.get(tool, 0) - qty
            if barn[tool] <= 0:
                del barn[tool]

        new_cap = user["barn_cap"] + BARN_UPGRADE["upgrade_amount"]
        new_lv = user["barn_level"] + 1
        await db.execute("UPDATE users SET barn_cap=?, barn_level=?, barn_items=?, coins=coins-? WHERE user_id=?",
                         (new_cap, new_lv, dump_json_field(barn), cost, user_id))
        await db.commit()
        return True, f"✅ Barn upgraded to Level {new_lv}! Capacity: {new_cap} 📦"

async def expand_farm(user_id: int) -> tuple[bool, str]:
    async with await get_db() as db:
        user = dict(await db.execute_fetchone("SELECT * FROM users WHERE user_id = ?", (user_id,)))
        barn = parse_json_field(user["barn_items"])

        # Check expansion tools
        required = {"land_deed": 1, "mallet": 1, "marker_stake": 1}
        missing = []
        for tool, qty in required.items():
            have = barn.get(tool, 0)
            if have < qty:
                emoji = get_item_emoji(tool)
                missing.append(f"{emoji} {get_item_name(tool)}")
        if missing:
            return False, f"❌ Need: {', '.join(missing)}"

        cost = user["plots"] * 200
        if user["coins"] < cost:
            return False, f"💰 Expansion costs {cost} coins (have {user['coins']})."

        for tool, qty in required.items():
            barn[tool] = barn.get(tool, 0) - qty
            if barn[tool] <= 0:
                del barn[tool]

        current_plots = user["plots"]
        new_plots = current_plots + PLOTS_PER_EXPANSION
        new_slots = list(range(current_plots, new_plots))

        await db.execute("UPDATE users SET plots=?, barn_items=?, coins=coins-? WHERE user_id=?",
                         (new_plots, dump_json_field(barn), cost, user_id))
        await db.commit()

    await generate_obstacles_for_expansion(user_id, new_slots)
    return True, f"✅ Farm expanded! +{PLOTS_PER_EXPANSION} plots (now {new_plots} total). Clear obstacles to use new land!"

async def expand_animal_pens(user_id: int) -> tuple[bool, str]:
    async with await get_db() as db:
        user = dict(await db.execute_fetchone("SELECT * FROM users WHERE user_id = ?", (user_id,)))
        barn = parse_json_field(user["barn_items"])

        required = {"land_deed": 1, "construction_permit": 1}
        missing = []
        for tool, qty in required.items():
            have = barn.get(tool, 0)
            if have < qty:
                emoji = get_item_emoji(tool)
                missing.append(f"{emoji} {get_item_name(tool)}")
        if missing:
            return False, f"❌ Need: {', '.join(missing)}"

        cost = user["animal_pens"] * 500
        if user["coins"] < cost:
            return False, f"💰 Costs {cost} coins (have {user['coins']})."

        for tool, qty in required.items():
            barn[tool] = barn.get(tool, 0) - qty
            if barn[tool] <= 0:
                del barn[tool]

        current_pens = user["animal_pens"]
        new_pens = current_pens + 2

        for slot in range(current_pens, new_pens):
            await db.execute("INSERT OR IGNORE INTO animal_pens (user_id, slot, status) VALUES (?, ?, 'empty')", (user_id, slot))

        await db.execute("UPDATE users SET animal_pens=?, barn_items=?, coins=coins-? WHERE user_id=?",
                         (new_pens, dump_json_field(barn), cost, user_id))
        await db.commit()
        return True, f"✅ +2 animal pens! (now {new_pens} total)"


# ─── SELL / DAILY ─────────────────────────────────────────────────────────────

async def sell_item(user_id: int, item_key: str, qty: int) -> tuple[bool, str]:
    price = 0
    if item_key in CROPS:
        price = CROPS[item_key]["sell_price"]
    else:
        for bld in BUILDINGS.values():
            if item_key in bld["recipes"]:
                price = bld["recipes"][item_key]["sell_price"]
                break

    if price == 0:
        return False, "❌ This item cannot be sold directly."

    ok, msg = await remove_from_inventory(user_id, item_key, qty)
    if not ok:
        return False, msg

    total = price * qty
    double = await get_setting("double_coins", "0")
    if double == "1":
        total *= 2

    async with await get_db() as db:
        await db.execute("UPDATE users SET coins = coins + ?, total_sales = total_sales + 1 WHERE user_id = ?", (total, user_id))
        await db.commit()

    emoji = get_item_emoji(item_key)
    return True, f"✅ Sold {qty}x {emoji} {get_item_name(item_key)} for {total} 💰!"

async def claim_daily(user_id: int) -> tuple[bool, str]:
    from datetime import date
    async with await get_db() as db:
        user = dict(await db.execute_fetchone("SELECT * FROM users WHERE user_id = ?", (user_id,)))
        today = date.today().isoformat()
        if user["last_daily"] == today:
            return False, "⏰ Already claimed today's reward! Come back tomorrow."

        coins = 100 + (user["level"] * 10)
        xp = 20 + (user["level"] * 2)
        await db.execute("UPDATE users SET coins = coins + ?, last_daily = ? WHERE user_id = ?", (coins, today, user_id))
        await db.commit()

    new_level, leveled_up, _ = await add_xp_and_check_level(user_id, xp)
    level_msg = f"\n🎉 Level Up! You're now Level {new_level}!" if leveled_up else ""
    return True, f"🎁 Daily reward claimed!\n+{coins} 💰  +{xp} XP{level_msg}"


# ─── HELPERS ─────────────────────────────────────────────────────────────────

async def get_user_full(user_id: int) -> dict | None:
    async with await get_db() as db:
        row = await db.execute_fetchone("SELECT * FROM users WHERE user_id = ?", (user_id,))
        if row:
            return dict(row)
        return None
