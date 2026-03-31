# game/data.py - Master game data for Harvest Kingdom

CROPS = {
    "wheat":      {"name": "Wheat",      "emoji": "🌾", "grow_time": 120,    "sell_price": 5,   "xp": 1,  "level_req": 1,  "seed_cost": 5},
    "corn":       {"name": "Corn",       "emoji": "🌽", "grow_time": 300,    "sell_price": 12,  "xp": 2,  "level_req": 1,  "seed_cost": 10},
    "carrot":     {"name": "Carrot",     "emoji": "🥕", "grow_time": 600,    "sell_price": 18,  "xp": 3,  "level_req": 2,  "seed_cost": 15},
    "soybean":    {"name": "Soybean",    "emoji": "🫘", "grow_time": 1200,   "sell_price": 30,  "xp": 4,  "level_req": 3,  "seed_cost": 25},
    "sugarcane":  {"name": "Sugarcane",  "emoji": "🎋", "grow_time": 1800,   "sell_price": 45,  "xp": 5,  "level_req": 4,  "seed_cost": 35},
    "rice":       {"name": "Rice",       "emoji": "🌾", "grow_time": 2700,   "sell_price": 60,  "xp": 6,  "level_req": 5,  "seed_cost": 50},
    "pumpkin":    {"name": "Pumpkin",    "emoji": "🎃", "grow_time": 3600,   "sell_price": 90,  "xp": 8,  "level_req": 7,  "seed_cost": 75},
    "cotton":     {"name": "Cotton",     "emoji": "☁️", "grow_time": 7200,   "sell_price": 140, "xp": 10, "level_req": 10, "seed_cost": 110},
    "potato":     {"name": "Potato",     "emoji": "🥔", "grow_time": 10800,  "sell_price": 200, "xp": 12, "level_req": 12, "seed_cost": 160},
    "tomato":     {"name": "Tomato",     "emoji": "🍅", "grow_time": 21600,  "sell_price": 340, "xp": 15, "level_req": 15, "seed_cost": 280},
    "strawberry": {"name": "Strawberry", "emoji": "🍓", "grow_time": 28800,  "sell_price": 500, "xp": 20, "level_req": 18, "seed_cost": 400},
}

ANIMALS = {
    "chicken": {"name": "Chicken", "emoji": "🐔", "product": "egg",         "prod_emoji": "🥚", "feed_time": 3600,   "buy_cost": 150,  "sell_price": 30,  "level_req": 2},
    "cow":     {"name": "Cow",     "emoji": "🐄", "product": "milk",        "prod_emoji": "🥛", "feed_time": 7200,   "buy_cost": 450,  "sell_price": 60,  "level_req": 5},
    "pig":     {"name": "Pig",     "emoji": "🐷", "product": "bacon",       "prod_emoji": "🥩", "feed_time": 10800,  "buy_cost": 600,  "sell_price": 80,  "level_req": 7},
    "sheep":   {"name": "Sheep",   "emoji": "🐑", "product": "wool",        "prod_emoji": "🧶", "feed_time": 14400,  "buy_cost": 800,  "sell_price": 100, "level_req": 9},
    "goat":    {"name": "Goat",    "emoji": "🐐", "product": "goat_milk",   "prod_emoji": "🧴", "feed_time": 10800,  "buy_cost": 700,  "sell_price": 90,  "level_req": 10},
    "bee":     {"name": "Bee",     "emoji": "🐝", "product": "honey",       "prod_emoji": "🍯", "feed_time": 18000,  "buy_cost": 1000, "sell_price": 130, "level_req": 12},
    "duck":    {"name": "Duck",    "emoji": "🦆", "product": "feather",     "prod_emoji": "🪶", "feed_time": 7200,   "buy_cost": 350,  "sell_price": 50,  "level_req": 8},
    "fish":    {"name": "Fish",    "emoji": "🐟", "product": "fish",        "prod_emoji": "🐠", "feed_time": 5400,   "buy_cost": 400,  "sell_price": 70,  "level_req": 6},
    "lobster": {"name": "Lobster", "emoji": "🦞", "product": "lobster",     "prod_emoji": "🦀", "feed_time": 21600,  "buy_cost": 1500, "sell_price": 200, "level_req": 15},
    "buffalo": {"name": "Buffalo", "emoji": "🐃", "product": "mozzarella",  "prod_emoji": "🧀", "feed_time": 28800,  "buy_cost": 2000, "sell_price": 260, "level_req": 18},
}

BUILDINGS = {
    "bakery": {
        "name": "Bakery", "emoji": "🏭", "slots": 2,
        "buy_cost": 800, "level_req": 3,
        "recipes": {
            "bread":      {"inputs": {"wheat": 3}, "time": 300, "xp": 5, "sell_price": 35},
            "popcorn":    {"inputs": {"corn": 2},  "time": 600, "xp": 8, "sell_price": 55},
        }
    },
    "feed_mill": {
        "name": "Feed Mill", "emoji": "⚙️", "slots": 2,
        "buy_cost": 600, "level_req": 2,
        "recipes": {
            "chicken_feed": {"inputs": {"wheat": 2, "corn": 1}, "time": 180, "xp": 3, "sell_price": 20},
            "cow_feed":     {"inputs": {"corn": 3, "soybean": 1}, "time": 360, "xp": 5, "sell_price": 35},
        }
    },
    "dairy": {
        "name": "Dairy", "emoji": "🧈", "slots": 2,
        "buy_cost": 1200, "level_req": 6,
        "recipes": {
            "butter":      {"inputs": {"milk": 2},                    "time": 900,  "xp": 12, "sell_price": 90},
            "cheese":      {"inputs": {"milk": 3, "goat_milk": 1},    "time": 1800, "xp": 20, "sell_price": 160},
            "syrup":       {"inputs": {"sugarcane": 4},               "time": 1200, "xp": 15, "sell_price": 120},
        }
    },
    "textile_mill": {
        "name": "Textile Mill", "emoji": "🧵", "slots": 2,
        "buy_cost": 1800, "level_req": 11,
        "recipes": {
            "cotton_fabric": {"inputs": {"cotton": 2},              "time": 1800, "xp": 20, "sell_price": 180},
            "wool_sweater":  {"inputs": {"wool": 3},                "time": 3600, "xp": 30, "sell_price": 280},
        }
    },
    "kitchen": {
        "name": "Kitchen", "emoji": "👨‍🍳", "slots": 3,
        "buy_cost": 2500, "level_req": 14,
        "recipes": {
            "pumpkin_pie":          {"inputs": {"pumpkin": 2, "butter": 1, "sugar": 2},            "time": 3600,  "xp": 35, "sell_price": 380},
            "pizza":                {"inputs": {"bread": 1, "tomato": 2, "cheese": 1},             "time": 5400,  "xp": 50, "sell_price": 550},
            "strawberry_ice_cream": {"inputs": {"strawberry": 3, "milk": 2},                       "time": 7200,  "xp": 60, "sell_price": 700},
            "carrot_juice":         {"inputs": {"carrot": 4},                                      "time": 1200,  "xp": 18, "sell_price": 160},
            "chocolate_cake":       {"inputs": {"wheat": 4, "egg": 2, "milk": 2, "honey": 1},     "time": 10800, "xp": 80, "sell_price": 950},
            "sugar":                {"inputs": {"sugarcane": 2},                                   "time": 600,   "xp": 8,  "sell_price": 60},
        }
    },
}

UPGRADE_TOOLS = {
    # Barn tools
    "bolt":          {"name": "Bolt",         "emoji": "🔩", "type": "barn"},
    "plank":         {"name": "Plank",        "emoji": "🪵", "type": "barn"},
    "duct_tape":     {"name": "Duct Tape",    "emoji": "🩹", "type": "barn"},
    # Silo tools
    "nail":          {"name": "Nail",         "emoji": "📌", "type": "silo"},
    "screw":         {"name": "Screw",        "emoji": "🔧", "type": "silo"},
    "wood_panel":    {"name": "Wood Panel",   "emoji": "🪟", "type": "silo"},
    # General upgrade
    "paint":         {"name": "Paint",        "emoji": "🎨", "type": "general"},
    "brick":         {"name": "Brick",        "emoji": "🧱", "type": "general"},
    "cement":        {"name": "Cement",       "emoji": "🪣", "type": "general"},
    "sledgehammer":  {"name": "Sledgehammer", "emoji": "🔨", "type": "general"},
}

EXPANSION_TOOLS = {
    "land_deed":           {"name": "Land Deed",           "emoji": "📜"},
    "mallet":              {"name": "Mallet",              "emoji": "🪛"},
    "marker_stake":        {"name": "Marker Stake",        "emoji": "📍"},
    "construction_permit": {"name": "Construction Permit", "emoji": "📋"},
    "map_piece":           {"name": "Map Piece",           "emoji": "🗺️"},
    "compass":             {"name": "Compass",             "emoji": "🧭"},
    "mayors_signature":    {"name": "Mayor's Signature",   "emoji": "✍️"},
    "wire_cutter":         {"name": "Wire Cutter",         "emoji": "✂️"},
    "notary_letter":       {"name": "Notary Letter",       "emoji": "📩"},
    "city_plan":           {"name": "City Plan",           "emoji": "🏙️"},
}

CLEARING_TOOLS = {
    "axe":          {"name": "Axe",          "emoji": "🪓"},
    "saw":          {"name": "Saw",          "emoji": "🔪"},
    "dynamite":     {"name": "Dynamite",     "emoji": "🧨"},
    "tnt_barrel":   {"name": "TNT Barrel",   "emoji": "💣"},
    "shovel":       {"name": "Shovel",       "emoji": "🪚"},
    "crowbar":      {"name": "Crowbar",      "emoji": "🔱"},
    "rusty_hoe":    {"name": "Rusty Hoe",    "emoji": "⚒️"},
    "pest_spray":   {"name": "Pest Spray",   "emoji": "💨"},
    "trash_cart":   {"name": "Trash Cart",   "emoji": "🛒"},
    "mini_tractor": {"name": "Mini Tractor", "emoji": "🚜"},
}

OBSTACLES = {
    "small_tree":  {"name": "Small Tree",  "emoji": "🌱", "tool": "axe",      "coins": 50,  "xp": 5},
    "big_tree":    {"name": "Big Tree",    "emoji": "🌳", "tool": "saw",      "coins": 120, "xp": 12},
    "rock":        {"name": "Rock",        "emoji": "🪨", "tool": "dynamite", "coins": 100, "xp": 10},
    "big_rock":    {"name": "Big Rock",    "emoji": "⛰️",  "tool": "tnt_barrel","coins": 250, "xp": 25},
    "swamp":       {"name": "Swamp",       "emoji": "🌿", "tool": "shovel",   "coins": 80,  "xp": 8},
    "dead_bush":   {"name": "Dead Bush",   "emoji": "🪵", "tool": "rusty_hoe","coins": 40,  "xp": 4},
}

LEVEL_THRESHOLDS = [
    0, 50, 130, 250, 420, 650, 950, 1320, 1770, 2310,
    2950, 3700, 4570, 5570, 6710, 8000, 9450, 11070, 12870, 14860,
    17050, 19450, 22070, 24920, 28010, 31350, 34950, 38820, 42970, 47410,
]

BARN_UPGRADE = {
    "base_capacity": 50,
    "upgrade_amount": 25,
    "tools_needed": {"bolt": 3, "plank": 2, "duct_tape": 1},
    "cost_per_upgrade": 1000,
}

SILO_UPGRADE = {
    "base_capacity": 100,
    "upgrade_amount": 50,
    "tools_needed": {"nail": 3, "screw": 2, "wood_panel": 1},
    "cost_per_upgrade": 800,
}

PLOTS_PER_EXPANSION = 4
BASE_PLOTS = 8
BASE_ANIMAL_PENS = 2
BONUS_DROP_RATE = 0.05

ALL_ITEMS = {}
ALL_ITEMS.update({k: {"name": v["name"], "emoji": v["emoji"], "category": "crop"} for k, v in CROPS.items()})
ALL_ITEMS.update({k: {"name": v["name"], "emoji": v["emoji"], "category": "animal_product"} for k, v in {
    "egg": {"name": "Egg", "emoji": "🥚"},
    "milk": {"name": "Milk", "emoji": "🥛"},
    "bacon": {"name": "Bacon", "emoji": "🥩"},
    "wool": {"name": "Wool", "emoji": "🧶"},
    "goat_milk": {"name": "Goat Milk", "emoji": "🧴"},
    "honey": {"name": "Honey", "emoji": "🍯"},
    "feather": {"name": "Feather", "emoji": "🪶"},
    "fish": {"name": "Fish", "emoji": "🐠"},
    "lobster": {"name": "Lobster", "emoji": "🦀"},
    "mozzarella": {"name": "Mozzarella", "emoji": "🧀"},
}.items()})
for b_data in BUILDINGS.values():
    for r_key in b_data["recipes"]:
        ALL_ITEMS[r_key] = {"name": r_key.replace("_", " ").title(), "emoji": "🍽️", "category": "processed"}

ALL_ITEMS.update({k: {"name": v["name"], "emoji": v["emoji"], "category": "upgrade_tool"} for k, v in UPGRADE_TOOLS.items()})
ALL_ITEMS.update({k: {"name": v["name"], "emoji": v["emoji"], "category": "expansion_tool"} for k, v in EXPANSION_TOOLS.items()})
ALL_ITEMS.update({k: {"name": v["name"], "emoji": v["emoji"], "category": "clearing_tool"} for k, v in CLEARING_TOOLS.items()})

# Aliases for processed goods emojis
PROCESSED_EMOJI = {
    "bread": "🍞", "popcorn": "🍿", "butter": "🧈", "sugar": "🍬",
    "cotton_fabric": "🧵", "syrup": "🍯", "cheese": "🧀", "pumpkin_pie": "🥧",
    "wool_sweater": "🧥", "pizza": "🍕", "strawberry_ice_cream": "🍦",
    "carrot_juice": "🥤", "chocolate_cake": "🎂", "chicken_feed": "🌾",
    "cow_feed": "🌿",
}

def get_item_emoji(item_key: str) -> str:
    if item_key in CROPS:
        return CROPS[item_key]["emoji"]
    if item_key in UPGRADE_TOOLS:
        return UPGRADE_TOOLS[item_key]["emoji"]
    if item_key in EXPANSION_TOOLS:
        return EXPANSION_TOOLS[item_key]["emoji"]
    if item_key in CLEARING_TOOLS:
        return CLEARING_TOOLS[item_key]["emoji"]
    if item_key in PROCESSED_EMOJI:
        return PROCESSED_EMOJI[item_key]
    animal_products = {
        "egg": "🥚", "milk": "🥛", "bacon": "🥩", "wool": "🧶",
        "goat_milk": "🧴", "honey": "🍯", "feather": "🪶", "fish": "🐠",
        "lobster": "🦀", "mozzarella": "🧀",
    }
    return animal_products.get(item_key, "📦")

def get_item_name(item_key: str) -> str:
    for db in [CROPS, UPGRADE_TOOLS, EXPANSION_TOOLS, CLEARING_TOOLS]:
        if item_key in db:
            return db[item_key]["name"]
    for b in BUILDINGS.values():
        if item_key in b["recipes"]:
            return item_key.replace("_", " ").title()
    animal_products = {
        "egg": "Egg", "milk": "Milk", "bacon": "Bacon", "wool": "Wool",
        "goat_milk": "Goat Milk", "honey": "Honey", "feather": "Feather",
        "fish": "Fish", "lobster": "Lobster", "mozzarella": "Mozzarella",
    }
    return animal_products.get(item_key, item_key.replace("_", " ").title())

def get_level_from_xp(xp: int) -> int:
    level = 1
    for i, threshold in enumerate(LEVEL_THRESHOLDS):
        if xp >= threshold:
            level = i + 1
        else:
            break
    return min(level, len(LEVEL_THRESHOLDS))

def get_xp_for_next_level(level: int) -> int:
    if level >= len(LEVEL_THRESHOLDS):
        return LEVEL_THRESHOLDS[-1]
    return LEVEL_THRESHOLDS[level]
