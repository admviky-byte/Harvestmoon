# 🌾 Harvest Kingdom — Telegram Bot Game

A full-featured text-based farming simulator inspired by Hay Day & Township, built with `python-telegram-bot`.

---

## 🚀 Quick Deploy to Railway

1. **Fork / push this repo to GitHub**
2. Go to [railway.app](https://railway.app) → New Project → Deploy from GitHub
3. Set environment variables:
   - `BOT_TOKEN` → your bot token from [@BotFather](https://t.me/BotFather)
   - `ADMIN_IDS` → your Telegram user ID(s), comma-separated (get from [@userinfobot](https://t.me/userinfobot))
4. Deploy! Railway auto-detects Python and runs `python main.py`

---

## 🛠️ Local Setup

```bash
git clone <your-repo>
cd harvest_kingdom

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env and add your BOT_TOKEN and ADMIN_IDS

# Run
python main.py
```

---

## 🎮 Game Features

### 🌾 Farming
- 11 crops: Wheat, Corn, Carrot, Soybean, Sugarcane, Rice, Pumpkin, Cotton, Potato, Tomato, Strawberry
- Real-time grow timers (2 min → 8 hours)
- Harvest All button for convenience
- 5% bonus drop rate for tools when harvesting

### 🐾 Animals
- 10 animals: Chicken, Cow, Pig, Sheep, Goat, Bee, Duck, Fish, Lobster, Buffalo
- Each produces unique products (Eggs, Milk, Honey, etc.)
- Expandable pens

### 🏭 Factories (Production Chain)
- **Bakery** — Bread, Popcorn
- **Feed Mill** — Animal Feed
- **Dairy** — Butter, Cheese, Syrup
- **Textile Mill** — Cotton Fabric, Wool Sweater
- **Kitchen** — Pumpkin Pie, Pizza, Strawberry Ice Cream, Carrot Juice, Chocolate Cake, Sugar

### 📦 Storage
- **Silo** — Crops & animal products (upgradeable)
- **Barn** — Processed goods & tools (upgradeable)
- Upgrade with special tools from bonus drops

### 🚚 Truck Orders
- 9 active delivery orders at all times
- Auto-refreshes when completed
- Rewards: Coins + XP

### 🏪 Global Market
- List items for other players to buy
- Browse listings with pagination
- Remove your own listings

### 🗺️ Land Expansion
- Clear obstacles (Trees, Rocks, Swamps) with tools
- 5 types of obstacles, each needs a specific clearing tool
- Expand farm plots and animal pens

### 📊 Progression
- 30 levels with XP thresholds
- Level-gated crops, animals, buildings
- Daily reward (scales with level)

---

## 👑 Admin Panel

Access via `/admin` command (admin IDs only).

### Admin Features:
- **User Management** — View all players, manage their stats
- **Give Items** — `/give <user_id> <item_key> <qty>`
- **Give Coins** — `/givecoins <user_id> <amount>`
- **Set Level/Coins/Gems** — Per player
- **Game Events** — Toggle 2x XP, 2x Coins
- **Maintenance Mode** — Lock game for all non-admins
- **Drop Rate Control** — Adjust bonus drop percentage
- **Welcome Message** — Customize start screen
- **Broadcast** — Send message to all players
- **Admin Logs** — View last 20 admin actions
- **Item Database** — View & add crops at runtime

### Quick Admin Commands:
```
/admin          - Open admin panel
/give 12345 wheat 50         - Give 50 wheat to user 12345
/give 12345 bolt 10          - Give 10 bolts (barn upgrade tool)
/give 12345 axe 5            - Give 5 axes (clearing tool)
/givecoins 12345 5000        - Give 5000 coins to user 12345
```

---

## 📋 Item Keys Reference

### Crops
`wheat` `corn` `carrot` `soybean` `sugarcane` `rice` `pumpkin` `cotton` `potato` `tomato` `strawberry`

### Animal Products
`egg` `milk` `bacon` `wool` `goat_milk` `honey` `feather` `fish` `lobster` `mozzarella`

### Processed Goods
`bread` `popcorn` `butter` `sugar` `cotton_fabric` `syrup` `cheese` `pumpkin_pie` `wool_sweater` `pizza` `strawberry_ice_cream` `carrot_juice` `chocolate_cake` `chicken_feed` `cow_feed`

### Upgrade Tools (Silo)
`nail` `screw` `wood_panel`

### Upgrade Tools (Barn)
`bolt` `plank` `duct_tape`

### Upgrade Tools (General)
`paint` `brick` `cement` `sledgehammer`

### Expansion Tools
`land_deed` `mallet` `marker_stake` `construction_permit` `map_piece` `compass` `mayors_signature` `wire_cutter` `notary_letter` `city_plan`

### Clearing Tools
`axe` `saw` `dynamite` `tnt_barrel` `shovel` `crowbar` `rusty_hoe` `pest_spray` `trash_cart` `mini_tractor`

---

## 🏗️ Project Structure

```
harvest_kingdom/
├── main.py              # Bot entry point, handler registration
├── requirements.txt
├── Procfile             # Railway deployment
├── railway.toml         # Railway config
├── .env.example         # Environment variable template
├── .gitignore
├── database/
│   └── db.py            # SQLite async DB manager
├── game/
│   ├── data.py          # All game data (items, crops, recipes, etc.)
│   └── engine.py        # Core game logic
├── handlers/
│   ├── main_handlers.py # User-facing command & callback handlers
│   └── admin_handlers.py # Admin panel handlers
└── utils/
    ├── keyboards.py     # Inline keyboard builders
    └── formatters.py    # Message text formatters
```

---

## 🌐 Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `BOT_TOKEN` | ✅ Yes | Telegram Bot API token |
| `ADMIN_IDS` | ✅ Yes | Comma-separated admin user IDs |
| `DB_PATH` | ❌ No | SQLite database path (default: `harvest_kingdom.db`) |

---

## 📝 User Commands

| Command | Description |
|---------|-------------|
| `/start` | Main menu |
| `/farm` | View your farm |
| `/storage` | Check silo & barn |
| `/market` | Global market |
| `/orders` | Delivery orders |
| `/daily` | Claim daily reward |
| `/profile` | View your stats |
| `/help` | Full tutorial |
| `/listitem <item> <qty> <price>` | List item on market |

---

Happy farming! 🌾👑
