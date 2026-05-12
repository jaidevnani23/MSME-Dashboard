# India Logistics Intelligence — Vercel App

A live AI-powered logistics demand intelligence platform for India, deployable to Vercel in minutes.

## 🚀 Deploy to Vercel

### Option A — Vercel CLI (recommended)

```bash
npm i -g vercel
cd vercel-app
vercel
```

When prompted, set the environment variable:
```
ANTHROPIC_API_KEY=sk-ant-...
```

### Option B — Vercel Dashboard (drag & drop)

1. Zip this folder
2. Go to [vercel.com/new](https://vercel.com/new)
3. Drag and drop the zip file
4. Under **Environment Variables**, add:
   - **Name**: `ANTHROPIC_API_KEY`
   - **Value**: your Anthropic API key (get one at [console.anthropic.com](https://console.anthropic.com))
5. Click **Deploy**

---

## Project Structure

```
vercel-app/
├── index.html                          # The full dashboard (static, single-file)
├── api/
│   └── claude.js                       # Edge serverless proxy — keeps your API key secure
├── data/
│   └── demand_products.json           # Product demand data (374 products)
├── patch_demand_products_multiday.py  # Batch script for safe data updates
├── vercel.json                        # Routing config
├── README.md                          # This file
├── README_MULTIDAY.md                 # Detailed batch update documentation
└── SCALING_GUIDE.md                   # Advanced scaling strategies (1000+ products)
```

---

## How it works

All Claude AI calls from the dashboard are routed through `/api/claude` (an Edge Function) which injects your `ANTHROPIC_API_KEY` server-side. The key is **never exposed** to the browser.

---

## Features

- 🔐 Role-based login (Sales / Exec views)
- 📊 Live KPI strip with AI-scored demand data
- 🗺️ State-level logistics heatmap (28 states)
- 📅 52-week demand calendar with AI scoring
- 🧠 Live market intelligence feed (Claude-powered)
- 📁 PDF / Excel report ingestion with demand extraction
- ✏️ NIC code editor with real-time recalculation
- 📤 Excel export

---

## 🔄 Updating Product Demand Data

The dashboard tracks demand trends for **374 products** using Google Trends data. Data should be updated **monthly** using our safe multi-day batch system.

### Quick Update Process

```bash
# Install dependency (first time only)
pip install pytrends --break-system-packages

# Run safe 4-day batch schedule (one batch per day)
python patch_demand_products_multiday.py --batch 1  # Day 1: Products 0-93
python patch_demand_products_multiday.py --batch 2  # Day 2: Products 94-187
python patch_demand_products_multiday.py --batch 3  # Day 3: Products 188-280
python patch_demand_products_multiday.py --batch 4  # Day 4: Products 281-374

# Commit and auto-deploy to Vercel
git add data/demand_products.json
git commit -m "Update demand data - $(date +%Y-%m-%d)"
git push
```

Each batch takes **6-8 minutes** with built-in rate limiting to avoid API blocks.

### Test Before Running

```bash
# Dry run (no API calls, shows what will happen)
python patch_demand_products_multiday.py --batch 1 --dry-run

# Test with first 10 products only
python patch_demand_products_multiday.py --start 0 --end 10
```

### Resume After Failures

```bash
# If a batch fails midway, resume from checkpoint
python patch_demand_products_multiday.py --batch 2 --resume
```

### 📖 Full Documentation

- **[README_MULTIDAY.md](./README_MULTIDAY.md)** — Complete batch script guide with features, troubleshooting, and best practices
- **[SCALING_GUIDE.md](./SCALING_GUIDE.md)** — Strategies for scaling to 1,000+ products safely

### Why Multi-Day?

- **374 products** × 2-3 API calls = ~750-1,100 requests
- Google Trends has soft limits around **100-200 requests/hour**
- Running all at once **will get you blocked** for 1-24 hours
- Multi-day approach: **safe, resumable, and scales to 1,000+ products**

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `ANTHROPIC_API_KEY` | ✅ Yes | Your Anthropic API key |

---

## Deployment Workflow

```bash
# 1. Update demand data (once per month)
python patch_demand_products_multiday.py --batch 1
python patch_demand_products_multiday.py --batch 2
python patch_demand_products_multiday.py --batch 3
python patch_demand_products_multiday.py --batch 4

# 2. Commit changes
git add data/demand_products.json
git commit -m "Monthly demand update - $(date +%Y-%m-%d)"
git push

# 3. Vercel auto-deploys (no manual action needed)
```

---

## License

MIT
