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
├── index.html       # The full dashboard (static, single-file)
├── api/
│   └── claude.js    # Edge serverless proxy — keeps your API key secure
├── vercel.json      # Routing config
└── README.md
```

## How it works

All Claude AI calls from the dashboard are routed through `/api/claude` (an Edge Function) which injects your `ANTHROPIC_API_KEY` server-side. The key is **never exposed** to the browser.

## Features

- 🔐 Role-based login (Sales / Exec views)
- 📊 Live KPI strip with AI-scored demand data
- 🗺️ State-level logistics heatmap (28 states)
- 📅 52-week demand calendar with AI scoring
- 🧠 Live market intelligence feed (Claude-powered)
- 📁 PDF / Excel report ingestion with demand extraction
- ✏️ NIC code editor with real-time recalculation
- 📤 Excel export

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `ANTHROPIC_API_KEY` | ✅ Yes | Your Anthropic API key |
