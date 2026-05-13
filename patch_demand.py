name: Update Demand Data
on:
  schedule:
    - cron: '0 2 * * 1'
  workflow_dispatch:

jobs:
  update-demand:
    runs-on: ubuntu-latest
    permissions:
      contents: write
    
    steps:
      - name: Checkout repo
        uses: actions/checkout@v4
        with:
          fetch-depth: 0
          token: ${{ secrets.GITHUB_TOKEN }}
      
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: |
          pip install --upgrade pip
          pip install 'urllib3<2.0' pandas requests
          pip install git+https://github.com/GeneralMills/pytrends.git
      
      - name: Run demand patcher - Batch 1
        run: python -u patch_demand.py --batch 1 --dashboard "index.html" --resume
        env:
          PYTHONUNBUFFERED: "1"
        continue-on-error: false
      
      - name: Wait between batches
        run: sleep 600
      
      - name: Run demand patcher - Batch 2
        run: python -u patch_demand.py --batch 2 --dashboard "index.html" --resume
        env:
          PYTHONUNBUFFERED: "1"
        continue-on-error: false
      
      - name: Commit and push
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add data/demand_products.json
          git add data/progress_batch_*.json 2>/dev/null || true
          
          if git diff --staged --quiet; then
            echo "No changes"
            exit 0
          fi
          
          git commit -m "chore: update demand data - $(date +'%Y-%m-%d') [skip ci]"
          git push
