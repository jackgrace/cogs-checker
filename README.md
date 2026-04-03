# COGS Checker Bot
Compares Shopify variant costs against supplier invoice prices across all stores. Reports discrepancies via Slack slash command.

## Setup
1. Each Shopify store needs a Custom App with read_products + read_inventory scopes
2. Deploy to Railway from this repo
3. Set env vars per .env.example
4. Create Slack slash commands: /cogs-check → POST https://your-railway-url/slack/cogs-check and /cogs-update → POST https://your-railway-url/slack/update-price

## Usage
/cogs-check — check all stores
/cogs-check au — check AU only
/cogs-update MIK_01 5.95 — update a supplier price
