"""
COGS Checker Bot
Compares Shopify variant costs against supplier invoice prices.
Triggered via Slack slash command /cogs-check.
"""

import os
import json
import logging
import threading
from datetime import datetime, timedelta

import requests
from flask import Flask, request, jsonify

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

PRICE_LIST_PATH = os.environ.get("PRICE_LIST_PATH", "supplier_prices.json")
with open(PRICE_LIST_PATH) as f:
    SUPPLIER_DATA = json.load(f)
SUPPLIER_PRICES = SUPPLIER_DATA["prices"]

SHOPIFY_API_VERSION = os.environ.get("SHOPIFY_API_VERSION", "2024-10")
TOLERANCE_USD = float(os.environ.get("COGS_TOLERANCE_USD", "0.10"))
TOLERANCE_PCT = float(os.environ.get("COGS_TOLERANCE_PCT", "3.0"))

_fx_cache = {"rates": {}, "fetched_at": None}
FX_CACHE_TTL = timedelta(hours=6)


STORE_CONFIG = {
    "au": {"domain": "us-domainholdings.myshopify.com", "currency": "AUD", "token_env": "SHOPIFY_TOKEN_AU"},
    "uk": {"domain": "uk-domainholdings.myshopify.com", "currency": "GBP", "token_env": "SHOPIFY_TOKEN_UK"},
    "us": {"domain": "domainholdings.myshopify.com", "currency": "USD", "token_env": "SHOPIFY_TOKEN_US"},
    "ca": {"domain": "lux-iplpro.myshopify.com", "currency": "CAD", "token_env": "SHOPIFY_TOKEN_CA"},
    "eu": {"domain": "lux-skin-europe.myshopify.com", "currency": "EUR", "token_env": "SHOPIFY_TOKEN_EU"},
}


def get_stores() -> dict:
    stores = {}
    for market, cfg in STORE_CONFIG.items():
        token = os.environ.get(cfg["token_env"], "")
        if token:
            stores[market] = {
                "domain": cfg["domain"],
                "token": token,
                "currency": cfg["currency"],
            }
    return stores


def fetch_fx_rates() -> dict:
    now = datetime.utcnow()
    if _fx_cache["fetched_at"] and (now - _fx_cache["fetched_at"]) < FX_CACHE_TTL:
        return _fx_cache["rates"]
    try:
        resp = requests.get("https://api.frankfurter.app/latest?from=USD", timeout=10)
        resp.raise_for_status()
        data = resp.json()
        rates = data.get("rates", {})
        rates["USD"] = 1.0
        _fx_cache["rates"] = rates
        _fx_cache["fetched_at"] = now
        log.info(f"FX rates refreshed: {len(rates)} currencies")
        return rates
    except Exception as e:
        log.error(f"FX rate fetch failed: {e}")
        if _fx_cache["rates"]:
            return _fx_cache["rates"]
        return {"USD": 1.0, "AUD": 1.55, "GBP": 0.79, "CAD": 1.37, "EUR": 0.92, "AED": 3.67}


def convert_to_usd(amount: float, from_currency: str, rates: dict) -> float:
    if from_currency == "USD":
        return amount
    rate = rates.get(from_currency)
    if rate is None or rate == 0:
        log.warning(f"No FX rate for {from_currency}, treating as 1:1")
        return amount
    return round(amount / rate, 4)


def shopify_get(domain: str, token: str, endpoint: str, params: dict = None) -> dict:
    url = f"https://{domain}/admin/api/{SHOPIFY_API_VERSION}/{endpoint}.json"
    headers = {"X-Shopify-Access-Token": token}
    resp = requests.get(url, headers=headers, params=params or {}, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_all_products(domain: str, token: str) -> list:
    variants = []
    params = {"limit": 250, "fields": "id,title,variants"}
    endpoint = f"https://{domain}/admin/api/{SHOPIFY_API_VERSION}/products.json"
    headers = {"X-Shopify-Access-Token": token}
    url = endpoint
    while url:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        for product in data.get("products", []):
            for v in product.get("variants", []):
                if v.get("sku"):
                    variants.append({
                        "product_title": product["title"],
                        "variant_title": v.get("title", ""),
                        "sku": v["sku"].strip(),
                        "inventory_item_id": v.get("inventory_item_id"),
                    })
        params = {}
        link = resp.headers.get("Link", "")
        url = None
        if 'rel="next"' in link:
            for part in link.split(","):
                if 'rel="next"' in part:
                    url = part.split("<")[1].split(">")[0]
                    break
    return variants


def fetch_inventory_costs(domain: str, token: str, item_ids: list) -> dict:
    costs = {}
    for i in range(0, len(item_ids), 100):
        batch = item_ids[i:i+100]
        ids_str = ",".join(str(x) for x in batch)
        data = shopify_get(domain, token, "inventory_items", {"ids": ids_str})
        for item in data.get("inventory_items", []):
            cost = item.get("cost")
            if cost is not None:
                costs[item["id"]] = float(cost)
            else:
                costs[item["id"]] = None
    return costs


def check_cogs_for_store(market: str, store_cfg: dict, rates: dict) -> dict:
    domain = store_cfg["domain"]
    token = store_cfg["token"]
    currency = store_cfg["currency"]
    log.info(f"[{market.upper()}] Fetching products from {domain}...")
    variants = fetch_all_products(domain, token)
    log.info(f"[{market.upper()}] Found {len(variants)} variants with SKUs")
    item_ids = [v["inventory_item_id"] for v in variants if v["inventory_item_id"]]
    costs = fetch_inventory_costs(domain, token, item_ids)
    results = {
        "market": market.upper(),
        "currency": currency,
        "total_skus": len(variants),
        "matches": [],
        "mismatches": [],
        "missing_from_supplier": [],
        "no_cost_in_shopify": [],
    }
    for v in variants:
        sku = v["sku"]
        sku_upper = sku.upper().strip()
        inv_id = v["inventory_item_id"]
        shopify_cost_local = costs.get(inv_id)
        supplier_entry = None
        for s_sku, s_data in SUPPLIER_PRICES.items():
            if s_sku.upper() == sku_upper:
                supplier_entry = (s_sku, s_data)
                break
        if shopify_cost_local is None or shopify_cost_local == 0:
            results["no_cost_in_shopify"].append({
                "sku": sku,
                "product": v["product_title"],
                "supplier_cost_usd": supplier_entry[1]["cost_usd"] if supplier_entry else None,
            })
            continue
        if supplier_entry is None:
            results["missing_from_supplier"].append({
                "sku": sku,
                "product": v["product_title"],
                "shopify_cost_local": shopify_cost_local,
                "shopify_cost_usd": convert_to_usd(shopify_cost_local, currency, rates),
            })
            continue
        supplier_cost_usd = supplier_entry[1]["cost_usd"]
        shopify_cost_usd = convert_to_usd(shopify_cost_local, currency, rates)
        diff = round(shopify_cost_usd - supplier_cost_usd, 2)
        pct_diff = round((diff / supplier_cost_usd) * 100, 1) if supplier_cost_usd else 0
        entry = {
            "sku": sku,
            "product": v["product_title"],
            "supplier_cost_usd": supplier_cost_usd,
            "shopify_cost_local": shopify_cost_local,
            "shopify_cost_usd": shopify_cost_usd,
            "diff_usd": diff,
            "diff_pct": pct_diff,
        }
        if abs(diff) <= TOLERANCE_USD or abs(pct_diff) <= TOLERANCE_PCT:
            results["matches"].append(entry)
        else:
            results["mismatches"].append(entry)
    results["mismatches"].sort(key=lambda x: abs(x["diff_usd"]), reverse=True)
    return results


def format_slack_message(all_results: list, rates: dict) -> dict:
    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": "📦 COGS Check Report", "emoji": True}},
        {"type": "context", "elements": [{"type": "mrkdwn", "text": f"*Source:* {SUPPLIER_DATA['source']}  |  *Invoice Date:* {SUPPLIER_DATA['date']}  |  *Tolerance:* ${TOLERANCE_USD} / {TOLERANCE_PCT}%"}]},
        {"type": "divider"},
    ]
    total_mismatches = 0
    total_no_cost = 0
    total_missing = 0
    for res in all_results:
        market = res["market"]
        currency = res["currency"]
        rate = rates.get(currency, 1.0)
        mismatches = res["mismatches"]
        total_mismatches += len(mismatches)
        total_no_cost += len(res["no_cost_in_shopify"])
        total_missing += len(res["missing_from_supplier"])
        status = "✅" if not mismatches else f"⚠️ {len(mismatches)} mismatches"
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"*{market}* ({currency}, 1 USD = {rate:.4f} {currency})  —  {res['total_skus']} SKUs  |  {status}"}})
        if mismatches:
            lines = []
            for m in mismatches[:15]:
                direction = "📈" if m["diff_usd"] > 0 else "📉"
                lines.append(f"{direction} `{m['sku']}` — Shopify: ${m['shopify_cost_usd']:.2f} vs Supplier: ${m['supplier_cost_usd']:.2f} (*{m['diff_pct']:+.1f}%*)")
            if len(mismatches) > 15:
                lines.append(f"_...and {len(mismatches) - 15} more_")
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}})
        if res["no_cost_in_shopify"]:
            skus = [x["sku"] for x in res["no_cost_in_shopify"][:10]]
            blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": f"🔲 *No cost in Shopify ({len(res['no_cost_in_shopify'])}):* {', '.join(f'`{s}`' for s in skus)}{'...' if len(res['no_cost_in_shopify']) > 10 else ''}"}]})
        blocks.append({"type": "divider"})
    blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"*Summary:*  {total_mismatches} mismatches  |  {total_no_cost} missing cost in Shopify  |  {total_missing} SKUs not in supplier list"}})
    return {"blocks": blocks, "response_type": "in_channel"}


def run_cogs_check(response_url: str, market_filter: str = None):
    try:
        stores = get_stores()
        if not stores:
            requests.post(response_url, json={"response_type": "ephemeral", "text": "❌ No Shopify stores configured. Set SHOPIFY_STORES env var."})
            return
        rates = fetch_fx_rates()
        all_results = []
        for market, cfg in stores.items():
            if market_filter and market != market_filter.lower():
                continue
            try:
                result = check_cogs_for_store(market, cfg, rates)
                all_results.append(result)
            except Exception as e:
                log.error(f"Error checking {market}: {e}")
                all_results.append({"market": market.upper(), "currency": cfg["currency"], "total_skus": 0, "matches": [], "mismatches": [], "missing_from_supplier": [], "no_cost_in_shopify": [], "error": str(e)})
        message = format_slack_message(all_results, rates)
        requests.post(response_url, json=message, timeout=10)
    except Exception as e:
        log.error(f"COGS check failed: {e}")
        try:
            requests.post(response_url, json={"response_type": "ephemeral", "text": f"❌ COGS check failed: {e}"})
        except Exception:
            pass


@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "cogs-checker", "skus_loaded": len(SUPPLIER_PRICES)})


@app.route("/slack/cogs-check", methods=["POST"])
def slack_cogs_check():
    response_url = request.form.get("response_url")
    text = request.form.get("text", "").strip().lower()
    market_filter = text if text else None
    stores = get_stores()
    if market_filter and market_filter not in stores:
        available = ", ".join(stores.keys())
        return jsonify({"response_type": "ephemeral", "text": f"❌ Unknown market `{market_filter}`. Available: {available}"})
    thread = threading.Thread(target=run_cogs_check, args=(response_url, market_filter), daemon=True)
    thread.start()
    scope = f"*{market_filter.upper()}*" if market_filter else "*all stores*"
    return jsonify({"response_type": "ephemeral", "text": f"⏳ Checking COGS for {scope}... Results incoming."})


@app.route("/slack/update-price", methods=["POST"])
def slack_update_price():
    text = request.form.get("text", "").strip()
    parts = text.split()
    if len(parts) != 2:
        return jsonify({"response_type": "ephemeral", "text": "Usage: `/cogs-update SKU PRICE` e.g. `/cogs-update MIK_01 5.45`"})
    sku = parts[0].upper()
    try:
        new_price = float(parts[1])
    except ValueError:
        return jsonify({"response_type": "ephemeral", "text": "❌ Invalid price."})
    found = False
    for s_sku in SUPPLIER_PRICES:
        if s_sku.upper() == sku:
            old_price = SUPPLIER_PRICES[s_sku]["cost_usd"]
            SUPPLIER_PRICES[s_sku]["cost_usd"] = new_price
            found = True
            with open(PRICE_LIST_PATH, "w") as f:
                json.dump(SUPPLIER_DATA, f, indent=2)
            return jsonify({"response_type": "in_channel", "text": f"✅ Updated `{sku}`: ${old_price:.2f} → ${new_price:.2f}"})
    if not found:
        return jsonify({"response_type": "ephemeral", "text": f"❌ SKU `{sku}` not found in supplier list."})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    log.info(f"Starting COGS Checker on port {port}")
    log.info(f"Loaded {len(SUPPLIER_PRICES)} SKUs from supplier price list")
    app.run(host="0.0.0.0", port=port)
