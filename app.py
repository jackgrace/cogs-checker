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


def convert_from_usd(amount: float, to_currency: str, rates: dict) -> float:
    if to_currency == "USD":
        return amount
    rate = rates.get(to_currency)
    if rate is None or rate == 0:
        log.warning(f"No FX rate for {to_currency}, treating as 1:1")
        return amount
    return round(amount * rate, 4)


SKU_ALIASES = {
    "CLERA01": "CLEAR01",
    "MEIP01": "MIEP01",
}


def normalize_sku(sku: str) -> str:
    norm = sku.upper().strip().replace(" ", "").replace("-", "").replace("_", "")
    return SKU_ALIASES.get(norm, norm)


def parse_sku_part(part: str):
    """Parse a SKU part, handling =QTYn suffix. Returns (base_sku, quantity)."""
    part = part.strip()
    if "=" in part:
        base, suffix = part.rsplit("=", 1)
        suffix_upper = suffix.upper()
        if suffix_upper.startswith("QTY"):
            try:
                qty = int(suffix_upper[3:])
                return base, qty
            except ValueError:
                pass
    return part, 1


def lookup_supplier_cost_usd(sku: str) -> float | None:
    parts = [s.strip() for s in sku.split("+") if s.strip()]
    total = 0.0
    for part in parts:
        base, qty = parse_sku_part(part)
        norm_part = normalize_sku(base)
        found = False
        for s_sku, s_data in SUPPLIER_PRICES.items():
            if normalize_sku(s_sku) == norm_part:
                total += s_data["cost_usd"] * qty
                found = True
                break
        if not found:
            log.warning(f"SKU part '{part}' (normalized: '{norm_part}') not found in supplier list")
            return None
    return total


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
        try:
            data = shopify_get(domain, token, "inventory_items", {"ids": ids_str})
            items = data.get("inventory_items", [])
            if i == 0 and items:
                log.info(f"Sample inventory item response: {json.dumps(items[0])}")
            fetched_ids = set()
            for item in items:
                fetched_ids.add(item["id"])
                cost = item.get("cost")
                if cost is not None and cost != "":
                    costs[item["id"]] = float(cost)
                else:
                    costs[item["id"]] = None
            missing_from_batch = set(batch) - fetched_ids
            if missing_from_batch:
                log.warning(f"Batch {i}: {len(missing_from_batch)} item IDs not returned by API")
        except Exception as e:
            log.error(f"Failed to fetch inventory batch starting at {i}: {e}")
    return costs


def check_cogs_for_store(market: str, store_cfg: dict, rates: dict) -> dict:
    domain = store_cfg["domain"]
    token = store_cfg["token"]
    currency = store_cfg["currency"]
    log.info(f"[{market.upper()}] Fetching products from {domain}...")
    variants = fetch_all_products(domain, token)
    log.info(f"[{market.upper()}] Found {len(variants)} variants with SKUs")
    item_ids = [v["inventory_item_id"] for v in variants if v["inventory_item_id"]]
    no_inv_id = [v["sku"] for v in variants if not v["inventory_item_id"]]
    if no_inv_id:
        log.warning(f"[{market.upper()}] {len(no_inv_id)} variants have no inventory_item_id: {no_inv_id[:10]}")
    costs = fetch_inventory_costs(domain, token, item_ids)
    null_cost_skus = [v["sku"] for v in variants if v["inventory_item_id"] and costs.get(v["inventory_item_id"]) is None]
    if null_cost_skus:
        log.warning(f"[{market.upper()}] {len(null_cost_skus)} variants have null cost from API: {null_cost_skus[:20]}")
    accessible_count = sum(1 for v in variants if v["inventory_item_id"] in costs)
    results = {
        "market": market.upper(),
        "currency": currency,
        "total_skus": accessible_count,
        "matches": [],
        "mismatches": [],
        "missing_from_supplier": [],
        "no_cost_in_shopify": [],
    }
    for v in variants:
        sku = v["sku"]
        inv_id = v["inventory_item_id"]
        if inv_id not in costs:
            # Item not returned by API — likely managed by a bundle/subscription app
            continue
        shopify_cost_local = costs.get(inv_id)
        supplier_cost_usd = lookup_supplier_cost_usd(sku)
        if shopify_cost_local is None:
            results["no_cost_in_shopify"].append({
                "sku": sku,
                "product": v["product_title"],
                "supplier_cost_usd": supplier_cost_usd,
            })
            continue
        if supplier_cost_usd is None:
            results["missing_from_supplier"].append({
                "sku": sku,
                "product": v["product_title"],
                "shopify_cost_local": shopify_cost_local,
            })
            continue
        supplier_cost_local = convert_from_usd(supplier_cost_usd, currency, rates)
        diff = round(shopify_cost_local - supplier_cost_local, 2)
        pct_diff = round((diff / supplier_cost_local) * 100, 1) if supplier_cost_local else 0
        entry = {
            "sku": sku,
            "product": v["product_title"],
            "supplier_cost_usd": supplier_cost_usd,
            "supplier_cost_local": supplier_cost_local,
            "shopify_cost_local": shopify_cost_local,
            "diff_local": diff,
            "diff_pct": pct_diff,
            "currency": currency,
        }
        if abs(diff) <= TOLERANCE_USD or abs(pct_diff) <= TOLERANCE_PCT:
            results["matches"].append(entry)
        else:
            results["mismatches"].append(entry)
    results["mismatches"].sort(key=lambda x: abs(x["diff_local"]), reverse=True)
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
            for m in mismatches:
                direction = "📈" if m["diff_local"] > 0 else "📉"
                c = m["currency"]
                lines.append(f"{direction} `{m['sku']}` — Shopify: {c} {m['shopify_cost_local']:.2f} vs Supplier: {c} {m['supplier_cost_local']:.2f} (*{m['diff_pct']:+.1f}%*)")
            chunk = []
            chunk_len = 0
            for line in lines:
                if chunk_len + len(line) + 1 > 2900:
                    blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(chunk)}})
                    chunk = []
                    chunk_len = 0
                chunk.append(line)
                chunk_len += len(line) + 1
            if chunk:
                blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(chunk)}})
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


def shopify_put(domain: str, token: str, endpoint: str, payload: dict) -> dict:
    url = f"https://{domain}/admin/api/{SHOPIFY_API_VERSION}/{endpoint}.json"
    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
    resp = requests.put(url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()


def update_variant_cost(domain, token, currency, variant, rates, results, market):
    sku = variant["sku"]
    inv_id = variant["inventory_item_id"]
    supplier_cost_usd = lookup_supplier_cost_usd(sku)
    if supplier_cost_usd is None:
        return
    target_cost = round(convert_from_usd(supplier_cost_usd, currency, rates), 2)
    # Fetch current cost to check if update needed
    item_ids = [inv_id]
    costs = fetch_inventory_costs(domain, token, item_ids)
    current_cost = costs.get(inv_id)
    if current_cost is not None and round(current_cost, 2) == target_cost:
        return
    try:
        shopify_put(domain, token, f"inventory_items/{inv_id}", {"inventory_item": {"id": inv_id, "cost": str(target_cost)}})
        old_str = f"{currency} {current_cost:.2f}" if current_cost is not None else "none"
        results.append(f"✅ *{market.upper()}* `{sku}` {old_str} → {currency} {target_cost:.2f}")
    except Exception as e:
        results.append(f"❌ *{market.upper()}* `{sku}` failed: {e}")


def run_update_price(response_url: str, sku_filter: str = None, market_filter: str = None):
    try:
        stores = get_stores()
        rates = fetch_fx_rates()
        if sku_filter:
            supplier_cost_usd = lookup_supplier_cost_usd(sku_filter)
            if supplier_cost_usd is None:
                requests.post(response_url, json={"response_type": "ephemeral", "text": f"❌ SKU `{sku_filter}` not found in supplier list."})
                return
        results = []
        for market, cfg in stores.items():
            if market_filter and market != market_filter.lower():
                continue
            domain = cfg["domain"]
            token = cfg["token"]
            currency = cfg["currency"]
            variants = fetch_all_products(domain, token)
            if sku_filter:
                matched = [v for v in variants if normalize_sku(v["sku"]) == normalize_sku(sku_filter)]
            else:
                matched = [v for v in variants if lookup_supplier_cost_usd(v["sku"]) is not None]
            for v in matched:
                update_variant_cost(domain, token, currency, v, rates, results, market)
        if not results:
            if sku_filter:
                requests.post(response_url, json={"response_type": "ephemeral", "text": f"❌ No updates needed for `{sku_filter}`."})
            else:
                requests.post(response_url, json={"response_type": "ephemeral", "text": f"✅ All costs already match for *{market_filter.upper()}*."})
            return
        header = f"*Updated `{sku_filter}`*" if sku_filter else f"*Updated all SKUs*"
        msg = header + f" ({len(results)} changes)\n" + "\n".join(results)
        requests.post(response_url, json={"response_type": "in_channel", "text": msg})
    except Exception as e:
        log.error(f"Update price failed: {e}")
        requests.post(response_url, json={"response_type": "ephemeral", "text": f"❌ Update failed: {e}"})


@app.route("/slack/update-price", methods=["POST"])
def slack_update_price():
    response_url = request.form.get("response_url")
    text = request.form.get("text", "").strip()
    parts = text.split()
    stores = get_stores()
    markets = set(stores.keys())
    if len(parts) == 0:
        return jsonify({"response_type": "ephemeral", "text": "Usage:\n`/cogs-update SKU` — update SKU in all stores\n`/cogs-update SKU au` — update SKU in AU only\n`/cogs-update au` — update all SKUs in AU"})
    if len(parts) == 1:
        arg = parts[0].lower()
        if arg in markets:
            # geo only — update all SKUs for that store
            thread = threading.Thread(target=run_update_price, args=(response_url, None, arg), daemon=True)
            thread.start()
            return jsonify({"response_type": "ephemeral", "text": f"⏳ Updating all SKU costs in *{arg.upper()}*... This may take a while."})
        else:
            # SKU only — update across all stores
            sku = parts[0].upper()
            thread = threading.Thread(target=run_update_price, args=(response_url, sku, None), daemon=True)
            thread.start()
            return jsonify({"response_type": "ephemeral", "text": f"⏳ Updating `{sku}` cost in *all stores*..."})
    if len(parts) == 2:
        sku = parts[0].upper()
        market = parts[1].lower()
        if market not in markets:
            available = ", ".join(markets)
            return jsonify({"response_type": "ephemeral", "text": f"❌ Unknown market `{market}`. Available: {available}"})
        thread = threading.Thread(target=run_update_price, args=(response_url, sku, market), daemon=True)
        thread.start()
        return jsonify({"response_type": "ephemeral", "text": f"⏳ Updating `{sku}` cost in *{market.upper()}*..."})
    return jsonify({"response_type": "ephemeral", "text": "Usage:\n`/cogs-update SKU` — update SKU in all stores\n`/cogs-update SKU au` — update SKU in AU only\n`/cogs-update au` — update all SKUs in AU"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    log.info(f"Starting COGS Checker on port {port}")
    log.info(f"Loaded {len(SUPPLIER_PRICES)} SKUs from supplier price list")
    app.run(host="0.0.0.0", port=port)
