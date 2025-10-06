#!/usr/bin/env python3
# token_price.py
# Pull symbol + price for a single token via Birdeye "Token - Overview",
# then upsert into SQLite (token_prices_live).

import json
import sys
from pathlib import Path
from typing import Dict, Any, Tuple
import requests

# import your DB helper (expects DB_PATH inside it from config.json or default)
from db_helper import upsert_token_price

BIRDEYE_URL = "https://public-api.birdeye.so/defi/token_overview"
CONFIG_FILE = Path("config.json")

# --- Token to query: WETH (Base) ---
CA = "0x4200000000000000000000000000000000000006"  # WETH on Base

def load_cfg() -> Dict[str, Any]:
    if not CONFIG_FILE.exists():
        sys.stderr.write("ERROR: config.json not found\n")
        sys.exit(1)
    try:
        cfg = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        sys.stderr.write(f"ERROR: failed to parse config.json: {e}\n")
        sys.exit(1)
    api_key = cfg.get("birdeye_api_key")
    if not api_key:
        sys.stderr.write("ERROR: birdeye_api_key missing in config.json\n")
        sys.exit(1)
    chain_id = cfg.get("chain_id", 8453)
    return {"api_key": api_key, "chain_id": int(chain_id)}

def chain_from_id(chain_id: int) -> str:
    mapping = {
        8453: "base",
        1: "ethereum",
        56: "bsc",
        # extend as needed (e.g., 137: "polygon") if Birdeye supports it
    }
    if chain_id not in mapping:
        raise ValueError(f"Unsupported/unknown chain_id for Birdeye: {chain_id}")
    return mapping[chain_id]

def birdeye_headers(api_key: str, chain_name: str) -> Dict[str, str]:
    return {
        "accept": "application/json",
        "X-API-KEY": api_key,
        "x-chain": chain_name,
    }

def fetch_token_overview(ca: str, api_key: str, chain_name: str) -> Dict[str, Any]:
    params = {"address": ca}
    r = requests.get(BIRDEYE_URL, headers=birdeye_headers(api_key, chain_name), params=params, timeout=20)
    if r.status_code != 200:
        raise RuntimeError(f"Birdeye error [{r.status_code}]: {r.text}")
    try:
        return r.json()
    except Exception as e:
        raise RuntimeError(f"Failed to decode JSON: {e}; body={r.text[:300]}")

def extract_symbol_price(payload: Dict[str, Any]) -> Tuple[str, float]:
    """
    Birdeye 'token_overview' returns data under 'data'.
    Use common keys for symbol and price across chains.
    """
    data = payload.get("data") or {}
    symbol = data.get("symbol") or data.get("tokenSymbol") or ""
    # Try common price keys (usually USD)
    price = (
        data.get("price") or
        data.get("priceUsd") or
        data.get("price_usd") or
        data.get("usd_price")
    )
    if price is None:
        market = data.get("market") or {}
        price = market.get("price") or market.get("priceUsd")
    if symbol == "" or price is None:
        raise RuntimeError(f"Unexpected schema; cannot find symbol/price in: {json.dumps(data)[:400]}")
    return str(symbol), float(price)

def main() -> None:
    cfg = load_cfg()
    chain_name = chain_from_id(cfg["chain_id"])

    # 1) Fetch from Birdeye
    resp = fetch_token_overview(CA, cfg["api_key"], chain_name)
    symbol, price = extract_symbol_price(resp)

    print("=== Birdeye Token Overview ===")
    print(f"chain:     {chain_name}")
    print(f"address:   {CA}")
    print(f"symbol:    {symbol}")
    print(f"price:     {price}")

    # 2) Upsert into SQLite live price table
    try:
        upsert_token_price(ca=CA, symbol=symbol, price=price)
        print(f"[ok] upserted live price for {symbol} ({CA}) = {price}")
    except Exception as e:
        sys.stderr.write(f"ERROR: failed to upsert into DB: {e}\n")
        sys.exit(1)

if __name__ == "__main__":
    main()
