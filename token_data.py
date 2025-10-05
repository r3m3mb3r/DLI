#!/usr/bin/env python3
# token_data.py
#
# Populate DB with the largest-liquidity pool for the given Base token.
# Uses db_helper.upsert_token_pair (reads DB path from config.json).

import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List
import requests

from db_helper import upsert_token_pair  # uses config.json for DB path

# ====== USER SETTINGS ======
token_address = "0x52b492a33E447Cdb854c7FC19F1e57E8BfA1777D"  # Base token contract (0x...)

# ====== CONSTANTS ======
CONFIG_FILE = "config.json"
BIRDEYE_MARKETS_URL = "https://public-api.birdeye.so/defi/v2/markets"
EVMA_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")

def load_config(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def get_api_key(cfg: dict) -> str:
    return cfg.get("birdeye_api_key") or os.getenv("BIRDEYE_API_KEY")

def is_valid_evm_address(addr: str) -> bool:
    return isinstance(addr, str) and bool(EVMA_RE.match(addr.strip()))

def birdeye_get(url: str, api_key: str, params: dict) -> Dict[str, Any]:
    headers = {
        "accept": "application/json",
        "X-API-KEY": api_key,
        "x-chain": "base",  # critical: treat 0x addresses as Base (not Solana)
    }
    resp = requests.get(url, headers=headers, params=params, timeout=20)
    if resp.status_code != 200:
        raise RuntimeError(f"Birdeye request failed [{resp.status_code}]: {resp.text}")
    return resp.json()

def normalize_markets(obj: Dict[str, Any]) -> List[Dict[str, Any]]:
    markets = obj.get("data") or obj.get("markets") or []
    if isinstance(markets, dict):
        markets = markets.get("items") or markets.get("list") or markets
        if isinstance(markets, dict):
            markets = [markets]
    return markets if isinstance(markets, list) else []

def pick_largest_by_liquidity(markets: List[Dict[str, Any]]) -> Dict[str, Any]:
    best, best_liq = None, -1.0
    for m in markets:
        liq = m.get("liquidity")
        try:
            v = float(liq) if liq is not None else 0.0
        except Exception:
            v = 0.0
        if v > best_liq:
            best_liq, best = v, m
    if not best:
        raise ValueError("No market with liquidity found.")
    return best

def main():
    # 1) Sanity
    if not is_valid_evm_address(token_address):
        sys.stderr.write("ERROR: token_address must be a 42-char EVM address (0x...).\n")
        sys.exit(1)

    # 2) API key
    cfg = load_config(Path(CONFIG_FILE))
    api_key = get_api_key(cfg)
    if not api_key:
        sys.stderr.write("ERROR: Missing Birdeye API key (config.json birdeye_api_key or BIRDEYE_API_KEY).\n")
        sys.exit(1)

    # 3) Fetch markets
    try:
        resp = birdeye_get(BIRDEYE_MARKETS_URL, api_key, {"address": token_address})
    except RuntimeError as e:
        if "invalid format" in str(e).lower():
            sys.stderr.write(
                "Birdeye says 'address is invalid format'. "
                "Ensure this is a Base TOKEN contract (not LP/NFT).\n"
            )
        raise

    markets = normalize_markets(resp)
    if not markets:
        sys.stderr.write("No markets returned for token.\n")
        sys.exit(2)

    # 4) Pick largest-liquidity pool
    market = pick_largest_by_liquidity(markets)

    # 5) Ensure DB base_* is your input token (swap if Birdeye has it as quote)
    base = market.get("base") or {}
    quote = market.get("quote") or {}
    pair_addr = market.get("address")

    b_addr = (base.get("address") or "")
    q_addr = (quote.get("address") or "")
    in_addr = token_address

    if in_addr == b_addr:
        base_addr, base_sym, base_dec = b_addr, base.get("symbol"), base.get("decimals")
        quote_addr, quote_sym, quote_dec = q_addr, quote.get("symbol"), quote.get("decimals")
    elif in_addr == q_addr:
        base_addr, base_sym, base_dec = q_addr, quote.get("symbol"), quote.get("decimals")
        quote_addr, quote_sym, quote_dec = b_addr, base.get("symbol"), base.get("decimals")
    else:
        # Shouldn't happen for /v2/markets?address=token_address, but guard anyway.
        sys.stderr.write("Selected market does not include the token on either side.\n")
        sys.exit(3)

    # Validate required ints
    if base_dec is None or quote_dec is None:
        sys.stderr.write("Missing decimals in market data.\n")
        sys.exit(4)

    # 6) Upsert into DB (no prints on success)
    upsert_token_pair(
        base_address=base_addr,
        base_symbol=base_sym,
        base_decimals=int(base_dec),
        pair_address=pair_addr,
        quote_address=quote_addr,
        quote_symbol=quote_sym,
        quote_decimals=int(quote_dec),
    )

if __name__ == "__main__":
    main()
