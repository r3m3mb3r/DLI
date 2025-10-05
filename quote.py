#!/usr/bin/env python3
# quote.py
#
# Indicative pricing via 0x Swap API v2 (AllowanceHolder) on Base (chainId=8453).
# Reads 0x API key from config.json -> {"zeroex_api_key": "YOUR_KEY"}.
# Hardcoded addresses per request. Both tokens use 18 decimals.

import json
import sys
from pathlib import Path
from typing import Dict, Any
import requests
from decimal import Decimal, getcontext, ROUND_DOWN

ZEROX_PRICE_URL = "https://api.0x.org/swap/allowance-holder/price"
CHAIN_ID = 8453  # Base

# --- Hardcoded addresses ---
BASE_PEPE = "0x52b492a33E447Cdb854c7FC19F1e57E8BfA1777D"  # BasedPepe (Base)
PAIR_ADDRESS = "0x0FB597D6cFE5bE0d5258A7f017599C2A4Ece34c7"  # not used by 0x; for reference only
WETH = "0x4200000000000000000000000000000000000006"        # WETH (Base)

# --- Decimals (both 18 as requested) ---
DECIMALS_TOKEN = 18
DECIMALS_WETH = 18

# --- Human-readable sell sizes (adjust freely) ---
SELL_WETH_AMOUNT_HUMAN = "0.01"       # 0.01 WETH
SELL_TOKEN_AMOUNT_HUMAN = "1000000"   # 1,000,000 BASE_PEPE tokens

CONFIG_FILE = Path("config.json")

def _load_api_key() -> str:
    if not CONFIG_FILE.exists():
        sys.stderr.write("ERROR: config.json not found.\n")
        sys.exit(1)
    try:
        cfg = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        sys.stderr.write(f"ERROR: failed to parse config.json: {e}\n")
        sys.exit(1)
    key = cfg.get("0x_api_key")
    if not key:
        sys.stderr.write("ERROR: zeroex_api_key missing in config.json.\n")
        sys.exit(1)
    return key

def _headers(api_key: str) -> Dict[str, str]:
    return {
        "accept": "application/json",
        "0x-api-key": api_key,
        "0x-version": "v2",
    }

def to_base_units(amount_str: str, decimals: int) -> str:
    """
    Convert a human-readable token amount (e.g., "0.01") to base units string
    for on-chain APIs. Uses Decimal to avoid floating errors.
    """
    getcontext().prec = 80
    amt = Decimal(amount_str)
    scale = Decimal(10) ** decimals
    # Truncate toward zero to avoid rounding up unexpectedly
    base_units = (amt * scale).to_integral_value(rounding=ROUND_DOWN)
    if base_units < 0:
        raise ValueError("Negative amounts are not allowed.")
    return str(base_units)

def get_price(*, sell_token: str, buy_token: str, sell_amount: str, api_key: str) -> Dict[str, Any]:
    params = {
        "chainId": CHAIN_ID,
        "sellToken": sell_token,
        "buyToken": buy_token,
        "sellAmount": sell_amount,
        "slippageBps": 0,  # no extra slippage buffer; raw route pricing
    }
    r = requests.get(ZEROX_PRICE_URL, headers=_headers(api_key), params=params, timeout=20)
    if r.status_code != 200:
        raise RuntimeError(f"0x price error [{r.status_code}]: {r.text}")
    return r.json()


def pretty(title: str, data: Dict[str, Any]) -> None:
    print(f"\n=== {title} ===")
    print(json.dumps(data, indent=2, ensure_ascii=False))

def main():
    api_key = _load_api_key()

    sell_weth_amount = to_base_units(SELL_WETH_AMOUNT_HUMAN, DECIMALS_WETH)
    sell_token_amount = to_base_units(SELL_TOKEN_AMOUNT_HUMAN, DECIMALS_TOKEN)

    # 1) Indicative price: sell WETH -> buy BASE_PEPE
    try:
        p1 = get_price(
            sell_token=WETH,
            buy_token=BASE_PEPE,
            sell_amount=sell_weth_amount,
            api_key=api_key,
        )
        pretty(f"Price: sell {SELL_WETH_AMOUNT_HUMAN} WETH -> buy BASE_PEPE", p1)
    except Exception as e:
        sys.stderr.write(f"[sell WETH] {e}\n")

    # 2) Indicative price: sell BASE_PEPE -> buy WETH
    try:
        p2 = get_price(
            sell_token=BASE_PEPE,
            buy_token=WETH,
            sell_amount=sell_token_amount,
            api_key=api_key,
        )
        pretty(f"Price: sell {SELL_TOKEN_AMOUNT_HUMAN} BASE_PEPE -> buy WETH", p2)
    except Exception as e:
        sys.stderr.write(f"[sell TOKEN] {e}\n")

if __name__ == "__main__":
    main()
