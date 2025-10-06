#!/usr/bin/env python3
# quote.py
#
# Indicative pricing via 0x Swap API v2 (AllowanceHolder) on Base (chainId=8453).
# Reads 0x API key from config.json -> {"0x_api_key": "YOUR_KEY"}  # NOTE: matches this file's current key
# Hardcoded addresses per request. Both tokens use 18 decimals.

import json
import sys
from pathlib import Path
from typing import Dict, Any, Optional, List
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
    # NOTE: This reads "0x_api_key" because that's what the current file uses.
    # If your config uses "zeroex_api_key", change the key here accordingly.
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

# --------------------------
# Added: parser for useful fields + derived metrics
# --------------------------
from decimal import Decimal, getcontext, ROUND_DOWN
from typing import Dict, Any, List

getcontext().prec = 80  # high precision for on-chain math

def _fmt_decimal(d: Decimal, max_decimals: int) -> str:
    """Round DOWN to max_decimals and trim trailing zeros."""
    q = Decimal(1) if max_decimals == 0 else Decimal("1." + "0"*max_decimals)
    s = format(d.quantize(q, rounding=ROUND_DOWN), "f")
    return s.rstrip("0").rstrip(".") if "." in s else s

def parse_0x_price_response(resp: Dict[str, Any], *, sell_decimals: int, buy_decimals: int) -> Dict[str, Any]:
    def to_int(x) -> int:
        return int(str(x)) if x is not None else 0

    sell_token = resp.get("sellToken")
    buy_token  = resp.get("buyToken")

    sell_amount = to_int(resp.get("sellAmount"))
    buy_amount  = to_int(resp.get("buyAmount"))

    # Human-readable (apply decimals)
    sell_hr = Decimal(sell_amount) / (Decimal(10) ** sell_decimals) if sell_amount else Decimal(0)
    buy_hr  = Decimal(buy_amount)  / (Decimal(10) ** buy_decimals)

    # Symbols (if provided in route.tokens)
    tokens: List[Dict[str, Any]] = (resp.get("route") or {}).get("tokens") or []
    sym_by_addr = { (t.get("address") or "").lower(): t.get("symbol") for t in tokens }
    sell_symbol = sym_by_addr.get((sell_token or "").lower())
    buy_symbol  = sym_by_addr.get((buy_token  or "").lower())

    # Route concentration (top source by proportionBps)
    fills: List[Dict[str, Any]] = (resp.get("route") or {}).get("fills") or []
    top_source, top_bps = None, -1
    slim_fills = []
    for f in fills:
        src = f.get("source")
        bps = to_int(f.get("proportionBps"))
        slim_fills.append({"source": src, "proportionBps": bps})
        if bps > top_bps:
            top_bps = bps
            top_source = src
    route_concentration_pct = float(Decimal(top_bps) / Decimal(100)) if top_bps >= 0 else 0.0

    # Fees & gas
    zfee = (resp.get("fees") or {}).get("zeroExFee") or {}
    fee_amount = to_int(zfee.get("amount"))
    fee_token  = zfee.get("token")

    out = {
        # raw ids/amounts
        "sell_token": sell_token,
        "buy_token":  buy_token,
        "sell_amount": sell_amount,
        "buy_amount":  buy_amount,

        # human-readable actual trade amounts
        # sell: show up to 8 decimals; buy: default to whole tokens (round down)
        "sell_amount_human": str(sell_hr),
        "sell_amount_human_str": _fmt_decimal(sell_hr, 8),
        "buy_amount_human": str(buy_hr),
        "buy_amount_human_str": _fmt_decimal(buy_hr, 8),

        # symbols if available
        "sell_symbol": sell_symbol,
        "buy_symbol":  buy_symbol,

        # convenience (still keep unit prices if you want them later)
        "unit_price_human": str(buy_hr / sell_hr) if sell_hr else "0",
        "unit_price_human_inverted": str(sell_hr / buy_hr) if buy_hr else "0",

        # liquidity & routing
        "liquidity_available": bool(resp.get("liquidityAvailable")),
        "block_number": int(resp["blockNumber"]) if resp.get("blockNumber") else None,
        "fills": slim_fills,
        "top_source": top_source,
        "route_concentration_percent": route_concentration_pct,

        # fees & costs
        "zeroex_fee_amount": fee_amount,
        "zeroex_fee_token": fee_token,
        "gas": to_int(resp.get("gas")),
        "gas_price": to_int(resp.get("gasPrice")),
        "total_network_fee": to_int(resp.get("totalNetworkFee")),

        # misc
        "min_buy_amount": to_int(resp.get("minBuyAmount")) if resp.get("minBuyAmount") else None,
        "zid": resp.get("zid"),
    }
    return out

def pretty(title: str, data: Dict[str, Any]) -> None:
    print(f"\n=== {title} ===")
    print(json.dumps(data, indent=2, ensure_ascii=False))

def print_parsed(title: str, parsed: Dict[str, Any]) -> None:
    print(f"\n--- {title} (parsed) ---")
    s_sym = parsed.get("sell_symbol") or ""
    b_sym = parsed.get("buy_symbol") or ""
    print(f"for {parsed['sell_amount_human_str']} {s_sym} you get ≈ {parsed['buy_amount_human_str']} {b_sym}")
    print(f"liquidityAvailable: {parsed['liquidity_available']}  block: {parsed['block_number']}")
    print(f"top_source: {parsed['top_source']}  route_concentration%: {parsed['route_concentration_percent']}")
    if parsed['zeroex_fee_amount']:
        print(f"zeroExFee: {parsed['zeroex_fee_amount']} in {parsed['zeroex_fee_token']}")
    if parsed['total_network_fee']:
        print(f"networkFee(wei): {parsed['total_network_fee']}  gas: {parsed['gas']}  gasPrice: {parsed['gas_price']}")


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
        # raw (optional)
        # pretty(f"Price: sell {SELL_WETH_AMOUNT_HUMAN} WETH -> buy BASE_PEPE", p1)
        # parsed
        parsed1 = parse_0x_price_response(p1, sell_decimals=DECIMALS_WETH, buy_decimals=DECIMALS_TOKEN)
        print_parsed(f"Price: sell {SELL_WETH_AMOUNT_HUMAN} WETH -> buy BASE_PEPE", parsed1)
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
        # raw (optional)
        # pretty(f"Price: sell {SELL_TOKEN_AMOUNT_HUMAN} BASE_PEPE -> buy WETH", p2)
        # parsed
        parsed2 = parse_0x_price_response(p2, sell_decimals=DECIMALS_TOKEN, buy_decimals=DECIMALS_WETH)
        print_parsed(f"Price: sell {SELL_TOKEN_AMOUNT_HUMAN} BASE_PEPE -> buy WETH", parsed2)
    except Exception as e:
        sys.stderr.write(f"[sell TOKEN] {e}\n")

if __name__ == "__main__":
    main()
