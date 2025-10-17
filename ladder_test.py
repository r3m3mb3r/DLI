#!/usr/bin/env python3
# ladder_test.py
#
# Test sweep: fixed USD ladder, both directions, compute impact (bps) vs baseline.
# Reuses your existing code:
#  - quote.get_price, quote.to_base_units, quote.parse_0x_price_response (0x v2, slippageBps=0)
#  - db_helper.get_token_price / upsert_token_price (for live WETH USD)
#  - token_price.fetch_token_overview / extract_symbol_price (Birdeye WETH USD fallback)
#
# Requirements:
#   - Run init_db.py at least once (schema).  - token_pairs must have your pair.
#   - config.json has 0x_api_key, birdeye_api_key, chain_id, db_path, optional pair_addresses.
#
# Output: console table with USD, BUY_bps, SELL_bps, and quick notes.

import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from decimal import Decimal, getcontext

# --- precision for on-chain unit math ---
getcontext().prec = 80

# --- local modules (existing code) ---
from quote import get_price, to_base_units, parse_0x_price_response  # 0x helpers (Base chainId=8453)  # noqa
from db_helper import DB_PATH, get_token_price, upsert_token_price                                           # noqa
from token_price import load_cfg as _load_cfg, chain_from_id as _chain_from_id, fetch_token_overview, extract_symbol_price  # noqa

CONFIG_FILE = Path("config.json")

# Ladder values (USD). Edit as you like; keep under rate limit budget.
USD_LADDER = [1, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000]
BASELINE_USD = 5  # baseline for impact bps (vs this size)

# Rate-limit: 10 rps -> ~0.1s; use small safety margin.
RPS_SLEEP_SEC = 0.15

def _load_cfg() -> Dict[str, Any]:
    cfg = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
    return cfg

def _pick_pair(conn: sqlite3.Connection, cfg: Dict[str, Any]) -> Optional[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    cur = None
    wanted = (cfg.get("pair_addresses") or [])
    print(wanted)
    if wanted:
        cur = conn.execute(
            """
            SELECT base_address, base_symbol, base_decimals,
                   pair_address,
                   quote_address, quote_symbol, quote_decimals
            FROM token_pairs
            WHERE pair_address IN ({})
            ORDER BY base_address LIMIT 1
            """.format(",".join("?" * len(wanted))),
            wanted,
        )
    else:
        cur = conn.execute(
            """
            SELECT base_address, base_symbol, base_decimals,
                   pair_address,
                   quote_address, quote_symbol, quote_decimals
            FROM token_pairs
            ORDER BY rowid LIMIT 1
            """
        )
    return cur.fetchone()

def _get_weth_usd(cfg: Dict[str, Any], weth_ca: str) -> float:
    """
    Try DB first; fallback to Birdeye token_overview; upsert to DB.
    """
    row = get_token_price(weth_ca)
    if row and row.get("price") not in (None, 0):
        return float(row["price"])

    # fetch from Birdeye via existing token_price helpers
    api = cfg.get("birdeye_api_key")
    chain = _chain_from_id(int(cfg.get("chain_id", 8453)))
    payload = fetch_token_overview(weth_ca, api, chain)
    symbol, price = extract_symbol_price(payload)
    # upsert for next time
    upsert_token_price(ca=weth_ca, symbol=symbol, price=float(price))
    return float(price)

def _fmt(n: Optional[Decimal], places: int = 8) -> str:
    if n is None:
        return "-"
    q = Decimal("1." + "0" * places)
    return format(n.quantize(q), "f").rstrip("0").rstrip(".")

def _impact_bps(unit_baseline: Decimal, unit_at_size: Decimal) -> Optional[Decimal]:
    if unit_baseline <= 0 or unit_at_size <= 0:
        return None
    # Positive when price gets worse at larger size
    return (unit_baseline / unit_at_size - Decimal(1)) * Decimal(10_000)

def _sleep():
    time.sleep(RPS_SLEEP_SEC)

def run() -> None:
    # --- config ---
    cfg = _load_cfg()  # has keys: 0x_api_key, birdeye_api_key, db_path, chain_id, pair_addresses  :contentReference[oaicite:5]{index=5}
    api_key = cfg.get("0x_api_key")
    if not api_key:
        raise SystemExit("Missing 0x_api_key in config.json")

    # --- DB / pair ---
    conn = sqlite3.connect(DB_PATH)  # DB_PATH from db_helper  :contentReference[oaicite:6]{index=6}
    pair = _pick_pair(conn, cfg)
    if not pair:
        raise SystemExit("No pair found in token_pairs. Run token_data.py first.")
    base_addr = pair["base_address"]
    base_sym  = pair["base_symbol"] or "BASE"
    base_dec  = int(pair["base_decimals"])
    pair_addr = pair["pair_address"]
    quote_addr = pair["quote_address"]
    quote_sym  = pair["quote_symbol"] or "QUOTE"
    quote_dec  = int(pair["quote_decimals"])

    # --- Expect WETH as quote; if not, we still proceed (USD conversion will use quote USD) ---
    WETH_CA = "0x4200000000000000000000000000000000000006"

    # --- fetch USD for quote token (assume WETH on Base; if not WETH, fetch that token's USD) ---
    quote_usd = _get_weth_usd(cfg, quote_addr) if quote_addr.lower() == WETH_CA.lower() else _get_weth_usd(cfg, quote_addr)
    # USD per 1 QUOTE token (e.g., WETH)
    QUOTE_USD = Decimal(str(quote_usd))

    # --- get baseline BUY (QUOTE->BASE) at BASELINE_USD to infer token USD ---
    baseline_quote_sell_hr = Decimal(Baseline := BASELINE_USD) / QUOTE_USD
    # 0x expects base units
    baseline_quote_sell_base_units = to_base_units(str(baseline_quote_sell_hr), quote_dec)
    resp_buy_baseline = get_price(
        sell_token=quote_addr,
        buy_token=base_addr,
        sell_amount=baseline_quote_sell_base_units,
        api_key=api_key,
    )
    parsed_buy_baseline = parse_0x_price_response(resp_buy_baseline, sell_decimals=quote_dec, buy_decimals=base_dec)  # :contentReference[oaicite:7]{index=7}
    # unit price: BASE per 1 QUOTE
    unit_buy_baseline = Decimal(parsed_buy_baseline["unit_price_human"])
    # token USD = QUOTE_USD / (BASE per QUOTE)
    BASE_USD = QUOTE_USD / unit_buy_baseline if unit_buy_baseline > 0 else Decimal(0)

    # --- also get baseline SELL (BASE->QUOTE) for SELL impact baseline ---
    baseline_base_sell_hr = Decimal(Baseline) / (BASE_USD if BASE_USD > 0 else Decimal("1"))
    baseline_base_sell_base_units = to_base_units(str(baseline_base_sell_hr), base_dec)
    resp_sell_baseline = get_price(
        sell_token=base_addr,
        buy_token=quote_addr,
        sell_amount=baseline_base_sell_base_units,
        api_key=api_key,
    )
    parsed_sell_baseline = parse_0x_price_response(resp_sell_baseline, sell_decimals=base_dec, buy_decimals=quote_dec)
    unit_sell_baseline = Decimal(parsed_sell_baseline["unit_price_human"])  # QUOTE per 1 BASE

    # --- print context header ---
    print("\n=== LADDER TEST (pair: {}) ===".format(pair_addr))
    print("base: {} ({})  quote: {} ({})".format(base_sym, base_addr, quote_sym, quote_addr))
    print("USD ladder:", USD_LADDER)
    print("Baseline USD:", BASELINE_USD)
    print("QUOTE_USD ({}): {}".format(quote_sym, _fmt(QUOTE_USD, 6)))
    print("BASE_USD  ({}): {}".format(base_sym, _fmt(BASE_USD, 12)))
    _sleep()

    # --- walk ladder both directions ---
    rows = []
    for usd in USD_LADDER:
        usd_d = Decimal(usd)

        # BUY: sell QUOTE for BASE at this USD
        sell_quote_hr = usd_d / QUOTE_USD if QUOTE_USD > 0 else Decimal(0)
        sell_quote_units = to_base_units(str(sell_quote_hr), quote_dec)
        buy_resp = get_price(
            sell_token=quote_addr,
            buy_token=base_addr,
            sell_amount=sell_quote_units,
            api_key=api_key,
        )
        buy_parsed = parse_0x_price_response(buy_resp, sell_decimals=quote_dec, buy_decimals=base_dec)
        unit_buy = Decimal(buy_parsed["unit_price_human"]) if buy_parsed["unit_price_human"] != "0" else Decimal(0)
        buy_bps = _impact_bps(unit_buy_baseline, unit_buy)
        _sleep()

        # SELL: sell BASE for QUOTE at this USD
        sell_base_hr = usd_d / (BASE_USD if BASE_USD > 0 else Decimal("1"))
        sell_base_units = to_base_units(str(sell_base_hr), base_dec)
        sell_resp = get_price(
            sell_token=base_addr,
            buy_token=quote_addr,
            sell_amount=sell_base_units,
            api_key=api_key,
        )
        sell_parsed = parse_0x_price_response(sell_resp, sell_decimals=base_dec, buy_decimals=quote_dec)
        unit_sell = Decimal(sell_parsed["unit_price_human"]) if sell_parsed["unit_price_human"] != "0" else Decimal(0)
        sell_bps = _impact_bps(unit_sell_baseline, unit_sell)
        _sleep()

        rows.append((
            usd,
            buy_bps if buy_bps is not None else None,
            sell_bps if sell_bps is not None else None,
            buy_parsed.get("liquidity_available"),
            sell_parsed.get("liquidity_available"),
            buy_parsed.get("top_source"),
            float(buy_parsed.get("route_concentration_percent") or 0.0),
            sell_parsed.get("top_source"),
            float(sell_parsed.get("route_concentration_percent") or 0.0),
        ))

    # --- print table ---
    print("\nUSD        BUY_bps   SELL_bps   buyLiq  sellLiq   buyTop(%)         sellTop(%)")
    print("--------------------------------------------------------------------------------")
    for (usd, b_bps, s_bps, b_liq, s_liq, b_top, b_conc, s_top, s_conc) in rows:
        print(
            f"{usd:>7}  "
            f"{_fmt(b_bps, 2):>8}  "
            f"{_fmt(s_bps, 2):>8}   "
            f"{str(b_liq)[0]:>5}    {str(s_liq)[0]:>6}   "
            f"{(b_top or '-')[:12]:<12}({b_conc:>5.1f})   "
            f"{(s_top or '-')[:12]:<12}({s_conc:>5.1f})"
        )

    print("\nNotes:")
    print("- BUY = {}→{} (route-only impact; fees separate)".format(quote_sym, base_sym))
    print("- SELL = {}→{}".format(base_sym, quote_sym))
    print("- Impact bps is vs ${} baseline (positive = worse price at size).".format(BASELINE_USD))
    print("- You can raise RPS by reducing sleep, but stay under 10 rps.")

if __name__ == "__main__":
    run()
