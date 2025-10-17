#!/usr/bin/env python3
# ladder.py (modular + DB persistence)
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
# Side-effect when run as __main__: persists results to DB via db_helper.save_ladder_result()

import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from decimal import Decimal, getcontext

# --- precision for on-chain unit math ---
getcontext().prec = 80

# --- local modules (existing code) ---
from quote import get_price, to_base_units, parse_0x_price_response  # noqa
from db_helper import (
    DB_PATH,
    get_token_price,
    upsert_token_price,
    save_ladder_result,   # NEW: save result dict to DB
)  # noqa
from token_price import (
    load_cfg as _load_cfg,
    chain_from_id as _chain_from_id,
    fetch_token_overview,
    extract_symbol_price,
)  # noqa

CONFIG_FILE = Path("config.json")

# Defaults (can be overridden by function args)
DEFAULT_USD_LADDER = [
    1, 5, 10, 25, 50, 100, 250, 500,
    1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000,
    250_000, 500_000
]
DEFAULT_BASELINE_USD = 5
DEFAULT_RPS_SLEEP_SEC = 0.15

WETH_CA = "0x4200000000000000000000000000000000000006"


# ----------------------------- Utility ---------------------------------

def fmt_decimal(n: Optional[Decimal], places: int = 8) -> str:
    """Nicely format Decimals (or None)."""
    if n is None:
        return "-"
    q = Decimal("1." + "0" * places)
    return format(n.quantize(q), "f").rstrip("0").rstrip(".")


def impact_bps(unit_baseline: Decimal, unit_at_size: Decimal) -> Optional[Decimal]:
    """Positive bps means price gets worse at larger size."""
    if unit_baseline <= 0 or unit_at_size <= 0:
        return None
    return (unit_baseline / unit_at_size - Decimal(1)) * Decimal(10_000)


def ratelimit_sleep(seconds: float) -> None:
    time.sleep(seconds)


# ------------------------- Config & Pair Select --------------------------

def load_cfg(path: Path = CONFIG_FILE) -> Dict[str, Any]:
    """Load config.json."""
    return json.loads(path.read_text(encoding="utf-8"))


def pick_pair(conn: sqlite3.Connection, cfg: Dict[str, Any]) -> Optional[sqlite3.Row]:
    """
    Pick a pair from token_pairs. If cfg['pair_addresses'] present, prefer those.
    Returns sqlite3.Row with:
      base_address, base_symbol, base_decimals,
      pair_address,
      quote_address, quote_symbol, quote_decimals
    """
    conn.row_factory = sqlite3.Row
    wanted = (cfg.get("pair_addresses") or [])
    if wanted:
        cur = conn.execute(
            f"""
            SELECT base_address, base_symbol, base_decimals,
                   pair_address,
                   quote_address, quote_symbol, quote_decimals
            FROM token_pairs
            WHERE pair_address IN ({",".join("?" * len(wanted))})
            ORDER BY base_address
            LIMIT 1
            """,
            wanted,
        )
    else:
        cur = conn.execute(
            """
            SELECT base_address, base_symbol, base_decimals,
                   pair_address,
                   quote_address, quote_symbol, quote_decimals
            FROM token_pairs
            ORDER BY rowid
            LIMIT 1
            """
        )
    return cur.fetchone()


# --------------------------- USD Pricing --------------------------------

def get_token_usd(cfg: Dict[str, Any], token_ca: str) -> float:
    """
    Try DB first; fallback to Birdeye token_overview; upsert to DB.
    Returns USD per 1 token (float).
    """
    row = get_token_price(token_ca)
    if row and row.get("price") not in (None, 0):
        return float(row["price"])

    api = cfg.get("birdeye_api_key")
    chain = _chain_from_id(int(cfg.get("chain_id", 8453)))
    payload = fetch_token_overview(token_ca, api, chain)
    symbol, price = extract_symbol_price(payload)
    upsert_token_price(ca=token_ca, symbol=symbol, price=float(price))
    return float(price)


# --------------------------- Baseline Quotes -----------------------------

def compute_baselines(
    api_key: str,
    base_addr: str,
    base_dec: int,
    quote_addr: str,
    quote_dec: int,
    quote_usd: Decimal,
    baseline_usd: Decimal,
) -> Tuple[Decimal, Decimal, Decimal, Decimal, Decimal]:
    """
    Returns:
      base_usd                         (Decimal)   USD per 1 BASE (inferred)
      unit_buy_baseline                (Decimal)   BASE per 1 QUOTE
      unit_sell_baseline               (Decimal)   QUOTE per 1 BASE
      buy_baseline_usd_per_base        (Decimal)   USD per 1 BASE using BUY baseline quote
      sell_baseline_usd_per_base       (Decimal)   USD per 1 BASE using SELL baseline quote
    """
    # BUY baseline: QUOTE -> BASE at $baseline_usd (derive BASE_USD)
    baseline_quote_sell_hr = baseline_usd / quote_usd
    baseline_quote_sell_units = to_base_units(str(baseline_quote_sell_hr), quote_dec)
    resp_buy = get_price(
        sell_token=quote_addr,
        buy_token=base_addr,
        sell_amount=baseline_quote_sell_units,
        api_key=api_key,
    )
    parsed_buy = parse_0x_price_response(resp_buy, sell_decimals=quote_dec, buy_decimals=base_dec)
    unit_buy_baseline = Decimal(parsed_buy["unit_price_human"])  # BASE per 1 QUOTE

    # BASE_USD inferred from BUY baseline
    base_usd = quote_usd / unit_buy_baseline if unit_buy_baseline > 0 else Decimal(0)

    # SELL baseline: BASE -> QUOTE at $baseline_usd (anchor for SELL impact)
    baseline_base_sell_hr = baseline_usd / (base_usd if base_usd > 0 else Decimal("1"))
    baseline_base_sell_units = to_base_units(str(baseline_base_sell_hr), base_dec)
    resp_sell = get_price(
        sell_token=base_addr,
        buy_token=quote_addr,
        sell_amount=baseline_base_sell_units,
        api_key=api_key,
    )
    parsed_sell = parse_0x_price_response(resp_sell, sell_decimals=base_dec, buy_decimals=quote_dec)
    unit_sell_baseline = Decimal(parsed_sell["unit_price_human"])  # QUOTE per 1 BASE

    # Common-unit baselines (USD per 1 BASE)
    buy_baseline_usd_per_base = quote_usd / unit_buy_baseline if unit_buy_baseline > 0 else Decimal(0)
    sell_baseline_usd_per_base = quote_usd * unit_sell_baseline

    return (
        base_usd,
        unit_buy_baseline,
        unit_sell_baseline,
        buy_baseline_usd_per_base,
        sell_baseline_usd_per_base,
    )


# ----------------------------- Ladder Sweep ------------------------------

def ladder_sweep(
    api_key: str,
    base_addr: str,
    base_dec: int,
    quote_addr: str,
    quote_dec: int,
    quote_usd: Decimal,
    base_usd: Decimal,
    unit_buy_baseline: Decimal,
    unit_sell_baseline: Decimal,
    usd_ladder: List[int],
    rps_sleep_sec: float = DEFAULT_RPS_SLEEP_SEC,
    *,
    buy_baseline_usd_per_base: Optional[Decimal] = None,  # kept for signature compatibility; unused
    sell_baseline_usd_per_base: Optional[Decimal] = None, # kept for signature compatibility; unused
) -> List[Tuple[int, Optional[Decimal], Optional[Decimal], bool, bool, Optional[str], float, Optional[str], float]]:
    """
    Returns rows:
      (usd, buy_bps, sell_bps, buy_liq, sell_liq, buy_top_source, buy_conc_pct, sell_top_source, sell_conc_pct)
    """
    rows = []
    for usd in usd_ladder:
        usd_d = Decimal(usd)

        # BUY: QUOTE -> BASE at this USD (match original ladder_test.py math)
        sell_quote_hr = usd_d / quote_usd if quote_usd > 0 else Decimal(0)
        sell_quote_units = to_base_units(str(sell_quote_hr), quote_dec)
        buy_resp = get_price(
            sell_token=quote_addr,
            buy_token=base_addr,
            sell_amount=sell_quote_units,
            api_key=api_key,
        )
        buy_parsed = parse_0x_price_response(buy_resp, sell_decimals=quote_dec, buy_decimals=base_dec)
        unit_buy = Decimal(buy_parsed["unit_price_human"]) if buy_parsed["unit_price_human"] != "0" else Decimal(0)
        buy_bps = impact_bps(unit_buy_baseline, unit_buy)
        ratelimit_sleep(rps_sleep_sec)

        # SELL: BASE -> QUOTE at this USD (match original ladder_test.py math)
        sell_base_hr = usd_d / (base_usd if base_usd > 0 else Decimal("1"))
        sell_base_units = to_base_units(str(sell_base_hr), base_dec)
        sell_resp = get_price(
            sell_token=base_addr,
            buy_token=quote_addr,
            sell_amount=sell_base_units,
            api_key=api_key,
        )
        sell_parsed = parse_0x_price_response(sell_resp, sell_decimals=base_dec, buy_decimals=quote_dec)
        unit_sell = Decimal(sell_parsed["unit_price_human"]) if sell_parsed["unit_price_human"] != "0" else Decimal(0)
        sell_bps = impact_bps(unit_sell_baseline, unit_sell)

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

    return rows


# ------------------------------ Rendering --------------------------------

def render_header(
    pair_addr: str,
    base_sym: str,
    base_addr: str,
    quote_sym: str,
    quote_addr: str,
    usd_ladder: List[int],
    baseline_usd: Decimal,
    quote_usd: Decimal,
    base_usd: Decimal,
) -> None:
    print(f"\n=== LADDER TEST (pair: {pair_addr}) ===")
    print(f"base: {base_sym} ({base_addr})  quote: {quote_sym} ({quote_addr})")
    print("USD ladder:", usd_ladder)
    print("Baseline USD:", int(baseline_usd))
    print(f"QUOTE_USD ({quote_sym}): {fmt_decimal(quote_usd, 6)}")
    print(f"BASE_USD  ({base_sym}): {fmt_decimal(base_usd, 12)}")


def render_rows(rows: List[Tuple[int, Optional[Decimal], Optional[Decimal], bool, bool, Optional[str], float, Optional[str], float]]) -> None:
    print("\nUSD        BUY_bps   SELL_bps   buyLiq  sellLiq   buyTop(%)         sellTop(%)")
    print("--------------------------------------------------------------------------------")
    for (usd, b_bps, s_bps, b_liq, s_liq, b_top, b_conc, s_top, s_conc) in rows:
        print(
            f"{usd:>7}  "
            f"{fmt_decimal(b_bps, 2):>8}  "
            f"{fmt_decimal(s_bps, 2):>8}   "
            f"{str(b_liq)[0]:>5}    {str(s_liq)[0]:>6}   "
            f"{(b_top or '-')[:12]:<12}({b_conc:>5.1f})   "
            f"{(s_top or '-')[:12]:<12}({s_conc:>5.1f})"
        )


def render_footer(baseline_usd: Decimal, base_sym: str, quote_sym: str) -> None:
    print("\nNotes:")
    print(f"- BUY = {quote_sym}→{base_sym} (route-only impact; fees separate)")
    print(f"- SELL = {base_sym}→{quote_sym}")
    print(f"- Impact bps is vs ${int(baseline_usd)} baseline (positive = worse price at size).")
    print("- You can raise RPS by reducing sleep, but stay under 10 rps.")


# ------------------------------ Orchestrator ------------------------------

def run(
    usd_ladder: Optional[List[int]] = None,
    baseline_usd: int = DEFAULT_BASELINE_USD,
    rps_sleep_sec: float = DEFAULT_RPS_SLEEP_SEC,
) -> Dict[str, Any]:
    """
    Orchestrates the test and prints the same table as before.
    Returns a dict with context & rows so other scripts can reuse programmatically.
    (No DB writes here—saving is done in __main__ to keep this reusable/pure.)
    """
    # --- config ---
    cfg = load_cfg()  # expects: 0x_api_key, birdeye_api_key, db_path, chain_id, pair_addresses
    api_key = cfg.get("0x_api_key")
    if not api_key:
        raise SystemExit("Missing 0x_api_key in config.json")

    # --- DB / pair ---
    conn = sqlite3.connect(DB_PATH)
    pair = pick_pair(conn, cfg)
    if not pair:
        raise SystemExit("No pair found in token_pairs. Run token_data.py first.")

    base_addr = pair["base_address"]
    base_sym = pair["base_symbol"] or "BASE"
    base_dec = int(pair["base_decimals"])
    pair_addr = pair["pair_address"]
    quote_addr = pair["quote_address"]
    quote_sym = pair["quote_symbol"] or "QUOTE"
    quote_dec = int(pair["quote_decimals"])

    # --- fetch USD for quote token (WETH typical, but works for any quote) ---
    quote_usd_f = get_token_usd(cfg, quote_addr)
    quote_usd = Decimal(str(quote_usd_f))

    # --- baselines ---
    baseline_d = Decimal(baseline_usd)  # int -> Decimal is safe
    (
        base_usd,
        unit_buy_baseline,
        unit_sell_baseline,
        buy_base_usd,     # USD per 1 BASE (from BUY anchor)
        sell_base_usd,    # USD per 1 BASE (from SELL anchor)
    ) = compute_baselines(
        api_key=api_key,
        base_addr=base_addr,
        base_dec=base_dec,
        quote_addr=quote_addr,
        quote_dec=quote_dec,
        quote_usd=quote_usd,
        baseline_usd=baseline_d,
    )

    # --- sweep ---
    ladder = usd_ladder or DEFAULT_USD_LADDER
    rows = ladder_sweep(
        api_key=api_key,
        base_addr=base_addr,
        base_dec=base_dec,
        quote_addr=quote_addr,
        quote_dec=quote_dec,
        quote_usd=quote_usd,
        base_usd=base_usd,
        unit_buy_baseline=unit_buy_baseline,
        unit_sell_baseline=unit_sell_baseline,
        usd_ladder=ladder,
        rps_sleep_sec=rps_sleep_sec,
        buy_baseline_usd_per_base=buy_base_usd,
        sell_baseline_usd_per_base=sell_base_usd,
    )

    # --- print (same UX as original) ---
    render_header(
        pair_addr=pair_addr,
        base_sym=base_sym,
        base_addr=base_addr,
        quote_sym=quote_sym,
        quote_addr=quote_addr,
        usd_ladder=ladder,
        baseline_usd=baseline_d,
        quote_usd=quote_usd,
        base_usd=base_usd,
    )
    render_rows(rows)
    render_footer(baseline_d, base_sym, quote_sym)

    # Return data for programmatic use
    return {
        "pair": {
            "pair_address": pair_addr,
            "base": {"address": base_addr, "symbol": base_sym, "decimals": base_dec},
            "quote": {"address": quote_addr, "symbol": quote_sym, "decimals": quote_dec},
        },
        "params": {
            "usd_ladder": ladder,
            "baseline_usd": baseline_usd,
            "rps_sleep_sec": rps_sleep_sec,
        },
        "prices": {
            "quote_usd": float(quote_usd),
            "base_usd": float(base_usd),
        },
        "baselines": {
            "unit_buy_baseline_base_per_quote": str(unit_buy_baseline),
            "unit_sell_baseline_quote_per_base": str(unit_sell_baseline),
            "buy_baseline_usd_per_base": float(buy_base_usd),
            "sell_baseline_usd_per_base": float(sell_base_usd),
        },
        "rows": [
            {
                "usd": usd,
                "buy_bps": float(b_bps) if b_bps is not None else None,
                "sell_bps": float(s_bps) if s_bps is not None else None,
                "buy_liquidity_available": bool(b_liq),
                "sell_liquidity_available": bool(s_liq),
                "buy_top_source": b_top,
                "buy_route_concentration_percent": float(b_conc),
                "sell_top_source": s_top,
                "sell_route_concentration_percent": float(s_conc),
            }
            for (usd, b_bps, s_bps, b_liq, s_liq, b_top, b_conc, s_top, s_conc) in rows
        ],
    }


# ------------------------------- Entrypoint -------------------------------

if __name__ == "__main__":
    result = run()
    # Persist the run + points
    try:
        run_id = save_ladder_result(result)
        print(f"\n[ok] saved ladder run_id: {run_id}")
    except Exception as e:
        print(f"\n[warn] failed to save ladder run: {e}")
