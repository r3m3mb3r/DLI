#!/usr/bin/env python3
# db_helper.py

import json
import sqlite3
from pathlib import Path
from typing import Optional, List, Dict, Any, Iterable, Tuple

CONFIG_FILE = Path("config.json")

def _load_db_path() -> str:
    try:
        cfg = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
        return cfg.get("db_path", "liquidity.db")
    except FileNotFoundError:
        return "liquidity.db"
    except Exception:
        return "liquidity.db"

DB_PATH = _load_db_path()


# ============================================================
# TOKEN DATA HELPERS (existing)
# ============================================================

SQL_UPSERT_TOKEN_PAIR = """
INSERT INTO token_pairs (
  base_address, base_symbol, base_decimals,
  pair_address,
  quote_address, quote_symbol, quote_decimals
) VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(base_address, pair_address) DO UPDATE SET
  base_symbol    = excluded.base_symbol,
  base_decimals  = excluded.base_decimals,
  quote_address  = excluded.quote_address,
  quote_symbol   = excluded.quote_symbol,
  quote_decimals = excluded.quote_decimals;
"""

def upsert_token_pair(
    *,
    base_address: str,
    base_symbol: Optional[str],
    base_decimals: int,
    pair_address: str,
    quote_address: str,
    quote_symbol: Optional[str],
    quote_decimals: int,
) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute(
            SQL_UPSERT_TOKEN_PAIR,
            (
                base_address, base_symbol, int(base_decimals),
                pair_address,
                quote_address, quote_symbol, int(quote_decimals),
            ),
        )
        conn.commit()


# --- LIVE PRICE HELPERS ---

SQL_UPSERT_TOKEN_PRICE = """
INSERT INTO token_prices_live (ca, symbol, price)
VALUES (?, ?, ?)
ON CONFLICT(ca) DO UPDATE SET
  symbol = excluded.symbol,
  price  = excluded.price;
-- timestamp auto-updated by trigger in schema
"""

SQL_SELECT_TOKEN_PRICE = """
SELECT ca, symbol, price, timestamp
FROM token_prices_live
WHERE ca = ?
"""

def upsert_token_price(*, ca: str, symbol: Optional[str], price: float) -> None:
    """
    Upsert the live price for a token (one row per CA).
    Respects the exact 'ca' casing you pass in (no normalization).
    """
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute(SQL_UPSERT_TOKEN_PRICE, (ca, symbol, float(price)))
        conn.commit()

def get_token_price(ca: str) -> Optional[dict]:
    """
    Return the current live price row for the given contract address, or None if not found.
    Shape: {"ca": ..., "symbol": ..., "price": float, "timestamp": int}
    """
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(SQL_SELECT_TOKEN_PRICE, (ca,))
        row = cur.fetchone()
        if not row:
            return None
        return {
            "ca": row["ca"],
            "symbol": row["symbol"],
            "price": float(row["price"]),
            "timestamp": int(row["timestamp"]),
        }


# ============================================================
# NEW: LADDER RUNS + POINTS HELPERS
# ============================================================

# ---------------------------
# Inserts / Updates
# ---------------------------

SQL_INSERT_LADDER_RUN = """
INSERT INTO ladder_runs (
  base_address, pair_address, quote_address,
  base_symbol, quote_symbol, base_decimals, quote_decimals,
  baseline_usd, quote_usd, base_usd,
  unit_buy_baseline, unit_sell_baseline,
  usd_ladder_json
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

def create_ladder_run(
    *,
    base_address: str,
    pair_address: str,
    quote_address: str,
    base_symbol: Optional[str],
    quote_symbol: Optional[str],
    base_decimals: int,
    quote_decimals: int,
    baseline_usd: int,
    quote_usd: float,
    base_usd: float,
    unit_buy_baseline: str,
    unit_sell_baseline: str,
    usd_ladder: Optional[Iterable[int]] = None,
) -> int:
    """
    Insert a ladder_runs row and return run_id.
    """
    ladder_json = json.dumps(list(usd_ladder)) if usd_ladder is not None else None
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        cur = conn.execute(
            SQL_INSERT_LADDER_RUN,
            (
                base_address, pair_address, quote_address,
                base_symbol, quote_symbol, int(base_decimals), int(quote_decimals),
                int(baseline_usd), float(quote_usd), float(base_usd),
                str(unit_buy_baseline), str(unit_sell_baseline),
                ladder_json,
            ),
        )
        run_id = cur.lastrowid
        conn.commit()
        return int(run_id)


SQL_INSERT_LADDER_POINT = """
INSERT INTO ladder_points (
  run_id, usd,
  buy_bps, sell_bps,
  buy_liquidity_available, sell_liquidity_available,
  buy_top_source, buy_route_concentration_percent,
  sell_top_source, sell_route_concentration_percent
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(run_id, usd) DO UPDATE SET
  buy_bps  = excluded.buy_bps,
  sell_bps = excluded.sell_bps,
  buy_liquidity_available  = excluded.buy_liquidity_available,
  sell_liquidity_available = excluded.sell_liquidity_available,
  buy_top_source = excluded.buy_top_source,
  buy_route_concentration_percent = excluded.buy_route_concentration_percent,
  sell_top_source = excluded.sell_top_source,
  sell_route_concentration_percent = excluded.sell_route_concentration_percent
"""

def insert_ladder_point(
    *,
    run_id: int,
    usd: int,
    buy_bps: Optional[float],
    sell_bps: Optional[float],
    buy_liquidity_available: Optional[bool],
    sell_liquidity_available: Optional[bool],
    buy_top_source: Optional[str],
    buy_route_concentration_percent: Optional[float],
    sell_top_source: Optional[str],
    sell_route_concentration_percent: Optional[float],
) -> None:
    """
    Insert or update a single ladder point for a run.
    """
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute(
            SQL_INSERT_LADDER_POINT,
            (
                int(run_id), int(usd),
                float(buy_bps) if buy_bps is not None else None,
                float(sell_bps) if sell_bps is not None else None,
                1 if buy_liquidity_available else 0 if buy_liquidity_available is not None else None,
                1 if sell_liquidity_available else 0 if sell_liquidity_available is not None else None,
                buy_top_source,
                float(buy_route_concentration_percent) if buy_route_concentration_percent is not None else None,
                sell_top_source,
                float(sell_route_concentration_percent) if sell_route_concentration_percent is not None else None,
            ),
        )
        conn.commit()

def bulk_insert_ladder_points(
    run_id: int,
    rows: Iterable[Tuple[
        int, Optional[float], Optional[float], Optional[bool], Optional[bool],
        Optional[str], Optional[float], Optional[str], Optional[float]
    ]],
) -> None:
    """
    Efficiently insert/update many points for a run.
    Each row is: (usd, buy_bps, sell_bps, buy_liq, sell_liq, buy_top, buy_conc, sell_top, sell_conc)
    """
    rows = list(rows)
    if not rows:
        return
    payload = []
    for (usd, buy_bps, sell_bps, buy_liq, sell_liq, buy_top, buy_conc, sell_top, sell_conc) in rows:
        payload.append((
            int(run_id), int(usd),
            float(buy_bps) if buy_bps is not None else None,
            float(sell_bps) if sell_bps is not None else None,
            1 if buy_liq else 0 if buy_liq is not None else None,
            1 if sell_liq else 0 if sell_liq is not None else None,
            buy_top,
            float(buy_conc) if buy_conc is not None else None,
            sell_top,
            float(sell_conc) if sell_conc is not None else None,
        ))
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executemany(SQL_INSERT_LADDER_POINT, payload)
        conn.commit()


# ---------------------------
# Reads
# ---------------------------

SQL_SELECT_LADDER_RUN = """
SELECT
  id, started_at,
  base_address, pair_address, quote_address,
  base_symbol, quote_symbol, base_decimals, quote_decimals,
  baseline_usd, quote_usd, base_usd,
  unit_buy_baseline, unit_sell_baseline,
  usd_ladder_json
FROM ladder_runs
WHERE id = ?
"""

def get_ladder_run(run_id: int) -> Optional[Dict[str, Any]]:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(SQL_SELECT_LADDER_RUN, (int(run_id),))
        row = cur.fetchone()
        if not row:
            return None
        return {
            "id": int(row["id"]),
            "started_at": int(row["started_at"]),
            "base_address": row["base_address"],
            "pair_address": row["pair_address"],
            "quote_address": row["quote_address"],
            "base_symbol": row["base_symbol"],
            "quote_symbol": row["quote_symbol"],
            "base_decimals": int(row["base_decimals"]),
            "quote_decimals": int(row["quote_decimals"]),
            "baseline_usd": int(row["baseline_usd"]),
            "quote_usd": float(row["quote_usd"]),
            "base_usd": float(row["base_usd"]),
            "unit_buy_baseline": row["unit_buy_baseline"],
            "unit_sell_baseline": row["unit_sell_baseline"],
            "usd_ladder": json.loads(row["usd_ladder_json"]) if row["usd_ladder_json"] else None,
        }

SQL_LIST_LADDER_RUNS = """
SELECT
  id, started_at,
  base_address, pair_address, quote_address,
  base_symbol, quote_symbol, baseline_usd
FROM ladder_runs
ORDER BY started_at DESC, id DESC
LIMIT ?
OFFSET ?
"""

def list_ladder_runs(limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(SQL_LIST_LADDER_RUNS, (int(limit), int(offset)))
        out = []
        for r in cur.fetchall():
            out.append({
                "id": int(r["id"]),
                "started_at": int(r["started_at"]),
                "base_address": r["base_address"],
                "pair_address": r["pair_address"],
                "quote_address": r["quote_address"],
                "base_symbol": r["base_symbol"],
                "quote_symbol": r["quote_symbol"],
                "baseline_usd": int(r["baseline_usd"]),
            })
        return out

SQL_SELECT_LADDER_POINTS = """
SELECT
  run_id, usd,
  buy_bps, sell_bps,
  buy_liquidity_available, sell_liquidity_available,
  buy_top_source, buy_route_concentration_percent,
  sell_top_source, sell_route_concentration_percent
FROM ladder_points
WHERE run_id = ?
ORDER BY usd ASC
"""

def get_ladder_points(run_id: int) -> List[Dict[str, Any]]:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(SQL_SELECT_LADDER_POINTS, (int(run_id),))
        out = []
        for r in cur.fetchall():
            out.append({
                "run_id": int(r["run_id"]),
                "usd": int(r["usd"]),
                "buy_bps": float(r["buy_bps"]) if r["buy_bps"] is not None else None,
                "sell_bps": float(r["sell_bps"]) if r["sell_bps"] is not None else None,
                "buy_liquidity_available": bool(r["buy_liquidity_available"]) if r["buy_liquidity_available"] is not None else None,
                "sell_liquidity_available": bool(r["sell_liquidity_available"]) if r["sell_liquidity_available"] is not None else None,
                "buy_top_source": r["buy_top_source"],
                "buy_route_concentration_percent": float(r["buy_route_concentration_percent"]) if r["buy_route_concentration_percent"] is not None else None,
                "sell_top_source": r["sell_top_source"],
                "sell_route_concentration_percent": float(r["sell_route_concentration_percent"]) if r["sell_route_concentration_percent"] is not None else None,
            })
        return out


# ---------------------------
# Deletes
# ---------------------------

SQL_DELETE_LADDER_RUN = "DELETE FROM ladder_runs WHERE id = ?"

def delete_ladder_run(run_id: int) -> int:
    """
    Delete a run (points cascade via FK). Returns number of deleted ladder_runs rows (0 or 1).
    """
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        cur = conn.execute(SQL_DELETE_LADDER_RUN, (int(run_id),))
        conn.commit()
        return cur.rowcount


# ---------------------------
# Convenience: save output from ladder.run()
# ---------------------------

def save_ladder_result(result: Dict[str, Any]) -> int:
    """
    Convenience wrapper to persist the dict returned by your ladder `run()` orchestrator.
    Expects the structure produced by your modular ladder script.
    Returns the created run_id.
    """
    pair = result["pair"]
    params = result["params"]
    prices = result["prices"]
    baselines = result.get("baselines", {})
    rows = result.get("rows", [])

    run_id = create_ladder_run(
        base_address=pair["base"]["address"],
        pair_address=pair["pair_address"],
        quote_address=pair["quote"]["address"],
        base_symbol=pair["base"]["symbol"],
        quote_symbol=pair["quote"]["symbol"],
        base_decimals=int(pair["base"]["decimals"]),
        quote_decimals=int(pair["quote"]["decimals"]),
        baseline_usd=int(params["baseline_usd"]),
        quote_usd=float(prices["quote_usd"]),
        base_usd=float(prices["base_usd"]),
        unit_buy_baseline=str(baselines.get("unit_buy_baseline_base_per_quote", "")),
        unit_sell_baseline=str(baselines.get("unit_sell_baseline_quote_per_base", "")),
        usd_ladder=params.get("usd_ladder"),
    )

    # Adapt rows into the bulk insert shape
    point_rows = []
    for r in rows:
        point_rows.append((
            int(r["usd"]),
            float(r["buy_bps"]) if r["buy_bps"] is not None else None,
            float(r["sell_bps"]) if r["sell_bps"] is not None else None,
            bool(r["buy_liquidity_available"]) if r.get("buy_liquidity_available") is not None else None,
            bool(r["sell_liquidity_available"]) if r.get("sell_liquidity_available") is not None else None,
            r.get("buy_top_source"),
            float(r["buy_route_concentration_percent"]) if r.get("buy_route_concentration_percent") is not None else None,
            r.get("sell_top_source"),
            float(r["sell_route_concentration_percent"]) if r.get("sell_route_concentration_percent") is not None else None,
        ))

    bulk_insert_ladder_points(run_id, point_rows)
    return run_id
