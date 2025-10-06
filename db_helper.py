#!/usr/bin/env python3
# db_helper.py

import json
import sqlite3
from pathlib import Path
from typing import Optional

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








# --- TOKEN DATA HELPERS ---

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




# --- TOKEN DATA HELPERS END ---



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


# --- LIVE PRICE HELPERS END ---