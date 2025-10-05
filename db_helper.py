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








""" TOKEN DATA HELPERS """

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




""" TOKEN DATA HELPERS END """