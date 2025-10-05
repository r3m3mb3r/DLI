#!/usr/bin/env python3
"""
init_db.py

Schema: base/quote with base_address as the main identifier.
- base_address: the asset being priced (main id)
- pair_address: LP/pool contract address
- quote_address: the pricing asset in the pair

If ./config.json has {"db_path": "..."} it's used; otherwise ./liquidity.db.
"""

import json
import sqlite3
from pathlib import Path

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS token_pairs (
  base_address     TEXT NOT NULL,
  base_symbol      TEXT,
  base_decimals    INTEGER NOT NULL,

  pair_address     TEXT NOT NULL,   -- LP/pool contract

  quote_address    TEXT NOT NULL,
  quote_symbol     TEXT,
  quote_decimals   INTEGER NOT NULL,

  PRIMARY KEY (base_address, pair_address)
);

CREATE INDEX IF NOT EXISTS idx_token_pairs_pair
  ON token_pairs(pair_address);

CREATE INDEX IF NOT EXISTS idx_token_pairs_quote
  ON token_pairs(quote_address);
"""

def get_db_path() -> Path:
    cfg_path = Path("config.json")
    if cfg_path.exists():
        try:
            cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
            if isinstance(cfg, dict) and cfg.get("db_path"):
                return Path(cfg["db_path"])
        except Exception:
            pass
    return Path("liquidity.db")

def init_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.executescript(SCHEMA_SQL)
        conn.commit()
    print(f"[ok] initialized schema at: {db_path}")

if __name__ == "__main__":
    init_db(get_db_path())
