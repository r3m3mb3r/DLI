#!/usr/bin/env python3
"""
init_db.py

Schema: base/quote with base_address as the main identifier.
If ./config.json has {"db_path": "..."} it's used; otherwise ./liquidity.db.
"""

import json
import sqlite3
from pathlib import Path

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

-- Token pairs (unchanged)
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

-- Live price (one row per token)
-- timestamp auto on INSERT; trigger updates it on price/symbol changes
CREATE TABLE IF NOT EXISTS token_prices_live (
  ca         TEXT PRIMARY KEY,                                  -- token contract address
  symbol     TEXT,
  price      REAL NOT NULL,                                     -- choose a single unit (e.g., USD or WETH)
  timestamp  INTEGER NOT NULL DEFAULT (strftime('%s','now'))    -- UNIX seconds on INSERT
);

CREATE TRIGGER IF NOT EXISTS trg_token_prices_live_touch
AFTER UPDATE OF price, symbol ON token_prices_live
BEGIN
  UPDATE token_prices_live
  SET timestamp = strftime('%s','now')
  WHERE ca = NEW.ca;
END;

CREATE INDEX IF NOT EXISTS idx_token_prices_live_symbol
  ON token_prices_live(symbol);

----------------------------------------------------------------
-- Fixed-ladder snapshot (one row per measurement)
-- Values are price impact in basis points (bps) vs an internal
-- baseline (e.g., baseline_usd=5.0) per direction.
----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS liquidity_ladder_fixed (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  ts             INTEGER NOT NULL DEFAULT (strftime('%s','now')),

  -- context
  base_address   TEXT NOT NULL,
  base_symbol    TEXT,
  pair_address   TEXT NOT NULL,
  quote_address  TEXT NOT NULL,
  quote_symbol   TEXT,
  chain          TEXT,

  price_usd      REAL,         -- base token USD price at snapshot (Birdeye or other)

  -- BUY side (quote -> base): impact bps at each USD rung
  buy_bps_usd_1        REAL,
  buy_bps_usd_100      REAL,
  buy_bps_usd_500      REAL,
  buy_bps_usd_1000     REAL,
  buy_bps_usd_5000     REAL,
  buy_bps_usd_10000    REAL,
  buy_bps_usd_25000    REAL,
  buy_bps_usd_75000    REAL,
  buy_bps_usd_100000   REAL,

  -- SELL side (base -> quote): impact bps at each USD rung
  sell_bps_usd_1       REAL,
  sell_bps_usd_100     REAL,
  sell_bps_usd_500     REAL,
  sell_bps_usd_1000    REAL,
  sell_bps_usd_5000    REAL,
  sell_bps_usd_10000   REAL,
  sell_bps_usd_25000   REAL,
  sell_bps_usd_75000   REAL,
  sell_bps_usd_100000  REAL
);

CREATE INDEX IF NOT EXISTS idx_liq_ladder_fixed_base_ts
  ON liquidity_ladder_fixed(base_address, ts DESC);

CREATE INDEX IF NOT EXISTS idx_liq_ladder_fixed_pair_ts
  ON liquidity_ladder_fixed(pair_address, ts DESC);
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
