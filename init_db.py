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

-- ============================================================
-- Token pairs (unchanged)
-- ============================================================
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

-- ============================================================
-- Live price (one row per token)
-- ============================================================
CREATE TABLE IF NOT EXISTS token_prices_live (
  ca         TEXT PRIMARY KEY,                                  -- token contract address
  symbol     TEXT,
  price      REAL NOT NULL,                                     -- choose a single unit (e.g., USD)
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

-- ============================================================
-- NEW: Ladder storage
-- ============================================================

-- One row per ladder execution (run). Holds the context/baselines used.
CREATE TABLE IF NOT EXISTS ladder_runs (
  id                     INTEGER PRIMARY KEY AUTOINCREMENT,
  started_at             INTEGER NOT NULL DEFAULT (strftime('%s','now')),

  -- Pair context (FK to token_pairs for referential integrity)
  base_address           TEXT NOT NULL,
  pair_address           TEXT NOT NULL,
  quote_address          TEXT NOT NULL,

  base_symbol            TEXT,
  quote_symbol           TEXT,
  base_decimals          INTEGER NOT NULL,
  quote_decimals         INTEGER NOT NULL,

  -- Run parameters / baselines
  baseline_usd           INTEGER NOT NULL,   -- e.g., 5
  quote_usd              REAL    NOT NULL,   -- USD per 1 QUOTE (e.g., WETH)
  base_usd               REAL    NOT NULL,   -- USD per 1 BASE inferred at baseline

  -- Store exact baselines as strings to avoid float rounding issues
  unit_buy_baseline      TEXT,  -- BASE per 1 QUOTE (from BUY baseline)
  unit_sell_baseline     TEXT,  -- QUOTE per 1 BASE (from SELL baseline)

  -- Optional: store the full ladder used as JSON (handy for replays)
  usd_ladder_json        TEXT,

  FOREIGN KEY (base_address, pair_address)
    REFERENCES token_pairs(base_address, pair_address)
    ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_ladder_runs_pair
  ON ladder_runs(pair_address);

CREATE INDEX IF NOT EXISTS idx_ladder_runs_started
  ON ladder_runs(started_at);

-- One row per USD step within a run.
CREATE TABLE IF NOT EXISTS ladder_points (
  run_id                            INTEGER NOT NULL,
  usd                               INTEGER NOT NULL,   -- the step (e.g., 1,5,10,...)

  buy_bps                           REAL,               -- route-only impact vs baseline (bps)
  sell_bps                          REAL,

  buy_liquidity_available           INTEGER,            -- 0/1 from aggregator
  sell_liquidity_available          INTEGER,

  buy_top_source                    TEXT,               -- e.g., "Swaap_V2"
  buy_route_concentration_percent   REAL,               -- 0..100
  sell_top_source                   TEXT,
  sell_route_concentration_percent  REAL,

  PRIMARY KEY (run_id, usd),
  FOREIGN KEY (run_id) REFERENCES ladder_runs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_ladder_points_run
  ON ladder_points(run_id);
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
        # Ensure foreign keys are on for this connection too
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.executescript(SCHEMA_SQL)
        conn.commit()
    print(f"[ok] initialized schema at: {db_path}")

if __name__ == "__main__":
    init_db(get_db_path())
