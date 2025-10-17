#!/usr/bin/env python3
# controller.py
#
# CLI controller for:
#  - Managing token pairs (add/remove/list)
#  - Managing ladder settings (values/baseline) in config.json
#  - Scheduling periodic ladder runs
#  - Running a ladder on demand and browsing saved results
#  - NEW: Interactive menu (python controller.py menu)
#
# Requirements:
#   - init_db.py has been run to create tables.  (ladder_runs, ladder_points, token_pairs, token_prices_live)
#   - config.json contains 0x_api_key, birdeye_api_key, chain_id, db_path.
#   - ladder.py, db_helper.py, token_price.py, quote.py are present.
#
# Notes:
#   - Settings live in config.json under keys:
#       * "pair_addresses": [<pair_addr>, ...]
#       * "ladder_values": [1,5,10,...]           (optional; defaults from ladder.py if missing)
#       * "ladder_baseline_usd": 5                (optional)
#       * "schedule_enabled": true/false          (optional; default false)
#       * "schedule_interval_secs": 900           (optional; default 900s)
#       * "last_scheduler_heartbeat": <unix>      (optional, for status only)
#       * "last_scheduler_error": "..."           (optional, for status only)
#
#   - The scheduler loop is foreground & blocking. Run it in a separate shell:
#       python controller.py schedule:run
#     Use schedule:enable / :disable / :set-interval to change behavior on the fly.
#
#   - Interactive menu:
#       python controller.py menu
#

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import sqlite3
import requests

# Local modules
from ladder import run as ladder_run, DEFAULT_USD_LADDER, DEFAULT_BASELINE_USD  # prints and returns dict result
from db_helper import (
    DB_PATH,
    save_ladder_result,
    list_ladder_runs,
    get_ladder_run,
    get_ladder_points,
    delete_ladder_run,
    upsert_token_pair,
)

CONFIG_FILE = Path("config.json")

# Birdeye helpers (mirror token_data.py behavior for auto-pair)
BIRDEYE_MARKETS_URL = "https://public-api.birdeye.so/defi/v2/markets"


# ----------------------------- Config I/O ------------------------------

def load_cfg() -> Dict[str, Any]:
    if not CONFIG_FILE.exists():
        sys.stderr.write("ERROR: config.json not found.\n")
        sys.exit(1)
    try:
        return json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        sys.stderr.write(f"ERROR: failed to parse config.json: {e}\n")
        sys.exit(1)


def save_cfg(cfg: Dict[str, Any]) -> None:
    CONFIG_FILE.write_text(json.dumps(cfg, indent=2, sort_keys=False), encoding="utf-8")


def get_cfg_list(cfg: Dict[str, Any], key: str) -> List[Any]:
    val = cfg.get(key)
    return list(val) if isinstance(val, list) else []


def ensure_defaults(cfg: Dict[str, Any]) -> Dict[str, Any]:
    changed = False
    if "ladder_values" not in cfg:
        cfg["ladder_values"] = list(DEFAULT_USD_LADDER)
        changed = True
    if "ladder_baseline_usd" not in cfg:
        cfg["ladder_baseline_usd"] = int(DEFAULT_BASELINE_USD)
        changed = True
    if "schedule_interval_secs" not in cfg:
        cfg["schedule_interval_secs"] = 900
        changed = True
    if "schedule_enabled" not in cfg:
        cfg["schedule_enabled"] = False
        changed = True
    if changed:
        save_cfg(cfg)
    return cfg


# ----------------------------- Pair Management ------------------------------

def list_pairs() -> List[dict]:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(
            """
            SELECT base_address, base_symbol, base_decimals,
                   pair_address, quote_address, quote_symbol, quote_decimals
            FROM token_pairs
            ORDER BY base_symbol, quote_symbol, pair_address
            """
        )
        return [dict(r) for r in cur.fetchall()]


def remove_pair(base_address: str, pair_address: str) -> int:
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            "DELETE FROM token_pairs WHERE base_address = ? AND pair_address = ?",
            (base_address, pair_address),
        )
        conn.commit()
        return cur.rowcount


def birdeye_get_markets(api_key: str, token_ca: str, chain_name: str = "base") -> List[Dict[str, Any]]:
    headers = {"accept": "application/json", "X-API-KEY": api_key, "x-chain": chain_name}
    r = requests.get(BIRDEYE_MARKETS_URL, headers=headers, params={"address": token_ca}, timeout=20)
    if r.status_code != 200:
        raise RuntimeError(f"Birdeye request failed [{r.status_code}]: {r.text[:300]}")
    obj = r.json()
    markets = obj.get("data") or obj.get("markets") or []
    if isinstance(markets, dict):
        markets = markets.get("items") or markets.get("list") or markets
        if isinstance(markets, dict):
            markets = [markets]
    if not isinstance(markets, list):
        markets = []
    return markets


def pick_largest_liquidity(markets: List[Dict[str, Any]]) -> Dict[str, Any]:
    best, best_liq = None, -1.0
    for m in markets:
        try:
            liq = float(m.get("liquidity") or 0.0)
        except Exception:
            liq = 0.0
        if liq > best_liq:
            best, best_liq = m, liq
    if not best:
        raise ValueError("No market with liquidity found.")
    return best


def add_pair_auto(token_ca: str) -> Tuple[str, str]:
    """
    Auto-discover the largest-liquidity pool for a Base token via Birdeye and upsert to DB.
    Returns (base_address, pair_address).
    """
    cfg = load_cfg()
    api_key = cfg.get("birdeye_api_key")
    if not api_key:
        raise SystemExit("Missing birdeye_api_key in config.json")

    markets = birdeye_get_markets(api_key, token_ca)
    if not markets:
        raise SystemExit("Birdeye returned no markets for that token.")

    m = pick_largest_liquidity(markets)

    base = m.get("base") or {}
    quote = m.get("quote") or {}
    pair_addr = m.get("address")

    # Ensure the provided token sits as "base" in our DB row
    b_addr, q_addr = base.get("address"), quote.get("address")
    if token_ca == b_addr:
        base_addr, base_sym, base_dec = b_addr, base.get("symbol"), base.get("decimals")
        quote_addr, quote_sym, quote_dec = q_addr, quote.get("symbol"), quote.get("decimals")
    elif token_ca == q_addr:
        base_addr, base_sym, base_dec = q_addr, quote.get("symbol"), quote.get("decimals")
        quote_addr, quote_sym, quote_dec = b_addr, base.get("symbol"), base.get("decimals")
    else:
        raise SystemExit("Selected market does not include the token on either side.")

    if base_dec is None or quote_dec is None:
        raise SystemExit("Missing decimals in Birdeye market response.")

    upsert_token_pair(
        base_address=base_addr,
        base_symbol=base_sym,
        base_decimals=int(base_dec),
        pair_address=pair_addr,
        quote_address=quote_addr,
        quote_symbol=quote_sym,
        quote_decimals=int(quote_dec),
    )
    return base_addr, pair_addr


# ----------------------------- Ladder & Scheduler ------------------------------

def apply_pair_override_for_run(pair_address: Optional[str]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Temporarily override cfg['pair_addresses'] to target a specific pair for the next run.
    Returns (original_cfg, temp_cfg). The caller should restore original after run.
    """
    original = load_cfg()
    tmp = dict(original)
    if pair_address:
        tmp["pair_addresses"] = [pair_address]
    save_cfg(tmp)
    return original, tmp


def restore_cfg(cfg: Dict[str, Any]) -> None:
    save_cfg(cfg)


def run_once(pair: Optional[str], use_all: bool, override_ladder: Optional[List[int]], override_baseline: Optional[int]) -> List[int]:
    """
    Run ladder once for one or all pairs listed in config["pair_addresses"] (or all DB pairs if --all).
    Returns list of created run_ids.
    """
    cfg = ensure_defaults(load_cfg())

    # Determine target pair list
    targets: List[str]
    if use_all:
        # Collect all pairs from DB
        targets = [row["pair_address"] for row in list_pairs()]
    else:
        targets = [pair] if pair else get_cfg_list(cfg, "pair_addresses")
        if not targets:
            raise SystemExit("No target pair found. Use --pair or set config.pair_addresses.")

    # Apply ladder overrides in config.json (read by ladder.run)
    if override_ladder is not None:
        cfg["ladder_values"] = list(map(int, override_ladder))
    if override_baseline is not None:
        cfg["ladder_baseline_usd"] = int(override_baseline)
    save_cfg(cfg)

    run_ids: List[int] = []
    for p in targets:
        # Temporarily set pair_addresses to the single pair, then restore after run
        orig_cfg, _tmp = apply_pair_override_for_run(p)
        try:
            # Call ladder runner (prints table)
            result = ladder_run(
                usd_ladder=cfg.get("ladder_values") or list(DEFAULT_USD_LADDER),
                baseline_usd=int(cfg.get("ladder_baseline_usd", DEFAULT_BASELINE_USD)),
            )
            rid = save_ladder_result(result)
            run_ids.append(rid)
            print(f"\n[ok] saved ladder run_id: {rid} for pair {p}")
        except KeyboardInterrupt:
            raise
        except Exception as e:
            print(f"[warn] ladder run failed for {p}: {e}")
        finally:
            restore_cfg(orig_cfg)
            time.sleep(1.0)  # small gap between pairs
    return run_ids


def scheduler_loop() -> None:
    """
    Foreground blocking loop. Reads config.json each cycle, honors:
      - schedule_enabled: bool
      - schedule_interval_secs: int
      - ladder_values: list[int]
      - ladder_baseline_usd: int
      - pair_addresses: list[str] (targets). If empty, uses all DB pairs.
    """
    print("[scheduler] starting … Ctrl-C to stop")
    while True:
        cfg = ensure_defaults(load_cfg())
        enabled = bool(cfg.get("schedule_enabled", False))
        interval = int(cfg.get("schedule_interval_secs", 900))
        if not enabled:
            print("[scheduler] disabled; sleeping 10s … (use schedule:enable)")
            time.sleep(10)
            continue

        try:
            # decide targets
            targets = get_cfg_list(cfg, "pair_addresses")
            if not targets:
                targets = [row["pair_address"] for row in list_pairs()]
            if not targets:
                print("[scheduler] no pairs found; sleeping")
                time.sleep(interval)
                continue

            print(f"[scheduler] running for {len(targets)} pair(s) …")
            created = run_once(pair=None, use_all=True, override_ladder=None, override_baseline=None)
            print(f"[scheduler] runs created: {created}")

            cfg["last_scheduler_heartbeat"] = int(time.time())
            cfg.pop("last_scheduler_error", None)
            save_cfg(cfg)
        except KeyboardInterrupt:
            print("\n[scheduler] interrupted; exiting")
            break
        except Exception as e:
            print(f"[scheduler] error: {e}")
            cfg["last_scheduler_error"] = str(e)
            cfg["last_scheduler_heartbeat"] = int(time.time())
            save_cfg(cfg)

        # sleep until next cycle
        time.sleep(interval)


# ----------------------------- CLI Handlers ------------------------------

def cmd_pairs_add_auto(args: argparse.Namespace) -> None:
    base_addr, pair_addr = add_pair_auto(args.token)
    print(f"[ok] upserted pair (base={base_addr}, pair={pair_addr})")
    if args.add_to_config:
        cfg = load_cfg()
        addrs = get_cfg_list(cfg, "pair_addresses")
        if pair_addr not in addrs:
            addrs.append(pair_addr)
            cfg["pair_addresses"] = addrs
            save_cfg(cfg)
            print(f"[ok] added pair to config.pair_addresses: {pair_addr}")


def cmd_pairs_add_manual(args: argparse.Namespace) -> None:
    upsert_token_pair(
        base_address=args.base,
        base_symbol=args.base_sym,
        base_decimals=int(args.base_dec),
        pair_address=args.pair,
        quote_address=args.quote,
        quote_symbol=args.quote_sym,
        quote_decimals=int(args.quote_dec),
    )
    print("[ok] upserted token pair")
    if args.add_to_config:
        cfg = load_cfg()
        addrs = get_cfg_list(cfg, "pair_addresses")
        if args.pair not in addrs:
            addrs.append(args.pair)
            cfg["pair_addresses"] = addrs
            save_cfg(cfg)
            print(f"[ok] added pair to config.pair_addresses: {args.pair}")


def cmd_pairs_remove(args: argparse.Namespace) -> None:
    n = remove_pair(args.base, args.pair)
    print(f"[ok] removed {n} row(s) from token_pairs")
    # Also remove from config list if present
    cfg = load_cfg()
    addrs = [x for x in get_cfg_list(cfg, "pair_addresses") if x.lower() != args.pair.lower()]
    cfg["pair_addresses"] = addrs
    save_cfg(cfg)


def cmd_pairs_list(_args: argparse.Namespace) -> None:
    rows = list_pairs()
    if not rows:
        print("(no pairs)")
        return
    print("base_symbol base_address                               pair_address                                quote_symbol quote_address                              decs")
    for r in rows:
        print(f"{(r['base_symbol'] or '-'):<11} {r['base_address']:<42} {r['pair_address']:<42} {(r['quote_symbol'] or '-'):<12} {r['quote_address']:<42} {r['base_decimals']}/{r['quote_decimals']}")


def cmd_ladder_set(args: argparse.Namespace) -> None:
    vals = [int(x) for x in args.values.split(",") if x.strip()]
    cfg = load_cfg()
    cfg["ladder_values"] = vals
    save_cfg(cfg)
    print(f"[ok] set ladder_values = {vals}")


def cmd_ladder_default(_args: argparse.Namespace) -> None:
    cfg = load_cfg()
    cfg["ladder_values"] = list(DEFAULT_USD_LADDER)
    cfg["ladder_baseline_usd"] = int(DEFAULT_BASELINE_USD)
    save_cfg(cfg)
    print("[ok] restored default ladder & baseline")


def cmd_ladder_show(_args: argparse.Namespace) -> None:
    cfg = ensure_defaults(load_cfg())
    print("ladder_values:", cfg["ladder_values"])
    print("ladder_baseline_usd:", cfg["ladder_baseline_usd"])


def cmd_ladder_baseline(args: argparse.Namespace) -> None:
    cfg = load_cfg()
    cfg["ladder_baseline_usd"] = int(args.usd)
    save_cfg(cfg)
    print(f"[ok] set ladder_baseline_usd = {args.usd}")


def cmd_run_once(args: argparse.Namespace) -> None:
    run_once(
        pair=args.pair,
        use_all=args.all,
        override_ladder=[int(x) for x in args.ladder.split(",")] if args.ladder else None,
        override_baseline=int(args.baseline) if args.baseline else None,
    )


def cmd_runs_list(args: argparse.Namespace) -> None:
    rows = list_ladder_runs(limit=int(args.limit), offset=0)
    if not rows:
        print("(no runs)")
        return
    print("id     started_at  base_symbol  quote_symbol  pair_address")
    for r in rows:
        print(f"{r['id']:<6} {r['started_at']:<11} {(r['base_symbol'] or '-'):<11} {(r['quote_symbol'] or '-'):<12} {r['pair_address']}")


def cmd_runs_show(args: argparse.Namespace) -> None:
    run = get_ladder_run(int(args.id))
    if not run:
        print("run not found")
        return
    pts = get_ladder_points(run["id"])
    print(json.dumps({"run": run, "points": pts}, indent=2))


def cmd_runs_delete(args: argparse.Namespace) -> None:
    n = delete_ladder_run(int(args.id))
    print(f"[ok] deleted ladder_runs rows: {n} (ladder_points cascade)")


def cmd_schedule_run(_args: argparse.Namespace) -> None:
    scheduler_loop()


def cmd_schedule_enable(_args: argparse.Namespace) -> None:
    cfg = load_cfg()
    cfg["schedule_enabled"] = True
    cfg["last_scheduler_heartbeat"] = int(time.time())
    save_cfg(cfg)
    print("[ok] scheduler enabled")


def cmd_schedule_disable(_args: argparse.Namespace) -> None:
    cfg = load_cfg()
    cfg["schedule_enabled"] = False
    save_cfg(cfg)
    print("[ok] scheduler disabled")


def cmd_schedule_set_interval(args: argparse.Namespace) -> None:
    cfg = load_cfg()
    cfg["schedule_interval_secs"] = int(args.seconds)
    save_cfg(cfg)
    print(f"[ok] scheduler interval = {args.seconds}s")


def cmd_schedule_status(_args: argparse.Namespace) -> None:
    cfg = ensure_defaults(load_cfg())
    enabled = cfg.get("schedule_enabled", False)
    interval = cfg.get("schedule_interval_secs", 900)
    beat = cfg.get("last_scheduler_heartbeat")
    err = cfg.get("last_scheduler_error")
    print(json.dumps({
        "enabled": enabled,
        "interval_secs": interval,
        "last_scheduler_heartbeat": beat,
        "last_scheduler_error": err
    }, indent=2))


# ----------------------------- Interactive Menu ------------------------------

def _input(prompt: str) -> str:
    try:
        return input(prompt)
    except EOFError:
        return ""
    except KeyboardInterrupt:
        print("\n[menu] interrupted")
        sys.exit(0)


def _press_enter() -> None:
    _input("\nPress ENTER to continue...")


def menu_pairs() -> None:
    while True:
        print("\n[Pairs]\n"
              "  1) Add pair (auto via Birdeye)\n"
              "  2) Add pair (manual)\n"
              "  3) Remove pair\n"
              "  4) List pairs\n"
              "  5) Back")
        choice = _input("> ").strip()
        if choice == "1":
            token = _input("Token (Base) address: ").strip()
            add_to_cfg = _input("Add pair to config.pair_addresses? [y/N]: ").strip().lower() == "y"
            try:
                base_addr, pair_addr = add_pair_auto(token)
                print(f"[ok] upserted pair base={base_addr}, pair={pair_addr}")
                if add_to_cfg:
                    cfg = load_cfg()
                    addrs = get_cfg_list(cfg, "pair_addresses")
                    if pair_addr not in addrs:
                        addrs.append(pair_addr)
                        cfg["pair_addresses"] = addrs
                        save_cfg(cfg)
                        print(f"[ok] added pair to config: {pair_addr}")
            except Exception as e:
                print(f"[err] {e}")
            _press_enter()
        elif choice == "2":
            base = _input("Base token address: ").strip()
            pair = _input("Pair (LP) address: ").strip()
            quote = _input("Quote token address: ").strip()
            base_dec = int(_input("Base decimals (e.g., 18): ").strip() or "18")
            quote_dec = int(_input("Quote decimals (e.g., 18): ").strip() or "18")
            base_sym = _input("Base symbol (optional): ").strip() or None
            quote_sym = _input("Quote symbol (optional): ").strip() or None
            add_to_cfg = _input("Add pair to config.pair_addresses? [y/N]: ").strip().lower() == "y"
            try:
                upsert_token_pair(
                    base_address=base, base_symbol=base_sym, base_decimals=base_dec,
                    pair_address=pair, quote_address=quote, quote_symbol=quote_sym, quote_decimals=quote_dec
                )
                print("[ok] upserted token pair")
                if add_to_cfg:
                    cfg = load_cfg()
                    addrs = get_cfg_list(cfg, "pair_addresses")
                    if pair not in addrs:
                        addrs.append(pair)
                        cfg["pair_addresses"] = addrs
                        save_cfg(cfg)
                        print(f"[ok] added pair to config: {pair}")
            except Exception as e:
                print(f"[err] {e}")
            _press_enter()
        elif choice == "3":
            base = _input("Base token address: ").strip()
            pair = _input("Pair (LP) address: ").strip()
            try:
                n = remove_pair(base, pair)
                print(f"[ok] removed {n} row(s) from token_pairs")
                cfg = load_cfg()
                addrs = [x for x in get_cfg_list(cfg, "pair_addresses") if x.lower() != pair.lower()]
                cfg["pair_addresses"] = addrs
                save_cfg(cfg)
            except Exception as e:
                print(f"[err] {e}")
            _press_enter()
        elif choice == "4":
            rows = list_pairs()
            if not rows:
                print("(no pairs)")
            else:
                print("base_symbol base_address                               pair_address                                quote_symbol quote_address                              decs")
                for r in rows:
                    print(f"{(r['base_symbol'] or '-'):<11} {r['base_address']:<42} {r['pair_address']:<42} {(r['quote_symbol'] or '-'):<12} {r['quote_address']:<42} {r['base_decimals']}/{r['quote_decimals']}")
            _press_enter()
        elif choice == "5":
            return
        else:
            print("Invalid choice.")


def menu_ladder() -> None:
    while True:
        cfg = ensure_defaults(load_cfg())
        print("\n[Ladder]\n"
              f"  Current values : {cfg['ladder_values']}\n"
              f"  Baseline (USD) : {cfg['ladder_baseline_usd']}\n"
              "  1) Set ladder values\n"
              "  2) Reset to default\n"
              "  3) Set baseline USD\n"
              "  4) Back")
        choice = _input("> ").strip()
        if choice == "1":
            raw = _input('Enter values (comma separated, e.g., "1,5,10,25"): ').strip()
            try:
                vals = [int(x) for x in raw.split(",") if x.strip()]
                cfg["ladder_values"] = vals
                save_cfg(cfg)
                print(f"[ok] set ladder_values = {vals}")
            except Exception as e:
                print(f"[err] {e}")
            _press_enter()
        elif choice == "2":
            cfg["ladder_values"] = list(DEFAULT_USD_LADDER)
            cfg["ladder_baseline_usd"] = int(DEFAULT_BASELINE_USD)
            save_cfg(cfg)
            print("[ok] restored defaults")
            _press_enter()
        elif choice == "3":
            usd = int(_input("Baseline USD (e.g., 5): ").strip() or "5")
            cfg["ladder_baseline_usd"] = usd
            save_cfg(cfg)
            print(f"[ok] baseline set = {usd}")
            _press_enter()
        elif choice == "4":
            return
        else:
            print("Invalid choice.")


def menu_run() -> None:
    while True:
        print("\n[Run]\n"
              "  1) Run once for config.pair_addresses\n"
              "  2) Run once for a specific pair\n"
              "  3) Run once for ALL pairs in DB\n"
              "  4) Back")
        choice = _input("> ").strip()
        if choice == "1":
            try:
                run_once(pair=None, use_all=False, override_ladder=None, override_baseline=None)
            except Exception as e:
                print(f"[err] {e}")
            _press_enter()
        elif choice == "2":
            p = _input("Pair (LP) address: ").strip()
            ladder_raw = _input('Override ladder? (comma list or blank to skip): ').strip()
            baseline_raw = _input("Override baseline USD? (blank to skip): ").strip()
            ladder_vals = [int(x) for x in ladder_raw.split(",")] if ladder_raw else None
            baseline_val = int(baseline_raw) if baseline_raw else None
            try:
                run_once(pair=p, use_all=False, override_ladder=ladder_vals, override_baseline=baseline_val)
            except Exception as e:
                print(f"[err] {e}")
            _press_enter()
        elif choice == "3":
            try:
                run_once(pair=None, use_all=True, override_ladder=None, override_baseline=None)
            except Exception as e:
                print(f"[err] {e}")
            _press_enter()
        elif choice == "4":
            return
        else:
            print("Invalid choice.")


def menu_runs() -> None:
    while True:
        print("\n[Runs]\n"
              "  1) List recent runs\n"
              "  2) Show a run (JSON)\n"
              "  3) Delete a run\n"
              "  4) Back")
        choice = _input("> ").strip()
        if choice == "1":
            limit = int(_input("Limit (default 20): ").strip() or "20")
            rows = list_ladder_runs(limit=limit, offset=0)
            if not rows:
                print("(no runs)")
            else:
                print("id     started_at  base_symbol  quote_symbol  pair_address")
                for r in rows:
                    print(f"{r['id']:<6} {r['started_at']:<11} {(r['base_symbol'] or '-'):<11} {(r['quote_symbol'] or '-'):<12} {r['pair_address']}")
            _press_enter()
        elif choice == "2":
            rid = int(_input("Run ID: ").strip())
            run = get_ladder_run(rid)
            if not run:
                print("run not found")
            else:
                pts = get_ladder_points(run["id"])
                print(json.dumps({"run": run, "points": pts}, indent=2))
            _press_enter()
        elif choice == "3":
            rid = int(_input("Run ID to delete: ").strip())
            n = delete_ladder_run(rid)
            print(f"[ok] deleted ladder_runs rows: {n} (ladder_points cascade)")
            _press_enter()
        elif choice == "4":
            return
        else:
            print("Invalid choice.")


def menu_scheduler() -> None:
    while True:
        cfg = ensure_defaults(load_cfg())
        print("\n[Scheduler]\n"
              f"  Enabled: {cfg.get('schedule_enabled', False)}\n"
              f"  Interval secs: {cfg.get('schedule_interval_secs', 900)}\n"
              f"  Last heartbeat: {cfg.get('last_scheduler_heartbeat')}\n"
              f"  Last error: {cfg.get('last_scheduler_error')}\n"
              "  1) Enable\n"
              "  2) Disable\n"
              "  3) Set interval\n"
              "  4) Start loop NOW (blocks)\n"
              "  5) Back")
        choice = _input("> ").strip()
        if choice == "1":
            cfg["schedule_enabled"] = True
            cfg["last_scheduler_heartbeat"] = int(time.time())
            save_cfg(cfg)
            print("[ok] enabled")
            _press_enter()
        elif choice == "2":
            cfg["schedule_enabled"] = False
            save_cfg(cfg)
            print("[ok] disabled")
            _press_enter()
        elif choice == "3":
            secs = int(_input("Interval seconds: ").strip() or "900")
            cfg["schedule_interval_secs"] = secs
            save_cfg(cfg)
            print(f"[ok] interval set = {secs}s")
            _press_enter()
        elif choice == "4":
            print("Starting scheduler loop… Ctrl-C to stop.")
            scheduler_loop()
        elif choice == "5":
            return
        else:
            print("Invalid choice.")


def menu_loop() -> None:
    ensure_defaults(load_cfg())
    while True:
        print("\n=== Ladder Controller Menu ===\n"
              "  1) Pairs (add/remove/list)\n"
              "  2) Ladder config (values/baseline)\n"
              "  3) Run once\n"
              "  4) Runs (list/show/delete)\n"
              "  5) Scheduler (enable/disable/interval/run)\n"
              "  6) Exit")
        choice = _input("> ").strip()
        if choice == "1":
            menu_pairs()
        elif choice == "2":
            menu_ladder()
        elif choice == "3":
            menu_run()
        elif choice == "4":
            menu_runs()
        elif choice == "5":
            menu_scheduler()
        elif choice == "6":
            print("Bye!")
            return
        else:
            print("Invalid choice.")


# ----------------------------- CLI ------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Ladder Controller (pairs, ladder, scheduler, runs, menu)")
    sub = p.add_subparsers(dest="cmd", required=True)

    # pairs:add-auto
    s = sub.add_parser("pairs:add-auto", help="Add pair by discovering largest-liquidity pool for a Base token")
    s.add_argument("--token", required=True, help="Token contract address (Base)")
    s.add_argument("--add-to-config", action="store_true", help="Also append pair to config.pair_addresses")
    s.set_defaults(func=cmd_pairs_add_auto)

    # pairs:add-manual
    s = sub.add_parser("pairs:add-manual", help="Add/overwrite a token pair manually")
    s.add_argument("--base", required=True, help="Base token contract address")
    s.add_argument("--pair", required=True, help="LP/pool pair address")
    s.add_argument("--quote", required=True, help="Quote token contract address")
    s.add_argument("--base-dec", required=True, type=int)
    s.add_argument("--quote-dec", required=True, type=int)
    s.add_argument("--base-sym", default=None)
    s.add_argument("--quote-sym", default=None)
    s.add_argument("--add-to-config", action="store_true", help="Also append pair to config.pair_addresses")
    s.set_defaults(func=cmd_pairs_add_manual)

    # pairs:remove
    s = sub.add_parser("pairs:remove", help="Remove a token pair from DB (and from config list if present)")
    s.add_argument("--base", required=True)
    s.add_argument("--pair", required=True)
    s.set_defaults(func=cmd_pairs_remove)

    # pairs:list
    s = sub.add_parser("pairs:list", help="List pairs in DB")
    s.set_defaults(func=cmd_pairs_list)

    # ladder:set
    s = sub.add_parser("ladder:set", help='Set ladder values, e.g. --values "1,5,10,25,50,100"')
    s.add_argument("--values", required=True)
    s.set_defaults(func=cmd_ladder_set)

    # ladder:default
    s = sub.add_parser("ladder:default", help="Reset ladder values & baseline to defaults")
    s.set_defaults(func=cmd_ladder_default)

    # ladder:show
    s = sub.add_parser("ladder:show", help="Show current ladder values & baseline")
    s.set_defaults(func=cmd_ladder_show)

    # ladder:baseline
    s = sub.add_parser("ladder:baseline", help="Set baseline USD (bps reference size)")
    s.add_argument("--usd", required=True, type=int)
    s.set_defaults(func=cmd_ladder_baseline)

    # run:once
    s = sub.add_parser("run:once", help="Run ladder once (prints + saves). Defaults to config.pair_addresses.")
    g = s.add_mutually_exclusive_group()
    g.add_argument("--pair", help="Target this pair address only")
    g.add_argument("--all", action="store_true", help="Run for all pairs in DB")
    s.add_argument("--ladder", help='Override ladder for this run only, e.g. "1,5,10,25"')
    s.add_argument("--baseline", help="Override baseline for this run only, e.g. 5")
    s.set_defaults(func=cmd_run_once)

    # runs:list
    s = sub.add_parser("runs:list", help="List recent ladder runs")
    s.add_argument("--limit", type=int, default=20)
    s.set_defaults(func=cmd_runs_list)

    # runs:show
    s = sub.add_parser("runs:show", help="Show one run (header + points)")
    s.add_argument("--id", required=True)
    s.set_defaults(func=cmd_runs_show)

    # runs:delete
    s = sub.add_parser("runs:delete", help="Delete one run (points cascade)")
    s.add_argument("--id", required=True)
    s.set_defaults(func=cmd_runs_delete)

    # schedule:run
    s = sub.add_parser("schedule:run", help="Start the blocking scheduler loop (foreground)")
    s.set_defaults(func=cmd_schedule_run)

    # schedule:enable
    s = sub.add_parser("schedule:enable", help="Enable scheduler in config")
    s.set_defaults(func=cmd_schedule_enable)

    # schedule:disable
    s = sub.add_parser("schedule:disable", help="Disable scheduler in config")
    s.set_defaults(func=cmd_schedule_disable)

    # schedule:set-interval
    s = sub.add_parser("schedule:set-interval", help="Set scheduler interval seconds")
    s.add_argument("--seconds", required=True, type=int)
    s.set_defaults(func=cmd_schedule_set_interval)

    # schedule:status
    s = sub.add_parser("schedule:status", help="Show scheduler status/heartbeat")
    s.set_defaults(func=cmd_schedule_status)

    # NEW: interactive menu
    s = sub.add_parser("menu", help="Interactive menu")
    s.set_defaults(func=lambda _args: menu_loop())

    return p


def main(argv: Optional[List[str]] = None) -> None:
    ensure_defaults(load_cfg())
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
