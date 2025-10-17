#!/usr/bin/env python3
# plot_menu.py — Token price with per-rung proportional S/R shading (interactive)
#
# What you get:
#   • One visualization: token price (BASE_USD) vs time
#   • Below the line: stacked GREEN bands per ladder rung (height ∝ USD^exp, darkness ∝ BUY dominance)
#   • Above the line: stacked RED bands per ladder rung (height ∝ USD^exp, darkness ∝ SELL dominance)
#   • The price line switches color per time segment (green=BUY stronger, red=SELL stronger)
#
# Menu:
#   1) Select pair(s)
#   2) Configure (limit, smoothing, show/save, outdir, weight exponent, refresh rate)
#   3) Plot S/R Banded Overlay
#   4) Watch live (uses configured refresh)
#   5) Exit
#
# Requires DB filled by ladder.py:
#   - ladder_runs(id, started_at, base_usd, pair_address, ...)
#   - ladder_points(run_id, usd, buy_bps, sell_bps)

import json
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import matplotlib.pyplot as plt

from db_helper import DB_PATH  # uses config.json under the hood

CONFIG_FILE = Path("config.json")

# --------------------------- Config helpers ---------------------------

_PM_DEFAULTS = {
    "limit": 500,
    "smooth": 1,
    "show": False,
    "save": True,
    "outdir": "plots",
    "weight_exp": 1.0,
    "refresh_sec": 5.0,
    "last_pair": None,  # persisted last selection from "Select pair(s)" menu
}

def _read_json(path: Path) -> Dict[str, Any]:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}

def _write_json(path: Path, data: Dict[str, Any]) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=False), encoding="utf-8")
    tmp.replace(path)

def load_cfg() -> Dict[str, Any]:
    return _read_json(CONFIG_FILE)

def save_cfg(cfg: Dict[str, Any]) -> None:
    _write_json(CONFIG_FILE, cfg)

def get_monitored_pairs_from_config() -> List[str]:
    cfg = load_cfg()
    addrs = cfg.get("pair_addresses")
    return list(addrs) if isinstance(addrs, list) else []

def _get_pm_cfg(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Return the plot_menu sub-config, creating/merging defaults if needed."""
    pm = dict(_PM_DEFAULTS)
    user_pm = cfg.get("plot_menu") or {}
    for k, v in user_pm.items():
        if k in pm:
            pm[k] = v
    return pm

def _store_pm_cfg(cfg: Dict[str, Any], pm: Dict[str, Any]) -> None:
    """Persist only the plot_menu subsection, preserving other config keys."""
    full = dict(cfg)
    full["plot_menu"] = pm
    save_cfg(full)

# --------------------------- DB helpers -------------------------------

def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def list_pairs(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    cur = conn.execute("""
        SELECT base_address, base_symbol, base_decimals,
               pair_address, quote_address, quote_symbol, quote_decimals
        FROM token_pairs
        ORDER BY base_symbol, quote_symbol, pair_address
    """)
    return [dict(r) for r in cur.fetchall()]

def get_pair_meta(conn: sqlite3.Connection, pair_address: str) -> Optional[Dict[str, Any]]:
    cur = conn.execute("""
        SELECT base_address, base_symbol, base_decimals,
               pair_address, quote_address, quote_symbol, quote_decimals
        FROM token_pairs
        WHERE lower(pair_address) = lower(?)
        LIMIT 1
    """, (pair_address,))
    r = cur.fetchone()
    return dict(r) if r else None

def get_runs_for_pair(conn: sqlite3.Connection, pair_address: str, limit: int) -> List[Dict[str, Any]]:
    # Fetch the latest N (DESC + LIMIT), then re-order ASC for plotting
    cur = conn.execute("""
        SELECT id, started_at, base_usd FROM (
            SELECT id, started_at, base_usd
            FROM ladder_runs
            WHERE lower(pair_address) = lower(?)
            ORDER BY started_at DESC, id DESC
            LIMIT ?
        ) AS recent
        ORDER BY started_at ASC, id ASC
    """, (pair_address, int(limit)))
    return [dict(r) for r in cur.fetchall()]


def get_points_for_run(conn: sqlite3.Connection, run_id: int) -> List[Dict[str, Any]]:
    cur = conn.execute("""
        SELECT usd, buy_bps, sell_bps
        FROM ladder_points
        WHERE run_id = ?
        ORDER BY usd ASC
    """, (int(run_id),))
    return [dict(r) for r in cur.fetchall()]

# --------------------------- Transforms -------------------------------

def _to_dt(unix_ts: int) -> datetime:
    try:
        return datetime.utcfromtimestamp(int(unix_ts))
    except Exception:
        return datetime.utcfromtimestamp(0)

def rolling_mean(xs: List[float], w: int) -> List[float]:
    if w <= 1 or w > len(xs):
        return xs[:]
    out = []
    s = 0.0
    for i, v in enumerate(xs):
        s += v
        if i >= w:
            s -= xs[i - w]
        if i >= w - 1:
            out.append(s / w)
        else:
            out.append(xs[i])
    return out

def _pad_minmax(vals: List[float], pad_ratio: float = 0.2) -> Tuple[float, float]:
    vmin, vmax = min(vals), max(vals)
    if vmax == vmin:
        pad = abs(vmin) * pad_ratio if vmin != 0 else 1.0
        return vmin - pad, vmax + pad
    span = vmax - vmin
    pad = span * pad_ratio
    return vmin - pad, vmax + pad

# ------- Per-rung dominance and weights (for one run / one ladder) -------

def _per_rung_strengths(points: List[Dict[str, Any]], weight_exp: float):
    """
    For each rung, compute:
      - weight: w_r = (USD_r ** weight_exp)
      - support_alpha_r (green)  = max(0, buy - sell) / (|buy| + |sell| + eps)
      - resist_alpha_r  (red)    = max(0, sell - buy) / (|buy| + |sell| + eps)
    Returns (usds, weights_norm, support_alphas, resist_alphas).
    """
    if not points:
        return [], [], [], []

    eps = 1e-6
    usds, weights, sup_alphas, res_alphas = [], [], [], []

    for p in points:
        usd = float(p.get("usd") or 0.0)
        b = p.get("buy_bps")
        s = p.get("sell_bps")
        b0 = 0.0 if b is None else float(b)
        s0 = 0.0 if s is None else float(s)

        w = (usd ** weight_exp) if usd > 0 else 0.0
        denom = abs(b0) + abs(s0) + eps
        sup = max(0.0, b0 - s0) / denom
        res = max(0.0, s0 - b0) / denom

        usds.append(usd)
        weights.append(w)
        # clamp alphas to a pleasing range
        sup_alphas.append(max(0.06, min(0.90, sup)))
        res_alphas.append(max(0.06, min(0.90, res)))

    total_w = sum(weights)
    if total_w <= 0:
        weights_norm = [1.0 / len(usds)] * len(usds)
    else:
        weights_norm = [w / total_w for w in weights]

    return usds, weights_norm, sup_alphas, res_alphas

# --------------------------- Series builder ----------------------------

def build_series(conn: sqlite3.Connection, pair: str, limit: int, smooth: int):
    runs = get_runs_for_pair(conn, pair, limit=limit)
    if not runs:
        return [], [], [], []  # times, tok_usd, per_run_points, runs

    times = [_to_dt(r["started_at"]) for r in runs]
    tok_usd = [float(r["base_usd"]) for r in runs]
    if smooth and smooth > 1:
        tok_usd = rolling_mean(tok_usd, smooth)

    per_run_points: List[List[Dict[str, Any]]] = []
    for r in runs:
        pts = get_points_for_run(conn, r["id"])
        per_run_points.append(pts)

    return times, tok_usd, per_run_points, runs

# --------------------------- Plotting ---------------------------------

def _draw_banded_side(ax, xseg, y_line0, y_line1, y_base0, y_base1, weights_norm, alphas, color: str):
    """
    Draw stacked bands between base and line across a segment.
    Each band k occupies fractional interval [cum, cum+wk] of the vertical gap.
    """
    if not weights_norm or sum(weights_norm) <= 0:
        return

    cum = 0.0
    for wk, ak in zip(weights_norm, alphas):
        lo = cum
        hi = cum + wk
        cum = hi

        # segment interpolation at endpoints
        y_lo0 = y_base0 + lo * (y_line0 - y_base0)
        y_hi0 = y_base0 + hi * (y_line0 - y_base0)
        y_lo1 = y_base1 + lo * (y_line1 - y_base1)
        y_hi1 = y_base1 + hi * (y_line1 - y_base1)

        ax.fill_between(
            xseg,
            [y_lo0, y_lo1],
            [y_hi0, y_hi1],
            color=color,
            alpha=ak,
            linewidth=0
        )

def plot_banded_sr_overlay_for_pair(
    conn: sqlite3.Connection,
    meta: Dict[str, Any],
    pair: str,
    *,
    limit: int,
    smooth: int,
    weight_exp: float,
    show: bool,
    save: bool,
    outdir: Path,
) -> Optional[Path]:
    # Build series and per-run ladder points
    times, tok_usd, per_run_points, runs = build_series(conn, pair, limit, smooth)
    if not times:
        return None

    bs = (meta.get("base_symbol") or "BASE").upper()
    fig, ax = plt.subplots()

    # Price axis with ±20% padding
    ymin, ymax = _pad_minmax(tok_usd, pad_ratio=0.2)
    ax.set_ylim(ymin, ymax)

    # Draw everything per segment so line/bands can change color/strength
    for i in range(len(times) - 1):
        xseg = [times[i], times[i + 1]]
        y0, y1 = tok_usd[i], tok_usd[i + 1]

        # rung info from the starting run for the segment
        pts = per_run_points[i]
        _, weights_norm, sup_alphas, res_alphas = _per_rung_strengths(pts, weight_exp=weight_exp)

        # 1) Stacked bands BELOW (support) and ABOVE (resistance)
        _draw_banded_side(
            ax, xseg,
            y_line0=y0, y_line1=y1,
            y_base0=ymin, y_base1=ymin,
            weights_norm=weights_norm,
            alphas=sup_alphas,
            color="green"
        )
        _draw_banded_side(
            ax, xseg,
            y_line0=y0, y_line1=y1,
            y_base0=ymax, y_base1=ymax,
            weights_norm=weights_norm,
            alphas=res_alphas,
            color="red"
        )

        # 2) Segment line color = stronger side; opacity scales with dominance
        seg_sup = sum(w * a for w, a in zip(weights_norm, sup_alphas))
        seg_res = sum(w * a for w, a in zip(weights_norm, res_alphas))
        seg_sum = seg_sup + seg_res
        if seg_sum <= 1e-6:
            line_color = "black"
            line_alpha = 0.6
        else:
            if seg_sup >= seg_res:
                line_color = "green"
                dom = seg_sup - seg_res
            else:
                line_color = "red"
                dom = seg_res - seg_sup
            line_alpha = 0.35 + 0.65 * (dom / seg_sum)

        ax.plot(xseg, [y0, y1], color=line_color, alpha=line_alpha, linewidth=2.4)

    ax.set_title(
        f"{bs} — Price with per-rung Support/Resistance bands\n"
        f"(band height ∝ USD^exp, band darkness ∝ rung dominance; line color = stronger side; exp={weight_exp:.2f})"
    )
    ax.set_xlabel("Time (UTC)")
    ax.set_ylabel("USD per 1 BASE")
    ax.grid(True)

    # --- saving ---
    save_path = None
    if save:
        try:
            outdir = outdir.resolve()
            outdir.mkdir(parents=True, exist_ok=True)

            pair_suffix = pair.lower()[-6:] if isinstance(pair, str) else "pair"
            base_fname = f"sr_banded_{bs}_{pair_suffix}_w{weight_exp:.2f}"
            ext = ".png"

            # Find next available filename with counter
            candidate = outdir / f"{base_fname}{ext}"
            counter = 0
            while candidate.exists():
                candidate = outdir / f"{base_fname}_{counter:03d}{ext}"
                counter += 1

            save_path = candidate
            fig.savefig(save_path, dpi=150, bbox_inches="tight")
            print(f"[save] wrote: {save_path}")
        except Exception as e:
            print(f"[save][error] {e!r}  (dir={outdir})")

    if show:
        try:
            plt.show()
        except KeyboardInterrupt:
            pass
    else:
        plt.close(fig)

    return save_path

def watch_live_overlay(sess: "Session", refresh_sec: float = 60.0) -> None:
    with connect() as conn:
        # pick pair (same logic as before) ...
        if sess.pair:
            pair = sess.pair
        else:
            cfg_targets = get_monitored_pairs_from_config()
            if cfg_targets:
                pair = cfg_targets[0]
            else:
                rows = list_pairs(conn)
                if not rows:
                    print("(no pairs to watch — add pairs or runs first)")
                    return
                pair = rows[0]["pair_address"]

        meta = get_pair_meta(conn, pair)
        if not meta:
            print(f"[watch] no metadata for pair {pair}")
            return

        bs = (meta.get("base_symbol") or "BASE").upper()

        plt.ion()
        fig, ax = plt.subplots()
        try:
            fig.canvas.manager.set_window_title(f"Live: {bs} banded S/R")
        except Exception:
            pass

        # cache fixed y-lims if you dislike bouncing; set to None for auto each refresh
        fixed_ylim = None  # e.g., fixed_ylim = (0.0001, 0.002)

        def redraw(_evt=None):
            ax.clear()
            times, tok_usd, per_run_points, runs = build_series(conn, pair, limit=sess.limit, smooth=sess.smooth)
            if times:
                if fixed_ylim:
                    ymin, ymax = fixed_ylim
                else:
                    ymin, ymax = _pad_minmax(tok_usd, pad_ratio=0.2)
                ax.set_ylim(ymin, ymax)

                for i in range(len(times) - 1):
                    xseg = [times[i], times[i + 1]]
                    y0, y1 = tok_usd[i], tok_usd[i + 1]
                    pts = per_run_points[i]
                    _, weights_norm, sup_alphas, res_alphas = _per_rung_strengths(pts, weight_exp=sess.weight_exp)

                    _draw_banded_side(ax, xseg, y0, y1, ymin, ymin, weights_norm, sup_alphas, "green")
                    _draw_banded_side(ax, xseg, y0, y1, ymax, ymax, weights_norm, res_alphas, "red")

                    seg_sup = sum(w * a for w, a in zip(weights_norm, sup_alphas))
                    seg_res = sum(w * a for w, a in zip(weights_norm, res_alphas))
                    seg_sum = seg_sup + seg_res
                    if seg_sum <= 1e-6:
                        line_color, line_alpha = "black", 0.6
                    else:
                        if seg_sup >= seg_res:
                            line_color = "green"; dom = seg_sup - seg_res
                        else:
                            line_color = "red"; dom = seg_res - seg_sup
                        line_alpha = 0.35 + 0.65 * (dom / seg_sum)
                    ax.plot(xseg, [y0, y1], color=line_color, alpha=line_alpha, linewidth=2.4)

                ax.set_title(f"Live — {bs} S/R Banded Overlay  (exp={sess.weight_exp:.2f}, smooth={sess.smooth})")
                ax.set_xlabel("Time (UTC)")
                ax.set_ylabel("USD per 1 BASE")
                ax.grid(True)
            else:
                ax.set_title("No runs yet — waiting for data...")
                ax.grid(True)

            fig.canvas.draw_idle()
            fig.canvas.flush_events()

        # first draw
        redraw()

        # non-blocking timer fires every refresh_sec
        timer = fig.canvas.new_timer(interval=int(refresh_sec * 1000))
        timer.add_callback(redraw)
        timer.start()

        try:
            plt.show(block=True)  # hand control to GUI loop; close window to exit
        except KeyboardInterrupt:
            pass
        finally:
            plt.ioff()

# --------------------------- Interactive menu -------------------------

def _inp(prompt: str) -> str:
    try:
        return input(prompt)
    except EOFError:
        return ""
    except KeyboardInterrupt:
        print("\n[menu] interrupted")
        raise SystemExit(0)

def _press_enter() -> None:
    _inp("\nPress ENTER to continue...")

class Session:
    def __init__(self) -> None:
        # Fill from config on construction
        cfg = load_cfg()
        pm = _get_pm_cfg(cfg)
        self.limit: int = int(pm.get("limit", _PM_DEFAULTS["limit"]))
        self.smooth: int = int(pm.get("smooth", _PM_DEFAULTS["smooth"]))        # price smoothing (only)
        self.show: bool = bool(pm.get("show", _PM_DEFAULTS["show"]))
        self.save: bool = bool(pm.get("save", _PM_DEFAULTS["save"]))
        self.outdir: Path = Path(str(pm.get("outdir", _PM_DEFAULTS["outdir"])))
        self.pair: Optional[str] = pm.get("last_pair") or None  # None => iterate all targets
        self.weight_exp: float = float(pm.get("weight_exp", _PM_DEFAULTS["weight_exp"]))
        self.refresh_sec: float = float(pm.get("refresh_sec", _PM_DEFAULTS["refresh_sec"]))

    # Persist current session values back to config.json
    def save_to_config(self) -> None:
        cfg = load_cfg()
        pm = _get_pm_cfg(cfg)
        pm.update({
            "limit": int(self.limit),
            "smooth": int(self.smooth),
            "show": bool(self.show),
            "save": bool(self.save),
            "outdir": str(self.outdir),
            "last_pair": self.pair,
            "weight_exp": float(self.weight_exp),
            "refresh_sec": float(self.refresh_sec),
        })
        _store_pm_cfg(cfg, pm)

def pick_pair_menu(conn: sqlite3.Connection, sess: Session) -> None:
    cfg_targets = get_monitored_pairs_from_config()
    db_targets = [r["pair_address"] for r in list_pairs(conn)]
    targets = cfg_targets if cfg_targets else db_targets
    if not targets:
        print("(no pairs found in config or DB)")
        _press_enter()
        return

    metas = [(pa, get_pair_meta(conn, pa)) for pa in targets]
    print("\n[Select Pair]")
    for i, (pa, meta) in enumerate(metas, start=1):
        bs = (meta.get("base_symbol") if meta else None) or "BASE"
        qs = (meta.get("quote_symbol") if meta else None) or "QUOTE"
        sel = " *" if sess.pair and sess.pair.lower() == pa.lower() else ""
        print(f"  {i}) {bs}/{qs}  {pa}{sel}")
    print(f"  {len(metas) + 1}) ALL (iterate over each)")

    choice = _inp("> ").strip()
    try:
        idx = int(choice)
    except Exception:
        print("Invalid selection.")
        _press_enter()
        return

    if 1 <= idx <= len(metas):
        sess.pair = metas[idx - 1][0]
        print(f"[ok] selected pair: {sess.pair}")
    elif idx == len(metas) + 1:
        sess.pair = None
        print("[ok] will iterate over all targets")
    else:
        print("Invalid selection.")
        _press_enter()
        return

    # persist the last selection
    sess.save_to_config()
    _press_enter()

def config_menu(sess: Session) -> None:
    while True:
        print("\n[Banded S/R Config]")
        print(f"  1) Limit runs: {sess.limit}")
        print(f"  2) Smoothing window (price): {sess.smooth}")
        print(f"  3) Save PNGs: {sess.save}")
        print(f"  4) Show interactively: {sess.show}")
        print(f"  5) Output directory: {sess.outdir}")
        print(f"  6) Rung weight exponent (USD^exp): {sess.weight_exp:.2f}")
        print(f"  7) Live refresh interval (sec): {sess.refresh_sec:.2f}")
        print("  8) Back")
        ch = _inp("> ").strip()
        if ch == "1":
            sess.limit = int(_inp("Max runs to load (e.g., 500): ").strip() or str(sess.limit))
            sess.save_to_config()
        elif ch == "2":
            sess.smooth = int(_inp("Smoothing window (>=2 to enable): ").strip() or str(sess.smooth))
            sess.save_to_config()
        elif ch == "3":
            sess.save = (_inp("Save PNGs? [Y/n]: ").strip().lower() != "n")
            sess.save_to_config()
        elif ch == "4":
            sess.show = (_inp("Show interactively? [y/N]: ").strip().lower() == "y")
            sess.save_to_config()
        elif ch == "5":
            p = _inp("Output folder (default plots): ").strip() or str(sess.outdir)
            sess.outdir = Path(p)
            sess.save_to_config()
        elif ch == "6":
            try:
                sess.weight_exp = float(_inp("exp (0=equal, 1=linear, 2=quadratic): ").strip() or str(sess.weight_exp))
            except Exception:
                print("[err] invalid float")
            sess.save_to_config()
        elif ch == "7":
            try:
                val = float(_inp("Live refresh interval in seconds (e.g., 5): ").strip() or str(sess.refresh_sec))
                if not (0.1 <= val <= 3600):
                    print("[warn] clamp refresh to [0.1, 3600] seconds")
                sess.refresh_sec = max(0.1, min(3600.0, val))
            except Exception:
                print("[err] invalid number")
            sess.save_to_config()
        elif ch == "8":
            return
        else:
            print("Invalid choice.")

def run_overlay_for_pair(conn: sqlite3.Connection, pair: str, sess: Session) -> None:
    meta = get_pair_meta(conn, pair)
    if not meta:
        print(f"[skip] no metadata for pair {pair}")
        return
    path = plot_banded_sr_overlay_for_pair(
        conn=conn,
        meta=meta,
        pair=pair,
        limit=sess.limit,
        smooth=sess.smooth,
        weight_exp=sess.weight_exp,
        show=sess.show,
        save=sess.save,
        outdir=sess.outdir,
    )
    if path:
        print(f"[saved] {path}")
    else:
        print(f"[skip] no runs for pair {pair}")

def overlay_menu(sess: Session) -> None:
    with connect() as conn:
        if sess.pair:
            targets = [sess.pair]
        else:
            cfg_targets = get_monitored_pairs_from_config()
            targets = cfg_targets if cfg_targets else [r["pair_address"] for r in list_pairs(conn)]
        if not targets:
            print("(no pairs found to plot — add pairs or runs first)")
            _press_enter()
            return

        print(f"\n[Banded S/R Overlay] smooth={sess.smooth} weight_exp={sess.weight_exp:.2f} outdir={sess.outdir} save={sess.save} show={sess.show}")
        for p in targets:
            run_overlay_for_pair(conn, p, sess)
        _press_enter()

def main_menu() -> None:
    sess = Session()  # auto-loads config
    while True:
        print("\n=== Token S/R Banded Overlay ===")
        print("  1) Select pair(s)")
        print("  2) Configure")
        print("  3) Plot S/R Banded Overlay")
        print(f"  4) Watch live ({sess.refresh_sec:.1f}s refresh)")
        print("  5) Exit")
        choice = _inp("> ").strip()
        if choice == "1":
            with connect() as conn:
                pick_pair_menu(conn, sess)
        elif choice == "2":
            config_menu(sess)
        elif choice == "3":
            overlay_menu(sess)
        elif choice == "4":
            watch_live_overlay(sess, refresh_sec=sess.refresh_sec)
        elif choice == "5":
            print("Bye!")
            return
        else:
            print("Invalid choice.")

if __name__ == "__main__":
    main_menu()
