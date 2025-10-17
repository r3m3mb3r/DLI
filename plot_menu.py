#!/usr/bin/env python3
# plot_menu.py — Token price with per-rung proportional S/R shading (interactive)
#
# What you get:
#   • One visualization: token price (BASE_USD) vs time
#   • Below the line: stacked GREEN bands per ladder rung (height ∝ USD^exp, darkness ∝ BUY dominance)
#   • Above the line: stacked RED bands per ladder rung (height ∝ USD^exp, darkness ∝ SELL dominance)
#   • NEW: The price line itself switches color per time segment:
#       - Green where support (BUY) dominates, Red where resistance (SELL) dominates
#       - Line opacity scales with dominance strength in that segment
#
# Menu:
#   1) Select pair(s)
#   2) Configure (limit, smoothing for price, show/save, outdir, weight exponent)
#   3) Plot S/R Banded Overlay
#   4) Exit
#
# Usage:
#   python plot_menu.py
#
# Requires DB filled by ladder.py:
#   - ladder_runs(id, started_at, base_usd, pair_address, ...)
#   - ladder_points(run_id, usd, buy_bps, sell_bps)

import json
import math
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import matplotlib.pyplot as plt

from db_helper import DB_PATH  # uses config.json under the hood

CONFIG_FILE = Path("config.json")


# --------------------------- Config helpers ---------------------------

def load_cfg() -> Dict[str, Any]:
    if CONFIG_FILE.exists():
        try:
            return json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}

def get_monitored_pairs_from_config() -> List[str]:
    cfg = load_cfg()
    addrs = cfg.get("pair_addresses")
    return list(addrs) if isinstance(addrs, list) else []


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
    cur = conn.execute("""
        SELECT id, started_at, base_usd
        FROM ladder_runs
        WHERE lower(pair_address) = lower(?)
        ORDER BY started_at ASC, id ASC
        LIMIT ?
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
        # keep alpha within pleasing visible range
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
        #    Compute weighted averages of rung dominance alphas
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
            # map dominance to alpha in [0.35, 1.0]
            line_alpha = 0.35 + 0.65 * (dom / seg_sum)

        ax.plot(xseg, [y0, y1], color=line_color, alpha=line_alpha, linewidth=2.4)

    ax.set_title(f"{bs} — Price with per-rung Support/Resistance bands\n"
                 f"(band height ∝ USD^exp, band darkness ∝ rung dominance; line color = stronger side; exp={weight_exp:.2f})")
    ax.set_xlabel("Time (UTC)")
    ax.set_ylabel("USD per 1 BASE")
    ax.grid(True)

        # --- saving ---
    save_path = None
    if save:
        try:
            outdir = outdir.resolve()  # absolute path for clarity
            outdir.mkdir(parents=True, exist_ok=True)

            # add a short pair suffix to avoid collisions when multiple tokens share the same symbol
            pair_suffix = pair.lower()[-6:] if isinstance(pair, str) else "pair"
            fname = f"sr_banded_{bs}_{pair_suffix}_w{weight_exp:.2f}.png"

            save_path = outdir / fname
            fig.savefig(save_path, dpi=150, bbox_inches="tight")  # use fig.savefig (not plt)
            print(f"[save] wrote: {save_path}")
        except Exception as e:
            print(f"[save][error] {e!r}  (dir={outdir})")
    if show:
        plt.show()
    else:
        plt.close(fig)
    return save_path



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
        self.limit: int = 500
        self.smooth: int = 1              # price smoothing (only)
        self.show: bool = False
        self.save: bool = True
        self.outdir: Path = Path("plots")
        self.pair: Optional[str] = None   # None => iterate all targets
        self.weight_exp: float = 1.0      # rung weight = USD^exp (0: equal per rung; 1: linear; 2: emphasize large)

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
        print(f"  {i}) {bs}/{qs}  {pa}")
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

def config_menu(sess: Session) -> None:
    while True:
        print("\n[Banded S/R Config]")
        print(f"  1) Limit runs: {sess.limit}")
        print(f"  2) Smoothing window (price): {sess.smooth}")
        print(f"  3) Save PNGs: {sess.save}")
        print(f"  4) Show interactively: {sess.show}")
        print(f"  5) Output directory: {sess.outdir}")
        print(f"  6) Rung weight exponent (USD^exp): {sess.weight_exp:.2f}")
        print("  7) Back")
        ch = _inp("> ").strip()
        if ch == "1":
            sess.limit = int(_inp("Max runs to load (e.g., 500): ").strip() or "500")
        elif ch == "2":
            sess.smooth = int(_inp("Smoothing window (>=2 to enable): ").strip() or "1")
        elif ch == "3":
            sess.save = (_inp("Save PNGs? [Y/n]: ").strip().lower() != "n")
        elif ch == "4":
            sess.show = (_inp("Show interactively? [y/N]: ").strip().lower() == "y")
        elif ch == "5":
            p = _inp("Output folder (default plots): ").strip() or "plots"
            sess.outdir = Path(p)
        elif ch == "6":
            try:
                sess.weight_exp = float(_inp("exp (0=equal, 1=linear, 2=quadratic): ").strip() or "1")
            except Exception:
                print("[err] invalid float")
        elif ch == "7":
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
    sess = Session()
    while True:
        print("\n=== Token S/R Banded Overlay ===")
        print("  1) Select pair(s)")
        print("  2) Configure")
        print("  3) Plot S/R Banded Overlay")
        print("  4) Exit")
        choice = _inp("> ").strip()
        if choice == "1":
            with connect() as conn:
                pick_pair_menu(conn, sess)
        elif choice == "2":
            config_menu(sess)
        elif choice == "3":
            overlay_menu(sess)
        elif choice == "4":
            print("Bye!")
            return
        else:
            print("Invalid choice.")

if __name__ == "__main__":
    main_menu()
