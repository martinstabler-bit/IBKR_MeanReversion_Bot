import os
import json
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd


BASE = Path(".")
RUNS_CSV = BASE / "analytics" / "logs" / "runs.csv"
TRADES_CSV = BASE / "trades.csv"
ACTIONS_CSV = BASE / "actions_queue.csv"
CACHE_DIR = BASE / "bar_cache"

OUT_DIR = BASE / "analytics" / "health"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_CSV = OUT_DIR / "health_snapshot.csv"
OUT_JSON = OUT_DIR / "health_snapshot.json"


def _safe_read_csv(path: Path) -> pd.DataFrame:
    if path.exists():
        try:
            return pd.read_csv(path)
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()


def _file_age_days(path: Path) -> float:
    try:
        mtime = path.stat().st_mtime
        age_sec = max(0.0, datetime.now(timezone.utc).timestamp() - mtime)
        return age_sec / 86400.0
    except Exception:
        return float("nan")


def build_snapshot() -> dict:
    now = datetime.now(timezone.utc).isoformat()

    runs = _safe_read_csv(RUNS_CSV)
    trades = _safe_read_csv(TRADES_CSV)
    actions = _safe_read_csv(ACTIONS_CSV)

    # ---- last run ----
    last_run = {}
    if not runs.empty and "ts_utc" in runs.columns:
        runs_sorted = runs.sort_values("ts_utc")
        last = runs_sorted.iloc[-1].to_dict()
        last_run = {
            "last_run_ts_utc": str(last.get("ts_utc", "")),
            "last_run_id": str(last.get("run_id", "")),
            "last_run_online": bool(last.get("ibkr_online", False)),
            "last_run_status": str(last.get("status", "")),
            "last_run_reason": str(last.get("reason", "")),
            "last_run_duration_ms": int(last.get("duration_ms", 0)) if str(last.get("duration_ms", "")).strip() else 0,
            "last_run_signals_found": int(last.get("signals_found", 0)) if str(last.get("signals_found", "")).strip() else 0,
            "last_run_entries_attempted": int(last.get("entries_attempted", 0)) if str(last.get("entries_attempted", "")).strip() else 0,
            "last_run_exits_sent": int(last.get("exits_sent", 0)) if str(last.get("exits_sent", "")).strip() else 0,
            "last_run_fills_applied": int(last.get("fills_applied", 0)) if str(last.get("fills_applied", "")).strip() else 0,
            "last_run_stale_cache_symbols": int(last.get("stale_cache_symbols", 0)) if str(last.get("stale_cache_symbols", "")).strip() else 0,
            "last_run_integrity_flags": int(last.get("integrity_flags", 0)) if str(last.get("integrity_flags", "")).strip() else 0,
        }

    # ---- cache freshness ----
    cache_files = list(CACHE_DIR.glob("*.csv")) if CACHE_DIR.exists() else []
    cache_count = len(cache_files)

    ages = [ _file_age_days(p) for p in cache_files ]
    ages = [a for a in ages if a == a]  # drop NaN
    newest_days = min(ages) if ages else None
    oldest_days = max(ages) if ages else None

    stale_threshold_days = 3.0  # tweak later; 3 calendar days is a decent proxy
    stale_symbols = []
    for p in cache_files:
        age = _file_age_days(p)
        if age > stale_threshold_days:
            stale_symbols.append(p.stem)

    # ---- trades health ----
    trades_health = {}
    integrity_flags = []

    if trades.empty:
        trades_health = {
            "trades_rows": 0,
            "open_like": 0,
            "open": 0,
            "entry_sent": 0,
            "exit_sent": 0,
            "queued_entry": 0,
            "closed": 0,
        }
    else:
        # normalize
        if "status" not in trades.columns:
            trades["status"] = ""

        open_like = trades[trades["status"].isin(["OPEN", "ENTRY_SENT", "EXIT_SENT"])]
        trades_health = {
            "trades_rows": int(len(trades)),
            "open_like": int(len(open_like)),
            "open": int((trades["status"] == "OPEN").sum()),
            "entry_sent": int((trades["status"] == "ENTRY_SENT").sum()),
            "exit_sent": int((trades["status"] == "EXIT_SENT").sum()),
            "queued_entry": int((trades["status"] == "QUEUED_ENTRY").sum()),
            "closed": int((trades["status"] == "CLOSED").sum()),
        }

        # Integrity checks
        # OPEN must have quantity and entry_price
        open_missing_qty = trades[(trades["status"] == "OPEN") & (trades.get("quantity", "").astype(str).str.strip() == "")]
        open_missing_entry = trades[(trades["status"] == "OPEN") & (trades.get("entry_price", "").astype(str).str.strip() == "")]
        closed_missing_exit = trades[(trades["status"] == "CLOSED") & (trades.get("exit_price", "").astype(str).str.strip() == "")]

        if len(open_missing_qty) > 0:
            integrity_flags.append(f"OPEN_missing_quantity={len(open_missing_qty)}")
        if len(open_missing_entry) > 0:
            integrity_flags.append(f"OPEN_missing_entry_price={len(open_missing_entry)}")
        if len(closed_missing_exit) > 0:
            integrity_flags.append(f"CLOSED_missing_exit_price={len(closed_missing_exit)}")

        # Duplicate open-like by symbol
        if "symbol" in trades.columns:
            dup = open_like["symbol"].value_counts()
            dups = dup[dup > 1]
            if len(dups) > 0:
                integrity_flags.append("duplicate_open_like_symbols=" + ",".join([f"{k}({v})" for k, v in dups.items()]))

    # ---- actions queue health ----
    actions_count = int(len(actions)) if not actions.empty else 0

    snapshot = {
        "generated_ts_utc": now,
        **last_run,
        "cache_count": cache_count,
        "cache_newest_age_days": newest_days,
        "cache_oldest_age_days": oldest_days,
        "stale_cache_symbols_count": len(stale_symbols),
        "stale_cache_symbols": ",".join(stale_symbols[:50]),  # cap
        "actions_queue_rows": actions_count,
        **trades_health,
        "integrity_flags_count": len(integrity_flags),
        "integrity_flags": ";".join(integrity_flags),
    }

    return snapshot


def main():
    snap = build_snapshot()
    df = pd.DataFrame([snap])
    df.to_csv(OUT_CSV, index=False)

    with open(OUT_JSON, "w") as f:
        json.dump(snap, f, indent=2)

    print(f"Wrote: {OUT_CSV}")
    print(f"Wrote: {OUT_JSON}")


if __name__ == "__main__":
    main()
