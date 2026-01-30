# reconcile_trades_csv.py
# One-time reconciliation:
# If IBKR position for a symbol is 0, mark any OPEN/ENTRY_SENT/EXIT_SENT rows in trades.csv as CLOSED_RECONCILED
# (with exit_price = entry_price as a neutral placeholder) so the dashboard open-trades snapshot is accurate.
#
# Usage:
#   cd C:\Users\marti\Desktop\IBKR_MeanReversion_Bot
#   py reconcile_trades_csv.py

import sys
import asyncio

if sys.platform.startswith("win"):
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except Exception:
        pass

try:
    asyncio.get_running_loop()
except RuntimeError:
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())

from pathlib import Path
import shutil
import pandas as pd
import yaml
from ib_insync import IB


EXPECTED_COLS = [
    "trade_id","symbol","entry_date","exit_date",
    "entry_price","exit_price","quantity","status",
    "entry_orderId","exit_orderId","note"
]


def load_config(root: Path) -> dict:
    p = root / "config.yaml"
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def safe_ib_connect(ib: IB, host: str, port: int, client_id: int) -> tuple[bool, str]:
    try:
        ib.connect(host, port, clientId=client_id, timeout=5)
        if ib.isConnected():
            return True, "connected"
        return False, "connect_failed_unknown"
    except Exception as e:
        try:
            ib.disconnect()
        except Exception:
            pass
        return False, f"{type(e).__name__}: {e}"


def get_long_positions(ib: IB) -> dict[str, int]:
    out: dict[str, int] = {}
    try:
        positions = ib.positions()
    except Exception:
        return out

    for p in positions:
        try:
            sym = str(getattr(p.contract, "symbol", "")).strip().upper()
            qty = float(getattr(p, "position", 0))
        except Exception:
            continue
        if sym and qty > 0:
            out[sym] = int(qty)
    return out


def main() -> int:
    root = Path(__file__).resolve().parent
    trades_path = root / "trades.csv"
    if not trades_path.exists():
        print(f"[ERROR] trades.csv not found at: {trades_path}")
        return 1

    # backup trades.csv
    bak = trades_path.with_suffix(".csv.reconcile.bak")
    if not bak.exists():
        shutil.copy2(trades_path, bak)
        print(f"[OK] Backup created: {bak}")

    df = pd.read_csv(trades_path, dtype=str, keep_default_na=False)

    # remove embedded header rows if present
    if "trade_id" in df.columns:
        df = df[df["trade_id"].astype(str).str.strip().str.lower() != "trade_id"]

    # ensure schema
    for c in EXPECTED_COLS:
        if c not in df.columns:
            df[c] = ""
    df = df[EXPECTED_COLS].copy()

    cfg = load_config(root)
    ib = IB()
    online, reason = safe_ib_connect(ib, cfg["ibkr_host"], int(cfg["ibkr_port"]), int(cfg["client_id"]))
    if not online:
        print(f"[ERROR] IBKR connect failed: {reason}")
        return 2

    try:
        pos = get_long_positions(ib)
    finally:
        try:
            ib.disconnect()
        except Exception:
            pass

    # Any symbol NOT in pos has 0 long shares
    df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
    df["status"] = df["status"].astype(str).str.strip().str.upper()

    open_like = {"OPEN", "ENTRY_SENT", "EXIT_SENT"}
    changed = 0

    for i, row in df.iterrows():
        sym = row["symbol"]
        st = row["status"]
        if st in open_like and sym and sym not in pos:
            # reconcile-close it
            entry_px = row.get("entry_price", "")
            # neutral placeholder if entry_price missing
            if entry_px == "":
                entry_px = "0"
            df.at[i, "exit_price"] = entry_px
            df.at[i, "status"] = "CLOSED_RECONCILED"
            note = (row.get("note", "") or "").strip()
            extra = "Reconciled closed: IBKR position=0; exit_price set to entry_price (placeholder)."
            df.at[i, "note"] = (note + " | " + extra).strip(" |")
            changed += 1

    df.to_csv(trades_path, index=False, lineterminator="\n")

    print("[OK] Reconciliation complete.")
    print(f"Rows reconciled/closed: {changed}")
    print("Restart Streamlit to refresh the Open trades snapshot.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
