# clean_trades_csv.py
# One-time cleaner for trades.csv:
# - removes duplicate header rows embedded in the file body
# - removes fully blank rows
# - removes exact duplicate rows
# - normalizes to the expected schema/column order
#
# Usage (from bot folder):
#   py clean_trades_csv.py
#
# It will create a backup: trades.csv.bak (and trades.csv.bak2 if bak already exists)

from __future__ import annotations

from pathlib import Path
import shutil
import pandas as pd


EXPECTED_COLS = [
    "trade_id", "symbol", "entry_date", "exit_date",
    "entry_price", "exit_price", "quantity", "status",
    "entry_orderId", "exit_orderId", "note"
]


def main() -> int:
    root = Path(__file__).resolve().parent
    trades_path = root / "trades.csv"

    if not trades_path.exists():
        print(f"[ERROR] trades.csv not found at: {trades_path}")
        return 1

    # Backups
    bak = trades_path.with_suffix(".csv.bak")
    bak2 = trades_path.with_suffix(".csv.bak2")
    if not bak.exists():
        shutil.copy2(trades_path, bak)
        print(f"[OK] Backup created: {bak}")
    else:
        shutil.copy2(trades_path, bak2)
        print(f"[OK] Backup snapshot created: {bak2}")

    # Read raw (dtype=str keeps everything stable)
    df = pd.read_csv(trades_path, dtype=str, keep_default_na=False)

    before_rows = len(df)

    # Drop rows that are actually header lines repeated inside the file
    if "trade_id" in df.columns:
        df = df[df["trade_id"].astype(str).str.strip().str.lower() != "trade_id"]

    # Drop fully blank rows
    df = df.dropna(how="all")
    df = df[(df.astype(str).apply(lambda r: "".join(r).strip(), axis=1) != "")]

    # Ensure expected columns exist; add missing as blanks
    for c in EXPECTED_COLS:
        if c not in df.columns:
            df[c] = ""

    # Keep only expected columns in correct order
    df = df[EXPECTED_COLS].copy()

    # Drop exact duplicate rows
    df = df.drop_duplicates().reset_index(drop=True)

    after_rows = len(df)

    # Write cleaned file
    df.to_csv(trades_path, index=False, lineterminator="\n")

    print("[OK] trades.csv cleaned successfully.")
    print(f"Rows before: {before_rows}")
    print(f"Rows after : {after_rows}")
    print(f"Output     : {trades_path}")
    print("If anything looks wrong, restore from the .bak file.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
