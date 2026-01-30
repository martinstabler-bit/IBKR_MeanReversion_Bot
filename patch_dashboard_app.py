from __future__ import annotations

from pathlib import Path
import shutil
import re


TARGET_REL_PATH = Path("analytics") / "dashboard" / "app.py"

# We previously inserted a patch block marker. We'll replace that block with a better one.
PATCH_BLOCK_RE = re.compile(
    r"^(?P<indent>[ \t]*)# --- PATCH: robust by_sym column handling.*?^(?P=indent)# --- END PATCH ---[ \t]*\r?\n?",
    re.MULTILINE | re.DOTALL,
)

# If no patch block exists, fall back to replacing the original rename line (captures indent)
RENAME_LINE_RE = re.compile(
    r'^(?P<indent>[ \t]*)by_sym\.columns\s*=\s*\[\s*"Symbol"\s*,\s*"Trades"\s*,\s*"Total_PnL"\s*,\s*"Avg_PnL"\s*\]\s*$',
    re.MULTILINE,
)


def indent_block(block: str, indent: str) -> str:
    lines = block.splitlines(True)
    out = []
    for ln in lines:
        if ln.strip() == "":
            out.append(ln)
        else:
            out.append(indent + ln)
    return "".join(out)


def main() -> int:
    root = Path(__file__).resolve().parent
    target = root / TARGET_REL_PATH

    if not target.exists():
        print(f"[ERROR] Could not find: {target}")
        print("Make sure this script is saved in the IBKR_MeanReversion_Bot folder.")
        return 1

    # Backup chain (keep the prior .bak, also create a new .bak2 snapshot)
    bak = target.with_suffix(".py.bak")
    bak2 = target.with_suffix(".py.bak2")

    if not bak.exists():
        shutil.copy2(target, bak)
        print(f"[OK] Created backup: {bak}")

    shutil.copy2(target, bak2)
    print(f"[OK] Created snapshot backup: {bak2}")

    text = target.read_text(encoding="utf-8")

    # New robust patch block (unindented; we will indent to match location)
    # Uses heuristics to identify columns and then normalizes to: Symbol, Trades, Total_PnL, Avg_PnL
    new_patch_unindented = """# --- PATCH: robust by_sym column handling (prevents length/typing errors) ---
# Normalize by_sym into columns: Symbol, Trades, Total_PnL, Avg_PnL
# Handles cases where by_sym comes back with extra columns or shuffled ordering.
import pandas as pd

def _pick_col(candidates, name_hints):
    for h in name_hints:
        for c in candidates:
            if h in str(c).lower():
                return c
    return None

# If symbol is in the index, bring it out
if getattr(by_sym, "index", None) is not None and getattr(by_sym.index, "name", None) and str(by_sym.index.name).lower() == "symbol":
    by_sym = by_sym.reset_index()

cols = list(getattr(by_sym, "columns", []))

# Find symbol column
sym_col = None
sym_col = _pick_col(cols, ["symbol"])
if sym_col is None and len(cols) > 0:
    # Heuristic: choose the first object-like column as symbol
    for c in cols:
        s = by_sym[c]
        if s.dtype == "object":
            sym_col = c
            break
    if sym_col is None:
        sym_col = cols[0]  # fallback

# Candidate numeric columns are everything else
other_cols = [c for c in cols if c != sym_col]

# Try to identify Trades/Total/Avg by column names first
trades_col = _pick_col(other_cols, ["trades", "trade", "count", "n_trades", "num"])
total_col  = _pick_col(other_cols, ["total_pnl", "total", "sum", "pnl_sum"])
avg_col    = _pick_col(other_cols, ["avg_pnl", "avg", "mean", "pnl_mean"])

# Coerce numeric for remaining columns to support heuristics
tmp = by_sym.copy()
for c in other_cols:
    tmp[c] = pd.to_numeric(tmp[c], errors="coerce")

numeric_cols = [c for c in other_cols if tmp[c].notna().any()]

# If trades_col not found, pick the numeric col that "looks like counts"
if trades_col is None and numeric_cols:
    best = None
    best_score = -1
    for c in numeric_cols:
        s = tmp[c].dropna()
        if s.empty:
            continue
        # score: how integer-like and non-negative
        int_like = (abs(s - s.round()) < 1e-9).mean()
        nonneg = (s >= 0).mean()
        score = 2.0 * int_like + 1.0 * nonneg + 0.1 * len(s)
        if score > best_score:
            best_score = score
            best = c
    trades_col = best

# For PnL columns, pick largest magnitude sum/mean among remaining numeric cols
remaining = [c for c in numeric_cols if c not in {trades_col}]
if total_col is None and remaining:
    total_col = max(remaining, key=lambda c: abs(float(tmp[c].fillna(0).sum())))
if avg_col is None and remaining:
    # Prefer a different column from total_col if possible
    rem2 = [c for c in remaining if c != total_col] or remaining
    avg_col = max(rem2, key=lambda c: abs(float(tmp[c].fillna(0).mean())))

# Build normalized frame
norm = pd.DataFrame()
norm["Symbol"] = by_sym[sym_col].astype(str)

# Trades
if trades_col is not None:
    norm["Trades"] = pd.to_numeric(tmp[trades_col], errors="coerce").fillna(0).astype(int)
else:
    norm["Trades"] = 0

# Total_PnL / Avg_PnL
if total_col is not None:
    norm["Total_PnL"] = pd.to_numeric(tmp[total_col], errors="coerce").fillna(0.0)
else:
    norm["Total_PnL"] = 0.0

if avg_col is not None:
    norm["Avg_PnL"] = pd.to_numeric(tmp[avg_col], errors="coerce").fillna(0.0)
else:
    norm["Avg_PnL"] = 0.0

# Drop obviously bad symbols
norm = norm[norm["Symbol"].str.len() > 0].copy()

by_sym = norm
# --- END PATCH ---
"""

    # First try: replace existing patch block
    m_patch = PATCH_BLOCK_RE.search(text)
    if m_patch:
        indent = m_patch.group("indent")
        replacement = indent_block(new_patch_unindented, indent)
        start, end = m_patch.span()
        patched = text[:start] + replacement + text[end:]
        target.write_text(patched, encoding="utf-8")
        print("[OK] Replaced existing patch block with improved normalization patch.")
        print(f"Backups: {bak} and {bak2}")
        return 0

    # Second try: replace original rename line
    m_rename = RENAME_LINE_RE.search(text)
    if not m_rename:
        print("[ERROR] Could not find an existing patch block OR the rename line to patch.")
        print("Search in analytics/dashboard/app.py for:")
        print('  by_sym.columns = ["Symbol", "Trades", "Total_PnL", "Avg_PnL"]')
        print("If it differs, tell me the exact line and I’ll adapt the patch script.")
        return 2

    indent = m_rename.group("indent")
    replacement = indent_block(new_patch_unindented, indent)
    start, end = m_rename.span()
    patched = text[:start] + replacement + text[end:]
    target.write_text(patched, encoding="utf-8")

    print("[OK] Patched rename line with improved normalization patch.")
    print(f"Backups: {bak} and {bak2}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
