# =========================
# MUST BE FIRST (Python 3.14+ / Windows asyncio fix BEFORE ib_insync import)
# =========================
import sys
import asyncio

if sys.platform.startswith("win"):
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except Exception:
        pass

# Ensure a loop exists in MainThread (needed because ib_insync/eventkit queries it at import time)
try:
    asyncio.get_running_loop()
except RuntimeError:
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())

# =========================
# Imports
# =========================
from datetime import datetime
from time import perf_counter
import uuid
from pathlib import Path

import pandas as pd
import pytz
import yaml

from ib_insync import IB, Stock, MarketOrder


# =========================
# Paths
# =========================
BOT_ROOT = Path(__file__).resolve().parent
ANALYTICS_DIR = BOT_ROOT / "analytics"
LOGS_DIR = ANALYTICS_DIR / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

RUNS_CSV = LOGS_DIR / "runs.csv"
ORDERS_CSV = LOGS_DIR / "orders.csv"
TRADES_CSV = BOT_ROOT / "trades.csv"


# =========================
# Schemas
# =========================
EXPECTED_TRADE_COLS = [
    "trade_id", "symbol", "entry_date", "exit_date",
    "entry_price", "exit_price", "quantity", "status",
    "entry_orderId", "exit_orderId", "note"
]

ORDERS_COLS = [
    "ts_local", "run_id", "symbol", "action", "order_id", "qty", "order_type", "tif",
    "status", "submitted_price", "avg_fill_price", "filled_qty", "remaining_qty",
    "error_code", "error_msg"
]

RUNS_COLS = [
    "ts_local", "run_id", "status", "reason", "duration_ms",
    "tws_connected", "ib_host", "ib_port", "client_id"
]


# =========================
# Helpers
# =========================
def now_local_iso(tz) -> str:
    return datetime.now(tz).isoformat(timespec="seconds")


def append_row_csv(path: Path, row: dict, columns: list[str]) -> None:
    df = pd.DataFrame([{c: row.get(c, "") for c in columns}])
    if path.exists():
        df.to_csv(path, mode="a", index=False, header=False, lineterminator="\n")
    else:
        df.to_csv(path, index=False, lineterminator="\n")


def load_config() -> dict:
    cfg_path = BOT_ROOT / "config.yaml"
    if not cfg_path.exists():
        raise FileNotFoundError(f"Missing config.yaml at {cfg_path}")
    with open(cfg_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_trades() -> pd.DataFrame:
    if not TRADES_CSV.exists():
        return pd.DataFrame(columns=EXPECTED_TRADE_COLS)

    df = pd.read_csv(TRADES_CSV, dtype=str, keep_default_na=False)

    # Remove embedded header rows
    if "trade_id" in df.columns:
        df = df[df["trade_id"].astype(str).str.strip().str.lower() != "trade_id"]

    # Ensure schema
    for c in EXPECTED_TRADE_COLS:
        if c not in df.columns:
            df[c] = ""

    df = df[EXPECTED_TRADE_COLS].drop_duplicates().reset_index(drop=True)
    return df


def save_trades(df: pd.DataFrame) -> None:
    df.to_csv(TRADES_CSV, index=False, lineterminator="\n")


def log_order_event(
    ts_local: str,
    run_id: str,
    symbol: str,
    action: str,
    order_id,
    qty,
    order_type: str,
    tif: str,
    status: str,
    avg_fill_price="",
    filled_qty="",
    remaining_qty="",
    error_code="",
    error_msg=""
) -> None:
    append_row_csv(
        ORDERS_CSV,
        {
            "ts_local": ts_local,
            "run_id": run_id,
            "symbol": symbol,
            "action": action,
            "order_id": order_id,
            "qty": qty,
            "order_type": order_type,
            "tif": tif,
            "status": status,
            "submitted_price": "",
            "avg_fill_price": avg_fill_price,
            "filled_qty": filled_qty,
            "remaining_qty": remaining_qty,
            "error_code": error_code,
            "error_msg": error_msg,
        },
        ORDERS_COLS
    )


def log_run(ts_local, run_id, status, reason, duration_ms, tws_connected, ib_host, ib_port, client_id) -> None:
    append_row_csv(
        RUNS_CSV,
        {
            "ts_local": ts_local,
            "run_id": run_id,
            "status": status,
            "reason": reason,
            "duration_ms": int(duration_ms),
            "tws_connected": bool(tws_connected),
            "ib_host": ib_host,
            "ib_port": int(ib_port),
            "client_id": int(client_id),
        },
        RUNS_COLS
    )


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


def avg_fill_from_executions(ib: IB, order_id: int) -> tuple[float | None, int]:
    """Weighted avg fill + total qty for a specific orderId."""
    try:
        fills = ib.reqExecutions()
    except Exception:
        return None, 0

    relevant = []
    for fl in fills:
        oid = getattr(fl.execution, "orderId", None)
        if oid is None:
            continue
        if int(oid) == int(order_id):
            relevant.append(fl)

    if not relevant:
        return None, 0

    total_qty = 0.0
    w_sum = 0.0
    for fl in relevant:
        sh = abs(float(fl.execution.shares))
        px = float(fl.execution.price)
        total_qty += sh
        w_sum += sh * px

    if total_qty <= 0:
        return None, 0

    return (w_sum / total_qty), int(round(total_qty))


def _to_int(x, default=0) -> int:
    try:
        s = str(x).strip()
        if not s:
            return default
        return int(float(s))
    except Exception:
        return default


def _norm_symbol(s: str) -> str:
    return str(s or "").strip().upper()


def get_position_qty(ib: IB, symbol: str) -> int:
    """
    Returns current LONG position quantity for symbol (0 if none or short).
    """
    sym_u = _norm_symbol(symbol)
    try:
        positions = ib.positions()
    except Exception:
        return 0

    for p in positions:
        try:
            sym = _norm_symbol(getattr(p.contract, "symbol", ""))
            qty = float(getattr(p, "position", 0))
        except Exception:
            continue
        if sym == sym_u:
            return int(qty) if qty > 0 else 0
    return 0


def get_open_rows_fifo(trades: pd.DataFrame, symbol: str) -> list[int]:
    """
    Returns OPEN row indexes for symbol, sorted oldest-first using entry_date when possible.
    """
    if trades.empty:
        return []

    t = trades.copy()
    t["symbol"] = t["symbol"].astype(str).str.strip().str.upper()
    t["status"] = t["status"].astype(str).str.strip().str.upper()

    open_idxs = t.index[(t["symbol"] == _norm_symbol(symbol)) & (t["status"] == "OPEN")].tolist()
    if not open_idxs:
        return []

    # Use entry_date to sort if parseable; else keep file order
    try:
        dt = pd.to_datetime(t.loc[open_idxs, "entry_date"], errors="coerce")
        # NaT sorts last; we want oldest first -> sort by dt then by index
        order = sorted(open_idxs, key=lambda i: (pd.to_datetime(t.at[i, "entry_date"], errors="coerce"), i))
        return order
    except Exception:
        return open_idxs


def attach_exit_fifo_multi(trades: pd.DataFrame, symbol: str, total_sell_qty: int, exit_order_id: int, note_prefix: str) -> int:
    """
    Attaches ONE exit orderId across MULTIPLE OPEN rows FIFO until total_sell_qty is consumed.
    If the last row is partially consumed, it splits the row:
      - creates a new EXIT_SENT row for the allocated qty
      - reduces the original OPEN row qty to the remainder
    Returns the qty successfully allocated to OPEN rows.
    """
    if total_sell_qty <= 0 or trades.empty:
        return 0

    sym = _norm_symbol(symbol)
    idxs = get_open_rows_fifo(trades, sym)
    if not idxs:
        return 0

    remaining = int(total_sell_qty)
    allocated_total = 0

    for idx in idxs:
        if remaining <= 0:
            break

        row_qty = _to_int(trades.at[idx, "quantity"], default=0)
        if row_qty <= 0:
            continue

        take = min(remaining, row_qty)

        if take == row_qty:
            # consume whole row -> mark EXIT_SENT on the same row
            trades.at[idx, "status"] = "EXIT_SENT"
            trades.at[idx, "exit_orderId"] = str(exit_order_id)
            existing_note = str(trades.at[idx, "note"] or "").strip()
            extra = f"{note_prefix} allocated_qty={take}"
            trades.at[idx, "note"] = (existing_note + " | " + extra).strip(" |")
        else:
            # partial consume -> split:
            # 1) reduce original OPEN row qty
            trades.at[idx, "quantity"] = str(row_qty - take)

            # 2) create a new EXIT_SENT row for the portion being sold
            new_row = {
                "trade_id": str(uuid.uuid4()),
                "symbol": sym,
                "entry_date": trades.at[idx, "entry_date"],
                "exit_date": trades.at[idx, "exit_date"],
                "entry_price": trades.at[idx, "entry_price"],
                "exit_price": "",
                "quantity": str(take),
                "status": "EXIT_SENT",
                "entry_orderId": trades.at[idx, "entry_orderId"],
                "exit_orderId": str(exit_order_id),
                "note": f"{note_prefix} split_from_trade_id={trades.at[idx, 'trade_id']} allocated_qty={take}",
            }
            trades.loc[len(trades)] = [new_row.get(c, "") for c in EXPECTED_TRADE_COLS]

        remaining -= take
        allocated_total += take

    return allocated_total


def close_all_rows_for_exit_order(trades: pd.DataFrame, exit_order_id: int, avg_px: float, fill_note: str) -> int:
    """
    Marks ALL rows with exit_orderId == exit_order_id as CLOSED and sets exit_price.
    Returns number of rows updated.
    """
    if trades.empty:
        return 0

    oid = str(exit_order_id).strip()
    if not oid:
        return 0

    mask = trades["exit_orderId"].astype(str).str.strip() == oid
    idxs = trades.index[mask].tolist()
    if not idxs:
        return 0

    for idx in idxs:
        trades.at[idx, "exit_price"] = str(round(float(avg_px), 4))
        trades.at[idx, "status"] = "CLOSED"
        existing_note = str(trades.at[idx, "note"] or "").strip()
        trades.at[idx, "note"] = (existing_note + " | " + fill_note).strip(" |")

    return len(idxs)


# =========================
# Commands
# =========================
def cmd_buy(symbol: str, qty: int) -> None:
    start = perf_counter()
    run_id = str(uuid.uuid4())

    cfg = load_config()
    tz = pytz.timezone(cfg.get("timezone", "America/New_York"))
    ts_local = now_local_iso(tz)

    ib = IB()
    online, reason = safe_ib_connect(ib, cfg["ibkr_host"], int(cfg["ibkr_port"]), int(cfg["client_id"]))
    if not online:
        log_run(ts_local, run_id, "error", reason, (perf_counter() - start) * 1000, False,
                cfg["ibkr_host"], cfg["ibkr_port"], cfg["client_id"])
        raise RuntimeError(f"IBKR connect failed: {reason}")

    try:
        trades = load_trades()

        c = Stock(symbol, "SMART", "USD")
        ib.qualifyContracts(c)

        order = MarketOrder("BUY", int(qty))
        order.tif = "DAY"
        tr = ib.placeOrder(c, order)

        trades.loc[len(trades)] = [
            str(uuid.uuid4()),
            _norm_symbol(symbol),
            "", "", "", "", "",
            "ENTRY_SENT",
            str(tr.order.orderId),
            "",
            f"TEST: manual BUY {qty} (DAY MKT); awaiting fill sync."
        ]
        save_trades(trades)

        log_order_event(ts_local, run_id, symbol, "BUY", tr.order.orderId, qty, "MKT", "DAY", "SUBMITTED")

        try:
            ib.sleep(1.0)
        except Exception:
            pass

        avg_px, filled_qty = avg_fill_from_executions(ib, tr.order.orderId)
        if avg_px is not None and filled_qty > 0:
            trades = load_trades()
            mask = trades["entry_orderId"].astype(str).str.strip() == str(tr.order.orderId)
            if mask.any():
                # update ALL rows that reference this entry order (rare, but safe)
                for idx in trades.index[mask].tolist():
                    trades.at[idx, "entry_price"] = str(round(float(avg_px), 4))
                    trades.at[idx, "quantity"] = str(int(filled_qty))  # for DAY MKT, usually all in one row
                    trades.at[idx, "entry_date"] = pd.Timestamp(datetime.now(tz).date())
                    trades.at[idx, "status"] = "OPEN"
                    trades.at[idx, "note"] = f"TEST: entry filled (avg={avg_px:.4f}, qty={filled_qty})."
                save_trades(trades)

            log_order_event(ts_local, run_id, symbol, "BUY", tr.order.orderId, filled_qty, "MKT", "DAY", "FILLED",
                            avg_fill_price=round(float(avg_px), 4), filled_qty=filled_qty, remaining_qty=0)

        log_run(ts_local, run_id, "ok", "manual buy complete", (perf_counter() - start) * 1000, True,
                cfg["ibkr_host"], cfg["ibkr_port"], cfg["client_id"])

    finally:
        try:
            ib.disconnect()
        except Exception:
            pass


def cmd_sell_from_trades(symbol: str, qty: int | None) -> None:
    """
    Sells using the OPEN trade row quantity in trades.csv (old behavior).
    If qty is None: sells full OPEN qty from the FIRST (oldest) OPEN row.
    If qty provided: sells that qty, but will still only attach to the first OPEN row (by design).
    Use 'sellpos' if you want multi-row closure.
    """
    start = perf_counter()
    run_id = str(uuid.uuid4())

    cfg = load_config()
    tz = pytz.timezone(cfg.get("timezone", "America/New_York"))
    ts_local = now_local_iso(tz)

    ib = IB()
    online, reason = safe_ib_connect(ib, cfg["ibkr_host"], int(cfg["ibkr_port"]), int(cfg["client_id"]))
    if not online:
        log_run(ts_local, run_id, "error", reason, (perf_counter() - start) * 1000, False,
                cfg["ibkr_host"], cfg["ibkr_port"], cfg["client_id"])
        raise RuntimeError(f"IBKR connect failed: {reason}")

    try:
        trades = load_trades()
        idxs = get_open_rows_fifo(trades, symbol)
        if not idxs:
            raise RuntimeError(f"No OPEN trade found in trades.csv for {symbol}.")
        idx = idxs[0]

        open_qty = _to_int(trades.at[idx, "quantity"], default=0)
        if open_qty <= 0:
            raise RuntimeError(f"OPEN trade row for {symbol} has no quantity.")

        sell_qty = open_qty if qty is None else int(qty)
        if sell_qty <= 0:
            raise RuntimeError("Sell qty must be > 0.")
        if sell_qty > open_qty:
            sell_qty = open_qty  # cap

        c = Stock(symbol, "SMART", "USD")
        ib.qualifyContracts(c)

        order = MarketOrder("SELL", int(sell_qty))
        order.tif = "DAY"
        tr = ib.placeOrder(c, order)

        trades.at[idx, "status"] = "EXIT_SENT"
        trades.at[idx, "exit_orderId"] = str(tr.order.orderId)
        trades.at[idx, "note"] = f"TEST: manual SELL {sell_qty} (DAY MKT) from trades.csv OPEN row; awaiting fill sync."
        save_trades(trades)

        log_order_event(ts_local, run_id, symbol, "SELL", tr.order.orderId, sell_qty, "MKT", "DAY", "SUBMITTED")

        try:
            ib.sleep(1.0)
        except Exception:
            pass

        avg_px, filled_qty = avg_fill_from_executions(ib, tr.order.orderId)
        if avg_px is not None and filled_qty > 0:
            trades = load_trades()
            updated = close_all_rows_for_exit_order(
                trades,
                exit_order_id=tr.order.orderId,
                avg_px=float(avg_px),
                fill_note=f"TEST: exit filled (avg={avg_px:.4f}, qty={filled_qty})."
            )
            if updated > 0:
                save_trades(trades)

            log_order_event(ts_local, run_id, symbol, "SELL", tr.order.orderId, filled_qty, "MKT", "DAY", "FILLED",
                            avg_fill_price=round(float(avg_px), 4), filled_qty=filled_qty, remaining_qty=0)

        log_run(ts_local, run_id, "ok", "manual sell (from trades.csv) complete", (perf_counter() - start) * 1000, True,
                cfg["ibkr_host"], cfg["ibkr_port"], cfg["client_id"])

    finally:
        try:
            ib.disconnect()
        except Exception:
            pass


def cmd_sell_from_position(symbol: str, qty: int | None) -> None:
    """
    Sells based on ACTUAL IBKR position size, not trades.csv.

    NEW BEHAVIOR:
    - Attaches ONE exit order across MULTIPLE OPEN rows FIFO until sell qty is fully allocated.
    - If partial allocation needed on a row, it SPLITS the row so accounting stays consistent.
    - When fills are detected, it closes ALL rows sharing the same exit_orderId.
    """
    start = perf_counter()
    run_id = str(uuid.uuid4())

    cfg = load_config()
    tz = pytz.timezone(cfg.get("timezone", "America/New_York"))
    ts_local = now_local_iso(tz)

    ib = IB()
    online, reason = safe_ib_connect(ib, cfg["ibkr_host"], int(cfg["ibkr_port"]), int(cfg["client_id"]))
    if not online:
        log_run(ts_local, run_id, "error", reason, (perf_counter() - start) * 1000, False,
                cfg["ibkr_host"], cfg["ibkr_port"], cfg["client_id"])
        raise RuntimeError(f"IBKR connect failed: {reason}")

    try:
        pos_qty = get_position_qty(ib, symbol)
        if pos_qty <= 0:
            raise RuntimeError(f"No LONG position found for {symbol} in IBKR positions().")

        sell_qty = pos_qty if qty is None else int(qty)
        if sell_qty <= 0:
            raise RuntimeError("Sell qty must be > 0.")
        if sell_qty > pos_qty:
            sell_qty = pos_qty

        c = Stock(symbol, "SMART", "USD")
        ib.qualifyContracts(c)

        order = MarketOrder("SELL", int(sell_qty))
        order.tif = "DAY"
        tr = ib.placeOrder(c, order)

        # Allocate this one exit order across multiple OPEN rows (FIFO)
        trades = load_trades()
        allocated = attach_exit_fifo_multi(
            trades,
            symbol=symbol,
            total_sell_qty=sell_qty,
            exit_order_id=tr.order.orderId,
            note_prefix=f"TEST: manual SELLPOS {sell_qty}/{pos_qty} (DAY MKT) from IBKR position; awaiting fill sync."
        )

        if allocated == 0:
            # No OPEN rows to attach -> still log it so it's visible
            trades.loc[len(trades)] = [
                str(uuid.uuid4()),
                _norm_symbol(symbol),
                "", "", "", "", str(sell_qty),
                "MANUAL_EXIT_SENT",
                "",
                str(tr.order.orderId),
                f"TEST: manual SELLPOS {sell_qty}/{pos_qty} (DAY MKT) from IBKR position; no OPEN rows to attach."
            ]
        elif allocated < sell_qty:
            # Sold more than we could allocate in trades.csv (usually means trades.csv is out of sync)
            trades.loc[len(trades)] = [
                str(uuid.uuid4()),
                _norm_symbol(symbol),
                "", "", "", "", str(sell_qty - allocated),
                "MANUAL_EXIT_SENT",
                "",
                str(tr.order.orderId),
                f"TEST: SELLPOS allocated {allocated} to OPEN rows; remaining {sell_qty-allocated} not matched to trades.csv."
            ]

        save_trades(trades)

        log_order_event(ts_local, run_id, symbol, "SELL", tr.order.orderId, sell_qty, "MKT", "DAY", "SUBMITTED")

        try:
            ib.sleep(1.0)
        except Exception:
            pass

        avg_px, filled_qty = avg_fill_from_executions(ib, tr.order.orderId)
        if avg_px is not None and filled_qty > 0:
            trades = load_trades()
            updated = close_all_rows_for_exit_order(
                trades,
                exit_order_id=tr.order.orderId,
                avg_px=float(avg_px),
                fill_note=f"TEST: exit filled (avg={avg_px:.4f}, qty={filled_qty})."
            )
            if updated > 0:
                save_trades(trades)

            log_order_event(ts_local, run_id, symbol, "SELL", tr.order.orderId, filled_qty, "MKT", "DAY", "FILLED",
                            avg_fill_price=round(float(avg_px), 4), filled_qty=filled_qty, remaining_qty=0)

        log_run(ts_local, run_id, "ok", "manual sellpos complete", (perf_counter() - start) * 1000, True,
                cfg["ibkr_host"], cfg["ibkr_port"], cfg["client_id"])

    finally:
        try:
            ib.disconnect()
        except Exception:
            pass


def usage():
    print(
        "Usage:\n"
        "  py manual_test.py buy     SPY 1\n"
        "  py manual_test.py sell    SPY        (sells qty from oldest OPEN row in trades.csv)\n"
        "  py manual_test.py sell    SPY 1      (caps to oldest OPEN row qty)\n"
        "  py manual_test.py sellpos SPY        (sells ALL shares from ACTUAL IBKR position; closes multiple OPEN rows FIFO)\n"
        "  py manual_test.py sellpos SPY 11     (sells 11 shares from ACTUAL IBKR position; closes multiple OPEN rows FIFO)\n"
    )


def main():
    if len(sys.argv) < 3:
        usage()
        sys.exit(1)

    cmd = sys.argv[1].strip().lower()
    symbol = sys.argv[2].strip().upper()

    if cmd == "buy":
        qty = int(sys.argv[3]) if len(sys.argv) >= 4 else 1
        cmd_buy(symbol, qty)
        print("Manual BUY submitted (and fill sync attempted).")
        return

    if cmd == "sell":
        qty = int(sys.argv[3]) if len(sys.argv) >= 4 else None
        cmd_sell_from_trades(symbol, qty)
        print("Manual SELL submitted (from trades.csv oldest OPEN row; fill sync attempted).")
        return

    if cmd == "sellpos":
        qty = int(sys.argv[3]) if len(sys.argv) >= 4 else None
        cmd_sell_from_position(symbol, qty)
        print("Manual SELLPOS submitted (from IBKR position; multi-row closure + fill sync attempted).")
        return

    usage()
    sys.exit(1)


if __name__ == "__main__":
    main()
