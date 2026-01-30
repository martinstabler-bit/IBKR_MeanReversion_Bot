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
from datetime import datetime, timedelta, date
from time import perf_counter
import uuid
from pathlib import Path

import pandas as pd
import pytz
import holidays
import yaml

from ib_insync import IB, Stock, MarketOrder, util


# =========================
# Paths
# =========================
BOT_ROOT = Path(__file__).resolve().parent
ANALYTICS_DIR = BOT_ROOT / "analytics"
LOGS_DIR = ANALYTICS_DIR / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

RUNS_CSV = LOGS_DIR / "runs.csv"
ORDERS_CSV = LOGS_DIR / "orders.csv"
ACCOUNT_CSV = LOGS_DIR / "account.csv"

TRADES_CSV = BOT_ROOT / "trades.csv"
ACTIONS_CSV = BOT_ROOT / "actions_queue.csv"

CACHE_DIR = BOT_ROOT / "bar_cache"
CACHE_DIR.mkdir(exist_ok=True)

RUN_LOG_TXT = BOT_ROOT / "run_log.txt"


# =========================
# CSV append helper
# =========================
def append_row_csv(path: Path, row: dict, columns: list[str]) -> None:
    df = pd.DataFrame([{c: row.get(c, "") for c in columns}])
    if path.exists():
        df.to_csv(path, mode="a", index=False, header=False, lineterminator="\n")
    else:
        df.to_csv(path, index=False, lineterminator="\n")


def now_local_iso(tz) -> str:
    return datetime.now(tz).isoformat(timespec="seconds")


# =========================
# Basic logging
# =========================
with open(RUN_LOG_TXT, "a", encoding="utf-8") as f:
    f.write(f"Ran at {datetime.now()}\n")


# =========================
# Trading day helpers
# =========================
def is_trading_day(d: date, us_holidays) -> bool:
    return d.weekday() < 5 and d not in us_holidays


def add_trading_days(d: date, n: int, us_holidays) -> date:
    cur = d
    added = 0
    while added < n:
        cur += timedelta(days=1)
        if is_trading_day(cur, us_holidays):
            added += 1
    return cur


# =========================
# Config + storage
# =========================
def load_config():
    with open(BOT_ROOT / "config.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def save_config(cfg):
    with open(BOT_ROOT / "config.yaml", "w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f, sort_keys=False)


def load_trades():
    cols = [
        "trade_id", "symbol", "entry_date", "exit_date",
        "entry_price", "exit_price", "quantity", "status",
        "entry_orderId", "exit_orderId", "note"
    ]
    if TRADES_CSV.exists():
        df = pd.read_csv(TRADES_CSV, dtype=str, keep_default_na=False)

        # ---- SANITIZE CORRUPT/EXCEL-SAVED FILES ----
        # Drop any duplicated header rows that got written into the file body
        if "trade_id" in df.columns:
            df = df[df["trade_id"].astype(str).str.strip().str.lower() != "trade_id"]

        # Drop fully blank rows
        df = df.dropna(how="all")
        df = df[(df.astype(str).apply(lambda r: "".join(r).strip(), axis=1) != "")]

        # Ensure required columns exist
        for c in cols:
            if c not in df.columns:
                df[c] = ""

        # Keep only expected cols and drop exact duplicates
        df = df[cols].drop_duplicates().reset_index(drop=True)
        return df

    return pd.DataFrame(columns=cols)


def save_trades(df: pd.DataFrame):
    df.to_csv(TRADES_CSV, index=False, lineterminator="\n")


def append_action(action: dict):
    df = pd.DataFrame([action])
    if ACTIONS_CSV.exists():
        df.to_csv(ACTIONS_CSV, mode="a", index=False, header=False, lineterminator="\n")
    else:
        df.to_csv(ACTIONS_CSV, index=False, lineterminator="\n")


# =========================
# Signal logic
# =========================
def compute_signal(df, threshold):
    """
    threshold is negative (e.g., -0.025 means -2.5%).
    We qualify if r2 <= threshold or r3 <= threshold (plus red-day requirements).
    """
    df = df.copy()
    if "close" not in df.columns or len(df) < 4:
        return False, 0.0

    df["ret"] = df["close"].pct_change()
    df["r2"] = df["close"] / df["close"].shift(2) - 1
    df["r3"] = df["close"] / df["close"].shift(3) - 1

    last = df.iloc[-1]
    red2 = (df.iloc[-2:]["ret"] < 0).sum()
    red3 = (df.iloc[-3:]["ret"] < 0).sum()

    candidates = []
    if pd.notna(last["r2"]) and float(last["r2"]) <= threshold and red2 == 2:
        candidates.append(float(last["r2"]))
    if pd.notna(last["r3"]) and float(last["r3"]) <= threshold and red3 >= 2:
        candidates.append(float(last["r3"]))

    if not candidates:
        return False, 0.0

    return True, min(candidates)


# =========================
# IBKR connect + cached bars
# =========================
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


def get_bars_df(ib: IB, sym: str, online: bool) -> pd.DataFrame:
    cache_path = CACHE_DIR / f"{sym}.csv"

    if online:
        try:
            c = Stock(sym, "SMART", "USD")
            ib.qualifyContracts(c)
            bars = ib.reqHistoricalData(
                c,
                endDateTime="",
                durationStr="60 D",
                barSizeSetting="1 day",
                whatToShow="TRADES",
                useRTH=True
            )
            df = util.df(bars)
            if df is not None and not df.empty:
                keep = [col for col in ["date", "open", "high", "low", "close", "volume"] if col in df.columns]
                df = df[keep].copy()
                df.to_csv(cache_path, index=False, lineterminator="\n")
                return df
        except Exception:
            pass

    if cache_path.exists():
        try:
            df = pd.read_csv(cache_path)
            if df is not None and not df.empty and "close" in df.columns:
                return df
        except Exception:
            pass

    return pd.DataFrame()


# =========================
# orders.csv logging (schema expected by app.py)
# =========================
ORDERS_COLS = [
    "ts_local", "run_id", "symbol", "action", "order_id", "qty", "order_type", "tif",
    "status", "submitted_price", "avg_fill_price", "filled_qty", "remaining_qty",
    "error_code", "error_msg"
]


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
    submitted_price="",
    avg_fill_price="",
    filled_qty="",
    remaining_qty="",
    error_code="",
    error_msg=""
):
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
            "submitted_price": submitted_price,
            "avg_fill_price": avg_fill_price,
            "filled_qty": filled_qty,
            "remaining_qty": remaining_qty,
            "error_code": error_code,
            "error_msg": error_msg,
        },
        ORDERS_COLS
    )


# =========================
# account.csv logging (IBKR snapshots)
# =========================
ACCOUNT_COLS = [
    "ts_local", "run_id", "account",
    "NetLiquidation", "TotalCashValue", "GrossPositionValue",
    "UnrealizedPnL", "RealizedPnL",
    "AvailableFunds", "BuyingPower", "EquityWithLoanValue"
]


def snapshot_account(ib: IB) -> dict:
    out = {
        "account": "",
        "NetLiquidation": "",
        "TotalCashValue": "",
        "GrossPositionValue": "",
        "UnrealizedPnL": "",
        "RealizedPnL": "",
        "AvailableFunds": "",
        "BuyingPower": "",
        "EquityWithLoanValue": "",
    }
    try:
        summ = ib.accountSummary()
    except Exception:
        return out

    last_any = {}
    last_usd = {}

    for av in summ:
        tag = getattr(av, "tag", "")
        val = getattr(av, "value", "")
        ccy = getattr(av, "currency", "")
        acct = getattr(av, "account", "")

        if acct and not out["account"]:
            out["account"] = acct

        if tag in out:
            last_any[tag] = val
            if str(ccy).upper() == "USD":
                last_usd[tag] = val

    for k in out.keys():
        if k == "account":
            continue
        if k in last_usd:
            out[k] = last_usd[k]
        elif k in last_any:
            out[k] = last_any[k]

    return out


def log_account(ts_local: str, run_id: str, snap: dict):
    row = {c: "" for c in ACCOUNT_COLS}
    row["ts_local"] = ts_local
    row["run_id"] = run_id
    for k in snap:
        if k in row:
            row[k] = snap[k]
    append_row_csv(ACCOUNT_CSV, row, ACCOUNT_COLS)


# =========================
# Fill sync (updates trades + logs fills)
# =========================
def _weighted_avg_price_and_qty(fills):
    total_qty = 0.0
    w_sum = 0.0
    for fl in fills:
        sh = abs(float(fl.execution.shares))
        px = float(fl.execution.price)
        total_qty += sh
        w_sum += sh * px
    if total_qty <= 0:
        return 0.0, 0
    return (w_sum / total_qty), int(round(total_qty))


def sync_fills(ib: IB, trades: pd.DataFrame, tz, us_holidays, hold_days: int, run_id: str) -> tuple[pd.DataFrame, int]:
    try:
        exec_fills = ib.reqExecutions()
    except Exception:
        return trades, 0

    if not exec_fills:
        return trades, 0

    fills_by_order = {}
    for fl in exec_fills:
        oid = getattr(fl.execution, "orderId", None)
        if oid is None:
            continue
        fills_by_order.setdefault(int(oid), []).append(fl)

    fills_applied = 0
    ts_local = now_local_iso(tz)

    trades["entry_orderId"] = trades["entry_orderId"].astype(str)
    trades["exit_orderId"] = trades["exit_orderId"].astype(str)
    trades["status"] = trades["status"].astype(str)

    for i, row in trades.iterrows():
        status = str(row.get("status", "")).strip().upper()
        sym = str(row.get("symbol", "")).strip()

        if status == "ENTRY_SENT":
            oid_raw = row.get("entry_orderId", "")
            if not str(oid_raw).strip():
                continue
            try:
                oid = int(float(oid_raw))
            except Exception:
                continue

            fills = fills_by_order.get(oid)
            if not fills:
                continue

            avg_px, qty = _weighted_avg_price_and_qty(fills)
            if qty <= 0:
                continue

            trades.at[i, "entry_price"] = round(avg_px, 4)
            trades.at[i, "quantity"] = int(qty)
            trades.at[i, "status"] = "OPEN"
            trades.at[i, "note"] = "Entry filled (synced)."

            fill_dt = fills[-1].time
            if fill_dt is not None:
                entry_d = fill_dt.astimezone(tz).date()
                trades.at[i, "entry_date"] = pd.Timestamp(entry_d)
                trades.at[i, "exit_date"] = pd.Timestamp(add_trading_days(entry_d, hold_days, us_holidays))

            log_order_event(
                ts_local=ts_local, run_id=run_id, symbol=sym,
                action="BUY", order_id=oid, qty=qty, order_type="MKT", tif="DAY",
                status="FILLED", avg_fill_price=round(avg_px, 4), filled_qty=qty, remaining_qty=0
            )
            fills_applied += 1

        if status == "EXIT_SENT":
            oid_raw = row.get("exit_orderId", "")
            if not str(oid_raw).strip():
                continue
            try:
                oid = int(float(oid_raw))
            except Exception:
                continue

            fills = fills_by_order.get(oid)
            if not fills:
                continue

            avg_px, qty = _weighted_avg_price_and_qty(fills)
            if qty <= 0:
                continue

            trades.at[i, "exit_price"] = round(avg_px, 4)
            trades.at[i, "status"] = "CLOSED"
            trades.at[i, "note"] = "Exit filled (synced)."

            log_order_event(
                ts_local=ts_local, run_id=run_id, symbol=sym,
                action="SELL", order_id=oid, qty=qty, order_type="MKT", tif="DAY",
                status="FILLED", avg_fill_price=round(avg_px, 4), filled_qty=qty, remaining_qty=0
            )
            fills_applied += 1

    return trades, fills_applied


# =========================
# runs.csv logging
# =========================
RUNS_COLS = [
    "ts_local", "run_id", "status", "reason", "duration_ms",
    "tws_connected", "ib_host", "ib_port", "client_id"
]


def log_run(ts_local, run_id, status, reason, duration_ms, tws_connected, ib_host, ib_port, client_id):
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


# =========================
# Main
# =========================
def main():
    start = perf_counter()
    run_id = str(uuid.uuid4())
    cfg = load_config()

    SYMBOLS = cfg.get("symbols", [])
    TRADE_SIZE_USD = float(cfg.get("trade_size_usd", 1000))
    MAX_R = int(cfg.get("max_R", 8))
    HOLD_DAYS = int(cfg.get("hold_days", 3))

    raw_threshold_pct = float(cfg.get("threshold_pct", 2.25))
    THRESHOLD = -abs(raw_threshold_pct) / 100.0

    tz_name = cfg.get("timezone", "America/New_York")
    try:
        tz = pytz.timezone(tz_name)
    except Exception:
        tz = pytz.timezone("America/New_York")

    us_holidays = holidays.US()
    ts_local = now_local_iso(tz)

    ib = IB()
    online, connect_reason = safe_ib_connect(
        ib,
        cfg["ibkr_host"],
        int(cfg["ibkr_port"]),
        int(cfg["client_id"])
    )
    print(f"IBKR ONLINE: {online}")

    if online:
        try:
            snap = snapshot_account(ib)
            log_account(ts_local=ts_local, run_id=run_id, snap=snap)
        except Exception:
            pass

    trades = load_trades()

    for col in ["entry_date", "exit_date"]:
        if col in trades.columns and len(trades) > 0:
            trades[col] = pd.to_datetime(trades[col], errors="coerce")

    fills_applied = 0
    if online and len(trades) > 0:
        trades, fills_applied = sync_fills(ib, trades, tz, us_holidays, HOLD_DAYS, run_id)

    exits_sent = 0
    entries_attempted = 0
    signals_found = 0

    # Exit due OPEN trades
    today_d = datetime.now(tz).date()
    today_ts = pd.Timestamp(today_d)

    if not trades.empty:
        exit_dt = pd.to_datetime(trades["exit_date"], errors="coerce")
        due = trades[
            (trades["status"].astype(str).str.upper() == "OPEN")
            & (exit_dt.notna())
            & (exit_dt <= today_ts)
        ].copy()
    else:
        due = pd.DataFrame()

    for idx, row in due.iterrows():
        sym = str(row["symbol"]).strip()
        qty_raw = row.get("quantity", "")
        try:
            qty = int(float(qty_raw)) if str(qty_raw).strip() else 0
        except Exception:
            qty = 0

        if qty <= 0:
            trades.at[idx, "note"] = "Exit due but missing qty."
            continue

        if not online:
            trades.at[idx, "status"] = "EXIT_DUE_OFFLINE"
            trades.at[idx, "note"] = "Exit queued (offline)."
            append_action({
                "ts_local": ts_local,
                "run_id": run_id,
                "type": "EXIT",
                "symbol": sym,
                "qty": qty,
                "tif": "OPG",
                "reason": "IBKR offline"
            })
            log_order_event(
                ts_local=ts_local, run_id=run_id, symbol=sym, action="SELL",
                order_id="", qty=qty, order_type="MKT", tif="OPG",
                status="QUEUED", error_code="OFFLINE", error_msg="IBKR offline"
            )
            continue

        try:
            c = Stock(sym, "SMART", "USD")
            ib.qualifyContracts(c)
            order = MarketOrder("SELL", qty)
            order.tif = "OPG"
            tr = ib.placeOrder(c, order)

            exits_sent += 1
            trades.at[idx, "status"] = "EXIT_SENT"
            trades.at[idx, "exit_orderId"] = tr.order.orderId
            trades.at[idx, "note"] = "Exit order sent (OPG); awaiting fill sync."

            log_order_event(
                ts_local=ts_local, run_id=run_id, symbol=sym, action="SELL",
                order_id=tr.order.orderId, qty=qty, order_type="MKT", tif="OPG",
                status="SUBMITTED"
            )
        except Exception as e:
            trades.at[idx, "status"] = "EXIT_ERROR"
            trades.at[idx, "note"] = f"Exit error: {e}"
            log_order_event(
                ts_local=ts_local, run_id=run_id, symbol=sym, action="SELL",
                order_id="", qty=qty, order_type="MKT", tif="OPG",
                status="ERROR", error_code=type(e).__name__, error_msg=str(e)
            )

    # Find new signals
    open_like = trades[trades["status"].astype(str).str.upper().isin(["OPEN", "ENTRY_SENT", "EXIT_SENT"])]
    available_R = MAX_R - len(open_like)

    signals = []
    if available_R > 0:
        active_syms = set(open_like["symbol"].astype(str))
        for sym in SYMBOLS:
            sym = str(sym).strip()
            if sym in active_syms:
                continue
            df = get_bars_df(ib, sym, online)
            if df.empty:
                continue
            qualifies, overshoot = compute_signal(df, THRESHOLD)
            if qualifies:
                signals.append((sym, overshoot))

    signals.sort(key=lambda x: x[1])
    final_entries = signals[:max(0, available_R)]
    signals_found = len(final_entries)

    # Place entries
    for sym, overshoot in final_entries:
        sym = str(sym).strip()
        entries_attempted += 1

        if not online:
            append_action({
                "ts_local": ts_local,
                "run_id": run_id,
                "type": "ENTRY",
                "symbol": sym,
                "cashQty": TRADE_SIZE_USD,
                "tif": "OPG",
                "reason": "IBKR offline"
            })
            trades.loc[len(trades)] = [
                str(uuid.uuid4()), sym, "", "", "", "", "", "QUEUED_ENTRY", "", "", "Queued (offline)."
            ]
            log_order_event(
                ts_local=ts_local, run_id=run_id, symbol=sym, action="BUY",
                order_id="", qty=TRADE_SIZE_USD, order_type="MKT", tif="OPG",
                status="QUEUED", error_code="OFFLINE", error_msg="IBKR offline"
            )
            continue

        try:
            c = Stock(sym, "SMART", "USD")
            ib.qualifyContracts(c)

            order = MarketOrder("BUY", 0)
            order.cashQty = TRADE_SIZE_USD
            order.tif = "OPG"
            tr = ib.placeOrder(c, order)

            trades.loc[len(trades)] = [
                str(uuid.uuid4()),
                sym,
                "", "", "", "", "",
                "ENTRY_SENT",
                tr.order.orderId,
                "",
                f"Entry sent OPG. overshoot={overshoot:.4f}"
            ]

            log_order_event(
                ts_local=ts_local, run_id=run_id, symbol=sym, action="BUY",
                order_id=tr.order.orderId, qty=TRADE_SIZE_USD, order_type="MKT", tif="OPG",
                status="SUBMITTED"
            )
        except Exception as e:
            trades.loc[len(trades)] = [
                str(uuid.uuid4()), sym, "", "", "", "", "", "ENTRY_ERROR", "", "", f"Entry error: {e}"
            ]
            log_order_event(
                ts_local=ts_local, run_id=run_id, symbol=sym, action="BUY",
                order_id="", qty=TRADE_SIZE_USD, order_type="MKT", tif="OPG",
                status="ERROR", error_code=type(e).__name__, error_msg=str(e)
            )

    # Final sync fills
    if online and len(trades) > 0:
        try:
            ib.sleep(1.0)
        except Exception:
            pass
        trades, extra = sync_fills(ib, trades, tz, us_holidays, HOLD_DAYS, run_id)
        fills_applied += extra

    save_trades(trades)

    if online:
        try:
            ib.disconnect()
        except Exception:
            pass

    duration_ms = (perf_counter() - start) * 1000.0
    status = "ok"
    reason = "run complete" if online else f"offline_mode: {connect_reason}"

    log_run(
        ts_local=ts_local,
        run_id=run_id,
        status=status,
        reason=reason,
        duration_ms=duration_ms,
        tws_connected=online,
        ib_host=cfg["ibkr_host"],
        ib_port=int(cfg["ibkr_port"]),
        client_id=int(cfg["client_id"])
    )

    print("Run complete.")
    print(
        f"ONLINE={online} | signals_found={signals_found} | entries_attempted={entries_attempted} | "
        f"exits_sent={exits_sent} | fills_applied={fills_applied} | cap={MAX_R} | threshold={THRESHOLD:.4f}"
    )


if __name__ == "__main__":
    main()
