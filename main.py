import os, json, time
from datetime import datetime, timedelta, timezone
from typing import Any

import gspread
from coinbase.rest import RESTClient

SHEET_NAME   = os.getenv("SHEET_NAME", "Trading Log")
LOG_TAB      = os.getenv("CRYPTO_LOG_TAB", "crypto_log")
COST_TAB     = os.getenv("CRYPTO_COST_TAB", "crypto_cost")

TARGET_GAIN_PCT = float(os.getenv("TARGET_GAIN_PCT", "5.0"))
SLEEP_SEC       = float(os.getenv("SLEEP_BETWEEN_ORDERS_SEC", "0.8"))
POLL_SEC        = float(os.getenv("POLL_INTERVAL_SEC", "0.8"))
POLL_TRIES      = int(os.getenv("POLL_MAX_TRIES", "25"))
DRY_RUN         = os.getenv("DRY_RUN", "").lower() in ("1","true","yes")

CB = RESTClient()  # reads COINBASE_API_KEY / COINBASE_API_SECRET


# ---------- utils ----------
def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def g(obj: Any, *names: str, default=None):
    """Get first present attr/key from an object or dict."""
    for n in names:
        if isinstance(obj, dict):
            if n in obj and obj[n] not in (None, ""):
                return obj[n]
        else:
            v = getattr(obj, n, None)
            if v not in (None, ""):
                return v
    return default

def get_gc():
    raw = os.getenv("GOOGLE_CREDS_JSON")
    if not raw:
        raise RuntimeError("Missing GOOGLE_CREDS_JSON")
    return gspread.service_account_from_dict(json.loads(raw))

def _ws(gc, tab):
    sh = gc.open(SHEET_NAME)
    try: return sh.worksheet(tab)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=tab, rows="2000", cols="50")

def ensure_log(ws):
    if not ws.get_all_values():
        ws.append_row(["Timestamp","Action","Product","ProceedsUSD","Qty","OrderID","Status","Note"])


# ---------- sheet cost basis ----------
def read_cost(ws):
    vals = ws.get_all_values()
    if not vals or len(vals) < 2:
        return []
    out = []
    for r in vals[1:]:
        if not any(r): 
            continue
        try:
            out.append({
                "product": r[0].strip().upper(),
                "qty": float(r[1] or 0.0),
                "dollar_cost": float(r[2] or 0.0),
                "rownum": len(out) + 2,  # sheet row number (1-indexed)
            })
        except:
            pass
    return out

def zero_cost_row(ws, rownum: int, product: str):
    ws.update(f"A{rownum}:E{rownum}", [[product, "0", "0", "0", now_iso()]])


# ---------- market data / orders ----------
def price_now(product_id: str) -> float:
    """Use last CLOSED 1-minute candle close as 'spot'."""
    end_dt = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start_dt = end_dt - timedelta(minutes=10)
    resp = CB.get_candles(
        product_id=product_id,
        start=int(start_dt.timestamp()),
        end=int(end_dt.timestamp()),
        granularity="ONE_MINUTE",
    )
    rows = g(resp, "candles") or (resp if isinstance(resp, list) else [])
    if not rows:
        raise RuntimeError("No recent candles")
    # Take the last item’s close
    last = rows[-1]
    close = g(last, "close")
    if close is None:
        raise RuntimeError("No close in last candle")
    return float(close)

def place_sell(product_id: str, base_qty: float) -> str:
    if DRY_RUN:
        return "DRYRUN"
    o = CB.market_order_sell(client_order_id="", product_id=product_id, base_size=f"{base_qty:.12f}")
    return g(o, "order_id", "id", default="")

def poll_fills_proceeds(order_id: str) -> float:
    """Return total USD proceeds for this order (best-effort)."""
    if DRY_RUN:
        return 0.0
    for _ in range(POLL_TRIES):
        try:
            f = CB.get_fills(order_id=order_id)
            fills = g(f, "fills") or (f if isinstance(f, list) else [])
            if fills:
                total = 0.0
                for x in fills:
                    qv = g(x, "quote_value", "commissionable_value")
                    if qv is not None:
                        total += float(qv)
                    else:
                        px = float(g(x, "price", default=0) or 0)
                        sz = float(g(x, "size", default=0) or 0)
                        total += px * sz
                if total > 0:
                    return total
        except Exception:
            pass
        time.sleep(POLL_SEC)
    return 0.0


# ---------- main ----------
def main():
    print("🏁 crypto-seller starting")
    gc = get_gc()
    ws_log  = _ws(gc, LOG_TAB);  ensure_log(ws_log)
    ws_cost = _ws(gc, COST_TAB)

    target = TARGET_GAIN_PCT / 100.0
    cost_rows = read_cost(ws_cost)
    if not cost_rows:
        print("ℹ️ No cost basis rows; nothing to sell.")
        return

    logs = []
    for row in cost_rows:
        pid = row["product"]; qty = row["qty"]; dollar_cost = row["dollar_cost"]
        if qty <= 0 or dollar_cost <= 0:
            continue

        try:
            px = price_now(pid)
            mkt = qty * px
            gain = (mkt - dollar_cost) / dollar_cost

            if gain >= target:
                oid = place_sell(pid, qty)
                proceeds = poll_fills_proceeds(oid)
                realized = (proceeds if proceeds > 0 else mkt) - dollar_cost
                status = "dry-run" if DRY_RUN else "submitted"
                logs.append([now_iso(), "CRYPTO-SELL", pid, f"{proceeds:.2f}" if proceeds else f"{mkt:.2f}", f"{qty:.12f}", oid, status, f"Gain {gain*100:.2f}% | Profit ${realized:.2f}"])

                if not DRY_RUN:
                    zero_cost_row(ws_cost, row["rownum"], pid)

                time.sleep(SLEEP_SEC)
            else:
                logs.append([now_iso(), "CRYPTO-SELL-SKIP", pid, f"{mkt:.2f}", f"{qty:.12f}", "", "SKIPPED", f"Gain {gain*100:.2f}% < {TARGET_GAIN_PCT:.2f}%"])
        except Exception as e:
            logs.append([now_iso(), "CRYPTO-SELL-ERROR", pid, "", f"{qty:.12f}", "", "ERROR", f"{type(e).__name__}: {e}"])

    for i in range(0, len(logs), 100):
        ws_log.append_rows(logs[i:i+100], value_input_option="USER_ENTERED")
    print("✅ crypto-seller done")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        print("❌ Fatal error:", e)
        traceback.print_exc()
