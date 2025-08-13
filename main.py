import os, json, time, random
from datetime import datetime, timedelta, timezone
from typing import Any, List
from decimal import Decimal, ROUND_DOWN, getcontext

import gspread
from coinbase.rest import RESTClient

# =========================
# Config (env or defaults)
# =========================
SHEET_NAME   = os.getenv("SHEET_NAME", "Trading Log")
LOG_TAB      = os.getenv("CRYPTO_LOG_TAB", "crypto_log")
COST_TAB     = os.getenv("CRYPTO_COST_TAB", "crypto_cost")

TARGET_GAIN_PCT = float(os.getenv("TARGET_GAIN_PCT", "5.0"))   # sell when ≥ +5% vs running cost
SLEEP_SEC       = float(os.getenv("SLEEP_BETWEEN_ORDERS_SEC", "0.8"))
POLL_SEC        = float(os.getenv("POLL_INTERVAL_SEC", "0.8"))
POLL_TRIES      = int(os.getenv("POLL_MAX_TRIES", "25"))
DRY_RUN         = os.getenv("DRY_RUN", "").lower() in ("1","true","yes")
DEBUG_PRICE     = os.getenv("DEBUG_PRICE", "").lower() in ("1","true","yes")
DEBUG_SELLER    = os.getenv("DEBUG_SELLER", "").lower() in ("1","true","yes")

CB = RESTClient()  # reads COINBASE_API_KEY / COINBASE_API_SECRET

# Decimal config
getcontext().prec = 28
getcontext().rounding = ROUND_DOWN

# Logging layout
HEADERS = ["Timestamp","Action","Product","ProceedsUSD","Qty","OrderID","Status","Note"]
TABLE_RANGE = "A1:H1"


# =========================
# Utils
# =========================
def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def g(obj: Any, *names: str, default=None):
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
    try:
        return sh.worksheet(tab)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=tab, rows="2000", cols="50")

def ensure_log(ws):
    vals = ws.get_values("A1:H1")
    if not vals or vals[0] != HEADERS:
        ws.update(range_name="A1:H1", values=[HEADERS])
    try:
        ws.freeze(rows=1)
    except Exception:
        pass

def append_logs(ws, rows: List[List[str]]):
    fixed = []
    for r in rows:
        if len(r) < 8:
            r = r + [""] * (8 - len(r))
        elif len(r) > 8:
            r = r[:8]
        fixed.append(r)
    try:
        for i in range(0, len(fixed), 100):
            ws.append_rows(
                fixed[i:i+100],
                value_input_option="RAW",
                table_range=TABLE_RANGE
            )
    except TypeError:
        start_row = len(ws.get_all_values()) + 1
        end_row = start_row + len(fixed) - 1
        ws.update(f"A{start_row}:H{end_row}", fixed, value_input_option="RAW")


# =========================
# Sheet cost basis
# =========================
def ensure_cost(ws):
    header = ["Product","Qty","DollarCost","AvgCostUSD","UpdatedAt"]
    vals = ws.get_values("A1:E1")
    if not vals or [h.strip() for h in vals[0]] != header:
        ws.update(range_name="A1:E1", values=[header])
    try:
        ws.freeze(rows=1)
    except Exception:
        pass

def _parse_num(x) -> float:
    if isinstance(x, (int, float)):
        return float(x)
    s = (x or "").strip()
    if not s:
        return 0.0
    s = s.replace(",", "").replace("$", "")
    if s.endswith("%"):
        s = s[:-1]
    try:
        return float(s)
    except Exception:
        return 0.0

def _find_col(header: list, candidates: list, default=None):
    norm = {h.strip().lower(): i for i, h in enumerate(header)}
    for name in candidates:
        i = norm.get(name.strip().lower())
        if i is not None:
            return i
    return default

def read_cost(ws) -> List[dict]:
    vals = ws.get_all_values()
    if not vals or len(vals) < 2:
        return []

    header = [h.strip() for h in vals[0]]
    i_prod = _find_col(header, ["product", "pair", "symbol", "asset", "product_id"], default=0)
    i_qty  = _find_col(header, ["qty", "quantity", "base_qty", "amount"], default=1)
    i_total = _find_col(header, ["total_cost", "cost_total", "invested", "dollar_cost", "usd_cost", "cost"])
    i_unit  = _find_col(header, ["unit_cost", "avg_cost", "avg_price", "price"])

    out = []
    for r in range(1, len(vals)):
        row = vals[r]
        if not any(row):
            continue
        try:
            product = (row[i_prod] if i_prod is not None else row[0]).strip().upper()
            qty = _parse_num(row[i_qty]) if i_qty is not None else _parse_num(row[1] if len(row) > 1 else 0.0)
            total_cost = None

            if i_total is not None and i_total < len(row) and row[i_total] not in ("", None):
                total_cost = _parse_num(row[i_total])

            if total_cost is None and i_unit is not None and i_unit < len(row) and row[i_unit] not in ("", None):
                unit_cost = _parse_num(row[i_unit])
                total_cost = qty * unit_cost

            if total_cost is None:
                total_cost = _parse_num(row[2] if len(row) > 2 else 0.0)

            if qty <= 0 or total_cost <= 0:
                continue

            out.append({
                "product": product,
                "qty": float(qty),
                "dollar_cost": float(total_cost),
                "rownum": r + 1,
            })
        except Exception:
            continue
    return out

def zero_cost_row(ws, rownum: int, product: str):
    ws.update(
        range_name=f"A{rownum}:E{rownum}",
        values=[[product, "0", "0", "0", now_iso()]]
    )


# =========================
# Market data / orders
# =========================
def last_closed_minute(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc).replace(second=0, microsecond=0) - timedelta(minutes=1)

def price_now(product_id: str) -> float:
    target_start_dt = last_closed_minute(datetime.now(timezone.utc))
    end_dt = target_start_dt + timedelta(minutes=1)
    start_dt = target_start_dt - timedelta(minutes=30)

    resp = CB.get_candles(
        product_id=product_id,
        start=int(start_dt.timestamp()),
        end=int(end_dt.timestamp()),
        granularity="ONE_MINUTE",
    )
    rows = g(resp, "candles") or (resp if isinstance(resp, list) else [])
    if not rows:
        raise RuntimeError("No recent candles")

    try:
        rows = sorted(rows, key=lambda x: int(g(x, "start", default=0)))
    except Exception:
        pass

    target_start_ts = int(target_start_dt.timestamp())
    exact = [r for r in rows if int(g(r, "start", default=-1)) == target_start_ts]
    use = exact[-1] if exact else max(
        (r for r in rows if int(g(r, "start", default=-1)) <= target_start_ts),
        key=lambda r: int(g(r, "start", default=0)),
        default=None
    )
    if not use:
        raise RuntimeError("No closed candle available")

    close = g(use, "close")
    if close is None:
        raise RuntimeError("No close on selected 1m candle")

    px = float(close)
    if DEBUG_PRICE:
        print(f"[PX] {product_id} 1m_close={px} ts={g(use, 'start')}")
    return px

def quantize_down(x: float, step: float) -> float:
    q = Decimal(str(step))
    return float(Decimal(str(x)).quantize(q))

def fetch_product_rules(pid: str):
    meta = CB.get_product(product_id=pid)
    base_inc = float(g(meta, "base_increment", "base_size_increment", "base_min_size", default=1e-8))
    quote_inc = float(g(meta, "quote_increment", "quote_size_increment", "quote_min_size", default=0.01))
    min_quote = float(g(meta, "min_market_funds", "quote_min_size", "min_funds", default=1.00))
    min_base  = float(g(meta, "min_order_size", "base_min_size", default=1e-8))
    return base_inc, quote_inc, min_quote, min_base

def place_sell(product_id: str, base_qty: float) -> str:
    if DRY_RUN:
        return "DRYRUN"
    base_inc, _, _, min_base = fetch_product_rules(product_id)
    qty = max(min_base, quantize_down(base_qty, base_inc))
    client_order_id = f"sell-{product_id}-{int(time.time()*1000)}"
    o = CB.market_order_sell(
        client_order_id=client_order_id,
        product_id=product_id,
        base_size=f"{qty:.12f}",
    )
    return g(o, "order_id", "id", default=client_order_id)

def _fees_sum(fills: list) -> float:
    total_fee = 0.0
    for x in fills:
        fx = g(x, "fee", "fees", "commission", default=0.0)
        try: total_fee += float(fx)
        except: pass
    return total_fee

def poll_fills_proceeds(order_id: str) -> float:
    if DRY_RUN:
        return 0.0
    for _ in range(POLL_TRIES):
        try:
            f = CB.get_fills(order_id=order_id)
            fills = g(f, "fills") or (f if isinstance(f, list) else [])
            if fills:
                gross = 0.0
                for x in fills:
                    qv = g(x, "quote_value", "commissionable_value")
                    if qv is not None: gross += float(qv)
                    else:
                        px = float(g(x, "price", default=0) or 0)
                        sz = float(g(x, "size", default=0) or 0)
                        gross += px * sz
                fee = _fees_sum(fills)
                net = max(0.0, gross - fee)
                if net > 0:
                    return net
        except Exception:
            pass
        time.sleep(POLL_SEC)
    return 0.0


# =========================
# Main
# =========================
def debug_dump_cost(ws):
    vals = ws.get_all_values()
    print(f"[DEBUG] {COST_TAB} raw rows (excluding header): {max(0, len(vals)-1)}")
    if vals:
        print(f"[DEBUG] header: {vals[0]}")
    for r in vals[1:6]:
        print(f"[DEBUG] row: {r}")

def main():
    print("🏁 crypto-seller starting")
    gc = get_gc()
    ws_log  = _ws(gc, LOG_TAB);  ensure_log(ws_log)
    ws_cost = _ws(gc, COST_TAB); ensure_cost(ws_cost)

    if DEBUG_SELLER:
        debug_dump_cost(ws_cost)

    target = TARGET_GAIN_PCT / 100.0
    cost_rows = read_cost(ws_cost)

    if DEBUG_SELLER:
        print(f"[DEBUG] parsed {len(cost_rows)} positions from {COST_TAB}")
        for x in cost_rows[:10]:
            print(f"[DEBUG] {x['product']} qty={x['qty']} dollar_cost=${x['dollar_cost']:.2f}")

    if not cost_rows:
        print("ℹ️ No cost basis rows; nothing to sell.")
        return

    logs = []
    for row in cost_rows:
        pid = row["product"]
        qty = row["qty"]
        dollar_cost = row["dollar_cost"]
        if qty <= 0 or dollar_cost <= 0:
            continue

        try:
            px = price_now(pid)
            mkt_val = qty * px
            gain = (mkt_val - dollar_cost) / dollar_cost

            # Check Coinbase min quote funds to avoid dust sells
            _, _, min_quote, _ = fetch_product_rules(pid)
            if mkt_val < min_quote:
                logs.append([now_iso(), "CRYPTO-SELL-SKIP", pid,
                             f"{mkt_val:.2f}", f"{qty:.8f}", "", "SKIPPED",
                             f"Below min quote ${min_quote:.2f} | Spot ${px:.6f}"])
                if DEBUG_SELLER:
                    print(f"[SKIP] {pid}: mkt ${mkt_val:.2f} < min_quote ${min_quote:.2f}")
                continue

            if DEBUG_SELLER:
                print(f"[EVAL] {pid}: qty={qty:.8f} cost=${dollar_cost:.2f} spot=${px:.6f} gain={gain*100:.2f}%")

            if gain >= target:
                oid = place_sell(pid, qty)
                proceeds = poll_fills_proceeds(oid)
                proceeds_used = proceeds if proceeds > 0 else mkt_val
                realized = proceeds_used - dollar_cost
                status = "dry-run" if DRY_RUN else "submitted"

                logs.append([
                    now_iso(), "CRYPTO-SELL", pid,
                    f"{proceeds_used:.2f}",
                    f"{qty:.8f}",
                    oid, status,
                    f"Spot ${px:.6f} | MktVal ${mkt_val:.2f} | Gain {gain*100:.2f}% | Profit ${realized:.2f}"
                ])

                if not DRY_RUN:
                    zero_cost_row(ws_cost, row["rownum"], pid)

                time.sleep(SLEEP_SEC * (0.8 + 0.4 * random.random()))
            else:
                logs.append([
                    now_iso(), "CRYPTO-SELL-SKIP", pid,
                    f"{mkt_val:.2f}", f"{qty:.8f}", "", "SKIPPED",
                    f"Spot ${px:.6f} | Gain {gain*100:.2f}% < {TARGET_GAIN_PCT:.2f}%"
                ])
        except Exception as e:
            logs.append([now_iso(), "CRYPTO-SELL-ERROR", pid, "", f"{qty:.8f}", "", "ERROR", f"{type(e).__name__}: {e}"])

    if logs:
        append_logs(ws_log, logs)
    print("✅ crypto-seller done")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        print("❌ Fatal error:", e)
        traceback.print_exc()
