import os, json, time, random, math
from datetime import datetime, timedelta, timezone
from typing import Any, List, Tuple, Optional
from decimal import Decimal, ROUND_DOWN, getcontext

import gspread
from coinbase.rest import RESTClient

# ---------- Config ----------
SHEET_NAME        = os.getenv("SHEET_NAME", "Trading Log")
LOG_TAB           = os.getenv("CRYPTO_LOG_TAB", "crypto_log")

TARGET_GAIN_PCT   = float(os.getenv("TARGET_GAIN_PCT", "5.0"))   # sell when ≥ +5%
MIN_POSITION_USD  = float(os.getenv("MIN_POSITION_USD", "1.00"))
POLL_SEC          = float(os.getenv("POLL_INTERVAL_SEC", "0.8"))
POLL_TRIES        = int(os.getenv("POLL_MAX_TRIES", "25"))
SLEEP_SEC         = float(os.getenv("SLEEP_BETWEEN_ORDERS_SEC", "0.8"))

COST_METHOD       = os.getenv("COST_METHOD", "FIFO").strip().upper()    # FIFO or LIFO
FILLS_PAGE_LIMIT  = int(os.getenv("FILLS_PAGE_LIMIT", "200"))
DRY_RUN           = os.getenv("DRY_RUN", "").lower() in ("1","true","yes")

DEBUG             = os.getenv("DEBUG_SELLER", "").lower() in ("1","true","yes")
DEBUG_PRICE       = os.getenv("DEBUG_PRICE", "").lower() in ("1","true","yes")

CB = RESTClient()

getcontext().prec = 28
getcontext().rounding = ROUND_DOWN

# log headers (match finder)
LOG_HEADERS = ["Timestamp","Action","Product","ProceedsUSD","Qty","OrderID","Status","Note"]
TABLE_RANGE = "A1:H1"

# ---------- Sheet logging helpers ----------
def get_gc():
    raw = os.getenv("GOOGLE_CREDS_JSON", "")
    if not raw: return None
    try: return gspread.service_account_from_dict(json.loads(raw))
    except Exception: return None

def _ws(gc, tab: str):
    sh = gc.open(SHEET_NAME)
    try: return sh.worksheet(tab)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=tab, rows="2000", cols="50")
        ws.update(range_name="A1:H1", values=[LOG_HEADERS])  # self-heal
        try: ws.freeze(rows=1)
        except Exception: pass
        return ws

def ensure_log(ws):
    vals = ws.get_values("A1:H1")
    if not vals or vals[0] != LOG_HEADERS:
        ws.update(range_name="A1:H1", values=[LOG_HEADERS])
    try: ws.freeze(rows=1)
    except Exception: pass

def append_logs(ws, rows: List[List[str]]):
    if not ws or not rows: return
    fixed = []
    for r in rows:
        if len(r) < 8:   r += [""] * (8 - len(r))
        elif len(r) > 8: r = r[:8]
        fixed.append(r)
    try:
        for i in range(0, len(fixed), 100):
            ws.append_rows(fixed[i:i+100], value_input_option="RAW", table_range=TABLE_RANGE)
    except TypeError:
        start = len(ws.get_all_values()) + 1
        ws.update(f"A{start}:H{start+len(fixed)-1}", fixed, value_input_option="RAW")

# ---------- Small utils ----------
def now_iso() -> str: return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

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

def norm_ccy(c) -> str:
    if c is None: return ""
    if isinstance(c, str): return c.upper()
    return (g(c, "code", "currency", "symbol", "asset", "base", default="") or "").upper()

def norm_amount(x) -> float:
    if x is None: return 0.0
    if isinstance(x, (int, float, str)):
        try: return float(x)
        except: return 0.0
    return float(g(x, "value", "amount", default=0.0) or 0.0)

def _parse_num(x) -> float:
    if isinstance(x, (int, float)): return float(x)
    s = (x or "").strip().replace(",", "").replace("$", "")
    if not s: return 0.0
    if s.endswith("%"): s = s[:-1]
    try: return float(s)
    except: return 0.0

# ---------- Coinbase helpers ----------
def quantize_down(x: float, step: float) -> float:
    q = Decimal(str(step))
    return float(Decimal(str(x)).quantize(q))

def fetch_product_rules(pid: str) -> tuple[float,float,float,float]:
    meta = CB.get_product(product_id=pid)
    base_inc = float(g(meta, "base_increment", "base_size_increment", "base_min_size", default=1e-8))
    quote_inc = float(g(meta, "quote_increment", "quote_size_increment", "quote_min_size", default=0.01))
    min_quote = float(g(meta, "min_market_funds", "quote_min_size", "min_funds", default=1.00))
    min_base  = float(g(meta, "min_order_size", "base_min_size", default=1e-8))
    return base_inc, quote_inc, min_quote, min_base

def last_closed_minute(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc).replace(second=0, microsecond=0) - timedelta(minutes=1)

def price_now(product_id: str) -> float:
    target_start_dt = last_closed_minute(datetime.now(timezone.utc))
    end_dt   = target_start_dt + timedelta(minutes=1)
    start_dt = target_start_dt - timedelta(minutes=30)
    resp = CB.get_candles(product_id=product_id,
                          start=int(start_dt.timestamp()),
                          end=int(end_dt.timestamp()),
                          granularity="ONE_MINUTE")
    rows = g(resp, "candles") or (resp if isinstance(resp, list) else [])
    if not rows: raise RuntimeError("No recent candles")
    try: rows = sorted(rows, key=lambda x: int(g(x, "start", default=0)))
    except Exception: pass
    ts = int(target_start_dt.timestamp())
    exact = [r for r in rows if int(g(r, "start", default=-1)) == ts]
    use = exact[-1] if exact else max((r for r in rows if int(g(r, "start", default=-1)) <= ts),
                                      key=lambda r: int(g(r, "start", default=0)), default=None)
    if not use: raise RuntimeError("No closed candle")
    px = float(g(use, "close"))
    if DEBUG_PRICE: print(f"[PX] {product_id} close={px} ts={g(use,'start')}")
    return px

def list_holdings_from_accounts() -> List[dict]:
    acc = CB.get_accounts()
    accounts = g(acc, "accounts") or (acc if isinstance(acc, list) else [])
    out = []
    for a in accounts:
        asset = norm_ccy(g(a, "currency", "currency_symbol", "asset", "currency_code"))
        if asset in ("", "USD"): continue
        avail = norm_amount(g(a, "available_balance", "available", "balance", "available_balance_value"))
        if avail <= 0: continue
        out.append({"asset": asset, "product": f"{asset}-USD", "available": avail})
    # combine duplicates
    byp = {}
    for x in out:
        p = x["product"]; byp[p] = byp.get(p, 0.0) + x["available"]
    return [{"asset": p.split("-")[0], "product": p, "available": q} for p, q in byp.items()]

def _ts_from_fill(f) -> float:
    t = g(f, "trade_time", "time", "created_time", "timestamp", "ts")
    if isinstance(t, (int, float)): return float(t)
    s = str(t or "")
    if not s: return 0.0
    try:
        if s.endswith("Z"): s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s).timestamp()
    except Exception:
        return 0.0

def _fill_side(f) -> str:
    s = (g(f, "side", "order_side", default="") or "").upper()
    if s in ("BUY","SELL"): return s
    return "BUY" if "buy" in str(s).lower() else "SELL"

def _fill_size(f) -> float: return float(g(f, "size", "filled_quantity", default=0) or 0)
def _fill_quote_value(f) -> float:
    qv = g(f, "quote_value", "commissionable_value")
    if qv is not None:
        try: return float(qv)
        except: pass
    px = float(g(f, "price", default=0) or 0)
    return px * _fill_size(f)
def _fill_fee(f) -> float:
    fx = g(f, "fee", "fees", "commission", default=0.0)
    try: return float(fx)
    except: return 0.0

def fetch_all_fills_for_product(pid: str, page_limit: int = FILLS_PAGE_LIMIT) -> List[dict]:
    fills, cursor, pages = [], None, 0
    while True:
        resp = CB.get_fills(product_id=pid, limit=250, cursor=cursor) if cursor else CB.get_fills(product_id=pid, limit=250)
        items = g(resp, "fills") or (resp if isinstance(resp, list) else [])
        for f in items:
            fills.append({"side": _fill_side(f), "size": _fill_size(f),
                          "quote": _fill_quote_value(f), "fee": _fill_fee(f), "ts": _ts_from_fill(f)})
        cursor = g(resp, "cursor"); pages += 1
        if not cursor or pages >= page_limit: break
    fills.sort(key=lambda x: x["ts"])
    return fills

def reconstruct_avg_cost(pid: str, current_qty: float, method: str = COST_METHOD) -> Tuple[float, float]:
    if current_qty <= 0: return 0.0, 0.0
    fills = fetch_all_fills_for_product(pid)
    if not fills: return 0.0, 0.0

    lots: List[List[float]] = []
    def push_buy(qty: float, usd: float):
        if qty > 0 and usd > 0: lots.append([qty, usd])
    def consume_sell(qty: float):
        remain = qty
        while remain > 1e-18 and lots:
            idx = -1 if method == "LIFO" else 0
            lot_qty, lot_cost = lots[idx]
            take = min(lot_qty, remain)
            cost_take = lot_cost * (take / lot_qty)
            lot_qty -= take; lot_cost -= cost_take; remain -= take
            if lot_qty <= 1e-18: lots.pop(idx)
            else: lots[idx] = [lot_qty, lot_cost]

    for f in fills:
        if f["size"] <= 0: continue
        if f["side"] == "BUY": push_buy(f["size"], f["quote"] + f["fee"])  # fee added to cost
        else: consume_sell(f["size"])

    inv_qty = sum(q for q, _ in lots)
    if inv_qty <= 1e-12: return 0.0, 0.0

    needed, total_cost = current_qty, 0.0
    while needed > 1e-18 and lots:
        idx = 0 if method == "FIFO" else len(lots) - 1
        lot_qty, lot_cost = lots[idx]
        take = min(lot_qty, needed)
        total_cost += lot_cost * (take / lot_qty)
        lot_qty -= take; needed -= take
        if lot_qty <= 1e-18: lots.pop(idx)
        else: lots[idx] = [lot_qty, lot_cost * (lot_qty / (lot_qty + take))]
    if current_qty - needed <= 1e-12: return 0.0, 0.0
    avg = total_cost / (current_qty - needed) if (current_qty - needed) > 0 else 0.0
    return avg, total_cost

def place_sell(product_id: str, base_qty: float) -> str:
    if DRY_RUN: return "DRYRUN"
    base_inc, _, _, min_base = fetch_product_rules(product_id)
    qty = max(min_base, quantize_down(base_qty, base_inc))
    client_order_id = f"sell-{product_id}-{int(time.time()*1000)}"
    o = CB.market_order_sell(client_order_id=client_order_id, product_id=product_id, base_size=f"{qty:.12f}")
    return g(o, "order_id", "id", default=client_order_id)

def poll_fills_proceeds(order_id: str) -> float:
    if DRY_RUN: return 0.0
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
                        px = float(g(x, "price", default=0) or 0); sz = float(g(x, "size", default=0) or 0)
                        gross += px * sz
                fee = sum(float(g(x, "fee", "fees", "commission", default=0) or 0) for x in fills)
                net = max(0.0, gross - fee)
                if net > 0: return net
        except Exception:
            pass
        time.sleep(POLL_SEC)
    return 0.0

# ---------- Main ----------
def main():
    print("🏁 crypto-seller (portfolio-native) starting")

    gc = get_gc()
    ws_log = _ws(gc, LOG_TAB) if gc else None
    if ws_log: ensure_log(ws_log)

    target = TARGET_GAIN_PCT / 100.0
    logs: List[List[str]] = []

    holdings = list_holdings_from_accounts()
    if DEBUG: print(f"[DEBUG] holdings: {holdings}")
    if not holdings:
        print("ℹ️ No non-USD holdings with available balance.")
        return

    for pos in holdings:
        pid = pos["product"]; qty = float(pos["available"])
        if qty <= 0: continue
        try:
            px = price_now(pid)
            mkt_val = qty * px
            if mkt_val < MIN_POSITION_USD:
                note = f"Below MIN_POSITION_USD ${MIN_POSITION_USD:.2f}"
                logs.append([now_iso(),"CRYPTO-SELL-SKIP",pid,f"{mkt_val:.2f}",f"{qty:.8f}","", "SKIPPED", note])
                if DEBUG: print(f"[SKIP] {pid}: {note}")
                continue

            _, _, min_quote, _ = fetch_product_rules(pid)
            if mkt_val < min_quote:
                note = f"MktVal ${mkt_val:.2f} < min_quote ${min_quote:.2f}"
                logs.append([now_iso(),"CRYPTO-SELL-SKIP",pid,f"{mkt_val:.2f}",f"{qty:.8f}","", "SKIPPED", note])
                if DEBUG: print(f"[SKIP] {pid}: {note}")
                continue

            avg, total_cost = reconstruct_avg_cost(pid, qty, COST_METHOD)
            if avg <= 0 or total_cost <= 0:
                note = "No cost basis from fills (transfers or missing history)"
                logs.append([now_iso(),"CRYPTO-SELL-SKIP",pid,f"{mkt_val:.2f}",f"{qty:.8f}","", "SKIPPED", note])
                if DEBUG: print(f"[SKIP] {pid}: {note}")
                continue

            gain = (px - avg) / avg
            if DEBUG: print(f"[EVAL] {pid}: qty={qty:.8f} avg=${avg:.6f} spot=${px:.6f} gain={gain*100:.2f}%")

            if gain >= target:
                oid = place_sell(pid, qty)
                proceeds = poll_fills_proceeds(oid)
                proceeds_used = proceeds if proceeds > 0 else mkt_val
                realized = proceeds_used - total_cost
                status = "dry-run" if DRY_RUN else "submitted"
                logs.append([now_iso(),"CRYPTO-SELL",pid,f"{proceeds_used:.2f}",f"{qty:.8f}",oid,status,
                             f"Spot ${px:.6f} | Avg ${avg:.6f} | Gain {gain*100:.2f}% | Profit ${realized:.2f}"])
                if DEBUG:
                    print(f"[SELL] {pid}: order={oid} proceeds=${proceeds_used:.2f} profit=${realized:.2f}")
                time.sleep(SLEEP_SEC * (0.8 + 0.4 * random.random()))
            else:
                logs.append([now_iso(),"CRYPTO-SELL-SKIP",pid,f"{mkt_val:.2f}",f"{qty:.8f}","", "SKIPPED",
                             f"Spot ${px:.6f} | Avg ${avg:.6f} | Gain {gain*100:.2f}% < {TARGET_GAIN_PCT:.2f}%"])
        except Exception as e:
            logs.append([now_iso(),"CRYPTO-SELL-ERROR",pid,"",f"{qty:.8f}","", "ERROR", f"{type(e).__name__}: {e}"])
            if DEBUG: print(f"[ERROR] {pid}: {type(e).__name__}: {e}")

    if logs and ws_log: append_logs(ws_log, logs)
    print("✅ crypto-seller done")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        print("❌ Fatal error:", e)
        traceback.print_exc()
