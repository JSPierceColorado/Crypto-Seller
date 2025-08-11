import os, json, time
from datetime import datetime, timezone

import gspread
from coinbase.rest import RESTClient

SHEET_NAME   = os.getenv("SHEET_NAME", "Trading Log")
LOG_TAB      = os.getenv("CRYPTO_LOG_TAB", "crypto_log")
COST_TAB     = os.getenv("CRYPTO_COST_TAB", "crypto_cost")

TARGET_GAIN_PCT = float(os.getenv("TARGET_GAIN_PCT", "5.0"))
SLEEP_SEC       = float(os.getenv("SLEEP_BETWEEN_ORDERS_SEC", "0.8"))
POLL_SEC        = float(os.getenv("POLL_INTERVAL_SEC", "0.8"))
POLL_TRIES      = int(os.getenv("POLL_MAX_TRIES", "25"))

CB = RESTClient()

def now_iso(): return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def get_gc():
    raw = os.getenv("GOOGLE_CREDS_JSON"); 
    if not raw: raise RuntimeError("Missing GOOGLE_CREDS_JSON")
    return gspread.service_account_from_dict(json.loads(raw))

def _ws(gc, tab):
    sh = gc.open(SHEET_NAME)
    try: return sh.worksheet(tab)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=tab, rows="2000", cols="50")

def ensure_log(ws):
    if not ws.get_all_values():
        ws.append_row(["Timestamp","Action","Product","ProceedsUSD","Qty","OrderID","Status","Note"])

def read_cost(ws):
    vals = ws.get_all_values()
    if not vals or len(vals) < 2: return []
    hdr = [h.strip() for h in vals[0]]
    out = []
    for r in vals[1:]:
        if not any(r): continue
        try:
            out.append({
                "product": r[0].strip().upper(),
                "qty": float(r[1] or 0.0),
                "dollar_cost": float(r[2] or 0.0),
            })
        except: pass
    return out

def price_now(product_id: str) -> float:
    p = CB.get_product(product_id=product_id)
    # defensive: price could be in 'price' or nested
    price = p.get("price") if isinstance(p, dict) else getattr(p, "price", None)
    if price is None and isinstance(p, dict) and "product" in p and "price" in p["product"]:
        price = p["product"]["price"]
    return float(price)

def place_sell(product_id: str, base_qty: float) -> str:
    o = CB.market_order_sell(client_order_id="", product_id=product_id, base_size=f"{base_qty:.12f}")
    return o.get("order_id", "") if isinstance(o, dict) else getattr(o, "order_id", "")

def poll_fills_proceeds(order_id: str) -> float:
    """Return total USD proceeds for this order (best-effort)."""
    total = 0.0
    for _ in range(POLL_TRIES):
        try:
            f = CB.get_fills(order_id=order_id)
            fills = f.get("fills", f) if isinstance(f, dict) else f.fills
            if fills:
                # prefer quote_value if present
                s = 0.0
                for x in fills:
                    qv = x.get("quote_value")
                    if qv:
                        s += float(qv)
                    else:
                        px = float(x.get("price", 0.0)); sz = float(x.get("size", 0.0))
                        s += px * sz
                if s > 0:
                    return s
        except Exception:
            pass
        time.sleep(POLL_SEC)
    return total

def update_cost_after_full_exit(ws, product: str):
    vals = ws.get_all_values()
    for r in range(1, len(vals)):
        if vals[r][0].strip().upper() == product:
            # zero it out
            ws.update(f"A{r+1}:E{r+1}", [[product, "0", "0", "0", now_iso()]])
            return

def main():
    print("🏁 crypto-seller starting")
    gc = get_gc()
    ws_log  = _ws(gc, LOG_TAB); ensure_log(ws_log)
    ws_cost = _ws(gc, COST_TAB)

    target = TARGET_GAIN_PCT / 100.0
    cost_rows = read_cost(ws_cost)
    if not cost_rows:
        print("ℹ️ No cost basis rows; nothing to sell.")
        return

    logs = []
    for row in cost_rows:
        pid = row["product"]; qty = row["qty"]; dollar_cost = row["dollar_cost"]
        if qty <= 0 or dollar_cost <= 0: continue

        try:
            px = price_now(pid)
            mkt = qty * px
            gain = (mkt - dollar_cost) / dollar_cost

            if gain >= target:
                oid = place_sell(pid, qty)
                proceeds = poll_fills_proceeds(oid)
                realized = proceeds - dollar_cost if proceeds > 0 else mkt - dollar_cost
                logs.append([now_iso(), "CRYPTO-SELL", pid, f"{proceeds:.2f}", f"{qty:.12f}", oid, "submitted", f"Gain {gain*100:.2f}% | Profit ${realized:.2f}"])
                update_cost_after_full_exit(ws_cost, pid)
                time.sleep(SLEEP_SEC)
            else:
                logs.append([now_iso(), "CRYPTO-SELL-SKIP", pid, f"{mkt:.2f}", f"{qty:.12f}", "", "SKIPPED", f"Gain {gain*100:.2f}% < {TARGET_GAIN_PCT:.2f}%"])
        except Exception as e:
            logs.append([now_iso(), "CRYPTO-SELL-ERROR", pid, "", f"{qty:.12f}", "", "ERROR", f"{type(e).__name__}: {e}"])

    for i in range(0, len(logs), 100):
        ws_log.append_rows(logs[i:i+100], value_input_option="USER_ENTERED")
    print("✅ crypto-seller done")

if __name__ == "__main__":
    main()
