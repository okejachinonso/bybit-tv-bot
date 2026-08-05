import hashlib
import json
import logging
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, Optional
from decimal import Decimal, ROUND_HALF_UP

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .bybit import BybitClient, BybitError, dstr, floor_to_step, now_ms, to_decimal

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.getenv("BOT_DATA_DIR", str(BASE_DIR / "data")))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = Path(os.getenv("BOT_DB_PATH", str(DATA_DIR / "bot_state.db")))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("tv-bybit-bot")

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()
BYBIT_API_KEY = os.getenv("BYBIT_API_KEY", "").strip()
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET", "").strip()
BYBIT_DEMO = os.getenv("BYBIT_DEMO", "true").lower() in {"1", "true", "yes", "y"}
BYBIT_BASE_URL = os.getenv("BYBIT_BASE_URL", "https://api-demo.bybit.com" if BYBIT_DEMO else "https://api.bybit.com").rstrip("/")
BYBIT_CATEGORY = os.getenv("BYBIT_CATEGORY", "linear").strip()
POSITION_IDX = int(os.getenv("POSITION_IDX", "0"))
PARTIAL_CLOSE_PCT = to_decimal(os.getenv("PARTIAL_CLOSE_PCT", "20"))
DRY_RUN = os.getenv("DRY_RUN", "false").lower() in {"1", "true", "yes", "y"}
RECV_WINDOW = int(os.getenv("BYBIT_RECV_WINDOW", "5000"))
REQUEST_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "15"))
CANCEL_ALL_ON_CLOSE = os.getenv("CANCEL_ALL_ON_CLOSE", "true").lower() in {"1", "true", "yes", "y", "on"}
CANCEL_ALL_ON_CANCEL_PENDING = os.getenv("CANCEL_ALL_ON_CANCEL_PENDING", "true").lower() in {"1", "true", "yes", "y", "on"}
ROUND_PRICES_TO_TICK = os.getenv("ROUND_PRICES_TO_TICK", "true").lower() in {"1", "true", "yes", "y", "on"}

SYMBOL_MAP_RAW = os.getenv("SYMBOL_MAP_JSON", "{}").strip()
try:
    SYMBOL_MAP: Dict[str, str] = {k.upper(): v.upper() for k, v in json.loads(SYMBOL_MAP_RAW).items()}
except Exception:
    SYMBOL_MAP = {}

# -----------------------------------------------------------------------------
# Database
# -----------------------------------------------------------------------------

def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS event_dedupe (
                fingerprint TEXT PRIMARY KEY,
                created_at INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS symbol_state (
                symbol TEXT PRIMARY KEY,
                side TEXT,
                entry REAL,
                stop REAL,
                target REAL,
                qty REAL,
                pending_order_id TEXT,
                updated_at INTEGER NOT NULL
            )
            """
        )
        conn.commit()


def seen_recent(fingerprint: str, ttl_seconds: int = 120) -> bool:
    now = int(time.time())
    cutoff = now - ttl_seconds
    with db() as conn:
        row = conn.execute("SELECT created_at FROM event_dedupe WHERE fingerprint = ?", (fingerprint,)).fetchone()
        if row and int(row["created_at"]) >= cutoff:
            return True
        conn.execute(
            "INSERT OR REPLACE INTO event_dedupe (fingerprint, created_at) VALUES (?, ?)",
            (fingerprint, now),
        )
        conn.commit()
    return False


def get_symbol_state(symbol: str) -> Optional[dict]:
    with db() as conn:
        row = conn.execute("SELECT * FROM symbol_state WHERE symbol = ?", (symbol,)).fetchone()
        return dict(row) if row else None


def upsert_symbol_state(symbol: str, **fields: Any) -> None:
    current = get_symbol_state(symbol) or {}
    now = int(time.time())
    data = {
        "symbol": symbol,
        "side": current.get("side"),
        "entry": current.get("entry"),
        "stop": current.get("stop"),
        "target": current.get("target"),
        "qty": current.get("qty"),
        "pending_order_id": current.get("pending_order_id"),
        "updated_at": now,
    }
    data.update({k: v for k, v in fields.items() if v is not None})
    data["updated_at"] = now
    with db() as conn:
        conn.execute(
            """
            INSERT INTO symbol_state (symbol, side, entry, stop, target, qty, pending_order_id, updated_at)
            VALUES (:symbol, :side, :entry, :stop, :target, :qty, :pending_order_id, :updated_at)
            ON CONFLICT(symbol) DO UPDATE SET
                side=excluded.side,
                entry=excluded.entry,
                stop=excluded.stop,
                target=excluded.target,
                qty=excluded.qty,
                pending_order_id=excluded.pending_order_id,
                updated_at=excluded.updated_at
            """,
            data,
        )
        conn.commit()

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def sha12(payload: str) -> str:
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]


def compact_json(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)


def normalize_symbol(symbol: str) -> str:
    sym = (symbol or "").strip().upper()
    # TradingView perpetual futures symbols often arrive as SYMBOL.P; Bybit expects SYMBOL.
    if sym.endswith(".P"):
        sym = sym[:-2]
    if sym in SYMBOL_MAP:
        return SYMBOL_MAP[sym]
    if sym.endswith("USDT"):
        return sym
    if sym.endswith("USD"):
        return sym[:-3] + "USDT"
    return sym


def normalize_side(side: str) -> str:
    s = (side or "").strip().lower()
    if s in {"buy", "long"}:
        return "Buy"
    if s in {"sell", "short"}:
        return "Sell"
    raise ValueError(f"Invalid side: {side}")


def normalize_action(action: str) -> str:
    a = (action or "").strip().lower()
    aliases = {
        "pending_buy": "place_pending",
        "buy_pending": "place_pending",
        "sell_pending": "place_pending",
        "entry": "place_pending",
        "partial": "partial_close",
        "breakeven": "move_stop",
        "be": "move_stop",
        "exit": "close",
        "cancel": "cancel_pending",
        "tp1": "partial_close",
    }
    return aliases.get(a, a)


def is_auth(payload: Dict[str, Any]) -> bool:
    if not WEBHOOK_SECRET:
        return False
    return str(payload.get("secret", "")).strip() == WEBHOOK_SECRET


def fingerprint_event(payload: Dict[str, Any]) -> str:
    keys = ["secret", "action", "symbol", "side", "entry", "stop", "target", "qty", "risk_usd", "new_stop", "percent"]
    subset = {k: payload.get(k) for k in keys if k in payload}
    return sha12(compact_json(subset))


def require_config() -> None:
    if not WEBHOOK_SECRET:
        raise RuntimeError("WEBHOOK_SECRET is required")
    if not BYBIT_API_KEY or not BYBIT_API_SECRET:
        raise RuntimeError("BYBIT_API_KEY and BYBIT_API_SECRET are required")


def get_client() -> BybitClient:
    return BybitClient(
        api_key=BYBIT_API_KEY,
        api_secret=BYBIT_API_SECRET,
        base_url=BYBIT_BASE_URL,
        recv_window=RECV_WINDOW,
        timeout=REQUEST_TIMEOUT,
    )


def instrument_info(client: BybitClient, symbol: str) -> Dict[str, Any]:
    return client.get_instrument(symbol, BYBIT_CATEGORY)


def min_qty_from_instrument(info: Dict[str, Any]) -> Decimal:
    lot = info.get("lotSizeFilter", {}) or {}
    return to_decimal(lot.get("minOrderQty") or lot.get("minTradeQty") or lot.get("minOrderAmt") or "0")


def qty_step_from_instrument(info: Dict[str, Any]) -> Decimal:
    lot = info.get("lotSizeFilter", {}) or {}
    return to_decimal(lot.get("qtyStep") or "0.001")


def tick_size_from_instrument(info: Dict[str, Any]) -> Decimal:
    pf = info.get("priceFilter", {}) or {}
    return to_decimal(pf.get("tickSize") or "0.5")


def round_price(client: BybitClient, symbol: str, price: Any) -> Decimal:
    info = instrument_info(client, symbol)
    tick = tick_size_from_instrument(info)
    return to_decimal(price) if not ROUND_PRICES_TO_TICK or tick == 0 else (to_decimal(price) / tick).to_integral_value(rounding=ROUND_HALF_UP) * tick


def round_qty(client: BybitClient, symbol: str, qty: Any) -> Decimal:
    info = instrument_info(client, symbol)
    step = qty_step_from_instrument(info)
    if step == 0:
        return to_decimal(qty)
    return floor_to_step(qty, step)


def ensure_min_qty(client: BybitClient, symbol: str, qty: Decimal) -> Decimal:
    info = instrument_info(client, symbol)
    min_qty = min_qty_from_instrument(info)
    if qty <= 0:
        return Decimal("0")
    if min_qty and qty < min_qty:
        return Decimal("0")
    return qty


def opposite_side(side: str) -> str:
    return "Sell" if normalize_side(side) == "Buy" else "Buy"


def trigger_direction(entry_price: Decimal, current_price: Decimal) -> int:
    # 1 = rise to trigger, 2 = fall to trigger
    return 1 if entry_price > current_price else 2


def build_order_link_id(action: str, payload: Dict[str, Any]) -> str:
    base = compact_json(
        {
            "action": action,
            "symbol": payload.get("symbol"),
            "side": payload.get("side"),
            "entry": payload.get("entry"),
            "stop": payload.get("stop"),
            "target": payload.get("target"),
            "qty": payload.get("qty"),
            "new_stop": payload.get("new_stop"),
        }
    )
    return f"tv_{sha12(base)}"


def current_position_qty(client: BybitClient, symbol: str) -> Decimal:
    pos = client.get_position(symbol, BYBIT_CATEGORY)
    return to_decimal(pos.get("size")) if pos else Decimal("0")

# -----------------------------------------------------------------------------
# Trading actions
# -----------------------------------------------------------------------------

def place_pending(client: BybitClient, symbol: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    entry = round_price(client, symbol, payload["entry"])
    stop = round_price(client, symbol, payload["stop"])
    target = round_price(client, symbol, payload["target"])
    qty = round_qty(client, symbol, payload["qty"])
    qty = ensure_min_qty(client, symbol, qty)
    if qty <= 0:
        raise BybitError(f"Qty too small after rounding for {symbol}")

    current_price = client.get_ticker_price(symbol, BYBIT_CATEGORY)
    order = {
        "category": BYBIT_CATEGORY,
        "symbol": symbol,
        "side": normalize_side(payload["side"]),
        "orderType": "Market",
        "qty": dstr(qty),
        "triggerPrice": dstr(entry),
        "triggerDirection": trigger_direction(entry, current_price),
        "triggerBy": "LastPrice",
        "reduceOnly": False,
        "closeOnTrigger": False,
        "positionIdx": POSITION_IDX,
        "takeProfit": dstr(target),
        "stopLoss": dstr(stop),
        "orderLinkId": build_order_link_id("place_pending", {**payload, "symbol": symbol}),
    }

    upsert_symbol_state(symbol, side=payload.get("side"), entry=float(entry), stop=float(stop), target=float(target), qty=float(qty), pending_order_id=order["orderLinkId"])
    if DRY_RUN:
        return {"dry_run": True, "order": order}
    return client.place_order(order)


def filled(client: BybitClient, symbol: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    stop = round_price(client, symbol, payload["stop"])
    target = round_price(client, symbol, payload["target"])
    logging.info(f"Setting TP/SL: symbol={symbol} stop={stop} target={target}")
    upsert_symbol_state(
        symbol,
        side=payload.get("side"),
        entry=payload.get("entry"),
        stop=float(stop),
        target=float(target),
        qty=payload.get("qty"),
        pending_order_id=None,
    )
    if DRY_RUN:
        return {"dry_run": True, "action": "filled", "symbol": symbol}
    return client.set_trading_stop(
        {
            "category": BYBIT_CATEGORY,
            "symbol": symbol,
            "tpslMode": "Full",
            "positionIdx": POSITION_IDX,
            "stopLoss": dstr(stop),
            "takeProfit": dstr(target),
        }
    )


def move_stop(client: BybitClient, symbol: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    new_stop = round_price(client, symbol, payload.get("new_stop"))
    state = get_symbol_state(symbol) or {}
    existing_target = state.get("target")
    tp = to_decimal(existing_target) if existing_target is not None else None
    if DRY_RUN:
        return {"dry_run": True, "action": "move_stop", "symbol": symbol, "new_stop": dstr(new_stop), "target": dstr(tp) if tp is not None else None}
    body = {
        "category": BYBIT_CATEGORY,
        "symbol": symbol,
        "tpslMode": "Full",
        "positionIdx": POSITION_IDX,
        "stopLoss": dstr(new_stop),
    }
    if tp is not None and tp != 0:
        body["takeProfit"] = dstr(tp)
    return client.set_trading_stop(body)


def partial_close(client: BybitClient, symbol: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    percent = to_decimal(payload.get("percent", PARTIAL_CLOSE_PCT))
    pos = client.get_position(symbol, BYBIT_CATEGORY)
    if not pos:
        raise BybitError(f"No open position for {symbol}")
    size = to_decimal(pos.get("size"))
    if size <= 0:
        raise BybitError(f"No open position size for {symbol}")
    step = qty_step_from_instrument(instrument_info(client, symbol))
    qty = floor_to_step(size * (percent / Decimal("100")), step)
    qty = ensure_min_qty(client, symbol, qty)
    if qty <= 0:
        qty = size
    side = pos.get("side", "Buy")
    order = {
        "category": BYBIT_CATEGORY,
        "symbol": symbol,
        "side": opposite_side(side),
        "orderType": "Market",
        "qty": dstr(qty),
        "reduceOnly": True,
        "positionIdx": POSITION_IDX,
        "orderLinkId": f"tv_{sha12(f'partial:{symbol}:{dstr(qty)}:{now_ms()}')}",
    }
    if DRY_RUN:
        return {"dry_run": True, "action": "partial_close", "symbol": symbol, "qty": dstr(qty)}
    return client.place_order(order)


def close_position(client: BybitClient, symbol: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    pos = client.get_position(symbol, BYBIT_CATEGORY)
    if not pos:
        return {"ok": True, "message": f"No open position for {symbol}"}
    size = to_decimal(pos.get("size"))
    side = pos.get("side", payload.get("side", "Buy"))
    if CANCEL_ALL_ON_CLOSE:
        try:
            client.cancel_all_orders(symbol, BYBIT_CATEGORY)
        except Exception as exc:
            log.warning("cancel-all on close failed for %s: %s", symbol, exc)
    order = {
        "category": BYBIT_CATEGORY,
        "symbol": symbol,
        "side": opposite_side(side),
        "orderType": "Market",
        "qty": dstr(size),
        "reduceOnly": True,
        "positionIdx": POSITION_IDX,
        "orderLinkId": f"tv_{sha12(f'close:{symbol}:{dstr(size)}:{now_ms()}')}",
    }
    if DRY_RUN:
        return {"dry_run": True, "action": "close", "symbol": symbol, "qty": dstr(size), "side": side}
    return client.place_order(order)


def cancel_pending(client: BybitClient, symbol: str) -> Dict[str, Any]:
    if DRY_RUN:
        return {"dry_run": True, "action": "cancel_pending", "symbol": symbol}
    return client.cancel_all_orders(symbol, BYBIT_CATEGORY)


def action_router(client: BybitClient, payload: Dict[str, Any], symbol: str) -> Dict[str, Any]:
    action = normalize_action(payload.get("action", ""))
    if action == "place_pending":
        return place_pending(client, symbol, payload)
    if action == "filled":
        return filled(client, symbol, payload)
    if action == "move_stop":
        return move_stop(client, symbol, payload)
    if action == "partial_close":
        return partial_close(client, symbol, payload)
    if action == "close":
        return close_position(client, symbol, payload)
    if action == "cancel_pending":
        return cancel_pending(client, symbol)
    raise ValueError(f"Unknown action: {action}")

# -----------------------------------------------------------------------------
# App
# -----------------------------------------------------------------------------

app = FastAPI(title="TradingView → Bybit Demo Bot", version="2.0.0")
client = get_client()


@app.on_event("startup")
def startup() -> None:
    init_db()
    require_config()
    log.info("Bot starting demo=%s base_url=%s category=%s dry_run=%s", BYBIT_DEMO, BYBIT_BASE_URL, BYBIT_CATEGORY, DRY_RUN)


@app.get("/healthz")
def healthz() -> Dict[str, Any]:
    return {
        "ok": True,
        "demo": BYBIT_DEMO,
        "base_url": BYBIT_BASE_URL,
        "category": BYBIT_CATEGORY,
        "dry_run": DRY_RUN,
        "has_secret": bool(WEBHOOK_SECRET),
    }


@app.get("/")
def root() -> Dict[str, Any]:
    return {"ok": True, "service": "TradingView → Bybit Demo Bot", "health": "/healthz", "webhook": "/webhook"}


@app.get("/state/{symbol}")
def state(symbol: str) -> Dict[str, Any]:
    sym = normalize_symbol(symbol)
    return {"symbol": sym, "state": get_symbol_state(sym)}


def parse_payload(raw: bytes) -> Optional[Dict[str, Any]]:
    try:
        data = json.loads(raw.decode("utf-8"))
    except Exception:
        return None
    return data if isinstance(data, dict) else None


@app.post("/webhook")
async def webhook(request: Request):
    raw = await request.body()
    payload = parse_payload(raw)

    if payload is None:
        return JSONResponse({"ok": True, "ignored": "non_json_payload"}, status_code=200)

    fingerprint = fingerprint_event(payload)
    if seen_recent(fingerprint):
        return JSONResponse({"ok": True, "ignored": "duplicate", "fingerprint": fingerprint}, status_code=200)

    if "secret" not in payload:
        return JSONResponse({"ok": True, "ignored": "missing_secret"}, status_code=200)

    if not is_auth(payload):
        return JSONResponse({"ok": False, "error": "invalid_secret"}, status_code=401)

    symbol = normalize_symbol(str(payload.get("symbol", "")).strip())
    if not symbol:
        return JSONResponse({"ok": False, "error": "missing_symbol"}, status_code=400)

    side = payload.get("side")
    if side:
        try:
            payload["side"] = normalize_side(side)
        except ValueError as exc:
            return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)

    # Validate numerics early
    for field in ("entry", "stop", "target", "qty", "new_stop", "percent"):
        if field in payload and payload[field] is not None:
            _ = to_decimal(payload[field])

    try:
        result = action_router(client, payload, symbol)
    except Exception as exc:
        log.exception("Webhook error payload=%s", payload)
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)

    if payload.get("action") in {"place_pending", "filled", "move_stop"}:
        upsert_symbol_state(
            symbol,
            side=payload.get("side"),
            entry=payload.get("entry"),
            stop=payload.get("stop"),
            target=payload.get("target"),
            qty=payload.get("qty"),
            pending_order_id=get_symbol_state(symbol).get("pending_order_id") if get_symbol_state(symbol) else None,
        )

    return JSONResponse(
        {
            "ok": True,
            "action": normalize_action(payload.get("action", "")),
            "symbol": symbol,
            "result": result if isinstance(result, dict) else {"raw": str(result)},
            "demo": BYBIT_DEMO,
            "dry_run": DRY_RUN,
        },
        status_code=200,
    )
