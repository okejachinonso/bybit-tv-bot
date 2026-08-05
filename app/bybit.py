import hashlib
import hmac
import json
import time
from decimal import Decimal, ROUND_CEILING, ROUND_DOWN, ROUND_HALF_UP, InvalidOperation
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlencode

import requests


class BybitError(RuntimeError):
    pass


def now_ms() -> int:
    return int(time.time() * 1000)


def compact_json(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)


def to_decimal(v: Any) -> Decimal:
    if isinstance(v, Decimal):
        return v
    if v is None:
        return Decimal("0")
    try:
        return Decimal(str(v))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")


def dstr(v: Any) -> str:
    d = to_decimal(v)
    s = format(d, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s or "0"


def normalize_symbol(symbol: Any) -> str:
    s = str(symbol or "").strip().upper()
    if s.endswith(".P"):
        s = s[:-2]
    return s


def round_to_step(value: Any, step: Any, rounding=ROUND_HALF_UP) -> Decimal:
    d_value = to_decimal(value)
    d_step = to_decimal(step)
    if d_step == 0:
        return d_value
    units = (d_value / d_step).to_integral_value(rounding=rounding)
    return units * d_step


def floor_to_step(value: Any, step: Any) -> Decimal:
    return round_to_step(value, step, rounding=ROUND_DOWN)


def ceil_to_step(value: Any, step: Any) -> Decimal:
    return round_to_step(value, step, rounding=ROUND_CEILING)


class BybitClient:
    def __init__(self, api_key: str, api_secret: str, base_url: str, recv_window: int = 5000, timeout: int = 15):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url.rstrip("/")
        self.recv_window = recv_window
        self.timeout = timeout
        self.session = requests.Session()
        self._instrument_cache: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self._ticker_cache: Dict[Tuple[str, str], Tuple[float, Decimal]] = {}

    def _sign(self, timestamp_ms: int, payload: str) -> str:
        prehash = f"{timestamp_ms}{self.api_key}{self.recv_window}{payload}"
        return hmac.new(self.api_secret.encode("utf-8"), prehash.encode("utf-8"), hashlib.sha256).hexdigest()

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
        auth: bool = True,
    ) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        headers = {"Content-Type": "application/json"}
        params = params or {}
        body = body or {}
        payload_str = ""

        if method.upper() == "GET" and params:
            query = urlencode([(k, v) for k, v in params.items() if v is not None], doseq=True)
            url = f"{url}?{query}"
            payload_str = query
        elif method.upper() in {"POST", "PUT", "DELETE"}:
            payload_str = compact_json(body)

        if auth:
            ts = now_ms()
            headers.update(
                {
                    "X-BAPI-API-KEY": self.api_key,
                    "X-BAPI-TIMESTAMP": str(ts),
                    "X-BAPI-RECV-WINDOW": str(self.recv_window),
                    "X-BAPI-SIGN": self._sign(ts, payload_str),
                }
            )

        resp = self.session.request(
            method.upper(),
            url,
            headers=headers,
            data=payload_str if method.upper() != "GET" else None,
            timeout=self.timeout,
        )
        try:
            data = resp.json()
        except Exception as exc:
            raise BybitError(f"Non-JSON response from Bybit (HTTP {resp.status_code}): {resp.text[:500]}") from exc

        if data.get("retCode") not in (0, "0", None):
            raise BybitError(f"Bybit retCode={data.get('retCode')} retMsg={data.get('retMsg')} data={data}")
        return data

    def get_ticker_price(self, symbol: str, category: str) -> Decimal:
        symbol = normalize_symbol(symbol)
        cache_key = (category, symbol)
        cached = self._ticker_cache.get(cache_key)
        now = time.time()
        if cached and now - cached[0] < 3:
            return cached[1]

        data = self._request("GET", "/v5/market/tickers", params={"category": category, "symbol": symbol}, auth=False)
        items = data.get("result", {}).get("list", [])
        if not items:
            raise BybitError(f"No ticker returned for {symbol}")
        item = items[0]
        price = item.get("markPrice") or item.get("lastPrice")
        if price is None:
            raise BybitError(f"Ticker missing price fields: {item}")
        d = to_decimal(price)
        self._ticker_cache[cache_key] = (now, d)
        return d

    def get_instrument(self, symbol: str, category: str) -> Dict[str, Any]:
        symbol = normalize_symbol(symbol)
        cache_key = (category, symbol)
        if cache_key in self._instrument_cache:
            return self._instrument_cache[cache_key]
        data = self._request("GET", "/v5/market/instruments-info", params={"category": category, "symbol": symbol}, auth=False)
        items = data.get("result", {}).get("list", [])
        if not items:
            raise BybitError(f"No instrument info returned for {symbol}")
        item = items[0]
        self._instrument_cache[cache_key] = item
        return item

    def get_position(self, symbol: str, category: str) -> Optional[Dict[str, Any]]:
        symbol = normalize_symbol(symbol)
        data = self._request("GET", "/v5/position/list", params={"category": category, "symbol": symbol}, auth=True)
        items = data.get("result", {}).get("list", [])
        if not items:
            return None
        for item in items:
            if to_decimal(item.get("size")) != 0:
                return item
        return items[0] if items else None

    def cancel_all_orders(self, symbol: str, category: str) -> Dict[str, Any]:
        symbol = normalize_symbol(symbol)
        return self._request("POST", "/v5/order/cancel-all", body={"category": category, "symbol": symbol}, auth=True)

    def place_order(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._request("POST", "/v5/order/create", body=payload, auth=True)

    def set_trading_stop(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._request("POST", "/v5/position/trading-stop", body=payload, auth=True)
