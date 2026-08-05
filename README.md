# TradingView → Bybit Demo Bot

A lightweight webhook service for your TradingView strategy.

It receives the JSON alerts your Pine Script already emits and translates them into Bybit API actions:

- place pending entry
- cancel pending order
- set TP/SL on fill
- partial close
- move stop to breakeven
- close position

## What this bot expects

Your Pine Script should send JSON like:

```json
{
  "secret": "the_same_secret_as_your_env_var",
  "symbol": "BTCUSD",
  "action": "place_pending",
  "side": "buy",
  "entry": 64000,
  "stop": 63250,
  "target": 67750,
  "qty": 0.02,
  "risk_usd": 25
}
```

Supported actions:

- `place_pending`
- `filled`
- `partial_close`
- `move_stop`
- `close`
- `cancel_pending`

Aliases that are also accepted:

- `pending_buy`, `buy_pending`, `sell_pending`
- `partial`
- `breakeven`
- `be`
- `exit`
- `cancel`

## Bybit Demo account

Use the **Bybit Demo Trading** API keys and the demo base URL:

- `https://api-demo.bybit.com`

## Railway deployment

Set these environment variables in Railway:

- `WEBHOOK_SECRET`
- `BYBIT_API_KEY`
- `BYBIT_API_SECRET`
- `BYBIT_DEMO=true`
- `BYBIT_BASE_URL=https://api-demo.bybit.com`
- `BYBIT_CATEGORY=linear`

Optional but useful:

- `SYMBOL_MAP_JSON={"BTCUSD":"BTCUSDT","ETHUSD":"ETHUSDT"}`
- `PARTIAL_CLOSE_PCT=20`
- `CANCEL_ALL_ON_CLOSE=true`
- `CANCEL_ALL_ON_CANCEL_PENDING=true`
- `ROUND_PRICES_TO_TICK=true`
- `DRY_RUN=false`

Railway start command:

```bash
uvicorn app.main:app --host 0.0.0.0 --port $PORT
```

## TradingView alert setup

Use:

- Condition: **Order fills and alert() function calls**
- Webhook URL: `https://YOUR-RAILWAY-URL/webhook`

## Endpoints

- `GET /healthz`
- `GET /state/{symbol}`
- `POST /webhook`

## Safety notes

- The bot ignores any non-JSON alert.
- The bot rejects any JSON that does not contain the correct `secret`.
- For demo testing, keep `DRY_RUN=false` only when you're ready to place real demo orders.
