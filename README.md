# Polymarket Express API (Railway-ready)

Express.js server that ports your Colab logic for:
- Polymarket URL parsing and market resolution (Gamma API)
- Orderbook + history pulls (CLOB API)
- Market analytics and signal generation
- DuckDB persistence
- Optional Discord alerting + scheduler

## Endpoints

- `GET /health`
- `POST /api/extract`
- `GET /api/stats/latest?limit=100`
- `POST /api/signals/check`

### `POST /api/extract` body

```json
{
  "url": "https://polymarket.com/event/your-event-slug",
  "depth": 10,
  "intervals": ["1w", "1m"],
  "fidelityMin": 60,
  "baseRate": 0.5,
  "persist": true
}
```

## Local run

1. Install dependencies:
   ```bash
   npm install
   ```
2. Create env file:
   ```bash
   cp .env.example .env
   ```
3. Start server:
   ```bash
   npm start
   ```

## Railway deployment

1. Push this folder to GitHub.
2. In Railway, create a new project from the repo.
3. Add environment variables from `.env.example`.
4. Railway auto-detects Node and runs `npm start`.
5. Ensure `PORT` is provided by Railway (it is by default).

## Required env vars

- `PORT`
- `DUCKDB_PATH` (default: `markets.duckdb`)

Optional:
- `DISCORD_WEBHOOK_URL`
- `ENABLE_SCHEDULER=true`
- `SCHEDULER_URL=https://polymarket.com/event/...`
- `SCHEDULER_INTERVAL_MS=7200000`

## Notes

- `markets.duckdb` is a local file DB. On Railway, use a mounted volume if you want persistence across deploys/restarts.
- Alerts are de-duplicated using `sent_alerts` by `(market_id, snapshot_ts, signal_type)`.
# polymarket_API
