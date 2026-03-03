const duckdb = require("duckdb");
const { config } = require("../config");

const DDL_ORDERBOOK = `
CREATE TABLE IF NOT EXISTS polymarket_orderbook (
  token_id TEXT,
  snapshot_ts TIMESTAMP,
  side TEXT,
  level INTEGER,
  price DOUBLE,
  size DOUBLE,
  PRIMARY KEY (token_id, snapshot_ts, side, level)
);
`;

const DDL_HISTORY = `
CREATE TABLE IF NOT EXISTS polymarket_prices_history (
  token_id TEXT,
  t TIMESTAMP,
  interval TEXT,
  fidelity_min INTEGER,
  price DOUBLE,
  PRIMARY KEY (token_id, t, interval, fidelity_min)
);
`;

const DDL_STATS = `
CREATE TABLE IF NOT EXISTS polymarket_market_stats (
  market_id TEXT,
  snapshot_ts TIMESTAMP,
  title TEXT,
  category TEXT,
  yes_token_id TEXT,
  no_token_id TEXT,
  yes_price DOUBLE,
  no_price DOUBLE,
  yes_midpoint DOUBLE,
  no_midpoint DOUBLE,
  yes_last_trade DOUBLE,
  no_last_trade DOUBLE,
  yes_display_price DOUBLE,
  no_display_price DOUBLE,
  ui_yes_price DOUBLE,
  ui_no_price DOUBLE,
  token_mapping_source TEXT,
  token_mapping_ok BOOLEAN,
  token_mapping_warning TEXT,
  token_mapping_anomaly BOOLEAN,
  clob_last_trade_anomaly BOOLEAN,
  best_ask_yes DOUBLE,
  best_bid_yes DOUBLE,
  best_ask_no DOUBLE,
  best_bid_no DOUBLE,
  last_trade_price DOUBLE,
  volume DOUBLE,
  volume_clob DOUBLE,
  volume_1wk DOUBLE,
  volume_1mo DOUBLE,
  liquidity DOUBLE,
  liquidity_clob DOUBLE,
  spread DOUBLE,
  order_min_size DOUBLE,
  min_tick DOUBLE,
  price_change_1d DOUBLE,
  price_change_1wk DOUBLE,
  price_change_1mo DOUBLE,
  price_change_1yr DOUBLE,
  start_date TIMESTAMP,
  end_date TIMESTAMP,
  accepting_orders_since TIMESTAMP,
  active BOOLEAN,
  closed BOOLEAN,
  funded BOOLEAN,
  ready BOOLEAN,
  neg_risk BOOLEAN,
  neg_risk_other BOOLEAN,
  uma_resolution_status TEXT,
  automatically_resolved BOOLEAN,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  volatility_1w DOUBLE,
  ma_short DOUBLE,
  ma_long DOUBLE,
  ema_slope DOUBLE,
  overreaction_flag BOOLEAN,
  orderbook_imbalance DOUBLE,
  slippage_notional_1k DOUBLE,
  slippage_notional_10k DOUBLE,
  fair_value DOUBLE,
  expected_value DOUBLE,
  kelly_fraction DOUBLE,
  trade_signal TEXT,
  late_overconfidence BOOLEAN,
  base_rate DOUBLE,
  base_rate_deviation DOUBLE,
  sentiment_momentum DOUBLE,
  liquidity_score DOUBLE,
  degen_risk DOUBLE,
  PRIMARY KEY (market_id, snapshot_ts)
);
`;

const DDL_ALERTS = `
CREATE TABLE IF NOT EXISTS sent_alerts (
  market_id TEXT,
  snapshot_ts TIMESTAMP,
  signal_type TEXT,
  PRIMARY KEY (market_id, snapshot_ts, signal_type)
);
`;

function createClient(path = config.duckdbPath) {
  const db = new duckdb.Database(path);

  function run(sql, params = []) {
    return new Promise((resolve, reject) => {
      const callback = (error) => {
        if (error) reject(error);
        else resolve();
      };

      if (params.length) {
        db.run(sql, params, callback);
      } else {
        db.run(sql, callback);
      }
    });
  }

  function all(sql, params = []) {
    return new Promise((resolve, reject) => {
      const callback = (error, rows) => {
        if (error) reject(error);
        else resolve(rows);
      };

      if (params.length) {
        db.all(sql, params, callback);
      } else {
        db.all(sql, callback);
      }
    });
  }

  function close() {
    return new Promise((resolve, reject) => {
      db.close((error) => {
        if (error) reject(error);
        else resolve();
      });
    });
  }

  return { run, all, close };
}

async function ensureTables(client) {
  await client.run(DDL_ORDERBOOK);
  await client.run(DDL_HISTORY);
  await client.run(DDL_STATS);
  await client.run(DDL_ALERTS);
}

async function upsertOrderbook(client, rows, asOf) {
  if (!rows?.length) return;

  const sql = `
    INSERT OR REPLACE INTO polymarket_orderbook
      (token_id, snapshot_ts, side, level, price, size)
    VALUES (?, ?, ?, ?, ?, ?)
  `;

  for (const row of rows) {
    await client.run(sql, [
      row.token_id,
      asOf.toISOString(),
      row.side,
      row.level,
      row.price,
      row.size
    ]);
  }
}

async function upsertHistory(client, rows) {
  if (!rows?.length) return;

  const sql = `
    INSERT OR REPLACE INTO polymarket_prices_history
      (token_id, t, interval, fidelity_min, price)
    VALUES (?, ?, ?, ?, ?)
  `;

  for (const row of rows) {
    const timestamp = row.t instanceof Date ? row.t.toISOString() : new Date(row.t).toISOString();
    await client.run(sql, [row.token_id, timestamp, row.interval, row.fidelity_min, row.price]);
  }
}

async function upsertMarketStats(client, rows) {
  if (!rows?.length) return;

  const columns = [
    "market_id", "snapshot_ts", "title", "category", "yes_token_id", "no_token_id",
    "yes_price", "no_price", "yes_midpoint", "no_midpoint", "yes_last_trade", "no_last_trade",
    "yes_display_price", "no_display_price", "ui_yes_price", "ui_no_price",
    "token_mapping_source", "token_mapping_ok", "token_mapping_warning", "token_mapping_anomaly",
    "clob_last_trade_anomaly", "best_ask_yes", "best_bid_yes", "best_ask_no", "best_bid_no",
    "last_trade_price", "volume", "volume_clob", "volume_1wk", "volume_1mo", "liquidity",
    "liquidity_clob", "spread", "order_min_size", "min_tick", "price_change_1d", "price_change_1wk",
    "price_change_1mo", "price_change_1yr", "start_date", "end_date", "accepting_orders_since",
    "active", "closed", "funded", "ready", "neg_risk", "neg_risk_other", "uma_resolution_status",
    "automatically_resolved", "created_at", "updated_at", "volatility_1w", "ma_short", "ma_long",
    "ema_slope", "overreaction_flag", "orderbook_imbalance", "slippage_notional_1k",
    "slippage_notional_10k", "fair_value", "expected_value", "kelly_fraction", "trade_signal",
    "late_overconfidence", "base_rate", "base_rate_deviation", "sentiment_momentum", "liquidity_score",
    "degen_risk"
  ];

  const placeholders = columns.map(() => "?").join(", ");
  const sql = `
    INSERT OR REPLACE INTO polymarket_market_stats (${columns.join(", ")})
    VALUES (${placeholders})
  `;

  const dateColumns = new Set([
    "snapshot_ts",
    "start_date",
    "end_date",
    "accepting_orders_since",
    "created_at",
    "updated_at"
  ]);

  for (const row of rows) {
    const values = columns.map((column) => {
      const value = row[column];
      if (value == null) return null;
      if (dateColumns.has(column)) {
        const date = value instanceof Date ? value : new Date(value);
        return Number.isNaN(date.getTime()) ? null : date.toISOString();
      }
      return value;
    });

    await client.run(sql, values);
  }
}

async function upsertAlerts(client, alertRows) {
  if (!alertRows?.length) return;
  const sql = `
    INSERT OR IGNORE INTO sent_alerts (market_id, snapshot_ts, signal_type)
    VALUES (?, ?, ?)
  `;

  for (const row of alertRows) {
    const timestamp = row.snapshot_ts instanceof Date
      ? row.snapshot_ts.toISOString()
      : new Date(row.snapshot_ts).toISOString();

    await client.run(sql, [row.market_id, timestamp, row.signal_type]);
  }
}

async function getLatestStats(client, limit = 100) {
  const rows = await client.all(
    `
      SELECT *
      FROM polymarket_market_stats
      QUALIFY row_number() OVER (PARTITION BY market_id ORDER BY snapshot_ts DESC) = 1
      ORDER BY snapshot_ts DESC
      LIMIT ?
    `,
    [limit]
  );
  return rows;
}

async function getPriceHistory(client, tokenId, interval = "1w", limit = 500) {
  if (!tokenId) return [];

  const safeLimit = Number.isFinite(Number(limit)) ? Math.max(1, Math.min(Number(limit), 5000)) : 500;
  const rows = await client.all(
    `
      SELECT t, price, interval, fidelity_min
      FROM polymarket_prices_history
      WHERE token_id = ?
        AND interval = ?
      ORDER BY t ASC
      LIMIT ?
    `,
    [tokenId, interval, safeLimit]
  );
  return rows;
}

module.exports = {
  createClient,
  ensureTables,
  upsertOrderbook,
  upsertHistory,
  upsertMarketStats,
  upsertAlerts,
  getLatestStats,
  getPriceHistory
};
