const dotenv = require("dotenv");

dotenv.config();

function toNumber(value, fallback) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

const config = {
  port: toNumber(process.env.PORT, 3000),
  gammaBase: process.env.GAMMA_BASE || "https://gamma-api.polymarket.com",
  clobBase: process.env.CLOB_BASE || "https://clob.polymarket.com",
  requestTimeoutMs: toNumber(process.env.REQUEST_TIMEOUT_MS, 30000),
  maxRetries: toNumber(process.env.MAX_RETRIES, 3),
  retryBackoffMs: toNumber(process.env.RETRY_BACKOFF_MS, 1000),
  duckdbPath: process.env.DUCKDB_PATH || "markets.duckdb",
  defaultDepth: toNumber(process.env.DEFAULT_DEPTH, 10),
  defaultIntervals: (process.env.DEFAULT_INTERVALS || "1w,1m")
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean),
  defaultFidelityMin: toNumber(process.env.DEFAULT_FIDELITY_MIN, 60),
  baseRate: toNumber(process.env.BASE_RATE, 0.5),
  enableScheduler: String(process.env.ENABLE_SCHEDULER || "false").toLowerCase() === "true",
  schedulerUrl: process.env.SCHEDULER_URL || "",
  schedulerIntervalMs: toNumber(process.env.SCHEDULER_INTERVAL_MS, 7200000),
  discordWebhookUrl: process.env.DISCORD_WEBHOOK_URL || ""
};

module.exports = { config };
