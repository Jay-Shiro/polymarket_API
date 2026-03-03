const axios = require("axios");
const { config } = require("../config");
const { extractFromUrl } = require("./extractor");
const {
  createClient,
  ensureTables,
  upsertAlerts,
  getLatestStats
} = require("../db/duckdb");

function formatDiscordMessage(row) {
  const direction = row.trade_signal === "long" ? "🟢 LONG" : "🔴 SHORT";
  const kelly = row.kelly_fraction != null ? `${(Number(row.kelly_fraction) * 100).toFixed(2)}%` : "n/a";

  return [
    `**${direction} SIGNAL**`,
    `Market: ${row.title || row.market_id}`,
    `Price: ${Number(row.yes_price || 0).toFixed(3)}`,
    `Fair Value: ${Number(row.fair_value || 0).toFixed(3)}`,
    `Expected Value: ${Number(row.expected_value || 0).toFixed(3)}`,
    `Kelly: ${kelly}`,
    `Volatility: ${row.volatility_1w != null ? Number(row.volatility_1w).toFixed(3) : "n/a"}`,
    `Degen Risk: ${row.degen_risk != null ? Number(row.degen_risk).toFixed(3) : "n/a"}`,
    `Liquidity Score: ${row.liquidity_score != null ? Number(row.liquidity_score).toFixed(2) : "n/a"}`
  ].join("\n");
}

async function sendDiscordAlert(content) {
  if (!config.discordWebhookUrl) return false;
  await axios.post(config.discordWebhookUrl, { content }, { timeout: config.requestTimeoutMs });
  return true;
}

async function checkSignals({ url }) {
  if (!url) throw new Error("url is required");

  await extractFromUrl({ url, persist: true });

  const client = createClient(config.duckdbPath);
  try {
    await ensureTables(client);
    const latestRows = await getLatestStats(client, 500);

    const candidates = latestRows.filter((row) => {
      const isActive = Boolean(row.active) && !Boolean(row.closed);
      const validSignal = row.trade_signal === "long" || row.trade_signal === "short";
      const ev = Number(row.expected_value || 0);
      const risk = Number(row.degen_risk || 0);
      return isActive && validSignal && Math.abs(ev) > 0.05 && risk < 0.1;
    });

    const newAlerts = [];
    const sent = [];

    for (const row of candidates) {
      const existing = await client.all(
        `
          SELECT 1
          FROM sent_alerts
          WHERE market_id = ?
            AND snapshot_ts = ?
            AND signal_type = ?
          LIMIT 1
        `,
        [row.market_id, new Date(row.snapshot_ts).toISOString(), row.trade_signal]
      );

      if (existing.length) continue;

      const message = formatDiscordMessage(row);
      await sendDiscordAlert(message);

      newAlerts.push({
        market_id: row.market_id,
        snapshot_ts: new Date(row.snapshot_ts),
        signal_type: row.trade_signal
      });

      sent.push({
        market_id: row.market_id,
        title: row.title,
        signal_type: row.trade_signal,
        snapshot_ts: row.snapshot_ts
      });
    }

    if (newAlerts.length) {
      await upsertAlerts(client, newAlerts);
    }

    return {
      total_candidates: candidates.length,
      sent_count: sent.length,
      sent
    };
  } finally {
    await client.close();
  }
}

module.exports = {
  checkSignals
};
