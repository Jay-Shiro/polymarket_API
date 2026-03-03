const express = require("express");
const { config } = require("./config");
const { extractFromUrl } = require("./services/extractor");
const { checkSignals } = require("./services/alerts");
const { createClient, ensureTables, getLatestStats, getPriceHistory } = require("./db/duckdb");
const { startScheduler } = require("./scheduler");

const app = express();
app.use(express.json({ limit: "1mb" }));

app.get("/health", async (_req, res) => {
  res.json({ status: "ok", service: "polymarket-api-server" });
});

app.post("/api/extract", async (req, res) => {
  try {
    const {
      url,
      depth = config.defaultDepth,
      intervals = config.defaultIntervals,
      fidelityMin = config.defaultFidelityMin,
      baseRate = config.baseRate,
      persist = true
    } = req.body || {};

    if (!url) {
      return res.status(400).json({ error: "url is required" });
    }

    const result = await extractFromUrl({ url, depth, intervals, fidelityMin, baseRate, persist });
    return res.json(result);
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

app.get("/api/stats/latest", async (req, res) => {
  const limit = Number(req.query.limit || 100);
  const safeLimit = Number.isFinite(limit) && limit > 0 ? Math.min(limit, 1000) : 100;

  const client = createClient(config.duckdbPath);
  try {
    await ensureTables(client);
    const rows = await getLatestStats(client, safeLimit);
    return res.json({ count: rows.length, rows });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  } finally {
    await client.close();
  }
});

app.get("/api/dashboard", async (req, res) => {
  const limit = Number(req.query.limit || 100);
  const safeLimit = Number.isFinite(limit) && limit > 0 ? Math.min(limit, 1000) : 100;
  const historyInterval = String(req.query.historyInterval || "1w");
  const historyLimit = Number(req.query.historyLimit || 500);
  const safeHistoryLimit = Number.isFinite(historyLimit) && historyLimit > 0
    ? Math.min(historyLimit, 5000)
    : 500;

  const client = createClient(config.duckdbPath);
  try {
    await ensureTables(client);
    const rows = await getLatestStats(client, safeLimit);
    const now = Date.now();

    const markets = rows.map((row) => {
      const expectedValue = row.expected_value == null ? null : Number(row.expected_value);
      const degenRisk = row.degen_risk == null ? null : Number(row.degen_risk);
      const mispricingScore =
        expectedValue == null || degenRisk == null
          ? null
          : expectedValue / (degenRisk + 1e-6);

      let timeToResolutionHours = null;
      if (row.end_date) {
        const endTs = new Date(row.end_date).getTime();
        if (!Number.isNaN(endTs)) {
          timeToResolutionHours = (endTs - now) / (1000 * 60 * 60);
        }
      }

      return {
        market_id: row.market_id,
        title: row.title,
        category: row.category,
        snapshot_ts: row.snapshot_ts,
        yes_token_id: row.yes_token_id,
        yes_price: row.yes_price,
        fair_value: row.fair_value,
        expected_value: row.expected_value,
        mispricing_score: mispricingScore,
        trade_signal: row.trade_signal,
        kelly_fraction: row.kelly_fraction,
        volatility_1w: row.volatility_1w,
        degen_risk: row.degen_risk,
        liquidity: row.liquidity,
        volume: row.volume,
        spread: row.spread,
        end_date: row.end_date,
        time_to_resolution_hours: timeToResolutionHours
      };
    });

    const rankedSignals = markets
      .filter((row) => row.mispricing_score != null)
      .sort((a, b) => b.mispricing_score - a.mispricing_score)
      .map((row, index) => ({
        rank: index + 1,
        ...row
      }));

    let selectedMarket = null;
    if (markets.length) {
      const requestedMarketId = req.query.marketId ? String(req.query.marketId) : null;
      selectedMarket = requestedMarketId
        ? markets.find((row) => row.market_id === requestedMarketId) || markets[0]
        : markets[0];
    }

    let selectedHistory = [];
    if (selectedMarket?.yes_token_id) {
      const historyRows = await getPriceHistory(
        client,
        selectedMarket.yes_token_id,
        historyInterval,
        safeHistoryLimit
      );
      selectedHistory = historyRows.map((row) => ({
        t: row.t,
        price: row.price,
        interval: row.interval,
        fidelity_min: row.fidelity_min
      }));
    }

    return res.json({
      count: markets.length,
      generated_at: new Date(now).toISOString(),
      markets,
      ranked_signals: rankedSignals,
      selected_market: selectedMarket,
      selected_market_history: selectedHistory
    });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  } finally {
    await client.close();
  }
});

app.post("/api/signals/check", async (req, res) => {
  try {
    const url = req.body?.url;
    if (!url) {
      return res.status(400).json({ error: "url is required" });
    }

    const result = await checkSignals({ url });
    return res.json(result);
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

app.listen(config.port, async () => {
  const client = createClient(config.duckdbPath);
  try {
    await ensureTables(client);
  } finally {
    await client.close();
  }

  startScheduler();
  console.log(`Server listening on port ${config.port}`);
});
