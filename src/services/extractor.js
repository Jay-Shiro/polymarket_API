const { config } = require("../config");
const { utcNow } = require("../utils/time");
const {
  resolveMarketsFromUrl,
  normalizeClobTokenIds,
  getYesNoTokenIds,
  fetchOrderbook,
  fetchPricesHistory
} = require("./polymarket");
const { assembleMarketStats } = require("./analytics");
const {
  createClient,
  ensureTables,
  upsertOrderbook,
  upsertHistory,
  upsertMarketStats
} = require("../db/duckdb");

async function extractFromUrl({
  url,
  depth = config.defaultDepth,
  intervals = config.defaultIntervals,
  fidelityMin = config.defaultFidelityMin,
  baseRate = config.baseRate,
  persist = true
}) {
  const { markets, eventObject } = await resolveMarketsFromUrl(url);
  const asOf = utcNow();

  const allOrderbookRows = [];
  const allHistoryRows = [];
  const allStatsRows = [];

  for (const marketInput of markets) {
    let market = marketInput;
    const marketId = market.id || market.marketId || market.conditionId;

    let clobIds = normalizeClobTokenIds(market.clobTokenIds);
    if (!clobIds.length && marketId != null) {
      clobIds = normalizeClobTokenIds(market.clobTokenIds);
    }

    const tokenResolution = getYesNoTokenIds(market);
    const tokenIds = [tokenResolution.yesTokenId, tokenResolution.noTokenId].filter(Boolean);

    const orderbookMap = new Map();
    const historyMap = new Map();

    for (const tokenId of tokenIds) {
      const orderbook = await fetchOrderbook(tokenId, depth);
      orderbookMap.set(tokenId, orderbook);
      allOrderbookRows.push(...orderbook.bids, ...orderbook.asks);

      for (const interval of intervals) {
        const historyRows = await fetchPricesHistory(tokenId, interval, fidelityMin);
        historyMap.set(`${tokenId}|${interval}`, historyRows);
        allHistoryRows.push(...historyRows);
      }
    }

    const statsRow = assembleMarketStats({
      market,
      eventObject,
      orderbookMap,
      historyMap,
      asOf,
      baseRate,
      tokenResolution
    });

    allStatsRows.push(statsRow);
  }

  if (persist) {
    const client = createClient(config.duckdbPath);
    try {
      await ensureTables(client);
      await upsertOrderbook(client, allOrderbookRows, asOf);
      await upsertHistory(client, allHistoryRows);
      await upsertMarketStats(client, allStatsRows);
    } finally {
      await client.close();
    }
  }

  return {
    asof: asOf.toISOString(),
    markets: markets.map((market) => ({
      market_id: String(market.id || market.marketId || market.conditionId || ""),
      title: market.question || market.title || null
    })),
    orderbook_rows: allOrderbookRows.length,
    history_rows: allHistoryRows.length,
    stats_rows: allStatsRows.length,
    stats: allStatsRows
  };
}

module.exports = { extractFromUrl };
