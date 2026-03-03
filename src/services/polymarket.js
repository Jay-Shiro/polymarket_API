const { URL } = require("url");
const { config } = require("../config");
const { retryGet } = require("../utils/http");

function parsePolymarketUrl(inputUrl) {
  const parsed = new URL(inputUrl);
  const parts = parsed.pathname.split("/").filter(Boolean);

  if (!parts.length) {
    throw new Error("Unrecognized Polymarket URL path");
  }

  if (["event", "events"].includes(parts[0])) {
    if (!parts[1]) throw new Error("Event URL missing slug segment");
    return { kind: "event", slug: parts[1] };
  }

  if (["market", "markets"].includes(parts[0])) {
    if (!parts[1]) throw new Error("Market URL missing slug segment");
    return { kind: "market", slug: parts[1] };
  }

  return { kind: "event", slug: parts[0] };
}

function normalizeClobTokenIds(raw) {
  if (raw == null) return [];

  if (Array.isArray(raw)) {
    return raw.map((x) => String(x)).filter(Boolean);
  }

  if (typeof raw === "string") {
    const text = raw.trim();
    if (!text) return [];

    if (text.startsWith("[") && text.endsWith("]")) {
      try {
        const parsed = JSON.parse(text);
        if (Array.isArray(parsed)) {
          return parsed.map((x) => String(x)).filter(Boolean);
        }
      } catch {
        return text.replaceAll(";", ",").split(",").map((x) => x.trim()).filter(Boolean);
      }
    }

    return text.replaceAll(";", ",").split(",").map((x) => x.trim()).filter(Boolean);
  }

  return [String(raw)];
}

async function getEventBySlug(slug) {
  const response = await retryGet(`${config.gammaBase}/events/slug/${slug}`);
  return response.data;
}

async function getEventById(id) {
  const response = await retryGet(`${config.gammaBase}/events/${id}`);
  return response.data;
}

async function getMarketBySlug(slug) {
  const response = await retryGet(`${config.gammaBase}/markets/slug/${slug}`);
  return response.data;
}

async function getMarketById(id) {
  const response = await retryGet(`${config.gammaBase}/markets/${id}`);
  return response.data;
}

function getYesNoTokenIds(market) {
  const metadata = {
    mapping_source: null,
    outcomes: null,
    yes_index: null,
    no_index: null,
    mapping_ok: false,
    mapping_warning: null
  };

  const clobIds = normalizeClobTokenIds(market.clobTokenIds);
  let outcomes = market.outcomes;

  if (typeof outcomes === "string") {
    const text = outcomes.trim();
    if (text.startsWith("[") && text.endsWith("]")) {
      try {
        outcomes = JSON.parse(text);
      } catch {
        outcomes = null;
      }
    } else {
      outcomes = null;
    }
  }

  if (Array.isArray(outcomes) && outcomes.length >= 2 && clobIds.length >= 2) {
    metadata.mapping_source = "outcomes";
    metadata.outcomes = outcomes;
    const norm = outcomes.map((value) => String(value).trim().toLowerCase());
    const yesIndex = norm.indexOf("yes");
    const noIndex = norm.indexOf("no");
    metadata.yes_index = yesIndex >= 0 ? yesIndex : null;
    metadata.no_index = noIndex >= 0 ? noIndex : null;

    if (
      yesIndex >= 0 &&
      noIndex >= 0 &&
      yesIndex !== noIndex &&
      yesIndex < clobIds.length &&
      noIndex < clobIds.length
    ) {
      metadata.mapping_ok = true;
      return {
        yesTokenId: clobIds[yesIndex],
        noTokenId: clobIds[noIndex],
        metadata
      };
    }

    metadata.mapping_warning = "outcomes_present_but_yes_no_not_found_or_misaligned";
  }

  metadata.mapping_source = "fallback_first_two";
  const yesTokenId = clobIds[0] || null;
  const noTokenId = clobIds[1] || null;
  metadata.mapping_ok = Boolean(yesTokenId && noTokenId);
  if (!metadata.mapping_ok) {
    metadata.mapping_warning = "missing_clob_token_ids";
  }

  return { yesTokenId, noTokenId, metadata };
}

async function resolveMarketsFromUrl(inputUrl) {
  const info = parsePolymarketUrl(inputUrl);
  const markets = [];
  let eventObject = null;

  if (info.kind === "event") {
    eventObject = await getEventBySlug(info.slug);
    const rawMarkets = (eventObject.markets || []).filter((market) => !market.closed);

    for (const market of rawMarkets) {
      let fullMarket = market;
      const clobIds = normalizeClobTokenIds(fullMarket.clobTokenIds);
      if (!clobIds.length) {
        const marketId = fullMarket.id || fullMarket.marketId || fullMarket.conditionId;
        if (marketId != null) {
          try {
            fullMarket = await getMarketById(marketId);
          } catch {
            fullMarket = market;
          }
        }
      }
      markets.push(fullMarket);
    }
  } else {
    let market;
    try {
      market = await getMarketBySlug(info.slug);
    } catch (error) {
      if (/^\d+$/.test(info.slug)) {
        market = await getMarketById(info.slug);
      } else {
        throw error;
      }
    }

    markets.push(market);

    const eventId = market.eventId || market.event_id || market.event || market.eventSlug;
    if (eventId) {
      try {
        eventObject = /^\d+$/.test(String(eventId))
          ? await getEventById(eventId)
          : await getEventBySlug(eventId);
      } catch {
        eventObject = null;
      }
    }
  }

  return { markets, eventObject };
}

function normalizeBookEntries(entries, tokenId, side, depth) {
  return (entries || []).slice(0, depth).map((entry, index) => {
    const price = entry.price ?? entry.px;
    const size = entry.size ?? entry.qty;
    if (price == null || size == null) return null;

    return {
      token_id: tokenId,
      side,
      level: index + 1,
      price: Number(price),
      size: Number(size)
    };
  }).filter(Boolean);
}

async function fetchOrderbook(tokenId, depth = config.defaultDepth) {
  try {
    const response = await retryGet(`${config.clobBase}/book`, { token_id: tokenId });
    const data = response.data || {};

    return {
      bids: normalizeBookEntries(data.bids, tokenId, "bid", depth),
      asks: normalizeBookEntries(data.asks, tokenId, "ask", depth),
      last_trade_price: data.last_trade_price != null && data.last_trade_price !== "" ? Number(data.last_trade_price) : null,
      tick_size: data.tick_size != null && data.tick_size !== "" ? Number(data.tick_size) : null,
      min_order_size: data.min_order_size != null && data.min_order_size !== "" ? Number(data.min_order_size) : null
    };
  } catch (error) {
    if (error.response && error.response.status === 404) {
      return { bids: [], asks: [], market: tokenId };
    }
    throw error;
  }
}

async function fetchPricesHistory(tokenId, interval, fidelityMin = config.defaultFidelityMin) {
  const response = await retryGet(`${config.clobBase}/prices-history`, {
    market: tokenId,
    interval,
    fidelity: fidelityMin
  });

  const payload = response.data || {};
  const rows = [];

  for (const point of payload.history || []) {
    if (point.t == null || point.p == null) continue;
    const timestamp = new Date(Number(point.t) * 1000);
    if (Number.isNaN(timestamp.getTime())) continue;

    rows.push({
      token_id: tokenId,
      t: timestamp,
      interval,
      fidelity_min: Number(fidelityMin),
      price: Number(point.p)
    });
  }

  return rows;
}

module.exports = {
  parsePolymarketUrl,
  normalizeClobTokenIds,
  getYesNoTokenIds,
  resolveMarketsFromUrl,
  fetchOrderbook,
  fetchPricesHistory
};
