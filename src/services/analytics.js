function firstOrNull(items) {
  return items && items.length ? items[0] : null;
}

function getCategory(market, eventObject) {
  for (const obj of [market, eventObject || {}]) {
    if (!obj) continue;
    if (obj.category) return obj.category;
    if (Array.isArray(obj.categories) && obj.categories.length) return obj.categories[0];
    if (typeof obj.categories === "string" && obj.categories.trim()) return obj.categories.trim();
  }
  return null;
}

function bestAsk(orderbook) {
  const ask = firstOrNull(orderbook?.asks);
  return ask ? Number(ask.price) : null;
}

function bestBid(orderbook) {
  const bid = firstOrNull(orderbook?.bids);
  return bid ? Number(bid.price) : null;
}

function latestHistoryPrice(history) {
  if (!history?.length) return null;
  const sorted = [...history].sort((a, b) => new Date(a.t) - new Date(b.t));
  return Number(sorted[sorted.length - 1].price);
}

function roundToTick(value, tick) {
  if (value == null) return null;
  if (tick == null || tick <= 0) return Number(value);
  return Number((Math.round(value / tick) * tick).toFixed(4));
}

function computeDepthLiquidity(orderbook, levels = 5) {
  if (!orderbook) return 0;
  let total = 0;
  for (const side of ["bids", "asks"]) {
    for (const level of (orderbook[side] || []).slice(0, levels)) {
      const price = Number(level.price || 0);
      const size = Number(level.size || 0);
      total += price * size;
    }
  }
  return total;
}

function toReturns(series) {
  const out = [];
  for (let i = 1; i < series.length; i += 1) {
    const prev = series[i - 1];
    const curr = series[i];
    if (!Number.isFinite(prev) || !Number.isFinite(curr) || prev === 0) continue;
    out.push(curr / prev - 1);
  }
  return out;
}

function std(values) {
  if (!values.length) return null;
  const mean = values.reduce((sum, value) => sum + value, 0) / values.length;
  const variance = values.reduce((sum, value) => sum + (value - mean) ** 2, 0) / values.length;
  return Math.sqrt(variance);
}

function computeVolatility(series) {
  if (!series || series.length < 30) return null;
  const returns = toReturns(series);
  if (returns.length < 20) return null;
  const rolling = returns.slice(-20);
  return std(rolling);
}

function movingAverage(series, windowSize) {
  if (!series || series.length < windowSize) return null;
  const window = series.slice(-windowSize);
  return window.reduce((sum, value) => sum + value, 0) / window.length;
}

function computeMovingAverages(series) {
  if (!series || series.length < 96) return { maShort: null, maLong: null };
  return {
    maShort: movingAverage(series, 24),
    maLong: movingAverage(series, 96)
  };
}

function computeEma(series, span) {
  if (!series?.length) return [];
  const alpha = 2 / (span + 1);
  const ema = [series[0]];
  for (let i = 1; i < series.length; i += 1) {
    ema.push(alpha * series[i] + (1 - alpha) * ema[i - 1]);
  }
  return ema;
}

function computeEmaSlope(series) {
  if (!series || series.length < 25) return null;
  const ema = computeEma(series, 48);
  const tail = ema.slice(-20);
  if (tail.length < 2) return null;

  const x = tail.map((_, index) => index);
  const xMean = x.reduce((sum, value) => sum + value, 0) / x.length;
  const yMean = tail.reduce((sum, value) => sum + value, 0) / tail.length;

  let covariance = 0;
  let varianceX = 0;
  for (let i = 0; i < x.length; i += 1) {
    covariance += (x[i] - xMean) * (tail[i] - yMean);
    varianceX += (x[i] - xMean) ** 2;
  }

  if (varianceX === 0) return null;
  return covariance / varianceX;
}

function detectOverreaction(series, z = 2.5) {
  if (!series || series.length < 60) return false;
  const returns = toReturns(series);
  if (returns.length < 41) return false;

  const base = returns.slice(-41, -1);
  const latest = returns[returns.length - 1];
  const mu = base.reduce((sum, value) => sum + value, 0) / base.length;
  const sigma = std(base);
  if (!sigma) return false;

  const zScore = (latest - mu) / sigma;
  return Math.abs(zScore) >= z;
}

function computeOrderbookImbalance(orderbook) {
  if (!orderbook) return null;
  const bids = (orderbook.bids || []).slice(0, 5);
  const asks = (orderbook.asks || []).slice(0, 5);
  const bidQty = bids.reduce((sum, value) => sum + Number(value.size || 0), 0);
  const askQty = asks.reduce((sum, value) => sum + Number(value.size || 0), 0);
  const total = bidQty + askQty;
  if (total === 0) return null;
  return (bidQty - askQty) / total;
}

function computeSlippage(orderbook, notional) {
  if (!orderbook) return null;
  const bids = orderbook.bids || [];
  const asks = orderbook.asks || [];
  if (!bids.length || !asks.length) return null;

  const mid = (Number(bids[0].price) + Number(asks[0].price)) / 2;
  let remaining = notional;
  let spent = 0;
  let shares = 0;

  for (const level of asks) {
    const price = Number(level.price);
    const qty = Number(level.size);
    const capacity = price * qty;
    const take = Math.min(remaining, capacity);
    const gotShares = take / price;
    spent += gotShares * price;
    shares += gotShares;
    remaining -= take;
    if (remaining <= 0) break;
  }

  if (shares === 0 || mid === 0) return null;
  const avg = spent / shares;
  return ((avg - mid) / mid) * 10000;
}

function computeFairValue(yesPrice, baseRate, momentum, volatility) {
  if (yesPrice == null) return baseRate;
  const shrink = 1 / (1 + 10 * (volatility || 0));
  const tilt = Math.tanh((momentum || 0) * 1e5);
  const fair = shrink * baseRate + (1 - shrink) * yesPrice + 0.02 * tilt;
  return Math.min(Math.max(fair, 0.01), 0.99);
}

function computeExpectedValue(fairValue, price) {
  if (fairValue == null || price == null) return null;
  return fairValue - price;
}

function computeKelly(fairValue, price) {
  if (fairValue == null || price == null || price <= 0 || price >= 1) return null;
  const p = fairValue;
  const q = 1 - p;
  const b = 1 / price - 1;
  if (b <= 0) return null;
  const fraction = (b * p - q) / b;
  return Math.max(0, Math.min(1, fraction)) * 0.5;
}

function computeTradeSignal(expectedValue, volatility) {
  if (expectedValue == null) return "no-trade";
  const expectedBasisPoints = expectedValue * 10000;
  if (expectedBasisPoints > 10 && (volatility == null || volatility < 0.05)) return "long";
  if (expectedBasisPoints < -10 && (volatility == null || volatility < 0.05)) return "short";
  return "no-trade";
}

function detectLateOverconfidence(yesPrice, imbalance, bestBidNo) {
  if (yesPrice == null || yesPrice < 0.9) return false;
  if (imbalance != null && imbalance > 0.5) return true;
  if (bestBidNo != null && bestBidNo < 0.05) return true;
  return false;
}

function regressionSlope(history) {
  if (!history || history.length < 3) return 0;
  const clean = history
    .map((row) => ({
      x: new Date(row.t).getTime() / 1000,
      y: Number(row.price)
    }))
    .filter((row) => Number.isFinite(row.x) && Number.isFinite(row.y))
    .sort((a, b) => a.x - b.x);

  if (clean.length < 3) return 0;

  const uniqueX = new Set(clean.map((row) => row.x));
  if (uniqueX.size < 2) return 0;

  const xMean = clean.reduce((sum, row) => sum + row.x, 0) / clean.length;
  const yMean = clean.reduce((sum, row) => sum + row.y, 0) / clean.length;

  let covariance = 0;
  let varianceX = 0;
  for (const row of clean) {
    covariance += (row.x - xMean) * (row.y - yMean);
    varianceX += (row.x - xMean) ** 2;
  }

  if (varianceX === 0) return 0;

  const slope = covariance / varianceX;
  const span = clean[clean.length - 1].x - clean[0].x;
  if (span <= 0) return 0;
  return slope / span;
}

function displayPriceFromOrderbook(orderbook, lastTradeFallback) {
  if (!orderbook) return lastTradeFallback;
  const bid = bestBid(orderbook);
  const ask = bestAsk(orderbook);
  let last = lastTradeFallback;
  if (last == null && orderbook.last_trade_price != null) {
    last = Number(orderbook.last_trade_price);
  }

  if (bid != null && ask != null) {
    const spread = ask - bid;
    if (spread > 0.1 && last != null) return last;
    return (bid + ask) / 2;
  }

  return last ?? bid ?? ask ?? null;
}

function assembleMarketStats({
  market,
  eventObject,
  orderbookMap,
  historyMap,
  asOf,
  baseRate,
  tokenResolution
}) {
  const marketId = String(market.id || market.marketId || market.conditionId || "");
  const title = market.question || market.title || null;
  const category = getCategory(market, eventObject);
  const yesTokenId = tokenResolution.yesTokenId;
  const noTokenId = tokenResolution.noTokenId;

  const orderbookYes = yesTokenId ? orderbookMap.get(yesTokenId) : null;
  const orderbookNo = noTokenId ? orderbookMap.get(noTokenId) : null;

  const yesBestAsk = bestAsk(orderbookYes);
  const yesBestBid = bestBid(orderbookYes);
  const noBestAsk = bestAsk(orderbookNo);
  const noBestBid = bestBid(orderbookNo);

  const yesMidpoint = yesBestAsk != null && yesBestBid != null ? (yesBestAsk + yesBestBid) / 2 : null;
  const noMidpoint = noBestAsk != null && noBestBid != null ? (noBestAsk + noBestBid) / 2 : null;

  let spread = market.spread;
  if (spread == null && yesBestAsk != null && yesBestBid != null) {
    spread = Math.max(0, yesBestAsk - yesBestBid);
  }

  const tickSize = (orderbookYes && orderbookYes.tick_size) || market.orderPriceMinTickSize || 0.01;
  const minOrderSize = (orderbookYes && orderbookYes.min_order_size) || market.orderMinSize || null;

  const histYes1w = yesTokenId ? historyMap.get(`${yesTokenId}|1w`) || [] : [];
  const histYes1m = yesTokenId ? historyMap.get(`${yesTokenId}|1m`) || [] : [];
  const lastTradeFromHist = latestHistoryPrice(histYes1w) ?? latestHistoryPrice(histYes1m);
  const lastTradePrice = lastTradeFromHist ?? orderbookYes?.last_trade_price ?? null;

  const negRisk = Boolean(market.negRisk);

  let clobLastTradeAnomaly = false;
  if (yesTokenId && noTokenId) {
    const yesLt = orderbookYes?.last_trade_price;
    const noLt = orderbookNo?.last_trade_price;
    clobLastTradeAnomaly = Boolean(
      negRisk && yesLt != null && noLt != null && Number(yesLt) === Number(noLt)
    );
  }

  const yesDisplayRaw = displayPriceFromOrderbook(orderbookYes, lastTradePrice);

  const histNo1w = noTokenId ? historyMap.get(`${noTokenId}|1w`) || [] : [];
  const histNo1m = noTokenId ? historyMap.get(`${noTokenId}|1m`) || [] : [];
  const noTradeFromHist = latestHistoryPrice(histNo1w) ?? latestHistoryPrice(histNo1m);
  const noLastTrade = noTradeFromHist ?? orderbookNo?.last_trade_price ?? null;
  const noDisplayRaw = displayPriceFromOrderbook(orderbookNo, noLastTrade);

  const isBinary = Boolean(yesTokenId && noTokenId);

  const yesDisplayPrice = roundToTick(yesDisplayRaw, tickSize);
  const noDisplayPrice = roundToTick(noDisplayRaw, tickSize);

  const uiYesPrice = yesDisplayPrice;
  const uiNoPrice = isBinary && uiYesPrice != null && !negRisk
    ? roundToTick(1 - uiYesPrice, tickSize)
    : noDisplayPrice;

  const yesSeries = histYes1w
    .slice()
    .sort((a, b) => new Date(a.t) - new Date(b.t))
    .map((row) => Number(row.price))
    .filter((value) => Number.isFinite(value));

  const volatility = computeVolatility(yesSeries);
  const { maShort, maLong } = computeMovingAverages(yesSeries);
  const emaSlope = computeEmaSlope(yesSeries);
  const overreaction = detectOverreaction(yesSeries);
  const imbalance = computeOrderbookImbalance(orderbookYes);
  const slippage1k = computeSlippage(orderbookYes, 1000);
  const slippage10k = computeSlippage(orderbookYes, 10000);

  const fairValue = computeFairValue(uiYesPrice, baseRate, emaSlope, volatility);
  const expectedValue = computeExpectedValue(fairValue, uiYesPrice);
  const kelly = computeKelly(fairValue, uiYesPrice);
  const tradeSignal = computeTradeSignal(expectedValue, volatility);
  const lateOverconfidence = detectLateOverconfidence(uiYesPrice, imbalance, noBestBid);

  const slope = regressionSlope(histYes1w);
  const depthLiquidity = computeDepthLiquidity(orderbookYes);
  const eventLiquidity = Number(eventObject?.liquidity || 0);
  const eventLiquidityClob = Number(eventObject?.liquidityClob || 0);

  const baseRateDeviation = uiYesPrice != null ? uiYesPrice - baseRate : null;
  const liquidityScore = Math.log1p(Math.max(depthLiquidity + eventLiquidity + eventLiquidityClob, 0)) / (1 + Number(spread || 0));
  const spreadNorm = Math.min(Math.max(Number(spread || 0), 0), 0.5) / 0.5;
  const momentumNorm = Math.min(Math.abs(slope) * 10, 1);
  const liqInv = 1 - liquidityScore / (1 + liquidityScore);
  const degenRisk = 0.45 * spreadNorm + 0.35 * momentumNorm + 0.2 * liqInv;

  return {
    market_id: marketId,
    snapshot_ts: asOf,
    title,
    category,
    yes_token_id: yesTokenId,
    no_token_id: noTokenId,
    yes_price: uiYesPrice,
    no_price: uiNoPrice,
    yes_midpoint: roundToTick(yesMidpoint, tickSize),
    no_midpoint: roundToTick(noMidpoint, tickSize),
    yes_last_trade: roundToTick(lastTradeFromHist, tickSize),
    no_last_trade: roundToTick(noTradeFromHist, tickSize),
    yes_display_price: yesDisplayPrice,
    no_display_price: noDisplayPrice,
    ui_yes_price: uiYesPrice,
    ui_no_price: uiNoPrice,
    token_mapping_source: tokenResolution.metadata.mapping_source,
    token_mapping_ok: Boolean(tokenResolution.metadata.mapping_ok),
    token_mapping_warning: tokenResolution.metadata.mapping_warning,
    token_mapping_anomaly: Boolean(tokenResolution.metadata.outcomes) && !Boolean(tokenResolution.metadata.mapping_ok),
    clob_last_trade_anomaly: clobLastTradeAnomaly,
    best_ask_yes: yesBestAsk,
    best_bid_yes: yesBestBid,
    best_ask_no: noBestAsk,
    best_bid_no: noBestBid,
    last_trade_price: lastTradePrice,
    volume: Number(market.volumeNum || 0),
    volume_clob: Number(market.volumeClob || 0),
    volume_1wk: Number(market.volume1wk || 0),
    volume_1mo: Number(market.volume1mo || 0),
    liquidity: eventLiquidity,
    liquidity_clob: eventLiquidityClob,
    spread,
    order_min_size: minOrderSize,
    min_tick: tickSize,
    price_change_1d: market.oneDayPriceChange ?? null,
    price_change_1wk: market.oneWeekPriceChange ?? null,
    price_change_1mo: market.oneMonthPriceChange ?? null,
    price_change_1yr: market.oneYearPriceChange ?? null,
    start_date: market.startDateIso || null,
    end_date: market.endDateIso || null,
    accepting_orders_since: market.acceptingOrdersTimestamp || null,
    active: Boolean(market.active),
    closed: Boolean(market.closed),
    funded: market.funded ?? null,
    ready: market.ready ?? null,
    neg_risk: negRisk,
    neg_risk_other: market.negRiskOther ?? null,
    uma_resolution_status: market.umaResolutionStatus ?? null,
    automatically_resolved: market.automaticallyResolved ?? null,
    created_at: market.createdAt || null,
    updated_at: market.updatedAt || null,
    volatility_1w: volatility,
    ma_short: maShort,
    ma_long: maLong,
    ema_slope: emaSlope,
    overreaction_flag: Boolean(overreaction),
    orderbook_imbalance: imbalance,
    slippage_notional_1k: slippage1k,
    slippage_notional_10k: slippage10k,
    fair_value: fairValue,
    expected_value: expectedValue,
    kelly_fraction: kelly,
    trade_signal: tradeSignal,
    late_overconfidence: lateOverconfidence,
    base_rate: baseRate,
    base_rate_deviation: baseRateDeviation,
    sentiment_momentum: slope,
    liquidity_score: liquidityScore,
    degen_risk: degenRisk
  };
}

module.exports = {
  assembleMarketStats
};
