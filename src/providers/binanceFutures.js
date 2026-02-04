import axios from "axios";

const BASE = "https://fapi.binance.com";

function isNum(x) {
  return typeof x === "number" && Number.isFinite(x);
}

function mean(arr) {
  const a = arr.filter(isNum);
  if (!a.length) return null;
  return a.reduce((s, v) => s + v, 0) / a.length;
}

function std(arr) {
  const m = mean(arr);
  if (!isNum(m)) return null;
  const a = arr.filter(isNum);
  if (a.length < 2) return null;
  const v = a.reduce((s, x) => s + (x - m) ** 2, 0) / (a.length - 1);
  return Math.sqrt(v);
}

async function fetchBookTicker({ symbol }) {
  // best bid/ask + sizes
  const url = `${BASE}/fapi/v1/ticker/bookTicker`;
  const res = await axios.get(url, { params: { symbol }, timeout: 60_000 });
  const d = res.data || {};
  return {
    bidPrice: d.bidPrice != null ? Number(d.bidPrice) : null,
    bidQty: d.bidQty != null ? Number(d.bidQty) : null,
    askPrice: d.askPrice != null ? Number(d.askPrice) : null,
    askQty: d.askQty != null ? Number(d.askQty) : null
  };
}

async function fetchDepth({ symbol, limit = 100 }) {
  // full depth snapshot (public). We'll compute a few liquidity/imbalance metrics from it.
  const url = `${BASE}/fapi/v1/depth`;
  const res = await axios.get(url, { params: { symbol, limit }, timeout: 60_000 });
  const d = res.data || {};

  const parseSide = (side) =>
    (Array.isArray(side) ? side : []).map((lvl) => ({
      price: lvl?.[0] != null ? Number(lvl[0]) : null,
      qty: lvl?.[1] != null ? Number(lvl[1]) : null
    })).filter((x) => isNum(x.price) && isNum(x.qty) && x.qty > 0);

  return {
    lastUpdateId: d.lastUpdateId ?? null,
    bids: parseSide(d.bids),
    asks: parseSide(d.asks)
  };
}

async function fetch24h({ symbol }) {
  const url = `${BASE}/fapi/v1/ticker/24hr`;
  const res = await axios.get(url, { params: { symbol }, timeout: 60_000 });
  const d = res.data || {};
  return {
    volume: d.volume != null ? Number(d.volume) : null,
    quoteVolume: d.quoteVolume != null ? Number(d.quoteVolume) : null,
    count: d.count != null ? Number(d.count) : null
  };
}

/**
 * Funding rate history: GET /fapi/v1/fundingRate
 * Open interest: GET /fapi/v1/openInterest
 * Premium index (mark/index): GET /fapi/v1/premiumIndex
 *
 * All are public endpoints.
 */
export async function fetchFundingRates({ symbol, limit = 21 }) {
  const url = `${BASE}/fapi/v1/fundingRate`;
  const res = await axios.get(url, { params: { symbol, limit }, timeout: 60_000 });
  // returns array newest->oldest? docs are chronological; we'll just sort by fundingTime
  const arr = Array.isArray(res.data) ? res.data : [];
  arr.sort((a, b) => (a.fundingTime ?? 0) - (b.fundingTime ?? 0));
  return arr.map((x) => ({
    fundingTime: x.fundingTime,
    fundingRate: x.fundingRate != null ? Number(x.fundingRate) : null
  }));
}

export async function fetchOpenInterest({ symbol }) {
  const url = `${BASE}/fapi/v1/openInterest`;
  const res = await axios.get(url, { params: { symbol }, timeout: 60_000 });
  return {
    openInterest: res.data?.openInterest != null ? Number(res.data.openInterest) : null,
    time: res.data?.time ?? null
  };
}

export async function fetchPremiumIndex({ symbol }) {
  const url = `${BASE}/fapi/v1/premiumIndex`;
  const res = await axios.get(url, { params: { symbol }, timeout: 60_000 });
  const d = res.data || {};
  const markPrice = d.markPrice != null ? Number(d.markPrice) : null;
  const indexPrice = d.indexPrice != null ? Number(d.indexPrice) : null;
  const basis = markPrice != null && indexPrice ? (markPrice - indexPrice) / indexPrice : null;
  return {
    markPrice,
    indexPrice,
    lastFundingRate: d.lastFundingRate != null ? Number(d.lastFundingRate) : null,
    nextFundingTime: d.nextFundingTime ?? null,
    basis
  };
}

export async function fetchDerivativesSnapshot({ symbol }) {
  // Best-effort: if any endpoint fails, throw and caller can ignore.
  // We intentionally compute temporal features *from the same run's fetched histories*
  // so they are available even on --run-now (no local history needed).
  const [funding, oi, prem, book, depth, t24h] = await Promise.all([
    fetchFundingRates({ symbol, limit: 21 }),
    fetchOpenInterest({ symbol }),
    fetchPremiumIndex({ symbol }),
    fetchBookTicker({ symbol }),
    fetchDepth({ symbol, limit: 100 }),
    fetch24h({ symbol })
  ]);

  const lastFunding = funding.length ? funding[funding.length - 1] : null;
  const avgFunding7d = (() => {
    // 3 funding intervals per day => 21 ~ 7 days
    const rates = funding.map((x) => x.fundingRate).filter((v) => typeof v === "number" && isFinite(v));
    if (!rates.length) return null;
    const sum = rates.reduce((a, v) => a + v, 0);
    return sum / rates.length;
  })();

  // Funding temporal features from the 7d history
  const rates = funding.map((x) => x.fundingRate).filter((v) => typeof v === "number" && isFinite(v));
  const fundingStd7d = std(rates);
  const fundingLast = lastFunding?.fundingRate ?? null;
  const fundingZ7d = isNum(fundingLast) && isNum(avgFunding7d) && isNum(fundingStd7d) && fundingStd7d > 0
    ? (fundingLast - avgFunding7d) / fundingStd7d
    : null;
  // 3 funding prints/day. Rough 1d delta: compare to 3 intervals ago.
  const fundingPrev1d = funding.length >= 4 ? funding[funding.length - 4]?.fundingRate : null;
  const fundingPrev7d = funding.length >= 21 ? funding[0]?.fundingRate : null;

  const fundingChange1d = isNum(fundingPrev1d) && isNum(fundingLast) ? fundingLast - fundingPrev1d : null;
  const fundingChange7d = isNum(fundingPrev7d) && isNum(fundingLast) ? fundingLast - fundingPrev7d : null;

  // Liquidity/order book features
  const bid = book?.bidPrice ?? null;
  const ask = book?.askPrice ?? null;
  const mid = isNum(bid) && isNum(ask) && bid > 0 && ask > 0 ? (bid + ask) / 2 : null;
  const spread = isNum(bid) && isNum(ask) ? (ask - bid) : null;
  const spreadBps = isNum(spread) && isNum(mid) && mid > 0 ? (spread / mid) * 10_000 : null;

  const sumQty = (arr) => (arr || []).reduce((s, x) => s + (isNum(x.qty) ? x.qty : 0), 0);
  const topN = (arr, n) => (arr || []).slice(0, Math.min(n, (arr || []).length));

  const top10BidQty = sumQty(topN(depth?.bids, 10));
  const top10AskQty = sumQty(topN(depth?.asks, 10));
  const top10Imbalance = (top10BidQty + top10AskQty) > 0
    ? (top10BidQty - top10AskQty) / (top10BidQty + top10AskQty)
    : null;

  // Depth within +/- 10 bps of mid
  const inBps = (price, side) => {
    if (!isNum(price) || !isNum(mid) || mid <= 0) return false;
    const bps = Math.abs((price - mid) / mid) * 10_000;
    // For bids, price <= mid; for asks, price >= mid.
    if (side === "bid" && price > mid) return false;
    if (side === "ask" && price < mid) return false;
    return bps <= 10;
  };

  const depthUsd10bpsBid = isNum(mid)
    ? (depth?.bids || []).filter((x) => inBps(x.price, "bid")).reduce((s, x) => s + x.qty * mid, 0)
    : null;
  const depthUsd10bpsAsk = isNum(mid)
    ? (depth?.asks || []).filter((x) => inBps(x.price, "ask")).reduce((s, x) => s + x.qty * mid, 0)
    : null;

  return {
    binance_symbol: symbol,
    funding: {
      last: lastFunding,
      avg_7d: avgFunding7d,
      std_7d: fundingStd7d,
      z_7d: fundingZ7d,
      change_1d: fundingChange1d,
      change_7d: fundingChange7d
    },
    open_interest: oi,
    pricing: prem,
    liquidity: {
      book: { bid, ask, bidQty: book?.bidQty ?? null, askQty: book?.askQty ?? null },
      spread_bps: spreadBps,
      top10_imbalance: top10Imbalance,
      depth_usd_10bps: {
        bid: depthUsd10bpsBid,
        ask: depthUsd10bpsAsk
      },
      vol_24h: {
        base: t24h?.volume ?? null,
        quote: t24h?.quoteVolume ?? null,
        trades: t24h?.count ?? null
      }
    }
  };
}
