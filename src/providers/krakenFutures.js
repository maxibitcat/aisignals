import axios from "axios";

/**
 * Kraken Futures public REST API.
 * Base: https://futures.kraken.com/derivatives/api/v3
 *
 * We use only public endpoints:
 * - GET /tickers (market snapshot for all instruments)
 * - GET /orderbook?symbol=...
 * - GET /historical-funding-rates?symbol=...
 *
 * This provider is intended as a drop-in replacement for the former Binance Futures provider.
 */

const BASE = "https://futures.kraken.com/derivatives/api/v3";

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

function normalizeSymbol(sym) {
  return String(sym || "").trim().toUpperCase();
}

/**
 * Existing app configs use Binance-style symbols like BTCUSDT, ETHUSDT.
 * Kraken Futures perpetuals commonly use PI_<BASE>USD where BTC is XBT.
 * This is best-effort and can be overridden by passing a Kraken symbol directly.
 */
function mapBinanceStyleToKraken(symbol) {
  const s = normalizeSymbol(symbol);
  // If user already provided a Kraken-style symbol, keep it.
  if (s.startsWith("PI_") || s.startsWith("PF_") || s.includes("_")) return s;

  const m = s.match(/^([A-Z0-9]+)(USDT|USD)$/);
  if (!m) return s; // fallback

  let base = m[1];
  const quote = "USD"; // we map USDT -> USD index perpetual where available
  if (base === "BTC") base = "XBT";
  return `PI_${base}${quote}`;
}

/**
 * Kraken lists perpetuals under multiple prefixes (commonly PI_ and PF_).
 * Some assets (e.g. KAS) may exist as PF_<BASE>USD rather than PI_<BASE>USD.
 * We resolve to a real listed symbol using the /tickers universe.
 */
async function resolveKrakenSymbol(inputSymbol) {
  const raw = normalizeSymbol(inputSymbol);

  // If the caller already gave something Kraken-like, try it as-is first.
  if (raw.startsWith("PI_") || raw.startsWith("PF_") || raw.includes("_")) return raw;

  const mapped = mapBinanceStyleToKraken(raw); // default PI_
  const baseGuess = mapped.replace(/^PI_/, "").replace(/USD$/, "");

  const tickers = await fetchTickers();
  const symbols = tickers.map((t) => normalizeSymbol(t?.symbol)).filter(Boolean);

  const candidates = [
    mapped,
    mapped.replace(/^PI_/, "PF_"),
    // some markets use the original base (BTC vs XBT) naming
    mapped.replace("PI_XBTUSD", "PI_BTCUSD"),
    mapped.replace("PI_XBTUSD", "PF_BTCUSD")
  ];

  for (const c of candidates) {
    if (symbols.includes(normalizeSymbol(c))) return normalizeSymbol(c);
  }

  // Fuzzy fallback: pick any perpetual-like USD symbol that contains the base.
  const fuzzy = symbols.find((s) => (s.startsWith("PF_") || s.startsWith("PI_")) && s.endsWith("USD") && s.includes(baseGuess));
  return fuzzy || mapped;
}

async function fetchTickers() {
  const url = `${BASE}/tickers`;
  const res = await axios.get(url, { timeout: 60_000 });
  // Response shape: { tickers: [ { symbol, bid, ask, markPrice, indexPrice, fundingRate, openInterest, ... }, ... ] }
  const tickers = Array.isArray(res.data?.tickers) ? res.data.tickers : [];
  return tickers;
}

async function fetchTickerBySymbol({ symbol }) {
  const sym = normalizeSymbol(symbol);
  const tickers = await fetchTickers();
  const t = tickers.find((x) => normalizeSymbol(x?.symbol) === sym);
  if (!t) {
    const available = tickers.slice(0, 25).map((x) => x?.symbol).filter(Boolean);
    throw new Error(`Kraken Futures ticker not found for symbol=${symbol}. Example symbols: ${available.join(", ")}`);
  }
  return t;
}

async function fetchOrderbook({ symbol }) {
  const url = `${BASE}/orderbook`;
  const res = await axios.get(url, { params: { symbol }, timeout: 60_000 });
  // Response shape: { result: "success", orderBook: { bids: [[price, qty], ...], asks: [[price, qty], ...] } }
  const ob = res.data?.orderBook || {};
  const parseSide = (side) =>
    (Array.isArray(side) ? side : [])
      .map((lvl) => ({
        price: lvl?.[0] != null ? Number(lvl[0]) : null,
        qty: lvl?.[1] != null ? Number(lvl[1]) : null
      }))
      .filter((x) => isNum(x.price) && isNum(x.qty) && x.qty > 0);

  return {
    bids: parseSide(ob.bids),
    asks: parseSide(ob.asks)
  };
}

export async function fetchFundingRates({ symbol, limit = 21 }) {
  const url = `${BASE}/historical-funding-rates`;
  const res = await axios.get(url, { params: { symbol }, timeout: 60_000 });
  // Response: { rates: [ { timestamp: ISO, fundingRate: number, relativeFundingRate: number }, ... ] }
  const rates = Array.isArray(res.data?.rates) ? res.data.rates : [];
  // Convert timestamp to ms for internal use, keep original ISO in case someone wants it.
  const mapped = rates
    .map((x) => ({
      fundingTime: x?.timestamp ? Date.parse(x.timestamp) : null,
      timestamp: x?.timestamp ?? null,
      fundingRate: x?.relativeFundingRate != null ? Number(x.relativeFundingRate) : (x?.fundingRate != null ? Number(x.fundingRate) : null)
    }))
    .filter((x) => x.fundingTime != null);

  mapped.sort((a, b) => (a.fundingTime ?? 0) - (b.fundingTime ?? 0));
  return mapped.slice(-Math.max(1, limit));
}

function pickClosestBefore(arr, targetMs) {
  // arr assumed sorted ascending by fundingTime
  let best = null;
  for (let i = arr.length - 1; i >= 0; i--) {
    const t = arr[i]?.fundingTime;
    if (t != null && t <= targetMs) {
      best = arr[i];
      break;
    }
  }
  return best;
}

export async function fetchDerivativesSnapshot({ symbol }) {
  const kSym = await resolveKrakenSymbol(symbol);

  const [ticker, orderbook, funding] = await Promise.all([
    fetchTickerBySymbol({ symbol: kSym }),
    fetchOrderbook({ symbol: kSym }),
    fetchFundingRates({ symbol: kSym, limit: 200 }) // pull more, we'll downsample by time windows
  ]);

  // Pricing (mark/index + basis)
  const markPrice = ticker?.markPrice != null ? Number(ticker.markPrice) : null;
  const indexPrice = ticker?.indexPrice != null ? Number(ticker.indexPrice) : null;
  const basis = isNum(markPrice) && isNum(indexPrice) && indexPrice !== 0 ? (markPrice - indexPrice) / indexPrice : null;

  // Funding features:
  // Use last 7d worth of points if available (we don't assume a fixed funding interval).
  const now = Date.now();
  const sevenDaysAgo = now - 7 * 24 * 60 * 60 * 1000;
  const funding7d = funding.filter((x) => (x.fundingTime ?? 0) >= sevenDaysAgo);
  const fundingUse = funding7d.length ? funding7d : funding; // fallback to whatever we have
  const lastFunding = fundingUse.length ? fundingUse[fundingUse.length - 1] : null;

  const rates = fundingUse.map((x) => x.fundingRate).filter((v) => typeof v === "number" && isFinite(v));
  const avgFunding7d = rates.length ? rates.reduce((a, v) => a + v, 0) / rates.length : null;
  const fundingStd7d = std(rates);
  const fundingLast = lastFunding?.fundingRate ?? null;
  const fundingZ7d =
    isNum(fundingLast) && isNum(avgFunding7d) && isNum(fundingStd7d) && fundingStd7d > 0 ? (fundingLast - avgFunding7d) / fundingStd7d : null;

  // 1d/7d change via timestamps (closest print <= target).
  const sortedAll = [...fundingUse].sort((a, b) => (a.fundingTime ?? 0) - (b.fundingTime ?? 0));
  const prev1d = pickClosestBefore(sortedAll, now - 24 * 60 * 60 * 1000);
  const prev7d = pickClosestBefore(sortedAll, now - 7 * 24 * 60 * 60 * 1000);
  const fundingChange1d = isNum(prev1d?.fundingRate) && isNum(fundingLast) ? fundingLast - prev1d.fundingRate : null;
  const fundingChange7d = isNum(prev7d?.fundingRate) && isNum(fundingLast) ? fundingLast - prev7d.fundingRate : null;

  // Top-of-book and spread
  const bid = ticker?.bid != null ? Number(ticker.bid) : null;
  const ask = ticker?.ask != null ? Number(ticker.ask) : null;
  const bidQty = ticker?.bidSize != null ? Number(ticker.bidSize) : null;
  const askQty = ticker?.askSize != null ? Number(ticker.askSize) : null;

  const mid = isNum(bid) && isNum(ask) && bid > 0 && ask > 0 ? (bid + ask) / 2 : null;
  const spread = isNum(bid) && isNum(ask) ? ask - bid : null;
  const spreadBps = isNum(spread) && isNum(mid) && mid > 0 ? (spread / mid) * 10_000 : null;

  // Liquidity/order book features: replicate old Binance-derived metrics.
  const sumQty = (arr) => (arr || []).reduce((s, x) => s + (isNum(x.qty) ? x.qty : 0), 0);
  const topN = (arr, n) => (arr || []).slice(0, Math.min(n, (arr || []).length));

  const top10BidQty = sumQty(topN(orderbook?.bids, 10));
  const top10AskQty = sumQty(topN(orderbook?.asks, 10));
  const top10Imbalance = top10BidQty + top10AskQty > 0 ? (top10BidQty - top10AskQty) / (top10BidQty + top10AskQty) : null;

  const inBps = (price, side) => {
    if (!isNum(price) || !isNum(mid) || mid <= 0) return false;
    const bps = Math.abs((price - mid) / mid) * 10_000;
    if (side === "bid" && price > mid) return false;
    if (side === "ask" && price < mid) return false;
    return bps <= 10;
  };

  const depthUsd10bpsBid = isNum(mid) ? (orderbook?.bids || []).filter((x) => inBps(x.price, "bid")).reduce((s, x) => s + x.qty * mid, 0) : null;
  const depthUsd10bpsAsk = isNum(mid) ? (orderbook?.asks || []).filter((x) => inBps(x.price, "ask")).reduce((s, x) => s + x.qty * mid, 0) : null;

  // 24h volume from ticker
  const volBase24h = ticker?.vol24h != null ? Number(ticker.vol24h) : null;
  const volQuote24h = ticker?.volumeQuote != null ? Number(ticker.volumeQuote) : null;

  return {
    // Keep compatibility with existing downstream logging/sanitization.
    derivatives_symbol: kSym,
    funding: {
      last: lastFunding ? { fundingTime: lastFunding.fundingTime, fundingRate: lastFunding.fundingRate } : null,
      avg_7d: avgFunding7d,
      std_7d: fundingStd7d,
      z_7d: fundingZ7d,
      change_1d: fundingChange1d,
      change_7d: fundingChange7d
    },
    open_interest: {
      openInterest: ticker?.openInterest != null ? Number(ticker.openInterest) : null,
      time: ticker?.lastTime ?? null
    },
    pricing: {
      markPrice,
      indexPrice,
      lastFundingRate: ticker?.fundingRate != null ? Number(ticker.fundingRate) : null,
      nextFundingTime: null,
      basis
    },
    liquidity: {
      book: { bid, ask, bidQty, askQty },
      spread_bps: spreadBps,
      top10_imbalance: top10Imbalance,
      depth_usd_10bps: { bid: depthUsd10bpsBid, ask: depthUsd10bpsAsk },
      vol_24h: {
        base: volBase24h,
        quote: volQuote24h,
        trades: null
      }
    }
  };
}
