/**
 * Minimal indicator set for regime/trend/vol control.
 * All functions assume arrays ordered oldest -> newest.
 */

export function pctChange(a, b) {
  if (a === 0 || a == null || b == null) return null;
  return (b - a) / a;
}

export function sma(values, period) {
  if (!values || values.length < period) return null;
  const slice = values.slice(values.length - period);
  const sum = slice.reduce((acc, v) => acc + v, 0);
  return sum / period;
}

export function realizedVol(returns, period) {
  // returns are simple returns (e.g., 0.01), oldest->newest
  if (!returns || returns.length < period) return null;
  const slice = returns.slice(returns.length - period);
  const mean = slice.reduce((a, v) => a + v, 0) / slice.length;
  const varr = slice.reduce((a, v) => a + (v - mean) ** 2, 0) / (slice.length - 1 || 1);
  return Math.sqrt(varr);
}

export function simpleReturns(prices) {
  if (!prices || prices.length < 2) return [];
  const out = [];
  for (let i = 1; i < prices.length; i++) {
    const r = pctChange(prices[i - 1], prices[i]);
    out.push(r ?? 0);
  }
  return out;
}

// RSI(14) using Wilder's smoothing
export function rsi(prices, period = 14) {
  if (!prices || prices.length < period + 1) return null;
  let gains = 0;
  let losses = 0;
  for (let i = 1; i <= period; i++) {
    const diff = prices[i] - prices[i - 1];
    if (diff >= 0) gains += diff;
    else losses -= diff;
  }
  gains /= period;
  losses /= period;
  let rs = losses === 0 ? 100 : gains / losses;
  let rsiVal = 100 - 100 / (1 + rs);

  for (let i = period + 1; i < prices.length; i++) {
    const diff = prices[i] - prices[i - 1];
    const gain = diff > 0 ? diff : 0;
    const loss = diff < 0 ? -diff : 0;
    gains = (gains * (period - 1) + gain) / period;
    losses = (losses * (period - 1) + loss) / period;
    rs = losses === 0 ? 100 : gains / losses;
    rsiVal = 100 - 100 / (1 + rs);
  }
  return rsiVal;
}

export function maxDrawdown(prices, lookback = 90) {
  if (!prices || prices.length < 2) return null;
  const slice = prices.slice(Math.max(0, prices.length - lookback));
  let peak = slice[0];
  let mdd = 0;
  for (const p of slice) {
    if (p > peak) peak = p;
    const dd = (p - peak) / peak;
    if (dd < mdd) mdd = dd;
  }
  return mdd; // negative number
}
