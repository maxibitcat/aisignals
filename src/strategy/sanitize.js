/**
 * Reduce token usage by:
 * - removing non-essential fields
 * - rounding numeric values
 * - converting some ratios to percentage points (pp)
 */

function isNum(x) {
  return typeof x === "number" && Number.isFinite(x);
}

function round(x, d) {
  if (!isNum(x)) return x;
  const p = 10 ** d;
  return Math.round(x * p) / p;
}

function pctPoints(x, d = 2) {
  if (!isNum(x)) return x;
  return round(x * 100, d);
}

function prune(o) {
  if (o === null || o === undefined) return o;
  if (typeof o !== "object") return o;
  if (Array.isArray(o)) return o.map(prune);
  const entries = Object.entries(o)
    .map(([k, v]) => [k, prune(v)])
    .filter(([_, v]) => v !== null && v !== undefined && !(typeof v === "object" && !Array.isArray(v) && Object.keys(v).length === 0));
  return Object.fromEntries(entries);
}

export function sanitizeFeatures(features) {
  const f = features || {};

  const out = {
    price: round(f.price, 6), // keep more precision for low-priced alts, but still trimmed
    daily: {
      price: round(f.daily?.price, 6),
      returns_pp: {
        d1: pctPoints(f.daily?.returns?.d1, 2),
        d7: pctPoints(f.daily?.returns?.d7, 2),
        d30: pctPoints(f.daily?.returns?.d30, 2),
        d90: pctPoints(f.daily?.returns?.d90, 2)
      },
      vol_pp: {
        d7: pctPoints(f.daily?.vol?.d7, 2),
        d30: pctPoints(f.daily?.vol?.d30, 2)
      },
      trend: {
        dist_to_sma20_pp: pctPoints(f.daily?.trend?.distToSma20, 2),
        dist_to_sma50_pp: pctPoints(f.daily?.trend?.distToSma50, 2),
        dist_to_sma200_pp: pctPoints(f.daily?.trend?.distToSma200, 2),
        sma20: round(f.daily?.trend?.sma20, 6),
        sma50: round(f.daily?.trend?.sma50, 6),
        sma200: round(f.daily?.trend?.sma200, 6)
      },
      momentum: { rsi14: round(f.daily?.momentum?.rsi14, 1) },
      risk: {
        max_dd_30d_pp: pctPoints(f.daily?.risk?.maxDrawdown30d, 2),
        max_dd_90d_pp: pctPoints(f.daily?.risk?.maxDrawdown90d, 2)
      },
      structure: {
        high20d: round(f.daily?.structure?.high20d, 6),
        low20d: round(f.daily?.structure?.low20d, 6),
        high90d: round(f.daily?.structure?.high90d, 6),
        low90d: round(f.daily?.structure?.low90d, 6)
      }
    },
    intraday: f.intraday
      ? {
          since_last_daily: {
            return_pp: pctPoints(f.intraday?.since_last_daily?.return, 2),
            max_dd_pp: pctPoints(f.intraday?.since_last_daily?.max_drawdown, 2),
            vol_pp: pctPoints(f.intraday?.since_last_daily?.vol, 2),
            hours: round(f.intraday?.since_last_daily?.hours, 1)
          },
          last_24h: {
            return_pp: pctPoints(f.intraday?.last_24h?.return, 2),
            vol_pp: pctPoints(f.intraday?.last_24h?.vol, 2)
          },
          trend: {
            dist_to_ma24_pp: pctPoints(f.intraday?.trend?.dist_to_ma24, 2),
            dist_to_ma72_pp: pctPoints(f.intraday?.trend?.dist_to_ma72, 2)
          }
        }
      : null
  };

  return prune(out);
}

export function sanitizeMacro(macro) {
  if (!macro) return null;
  const s = (x) => ({
    last: round(x?.last_close, 4),
    ret1d_pp: pctPoints(x?.return_1d, 2),
    ret3d_pp: pctPoints(x?.return_3d, 2)
  });
  return prune({    spx: s(macro.spx),
    dxy: s(macro.dxy),
    us10y: s(macro.us10y)
  });
}

export function sanitizeDerivatives(derivs) {
  if (!derivs) return null;
  return prune({
    funding: {
      // funding is already small; show as pp with 4 decimals
      last_pp: pctPoints(derivs?.funding?.last?.fundingRate, 4),
      avg_7d_pp: pctPoints(derivs?.funding?.avg_7d, 4),
      std_7d_pp: pctPoints(derivs?.funding?.std_7d, 4),
      z_7d: round(derivs?.funding?.z_7d, 2),
      change_1d_pp: pctPoints(derivs?.funding?.change_1d, 4),
      change_7d_pp: pctPoints(derivs?.funding?.change_7d, 4)
    },
    open_interest: {
      oi: round(derivs?.open_interest?.openInterest, 0),
      oi_change_1d_pp: pctPoints(derivs?.open_interest?.oi_change_1d, 2),
      oi_change_7d_pp: pctPoints(derivs?.open_interest?.oi_change_7d, 2)
    },
    basis_pp: pctPoints(derivs?.pricing?.basis, 3),
    basis_change_1d_pp: pctPoints(derivs?.pricing?.basis_change_1d, 3),
    basis_change_7d_pp: pctPoints(derivs?.pricing?.basis_change_7d, 3),
    liquidity: {
      spread_bps: round(derivs?.liquidity?.spread_bps, 1),
      top10_imbalance_pp: pctPoints(derivs?.liquidity?.top10_imbalance, 2),
      depth_usd_10bps: {
        bid: round(derivs?.liquidity?.depth_usd_10bps?.bid, 0),
        ask: round(derivs?.liquidity?.depth_usd_10bps?.ask, 0)
      },
      vol_24h: {
        quote: round(derivs?.liquidity?.vol_24h?.quote, 0),
        trades: round(derivs?.liquidity?.vol_24h?.trades, 0)
      },
      // Keep book top-of-book for interpretability, but trimmed.
      book: {
        bid: round(derivs?.liquidity?.book?.bid, 6),
        ask: round(derivs?.liquidity?.book?.ask, 6)
      }
    }
  });
}

export function sanitizeSentiment(sentiment) {
  if (!sentiment) return null;
  return prune({
    fear_greed: {
      value: round(sentiment?.fear_greed?.value, 0),
      classification: sentiment?.fear_greed?.classification ?? null,
      change_1d: round(sentiment?.fear_greed?.change_1d, 0)
    }
  });
}

export function sanitizeTrends(trends) {
  if (!trends) return null;
  return prune({
    keyword: trends?.keyword ?? null,
    last: round(trends?.last, 0),
    change_1d: round(trends?.change_1d, 0),
    change_7d: round(trends?.change_7d, 0),
    z: round(trends?.z, 2)
  });
}

