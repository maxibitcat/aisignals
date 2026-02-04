import { sma, simpleReturns, realizedVol, rsi, maxDrawdown, pctChange } from "../lib/indicators.js";

function maxDrawdownFromSeries(prices) {
  if (!prices || prices.length < 2) return null;
  let peak = prices[0];
  let mdd = 0;
  for (const p of prices) {
    if (p > peak) peak = p;
    const dd = (p - peak) / peak;
    if (dd < mdd) mdd = dd;
  }
  return mdd;
}

/**
 * Build daily regime features + intraday overlay.
 * Arrays are oldest -> newest.
 * Includes meta timestamps for internal validity checks (will be stripped before AI call).
 */
export function buildFeatures({ dailyPrices, hourlyPrices }) {
  const daily = dailyPrices.map(p => p.price);
  const hourly = hourlyPrices.map(p => p.price);

  const lastDaily = daily[daily.length - 1] ?? null;
  const lastHourly = hourly[hourly.length - 1] ?? null;

  const lastDailyTs = dailyPrices[dailyPrices.length - 1]?.ts ?? null;
  const lastHourlyTs = hourlyPrices[hourlyPrices.length - 1]?.ts ?? null;

  const retsDaily = simpleReturns(daily);

  const dailyBlock = {
    price: lastDaily,
    returns: {
      d1: daily.length >= 2 ? pctChange(daily[daily.length - 2], lastDaily) : null,
      d7: daily.length >= 8 ? pctChange(daily[daily.length - 8], lastDaily) : null,
      d30: daily.length >= 31 ? pctChange(daily[daily.length - 31], lastDaily) : null,
      d90: daily.length >= 91 ? pctChange(daily[daily.length - 91], lastDaily) : null
    },
    vol: {
      d7: realizedVol(retsDaily, 7),
      d30: realizedVol(retsDaily, 30)
    },
    trend: {
      sma20: sma(daily, 20),
      sma50: sma(daily, 50),
      sma200: sma(daily, 200),
      distToSma20: sma(daily, 20) ? (lastDaily - sma(daily, 20)) / sma(daily, 20) : null,
      distToSma50: sma(daily, 50) ? (lastDaily - sma(daily, 50)) / sma(daily, 50) : null,
      distToSma200: sma(daily, 200) ? (lastDaily - sma(daily, 200)) / sma(daily, 200) : null
    },
    momentum: {
      rsi14: rsi(daily, 14)
    },
    risk: {
      maxDrawdown30d: maxDrawdown(daily, 30),
      maxDrawdown90d: maxDrawdown(daily, 90)
    },
    structure: {
      high20d: daily.length >= 20 ? Math.max(...daily.slice(-20)) : null,
      low20d: daily.length >= 20 ? Math.min(...daily.slice(-20)) : null,
      high90d: daily.length >= 90 ? Math.max(...daily.slice(-90)) : null,
      low90d: daily.length >= 90 ? Math.min(...daily.slice(-90)) : null
    }
  };

  let intraday = null;
  if (hourlyPrices?.length && dailyPrices?.length && lastDailyTs) {
    const idx = hourlyPrices.findIndex(p => p.ts >= lastDailyTs);
    const startIdx = idx >= 0 ? idx : Math.max(0, hourlyPrices.length - 24);
    const slice = hourlyPrices.slice(startIdx);

    if (slice.length >= 2) {
      const startTs = slice[0].ts;
      const endTs = slice[slice.length - 1].ts;
      const hours = (endTs - startTs) / 3600000;

      const startPrice = slice[0].price;
      const endPrice = slice[slice.length - 1].price;

      const slicePrices = slice.map(p => p.price);
      const sliceRets = simpleReturns(slicePrices);

      const last24 = hourlyPrices.slice(-Math.min(24, hourlyPrices.length));
      const last24Prices = last24.map(p => p.price);
      const last24Rets = simpleReturns(last24Prices);

      const ma24 = sma(hourly, Math.min(24, hourly.length));
      const ma72 = sma(hourly, Math.min(72, hourly.length));

      intraday = {
        since_last_daily: {
          return: pctChange(startPrice, endPrice),
          max_drawdown: maxDrawdownFromSeries(slicePrices),
          vol: realizedVol(sliceRets, Math.min(sliceRets.length, 48)),
          hours
        },
        last_24h: {
          return: last24Prices.length >= 2
            ? pctChange(last24Prices[0], last24Prices[last24Prices.length - 1])
            : null,
          vol: realizedVol(last24Rets, Math.min(last24Rets.length, 23))
        },
        trend: {
          dist_to_ma24: ma24 ? (endPrice - ma24) / ma24 : null,
          dist_to_ma72: ma72 ? (endPrice - ma72) / ma72 : null
        }
      };
    }
  }

  return {
    price: lastHourly ?? lastDaily,
    daily: dailyBlock,
    intraday,
    meta: { lastDailyTs, lastHourlyTs }
  };
}
