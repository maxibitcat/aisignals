import axios from "axios";

// Root URLs (CoinGecko):
// - Demo (free) plan: https://api.coingecko.com/api/v3  with header x-cg-demo-api-key
// - Pro (paid) plan: https://pro-api.coingecko.com/api/v3 with header x-cg-pro-api-key
//
// We default to the Demo (free) root URL because it works for everyone (keyed or keyless).
// If you have a Pro plan, set COINGECKO_API_BASE to the pro root URL.

const DEFAULT_API_BASE = "https://api.coingecko.com/api/v3";

function buildHeaders(apiKey, apiBase) {
  if (!apiKey) return {};
  const isPro = (apiBase || DEFAULT_API_BASE).includes("pro-api.coingecko.com");
  return isPro ? { "x-cg-pro-api-key": apiKey } : { "x-cg-demo-api-key": apiKey };
}

function describeAxiosError(err) {
  return {
    message: err?.message,
    status: err?.response?.status,
    statusText: err?.response?.statusText,
    url: err?.config?.url,
    params: err?.config?.params,
    responseData: err?.response?.data
  };
}

/**
 * /coins/{id}/market_chart
 * - For days <= 90, CoinGecko typically returns hourly points.
 * - For larger days, CoinGecko returns daily points.
 * We intentionally DO NOT pass `interval` to remain compatible with Demo/free plan behavior.
 */
export async function fetchMarketChart({ apiKey, id, vsCurrency, days, apiBase }) {
  const base = apiBase || DEFAULT_API_BASE;
  const url = `${base}/coins/${encodeURIComponent(id)}/market_chart`;
  const params = { vs_currency: vsCurrency, days };

  try {
    const res = await axios.get(url, {
      headers: buildHeaders(apiKey, base),
      params,
      timeout: 60_000
    });
    return res.data;
  } catch (err) {
    const info = describeAxiosError(err);
    console.error("[CoinGecko] market_chart error:", JSON.stringify(info, null, 2));
    throw new Error(`CoinGecko market_chart failed: ${JSON.stringify(info)}`);
  }
}

/**
 * Returns:
 * - "daily" series for ~daysDaily (CoinGecko will return daily points for large windows)
 * - "hourly" series for ~daysHourly (<=90)
 *
 * Note: For Demo/free keys, keep daysDaily <= 365 for reliability.
 */
export async function fetchPriceSeries({
  apiKey,
  id,
  vsCurrency,
  daysDaily = 365,
  daysHourly = 14,
  apiBase
}) {
  const [daily, hourly] = await Promise.all([
    fetchMarketChart({ apiKey, id, vsCurrency, days: daysDaily, apiBase }),
    fetchMarketChart({ apiKey, id, vsCurrency, days: Math.min(daysHourly, 90), apiBase })
  ]);

  const dailyPrices = (daily.prices || []).map(([ts, price]) => ({ ts, price }));
  const hourlyPrices = (hourly.prices || []).map(([ts, price]) => ({ ts, price }));

  return { dailyPrices, hourlyPrices };
}
