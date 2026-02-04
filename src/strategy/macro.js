import { fetchStooqSnapshot } from "../providers/stooq.js";

async function firstOk(symbols) {
  let lastErr = null;
  for (const s of symbols) {
    try {
      return await fetchStooqSnapshot({ symbol: s });
    } catch (e) {
      lastErr = e;
    }
  }
  throw lastErr || new Error("No symbols succeeded");
}

/**
 * Macro proxies (best-effort):
 * - S&P 500 index: ^spx (Stooq)
 * - DXY proxy: try DX.F (ICE DXY futures) then fallback to USD_I (Stooq USD index)
 * - US 10Y yield: 10yusy.b
 */
export async function fetchMacroSnapshot() {
  const [spx, dxy, us10y] = await Promise.all([
    fetchStooqSnapshot({ symbol: "^spx" }),
    firstOk(["dx.f", "usd_i"]),
    fetchStooqSnapshot({ symbol: "10yusy.b" })
  ]);

  return { spx, dxy, us10y };
}
