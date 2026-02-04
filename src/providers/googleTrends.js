import googleTrends from "google-trends-api";

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

/**
 * Fetch Google Trends interest for the given keyword.
 * Returns a lightweight snapshot with last value + temporal stats.
 *
 * Notes:
 * - Uses an unofficial Google endpoint via google-trends-api.
 * - Best-effort; caller should catch errors and treat as optional.
 */
export async function fetchGoogleTrends({ keyword, geo = "", timeframe = "today 3-m" }) {
  const raw = await googleTrends.interestOverTime({ keyword, geo, timeframe });
  const json = JSON.parse(raw);
  const timeline = json?.default?.timelineData || [];

  const values = timeline
    .map((x) => (Array.isArray(x?.value) ? x.value[0] : null))
    .map((v) => (v != null ? Number(v) : null))
    .filter(isNum);

  const last = values.length ? values[values.length - 1] : null;
  const prev1 = values.length >= 2 ? values[values.length - 2] : null;
  const prev7 = values.length >= 8 ? values[values.length - 8] : null;
  const m = mean(values);
  const s = std(values);
  const z = isNum(last) && isNum(m) && isNum(s) && s > 0 ? (last - m) / s : null;

  return {
    keyword,
    last,
    change_1d: isNum(last) && isNum(prev1) ? last - prev1 : null,
    change_7d: isNum(last) && isNum(prev7) ? last - prev7 : null,
    mean: m,
    std: s,
    z
  };
}
