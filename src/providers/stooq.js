import axios from "axios";

const UA = "Mozilla/5.0 (aisignals; +https://example.local)";

function detectDelimiter(headerLine) {
  const commas = (headerLine.match(/,/g) || []).length;
  const semis = (headerLine.match(/;/g) || []).length;
  return semis > commas ? ";" : ",";
}

/**
 * Fetch last N daily bars from Stooq as CSV/TSV-like.
 * Stooq sometimes returns comma-separated or semicolon-separated depending on instrument.
 * Returns array sorted oldest -> newest: [{date, close}]
 */
export async function fetchStooqDaily({ symbol, count = 4 }) {
  const url = "https://stooq.com/q/d/l/";
  const res = await axios.get(url, {
    params: { s: symbol, i: "d", c: count },
    headers: { "User-Agent": UA, "Accept": "text/csv,text/plain,*/*" },
    timeout: 60_000
  });

  const raw = (res.data ?? "").toString().trim();
  const lines = raw.split(/\r?\n/).filter(Boolean);

  if (lines.length < 2) {
    const snippet = raw.slice(0, 200);
    throw new Error(`Stooq returned no data for ${symbol}. Snippet: ${snippet}`);
  }

  const delim = detectDelimiter(lines[0]);
  const header = lines[0].split(delim).map((h) => h.trim().toLowerCase());

  const idx = (name) => header.findIndex((h) => h === name);
  const iDate = idx("date");
  const iClose = idx("close");

  if (iDate < 0 || iClose < 0) {
    const snippet = raw.slice(0, 200);
    throw new Error(`Stooq unexpected format for ${symbol}. Delim=${delim}. Snippet: ${snippet}`);
  }

  const rows = [];
  for (let k = 1; k < lines.length; k++) {
    const parts = lines[k].split(delim);
    const toNum = (v) => (v === undefined || v === "" ? null : Number(String(v).replace(",", ".").trim()));
    rows.push({
      date: parts[iDate]?.trim(),
      close: toNum(parts[iClose])
    });
  }
  return rows;
}

export async function fetchStooqSnapshot({ symbol }) {
  const rows = await fetchStooqDaily({ symbol, count: 4 });
  const n = rows.length;
  const last = rows[n - 1]?.close ?? null;
  const prev1 = rows[n - 2]?.close ?? null;
  const prev3 = rows[n - 4]?.close ?? null;

  const ret1d = last && prev1 ? (last - prev1) / prev1 : null;
  const ret3d = last && prev3 ? (last - prev3) / prev3 : null;

  return {
    symbol,
    last_close: last,
    return_1d: ret1d,
    return_3d: ret3d
  };
}
