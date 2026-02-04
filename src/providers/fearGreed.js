import axios from "axios";

// Alternative.me Fear & Greed index (public).
// Docs are informal; response typically:
// { data: [{ value:"74", value_classification:"Greed", timestamp:"..." }], ... }
const URL = "https://api.alternative.me/fng/";

export async function fetchFearGreed({ limit = 1 } = {}) {
  const res = await axios.get(URL, { params: { limit, format: "json" }, timeout: 60_000 });
  const d = res.data || {};
  const row = Array.isArray(d.data) && d.data.length ? d.data[0] : null;
  const value = row?.value != null ? Number(row.value) : null;
  return {
    value: Number.isFinite(value) ? value : null,
    classification: row?.value_classification ?? null,
    timestamp: row?.timestamp != null ? Number(row.timestamp) * 1000 : null
  };
}
