import fs from "fs";
import path from "path";

/**
 * Read NDJSON file safely. Returns [] if file doesn't exist.
 */
export function readNdjson(filePath) {
  try {
    if (!fs.existsSync(filePath)) return [];
    const txt = fs.readFileSync(filePath, "utf8");
    return txt
      .split(/\r?\n/)
      .filter(Boolean)
      .map((line) => {
        try { return JSON.parse(line); } catch { return null; }
      })
      .filter(Boolean);
  } catch {
    return [];
  }
}

/**
 * Append object as NDJSON line. Ensures directory exists.
 */
export function appendNdjsonLine(filePath, obj) {
  const dir = path.dirname(filePath);
  fs.mkdirSync(dir, { recursive: true });
  fs.appendFileSync(filePath, JSON.stringify(obj) + "\n");
}

/**
 * Get last N records for a given symbol from an NDJSON file.
 * Assumes file records are in chronological order.
 */
export function getLastNBySymbol(records, symbol, n) {
  const filtered = records.filter((r) => r && r.symbol === symbol);
  return filtered.slice(Math.max(0, filtered.length - n));
}

export function pctChange(a, b) {
  if (a === null || a === undefined || b === null || b === undefined) return null;
  const aa = Number(a);
  const bb = Number(b);
  if (!Number.isFinite(aa) || !Number.isFinite(bb) || aa === 0) return null;
  return (bb - aa) / aa;
}
