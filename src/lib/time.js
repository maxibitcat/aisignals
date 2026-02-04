export function nowIsoWithOffset() {
  // Use runtime locale offset; server in Europe/Paris typically, but not required.
  // We store ISO string with offset if available (Date.toISOString is UTC), so we also store a local ISO.
  const d = new Date();
  const tzOffsetMin = d.getTimezoneOffset();
  const sign = tzOffsetMin <= 0 ? "+" : "-";
  const abs = Math.abs(tzOffsetMin);
  const hh = String(Math.floor(abs / 60)).padStart(2, "0");
  const mm = String(abs % 60).padStart(2, "0");
  // Build local ISO-like string with offset
  const local = new Date(d.getTime() - tzOffsetMin * 60000).toISOString().replace("Z", "");
  return `${local}${sign}${hh}:${mm}`;
}
