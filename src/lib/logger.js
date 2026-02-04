import fs from "fs";
import path from "path";

export function ensureDir(dir) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

export function appendNdjson(filePath, obj) {
  const line = JSON.stringify(obj);
  fs.appendFileSync(filePath, line + "\n", "utf8");
}

export function resolveLogPath(logDir, filename) {
  ensureDir(logDir);
  return path.resolve(logDir, filename);
}
