import fs from "fs";
import path from "path";
import cron from "node-cron";
import dotenv from "dotenv";

import { readNdjson, appendNdjsonLine, getLastNBySymbol, pctChange as pctChg } from "./lib/history.js";
import { nowIsoWithOffset } from "./lib/time.js";

import { OpenAIProvider } from "./providers/openai.js";
import { fetchPriceSeries } from "./providers/coingecko.js";
import { fetchDerivativesSnapshot } from "./providers/binanceFutures.js";
import { fetchFearGreed } from "./providers/fearGreed.js";
import { fetchGoogleTrends } from "./providers/googleTrends.js";

import { fetchMacroSnapshot } from "./strategy/macro.js";
import { buildFeatures } from "./strategy/features.js";
import { buildContext } from "./strategy/promptContext.js";
import { sanitizeFeatures, sanitizeMacro, sanitizeDerivatives, sanitizeSentiment, sanitizeTrends } from "./strategy/sanitize.js";

import { ChainSignalsClient } from "./lib/chainsignals.js";

dotenv.config();

function must(name, value) {
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}

function envBool(name, def = false) {
  const v = process.env[name];
  if (v === undefined) return def;
  const s = String(v).trim().toLowerCase();
  if (["1", "true", "yes", "y", "on"].includes(s)) return true;
  if (["0", "false", "no", "n", "off"].includes(s)) return false;
  return def;
}

function tidyExplanation(text) {
  if (!text) return text;
  let t = String(text).trim();
  t = t.replace(/\s*\n\s*/g, " ");
  t = t.replace(/\s+/g, " ");

  const MAX = 280;
  if (t.length > MAX) t = t.slice(0, MAX).trim();

  if (t.length && /[A-Za-z0-9]$/.test(t) && !/[\.\!\?]$/.test(t)) {
    const lastPunct = Math.max(t.lastIndexOf("."), t.lastIndexOf("!"), t.lastIndexOf("?"));
    if (lastPunct >= 40) {
      t = t.slice(0, lastPunct + 1).trim();
    } else {
      const lastSpace = t.lastIndexOf(" ");
      if (lastSpace >= 40) t = t.slice(0, lastSpace).trim();
      if (!/[\.\!\?]$/.test(t)) t = t + ".";
    }
  }
  if (!/[\.\!\?]$/.test(t)) t = t + ".";
  return t;
}

// -------------------------
// CLI flags
// -------------------------

const argv = process.argv.slice(2);
const RUN_NOW = argv.includes("--run-now") || String(process.env.RUN_ON_START || "false").toLowerCase() === "true";
const FORCE_SEND_RUN_NOW =
  argv.includes("--send-run-now") ||
  argv.includes("--force-send") ||
  String(process.env.SEND_RUN_NOW_TO_CHAIN || "false").toLowerCase() === "true";

// -------------------------
// Env
// -------------------------

const {
  OPENAI_API_KEY,
  COINGECKO_API_KEY,
  COINGECKO_API_BASE,
  VS_CURRENCY = "usd",
  LOG_DIR = "./logs",
  CRON_EXPR = "0 12 * * *",
  INCLUDE_BTC_REGIME = "true",
  CHAIN_ID,
  CHAIN_NAME,
  CHAIN_RPC_URL,
  CHAIN_SIGNALS_ADDRESS,
  DEPLOYER_PK,
  CHAIN_GAS_PRICE_GWEI,
  CHAIN_GAS_LIMIT
} = process.env;

const CHAIN_EXPLORER_BASE = String(process.env.CHAIN_EXPLORER_BASE || "https://explorer.kasplex.org").replace(/\/$/, "");
function txUrl(hash) {
  if (!hash) return "";
  return `${CHAIN_EXPLORER_BASE}/tx/${hash}`;
}

const cgKey = must("COINGECKO_API_KEY", COINGECKO_API_KEY);
const openaiKey = must("OPENAI_API_KEY", OPENAI_API_KEY);

// Absolute log paths (single source of truth)
const logDirAbs = path.resolve(process.cwd(), LOG_DIR);
const logPath = path.join(logDirAbs, "signals.ndjson");
const derivHistPath = path.join(logDirAbs, "derivatives_history.ndjson");
const sentimentHistPath = path.join(logDirAbs, "sentiment_history.ndjson");

function appendNdjson(filePath, obj) {
  appendNdjsonLine(filePath, obj);
}

function ensureDir(p) {
  if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true });
}

// -------------------------
// Strategies config
// -------------------------

function loadStrategies() {
  const strategiesPath = path.resolve(process.cwd(), "strategies.json");
  const legacyAssetsPath = path.resolve(process.cwd(), "assets.json");

  if (fs.existsSync(strategiesPath)) {
    const raw = JSON.parse(fs.readFileSync(strategiesPath, "utf8"));
    if (!Array.isArray(raw.strategies)) throw new Error("strategies.json must contain { strategies: [...] }");
    return raw.strategies;
  }
  if (fs.existsSync(legacyAssetsPath)) {
    const raw = JSON.parse(fs.readFileSync(legacyAssetsPath, "utf8"));
    if (!Array.isArray(raw.assets)) throw new Error("assets.json must contain { assets: [...] }");
    // Legacy fallback: map assets -> strategies with defaults
    return raw.assets.map((a) => ({
      ...a,
      chainAsset: a.symbol,
      model: process.env.OPENAI_MODEL || "gpt-4.1-mini",
      version: 0
    }));
  }
  throw new Error(`strategies.json not found at ${strategiesPath} (and no legacy assets.json found)`);
}

function normalizeStrategy(s) {
  const symbol = String(s.symbol || "").trim().toUpperCase();
  const chainAsset = String(s.chainAsset || "").trim().toUpperCase();
  const model = String(s.model || "").trim();
  const version = Number(s.version);

  if (!symbol) throw new Error("Strategy missing symbol");
  if (!s.coingeckoId) throw new Error(`Strategy ${symbol} missing coingeckoId`);
  if (!chainAsset) throw new Error(`Strategy ${symbol} missing chainAsset`);
  if (!model) throw new Error(`Strategy ${symbol} missing model`);
  if (!Number.isInteger(version) || version < 0) throw new Error(`Strategy ${symbol} has invalid version`);

  return {
    ...s,
    symbol,
    chainAsset,
    model,
    version
  };
}

const strategies = loadStrategies().map(normalizeStrategy);

// Unique market assets (so we fetch data and compute lagged deltas once per symbol)
const marketAssets = new Map();
for (const s of strategies) {
  if (!marketAssets.has(s.symbol)) marketAssets.set(s.symbol, s);
}

function trendsKeywordFor(symbol) {
  const s = String(symbol || "").toUpperCase();
  // A few common tickers where the coin name matters more than the ticker.
  const map = {
    BTC: "Bitcoin",
    ETH: "Ethereum",
    SOL: "Solana",
    XRP: "XRP",
    BNB: "BNB",
    ADA: "Cardano",
    DOGE: "Dogecoin",
    DOT: "Polkadot",
    AVAX: "Avalanche",
    LINK: "Chainlink"
  };
  return map[s] || `${s} crypto`;
}

// Provider cache (per model)
const aiByModel = new Map();
function getAi(model) {
  if (!aiByModel.has(model)) {
    aiByModel.set(
      model,
      new OpenAIProvider({
        apiKey: openaiKey,
        model
      })
    );
  }
  return aiByModel.get(model);
}

// Chain client is only constructed if we may ever send
function buildChainClient() {
  return new ChainSignalsClient({
    rpcUrl: must("CHAIN_RPC_URL", CHAIN_RPC_URL),
    chainId: must("CHAIN_ID", CHAIN_ID),
    chainName: CHAIN_NAME || "Kasplex L2",
    contractAddress: must("CHAIN_SIGNALS_ADDRESS", CHAIN_SIGNALS_ADDRESS),
    privateKey: must("DEPLOYER_PK", DEPLOYER_PK),
    gasPriceGwei: CHAIN_GAS_PRICE_GWEI,
    gasLimit: CHAIN_GAS_LIMIT
  });
}

async function computeAssetSnapshot(asset) {
  const { dailyPrices, hourlyPrices } = await fetchPriceSeries({
    apiKey: cgKey,
    id: asset.coingeckoId,
    vsCurrency: VS_CURRENCY,
    daysDaily: 365,
    daysHourly: 14,
    apiBase: COINGECKO_API_BASE
  });
  const features = buildFeatures({ dailyPrices, hourlyPrices });
  return { asset, features };
}

function buildStrategyName(strategy, runType) {
  const suffix = runType === "scheduled" ? `v${strategy.version}` : "test";
  // Contract constraint: strategy name <= 30 chars
  const asset = String(strategy.chainAsset || strategy.symbol || "").trim();
  const model = String(strategy.model || "").trim();

  let name = `${asset} ${model} ${suffix}`.replace(/\s+/g, " ").trim();
  if (name.length <= 30) return name;

  // Try removing spaces in model
  const compactModel = model.replace(/\s+/g, "");
  name = `${asset} ${compactModel} ${suffix}`.replace(/\s+/g, " ").trim();
  if (name.length <= 30) return name;

  // Truncate model to fit
  const fixed = `${asset}  ${suffix}`.replace(/\s+/g, " ").trim();
  const remaining = 30 - fixed.length - 1; // space between asset and model
  const clippedModel = remaining > 0 ? compactModel.slice(0, remaining) : "";
  name = `${asset} ${clippedModel} ${suffix}`.replace(/\s+/g, " ").trim();

  return name.slice(0, 30);
}

function mapDecisionToChain(decisionSignal, chainAsset) {
  // Contract enum PositionIntent: Long=0, Short=1
  // Our signals: LONG_ASSET, LONG_CASH, SHORT_ASSET
  if (decisionSignal === "SHORT_ASSET") {
    return { asset: chainAsset, target: "Short" };
  }
  if (decisionSignal === "LONG_CASH") {
    return { asset: "USD", target: "Long" };
  }
  return { asset: chainAsset, target: "Long" };
}

async function runOnce({ runType }) {
  const timestamp = nowIsoWithOffset();
  const scheduled = runType === "scheduled";
  const shouldSendToChain = scheduled || (runType === "manual" && FORCE_SEND_RUN_NOW);

  ensureDir(logDirAbs);

  // Load history once
  const derivHistRecords = readNdjson(derivHistPath);
  const sentimentHistRecords = readNdjson(sentimentHistPath);

  // Sentiment (best-effort): Fear & Greed
  let sentimentRaw = null;
  let sentimentOk = false;
  try {
    const fg = await fetchFearGreed({ limit: 1 });
    // compute 1d change from local history (if any)
    const last = sentimentHistRecords.length ? sentimentHistRecords[sentimentHistRecords.length - 1] : null;
    const change1d = last?.fear_greed_value != null && fg?.value != null ? fg.value - last.fear_greed_value : null;
    sentimentRaw = { fear_greed: { ...fg, change_1d: change1d } };
    sentimentOk = true;
    if (scheduled && fg?.value != null) {
      appendNdjsonLine(sentimentHistPath, {
        timestamp,
        fear_greed_value: fg.value,
        fear_greed_classification: fg.classification ?? null
      });
    }
    console.log(`[${timestamp}] Sentiment fetch (fear&greed): OK`);
  } catch (e) {
    sentimentRaw = null;
    sentimentOk = false;
    console.warn(`[${timestamp}] Sentiment fetch (fear&greed): FAIL - ${String(e.message || e)}`);
  }
  const sentiment = sentimentRaw ? sanitizeSentiment(sentimentRaw) : null;

  // Macro proxies (best-effort)
  let macroRaw = null;
  let macroOk = false;
  try {
    macroRaw = await fetchMacroSnapshot();
    macroOk = true;
    console.log(`[${timestamp}] Macro fetch: OK`);
  } catch (e) {
    macroRaw = null;
    macroOk = false;
    console.error(`[${timestamp}] Macro fetch: FAIL - ${String(e.message || e)}`);
  }
  const macro = macroRaw ? sanitizeMacro(macroRaw) : null;

  // BTC regime (best-effort)
  let btc = null;
  if (String(INCLUDE_BTC_REGIME).toLowerCase() === "true") {
    const btcAsset = marketAssets.get("BTC") || null;
    if (btcAsset) {
      try {
        const snap = await computeAssetSnapshot(btcAsset);
        btc = sanitizeFeatures(snap.features);
      } catch {
        btc = null;
      }
    }
  }

  // Compute price/feature snapshots (once per symbol)
  const marketList = Array.from(marketAssets.values());
  const snaps = await Promise.allSettled(marketList.map((a) => computeAssetSnapshot(a)));
  const featuresBySymbol = new Map();

  for (let i = 0; i < snaps.length; i++) {
    const s = snaps[i];
    const asset = marketList[i];

    if (s.status !== "fulfilled") {
      const reason = s.reason;
      const msg = reason?.message || String(reason);
      const errObj = {
        timestamp,
        symbol: asset?.symbol || "UNKNOWN",
        strategy: null,
        model: asset?.model || null,
        version: asset?.version || null,
        signal: "LONG_CASH",
        explanation: `Data fetch/feature error for ${asset?.symbol || "UNKNOWN"}: ${msg}`.slice(0, 600),
        error: { message: msg, stack: reason?.stack || null }
      };
      appendNdjson(logPath, errObj);
      console.error(errObj.explanation);
      continue;
    }

    const { asset: computedAsset, features } = s.value;
    featuresBySymbol.set(computedAsset.symbol, features);
  }

  // Google Trends (best-effort): per-symbol interest series
  const trendsBySymbol = new Map();
  // Avoid hammering Trends on --run-now unless explicitly enabled.
  const enableTrends = envBool("ENABLE_GOOGLE_TRENDS", false) || scheduled;
  if (enableTrends) {
    const trendJobs = marketList.map(async (asset) => {
      const keyword = asset.googleTrendsKeyword || trendsKeywordFor(asset.symbol);
      try {
        const t = await fetchGoogleTrends({ keyword, geo: String(process.env.GOOGLE_TRENDS_GEO || "") });
        trendsBySymbol.set(asset.symbol, sanitizeTrends(t));
      } catch {
        trendsBySymbol.set(asset.symbol, null);
      }
    });
    await Promise.allSettled(trendJobs);
    console.log(`[${timestamp}] Google Trends: ${scheduled ? "scheduled" : "manual"} fetch ${enableTrends ? "enabled" : "disabled"}`);
  } else {
    for (const a of marketList) trendsBySymbol.set(a.symbol, null);
  }

  // Derivatives snapshots (once per symbol) + lagged deltas computed from scheduled-history only
  const derivativesBySymbol = new Map();
  const derivativesOkBySymbol = new Map();
  const derivAppendDone = new Set();

  for (const asset of marketList) {
    let derivativesRaw = null;
    let derivativesOk = false;
    try {
      let bSym = asset.binanceSymbol || null;
      let derivSource = asset.symbol;
      if (!bSym && asset.derivativesProxySymbol) {
        bSym = asset.derivativesProxySymbol;
        derivSource = `proxy:${asset.derivativesProxySymbol}`;
      }
      if (!bSym) throw new Error("Missing binanceSymbol/proxy");

      derivativesRaw = await fetchDerivativesSnapshot({ symbol: bSym });
      derivativesRaw._source = derivSource;
      derivativesOk = true;
      console.log(`[${timestamp}] Derivatives fetch ${asset.symbol}: OK (${bSym}) source=${derivativesRaw?._source || asset.symbol}`);
    } catch (e) {
      derivativesRaw = null;
      derivativesOk = false;
      console.warn(`[${timestamp}] Derivatives fetch ${asset.symbol}: FAIL - ${String(e.message || e)}`);
    }

    if (derivativesRaw) {
      const hist = getLastNBySymbol(derivHistRecords, asset.symbol, 8); // up to ~7d + today
      const last = hist.length ? hist[hist.length - 1] : null;
      const last7 = hist.length >= 7 ? hist[hist.length - 7] : null;

      const oiNow = derivativesRaw.open_interest?.openInterest ?? null;
      const basisNow = derivativesRaw.pricing?.basis ?? null;

      derivativesRaw.open_interest.oi_change_1d = last?.oi != null ? pctChg(last.oi, oiNow) : null;
      derivativesRaw.open_interest.oi_change_7d = last7?.oi != null ? pctChg(last7.oi, oiNow) : null;

      derivativesRaw.pricing.basis_change_1d = last?.basis != null ? (basisNow != null ? basisNow - last.basis : null) : null;
      derivativesRaw.pricing.basis_change_7d = last7?.basis != null ? (basisNow != null ? basisNow - last7.basis : null) : null;

      if (scheduled && !derivAppendDone.has(asset.symbol)) {
        derivAppendDone.add(asset.symbol);
        appendNdjsonLine(derivHistPath, {
          timestamp,
          symbol: asset.symbol,
          oi: oiNow,
          basis: basisNow
        });
      }
    }

    derivativesBySymbol.set(asset.symbol, derivativesRaw ? sanitizeDerivatives(derivativesRaw) : null);
    derivativesOkBySymbol.set(asset.symbol, derivativesOk);
  }

  const context = buildContext({ rebalanceTimeLocal: "12:00 Europe/Paris" });
  const nSignalsFeedback = Math.max(0, Number(process.env.N_SIGNALS_FEEDBACK || 0));
  const debugSignalsFeedback = envBool("DEBUG_SIGNALS_FEEDBACK", false);
  const chain = (shouldSendToChain || nSignalsFeedback > 0) ? buildChainClient() : null;

  for (const strategy of strategies) {
    const symbol = strategy.symbol;
    const features = featuresBySymbol.get(symbol) || null;

    // Basic validity check
    const hasPrice = typeof features?.price === "number" && isFinite(features.price);
    if (!hasPrice || !features?.meta?.lastDailyTs) {
      const obj = {
        timestamp,
        symbol,
        strategy: buildStrategyName(strategy, runType),
        model: strategy.model,
        version: strategy.version,
        chain_asset: strategy.chainAsset,
        run_type: runType,
        signal: "LONG_CASH",
        explanation: "Insufficient or missing market data from provider; defaulting to cash.",
        features: sanitizeFeatures(features)
      };
      appendNdjson(logPath, obj);
      console.warn(`[${timestamp}] ${symbol}: insufficient market data (logged sanitized features)`);
      continue;
    }

    // Optional: include the last N on-chain signals produced by this bot for this strategy,
    // so the model can avoid repeating itself and can say "nothing changed" when appropriate.

// Optional: include the last N on-chain signals produced by this bot for this strategy,
// so the model can avoid repeating itself and can say "nothing changed" when appropriate.
let feedbackSignals = [];
if (chain && nSignalsFeedback > 0) {
  try {
    const strategyNameForFeedback = buildStrategyName(strategy, runType);

    if (debugSignalsFeedback) {
      console.log(
        `[DEBUG][signals-feedback] Fetching last ${nSignalsFeedback} signals from chain for strategy="${strategyNameForFeedback}"...`
      );
    }

    const raw = await chain.getRecentSignalsForStrategy({ strategyName: strategyNameForFeedback, n: nSignalsFeedback });

    if (debugSignalsFeedback) {
      console.log(`[DEBUG][signals-feedback] Raw signals returned: ${raw.length}`);
    }

    feedbackSignals = raw.map((s) => {
      const isCash = String(s.asset || "").toUpperCase() === "USD" && Number(s.target) === 0;
      const signal = isCash ? "LONG_CASH" : (Number(s.target) === 1 ? "SHORT_ASSET" : "LONG_ASSET");
      const iso = s.timestamp ? new Date(Number(s.timestamp) * 1000).toISOString() : null;
      return { timestamp: iso, signal, explanation: String(s.message || "").trim() };
    });

    if (debugSignalsFeedback) {
      console.log(`[DEBUG][signals-feedback] Mapped previous_signals (${feedbackSignals.length}):`, feedbackSignals);
    }
  } catch (e) {
    // If the chain read fails, we simply proceed without feedback.
    feedbackSignals = [];
    if (debugSignalsFeedback) {
      console.warn(`[DEBUG][signals-feedback] Failed to load previous signals: ${String(e?.message || e)}`);
    }
  }
}



    const payload = {
      symbol,
      features: {
        asset: sanitizeFeatures(features),
        ...(String(INCLUDE_BTC_REGIME).toLowerCase() === "true" && strategy.type === "crypto" && btc && symbol !== "BTC" ? { btc_regime: btc } : {}),
        ...(macro ? { macro } : {}),
        ...(derivativesBySymbol.get(symbol) ? { derivatives: derivativesBySymbol.get(symbol) } : {}),
        ...(sentiment ? { sentiment } : {}),
        ...(trendsBySymbol.get(symbol) ? { trends: trendsBySymbol.get(symbol) } : {})
      },
      context: {
        ...context,
        previous_signals: feedbackSignals
      }
    };

    let decision;
    try {
      decision = await getAi(strategy.model).decideSignal(payload);
    } catch (e) {
      decision = {
        signal: "LONG_CASH",
        explanation: `AI call failed; defaulting to cash. Error: ${String(e.message || e)}`.slice(0, 600)
      };
    }

    const record = {
      timestamp,
      symbol,
      strategy: buildStrategyName(strategy, runType),
      model: strategy.model,
      version: strategy.version,
      chain_asset: strategy.chainAsset,
      run_type: runType,
      signal: decision.signal,
      explanation: tidyExplanation(decision.explanation),
      data_status: {
        macro: macroOk,
        derivatives: derivativesOkBySymbol.get(symbol) ?? false,
        btc_regime: Boolean(btc),
        sentiment: sentimentOk,
        trends: Boolean(trendsBySymbol.get(symbol))
      },
      features: payload.features
    };

    appendNdjson(logPath, record);
    console.log(`[${timestamp}] ${record.strategy}: ${decision.signal} — ${record.explanation}`);

    if (chain) {
      try {
        const strategyName = buildStrategyName(strategy, runType);
        const chainMapped = mapDecisionToChain(decision.signal, strategy.chainAsset);
        console.log(
          `[${timestamp}] Sending on-chain... strategy="${strategyName}" asset=${chainMapped.asset} target=${chainMapped.target}`
        );

        // Optional progress logs (off by default).
        const showProgress = envBool("CHAIN_SHOW_PROGRESS", false) || envBool("CHAIN_WAIT_FOR_RECEIPT", false);
        let ticks = 0;
        const timer = showProgress
          ? setInterval(() => {
              ticks += 1;
              if (ticks <= 60) {
                console.log(`[${nowIsoWithOffset()}] ... still waiting for RPC / confirmation (${ticks * 2}s)`);
              }
            }, 2000)
          : null;

        const sendTimeoutMs = Number(process.env.CHAIN_SEND_TIMEOUT_MS || 120000);

        let tx;
        try {
          tx = await Promise.race([
            chain.postSignal({
              strategyName,
              asset: chainMapped.asset,
              message: record.explanation,
              target: chainMapped.target,
              leverage: 1,
              weight: 100
            }),
            new Promise((_, reject) =>
              setTimeout(() => reject(new Error(`On-chain send timed out after ${sendTimeoutMs}ms`)), sendTimeoutMs)
            )
          ]);
        } finally {
          if (timer) clearInterval(timer);
        }
        const url = txUrl(tx.hash);
        const receiptStatus = tx.receipt?.status ? Number(BigInt(tx.receipt.status)) : null;
        const gasGwei = tx.gasPriceWei ? Number(tx.gasPriceWei) / 1e9 : null;
        if (tx.receipt) {
          const ok = receiptStatus === 1;
          console.log(
            `[${timestamp}] Posted on-chain (mined): ${strategyName} asset=${chainMapped.asset} target=${chainMapped.target} tx=${tx.hash} status=${ok ? "SUCCESS" : "REVERT"}${gasGwei ? ` gasPrice≈${gasGwei.toFixed(0)} gwei` : ""} ${url}`
          );
        } else if (tx.seenTx) {
          console.log(
            `[${timestamp}] Posted on-chain (broadcast): ${strategyName} asset=${chainMapped.asset} target=${chainMapped.target} tx=${tx.hash} (not mined yet)${gasGwei ? ` gasPrice≈${gasGwei.toFixed(0)} gwei` : ""} ${url}`
          );
        } else {
          console.warn(
            `[${timestamp}] Posted on-chain (unconfirmed): ${strategyName} tx=${tx.hash} — RPC returned a hash but the tx is not queryable yet. It was likely dropped by the node. Try raising CHAIN_GAS_PRICE_MULTIPLIER or CHAIN_MAX_GAS_PRICE_GWEI.${gasGwei ? ` last gasPrice≈${gasGwei.toFixed(0)} gwei` : ""} ${url}`
          );
        }
      } catch (e) {
        const msg = String(e.message || e);
        const strategyName = buildStrategyName(strategy, runType);
        console.error(`[${timestamp}] On-chain post failed for ${symbol} (${strategyName}): ${msg}`);
        appendNdjson(logPath, {
          timestamp,
          symbol,
          strategy: strategyName,
          model: strategy.model,
          version: strategy.version,
          chain_asset: strategy.chainAsset,
          run_type: runType,
          signal: decision.signal,
          explanation: record.explanation,
          chain_error: msg.slice(0, 800)
        });
      }
    }
  }
}

function schedule() {
  cron.schedule(
    CRON_EXPR,
    async () => {
      try {
        await runOnce({ runType: "scheduled" });
      } catch (e) {
        const timestamp = nowIsoWithOffset();
        const obj = {
          timestamp,
          symbol: "SYSTEM",
          strategy: "SYSTEM",
          run_type: "scheduled",
          signal: "LONG_CASH",
          explanation: `Scheduled run error: ${String(e.message || e)}`.slice(0, 600)
        };
        appendNdjson(logPath, obj);
        console.error(obj.explanation);
      }
    },
    { timezone: "Europe/Paris" }
  );

  console.log(`Scheduler active: '${CRON_EXPR}' (Europe/Paris). Logs: ${logPath}`);
}

(async () => {
  schedule();
  if (RUN_NOW) {
    await runOnce({ runType: "manual" });
  }
})();
