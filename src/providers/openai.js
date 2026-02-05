import axios from "axios";
import { z } from "zod";

const OutputSchema = z.object({
  signal: z.enum(["LONG_ASSET", "LONG_CASH", "SHORT_ASSET"]),
  explanation: z.string().min(1).max(280)
});

function safeJsonKeys(configData) {
  try {
    const obj = JSON.parse(configData);
    return Object.keys(obj);
  } catch {
    return null;
  }
}

function describeAxiosError(err) {
  return {
    message: err?.message,
    status: err?.response?.status,
    statusText: err?.response?.statusText,
    url: err?.config?.url,
    method: err?.config?.method,
    requestBodyKeys: err?.config?.data ? safeJsonKeys(err.config.data) : null,
    responseData: err?.response?.data
  };
}


function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function shouldRetryAxiosError(err) {
  const status = err?.response?.status;
  // Retry on network errors / timeouts (no response), rate limits, and transient server errors.
  if (!status) return true;
  if (status === 408 || status === 409 || status === 425 || status === 429) return true;
  if (status >= 500 && status <= 599) return true;
  return false;
}

async function postWithRetry(url, body, axiosConfig, { retries = 3, baseDelayMs = 750 } = {}) {
  let attempt = 0;
  let lastErr = null;

  while (attempt <= retries) {
    try {
      return await axios.post(url, body, axiosConfig);
    } catch (err) {
      lastErr = err;
      const retryable = shouldRetryAxiosError(err);
      if (!retryable || attempt === retries) break;

      const jitter = Math.floor(Math.random() * 250);
      const delay = baseDelayMs * Math.pow(2, attempt) + jitter;
      console.warn(
        `[OpenAI] transient error (attempt ${attempt + 1}/${retries + 1}); retrying in ${delay}ms...`
      );
      await sleep(delay);
      attempt += 1;
    }
  }
  throw lastErr;
}

export class OpenAIProvider {
  constructor({ apiKey, model }) {
    this.apiKey = apiKey;
    this.model = model;
  }

  async decideSignal({ symbol, features, context }) {
    const instructions = [
      "You are a trading signal engine for a DAILY strategy.",
      "- LONG_ASSET: fully long the asset",
      "- LONG_CASH: no position (cash)",
      "- SHORT_ASSET: short the asset (assume borrowing is possible; no leverage)",
      "At decision time, you must choose exactly one: LONG_ASSET, LONG_CASH, or SHORT_ASSET.",
      "Objective: maximize long-term Sharpe ratio (return/volatility) of a strategy that follows your daily signal.",
      "You MUST base your decision ONLY on the provided JSON inputs (asset regime + optional BTC regime + optional macro + optional derivatives (including liquidity) + optional sentiment + optional trends).",
      "If context.previous_signals is present, treat it as feedback about what you said recently. Do NOT repeat the same explanation day after day; keep it fresh or say nothing changed.",
      "If inputs are missing, stale, contradictory, or too uncertain, choose LONG_CASH.",
      "Output MUST match the required JSON schema exactly. You are producing data that will be written ON-CHAIN. Your explanation MUST be AT MOST 280 CHARACTERS (including spaces and punctuation). If you exceed 280 characters, the signal becomes invalid. Write a SHORT explanation: 1–2 sentences, mostly qualitative. End with a period. Do not include newlines. Write in English only. Do NOT include many numbers; at most one or two key figures if absolutely necessary. Do NOT mention Sharpe ratio or optimization. Do NOT restate the signal; only give the reasoning. SHORT_ASSET is fully allowed. If you expect negative returns over the next holding period, you may choose SHORT_ASSET instead of LONG_CASH."
    ].join(" ");

    const user = {
      task: "Decide today's position for the given asset for the next 24h until the next rebalance.",
      symbol,
      features,
      context
    };

    // Responses API Structured Outputs:
    // `text.format` requires a top-level `name` field.
    const body = {
      model: this.model,
      instructions,
      input: "INPUT_JSON:\n" + JSON.stringify(user),
      text: {
        format: {
          type: "json_schema",
          name: "signal_output",
          strict: true,
          schema: {
            type: "object",
            additionalProperties: false,
            properties: {
              signal: { type: "string", enum: ["LONG_ASSET", "LONG_CASH", "SHORT_ASSET"] },
              explanation: { type: "string", minLength: 1, maxLength: 280 }
            },
            required: ["signal", "explanation"]
          }
        }
      }
    };

    let response;
    try {
      const maxRetries = Math.max(0, Number(process.env.OPENAI_MAX_RETRIES ?? 3));
      response = await postWithRetry(
        "https://api.openai.com/v1/responses",
        body,
        {
          headers: {
            Authorization: `Bearer ${this.apiKey}`,
            "Content-Type": "application/json"
          },
          timeout: 60_000
        },
        { retries: maxRetries, baseDelayMs: 750 }
      );
    } catch (err) {
      const info = describeAxiosError(err);
      console.error("[OpenAI] responses error:", JSON.stringify(info, null, 2));
      throw new Error(`OpenAI responses failed: ${JSON.stringify(info)}`);
    }

    const outText = response?.data?.output_text ?? null;

    let parsed;
    if (outText) {
      parsed = JSON.parse(outText);
    } else {
      const output = response?.data?.output || [];
      let text = null;
      for (const item of output) {
        if (item?.type === "message") {
          const content = item?.content || [];
          const textPart = content.find((c) => c.type === "output_text" && c.text);
          if (textPart) {
            text = textPart.text;
            break;
          }
        }
      }
      if (!text) throw new Error("OpenAI response missing output_text.");
      parsed = JSON.parse(text);
    }

    return OutputSchema.parse(parsed);
  }
}