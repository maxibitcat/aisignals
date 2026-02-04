import { ethers } from "ethers";
// Use undici directly so we can control timeouts + keep-alive behavior.
// Some L2 RPC gateways will accept the TCP connection but never respond,
// and Node's global fetch pooling can make this look like a "hang".
import { Agent, fetch as undiciFetch } from "undici";

const ABI = [
  "function postSignal(string strategy,string asset,string message,uint8 target,uint8 leverage,uint16 weight) payable",
  "function postFee() view returns (uint256)",
  "function getTraderSignalIds(address trader) view returns (uint256[] memory)",
  "function getSignal(uint256 id) view returns (tuple(address trader,string strategy,string asset,string message,uint8 target,uint8 leverage,uint16 weight,uint64 timestamp))"
];

function must(name, value) {
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}

function isRpcInvalidRequest(err) {
  // ethers coalesces many error shapes; we look for JSON-RPC -32600 and/or "invalid request"
  const code = err?.code;
  const msg = String(err?.message || err?.error?.message || "");
  const innerCode = err?.error?.code;
  return innerCode === -32600 || code === -32600 || /invalid request/i.test(msg);
}

function hexToInt(hex) {
  if (typeof hex !== "string") throw new Error(`Expected hex string, got: ${typeof hex}`);
  return Number(BigInt(hex));
}

function clampBigInt(v, min, max) {
  if (v < min) return min;
  if (v > max) return max;
  return v;
}

function isUnderpriced(err) {
  const msg = String(err?.message || err?.error?.message || "");
  return /underpriced|fee too low|gas price too low|min gas price|insufficient fee/i.test(msg);
}

function bumpFactorForAttempt(attemptIndex) {
  // attemptIndex is 0-based. attempt 0 => 1.00x, attempt 1 => 1.20x, attempt 2 => 1.44x ...
  // Using a mild exponential bump avoids spamming the network while still escaping low-fee floors.
  const base = 1.2;
  return Math.pow(base, Math.max(0, attemptIndex));
}

export class ChainSignalsClient {
  /**
   * @param {{ rpcUrl: string, chainId: number|string, chainName?: string, contractAddress: string, privateKey: string, gasPriceGwei?: number|string, gasLimit?: number|string }} opts
   */
  constructor(opts) {
    this.rpcUrl = must("CHAIN_RPC_URL", opts.rpcUrl);

    // Fail fast on a very common misconfiguration: using a WebSocket URL for HTTP JSON-RPC.
    // This client uses undici's HTTP fetch, so CHAIN_RPC_URL must be http(s)://...
    if (/^wss?:\/\//i.test(this.rpcUrl)) {
      throw new Error(
        `Invalid CHAIN_RPC_URL scheme: expected http(s):// but got ${this.rpcUrl.slice(0, 6)}... (full=${this.rpcUrl})`
      );
    }
    this.chainId = Number(opts.chainId);
    if (!Number.isFinite(this.chainId)) throw new Error("Invalid CHAIN_ID");
    this.chainName = String(opts.chainName || "kasplex l2");
    this.contractAddress = must("CHAIN_SIGNALS_ADDRESS", opts.contractAddress);
    this.privateKey = must("DEPLOYER_PK", opts.privateKey);

    // Many non-mainstream RPCs can be picky about:
    // - network autodetection
    // - certain JSON-RPC parameter encodings
    // - EIP-1559 fee methods
    //
    // We therefore:
    // 1) Use a static network (prevents repeated detection / retries). 
    // 2) Craft and broadcast a *legacy type-0* tx with a fixed gasPrice.
    // 3) Fetch nonce using a fallback ladder if eth_getTransactionCount rejects default params.
    // Gas price handling:
    // - If CHAIN_GAS_PRICE_GWEI is set, we use it (clamped to MAX).
    // - Otherwise we query eth_gasPrice and apply a multiplier, also clamped.
    this.gasPriceGwei = Number(opts.gasPriceGwei ?? process.env.CHAIN_GAS_PRICE_GWEI ?? NaN);
    this.maxGasPriceGwei = Number(process.env.CHAIN_MAX_GAS_PRICE_GWEI ?? 50000);
    if (!Number.isFinite(this.maxGasPriceGwei) || this.maxGasPriceGwei <= 0) this.maxGasPriceGwei = 50000;

    this.gasPriceMultiplier = Number(process.env.CHAIN_GAS_PRICE_MULTIPLIER ?? 1.0);
    if (!Number.isFinite(this.gasPriceMultiplier) || this.gasPriceMultiplier <= 0) this.gasPriceMultiplier = 1.0;

    this.broadcastRetryAttempts = Number(process.env.CHAIN_BROADCAST_RETRIES ?? 5);
    if (!Number.isFinite(this.broadcastRetryAttempts) || this.broadcastRetryAttempts < 1) this.broadcastRetryAttempts = 5;

    this.waitReceipt = String(process.env.CHAIN_WAIT_RECEIPT ?? "false").toLowerCase() === "true";

    // Some zkEVM/L2 nodes require EIP-1559 (type 2) txs, others only accept legacy.
    // Default: auto-detect by probing eth_feeHistory. You can override with:
    //   CHAIN_TX_TYPE=legacy|eip1559
    this.txTypePref = String(process.env.CHAIN_TX_TYPE ?? "auto").toLowerCase();

    this.gasLimit = Number(opts.gasLimit ?? process.env.CHAIN_GAS_LIMIT ?? 300000);
    if (!Number.isFinite(this.gasLimit) || this.gasLimit <= 0) this.gasLimit = 300000;

    // IMPORTANT:
    // Some RPC frontends behave differently depending on HTTP connection reuse.
    // We:
    // - disable keep-alive (new connection per call) to avoid stuck pooled sockets
    // - set strict connect/headers/body timeouts
    // - still keep an AbortController as an extra escape hatch
    this._rpcId = 1;
    // Logging controls:
    // - CHAIN_RPC_DEBUG=1/true -> minimal debug (errors + fee decisions)
    // - CHAIN_RPC_TRACE=1/true -> log every RPC request/response
    const __dbg = String(process.env.CHAIN_RPC_DEBUG ?? "").trim().toLowerCase();
    const __trace = String(process.env.CHAIN_RPC_TRACE ?? "").trim().toLowerCase();
    this._rpcDebug = (__dbg === "1" || __dbg === "true" || __dbg === "yes" || __dbg === "on" || __dbg === "debug");
    this._rpcTrace = (__trace === "1" || __trace === "true" || __trace === "yes" || __trace === "on" || __trace === "trace");

    // Local nonce allocator to support back-to-back txs in the same run.
    // Some RPCs return the same "pending" nonce until the tx is visible in
    // their mempool / indexing layer. If we ask for the nonce twice in quick
    // succession, we may reuse the same nonce and effectively drop the 2nd tx.
    this._nextNonce = null;

    const connectTimeout = Number(process.env.CHAIN_RPC_CONNECT_TIMEOUT_MS ?? 7_500);
    const headersTimeout = Number(process.env.CHAIN_RPC_HEADERS_TIMEOUT_MS ?? 10_000);
    const bodyTimeout = Number(process.env.CHAIN_RPC_BODY_TIMEOUT_MS ?? 15_000);
    // NOTE (Kasplex RPC / undici compatibility):
    // Some undici versions treat keepAliveTimeout=0 as an invalid argument
    // and throw: UND_ERR_INVALID_ARG (cause=invalid keepAliveTimeout).
    // We still want effectively "no keep-alive" behavior, so we:
    // - keep sending `Connection: close` on every request
    // - use the minimum valid keepAliveTimeout values (1ms)
    // This avoids the constructor throw while still preventing pooled sockets.
    this._dispatcher = new Agent({
      keepAliveTimeout: 1,
      keepAliveMaxTimeout: 1,
      connectTimeout,
      headersTimeout,
      bodyTimeout
    });

    // Wallet is used only for signing (offline). Broadcasting is done via raw JSON-RPC.
    this.wallet = new ethers.Wallet(this.privateKey);
    this.contract = new ethers.Contract(this.contractAddress, ABI);
    this.iface = new ethers.Interface(ABI);
    this._postFeeWei = null;
    this._postFeeFetchedAt = 0;
    this._supports1559 = null;
    this._remoteChainId = null;

    // Read-only caches (per process) for feedback fetching
    this._traderSignalIdsCache = new Map();
    this._signalByIdCache = new Map();
  }

  async _rpcCall(method, params) {
    const payload = {
      jsonrpc: "2.0",
      id: this._rpcId++,
      method,
      params
    };

    // Extra guard: a hard overall timeout around the request.
    const timeoutMs = Number(process.env.CHAIN_RPC_TIMEOUT_MS ?? 20_000);
    const controller = new AbortController();
    const t = setTimeout(() => controller.abort(), timeoutMs);

    if (this._rpcTrace) {
      // eslint-disable-next-line no-console
      console.log(`[rpc] -> ${method}`, JSON.stringify(payload).slice(0, 500));
    }

    let res;
    try {
      res = await undiciFetch(this.rpcUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
          // Many RPC gateways have buggy keep-alive behavior.
          Connection: "close"
        },
        body: JSON.stringify(payload),
        signal: controller.signal,
        dispatcher: this._dispatcher
      });
    } catch (e) {
      const isAbort = e?.name === "AbortError";
      const code = e?.code || e?.cause?.code;
      const causeMsg = e?.cause?.message;
      const msg = isAbort
        ? `RPC ${method} timed out after ${timeoutMs}ms (url=${this.rpcUrl})`
        : `RPC ${method} failed: ${String(e?.message || e)}${code ? ` (code=${code})` : ""}${causeMsg ? ` (cause=${causeMsg})` : ""} (url=${this.rpcUrl})`;
      const err = new Error(msg);
      err.cause = e;
      err.payload = payload;
      throw err;
    } finally {
      clearTimeout(t);
    }

    // If the endpoint is behind a proxy, it may still respond with non-200 codes.
    const text = await res.text();
    let json;
    try {
      json = JSON.parse(text);
    } catch {
      throw new Error(`RPC ${method} failed: non-JSON response (status=${res.status}) ${text.slice(0, 200)}`);
    }

    if (this._rpcTrace) {
      // eslint-disable-next-line no-console
      console.log(`[rpc] <- ${method} status=${res.status}`, JSON.stringify(json).slice(0, 500));
    }

    if (json?.error) {
      const err = new Error(json.error.message || `RPC error calling ${method}`);
      // Preserve code in a place our helpers already check.
      err.error = { code: json.error.code, message: json.error.message };
      err.code = json.error.code;
      err.payload = payload;
      throw err;
    }

    return json.result;
  }
  async _getTransactionCount(address, blockTag = "pending") {
    const addr = ethers.getAddress(address);
    // Some RPCs are picky about the second param; try pending -> latest -> omit.
    const tryParams = [
      [addr, blockTag],
      [addr, "latest"],
      [addr],
    ];
    let lastErr;
    for (const params of tryParams) {
      try {
        const hex = await this._rpcCall("eth_getTransactionCount", params);
        return BigInt(hex);
      } catch (e) {
        lastErr = e;
      }
    }
    throw lastErr;
  }

  async _allocNonce(address) {
    // Allocate a nonce locally (monotonic) to allow multiple signals in a single run.
    // Some RPCs do not reflect freshly-broadcast txs in `pending` immediately.
    if (this._nextNonce === null) {
      const n = await this._getTransactionCount(address, "pending");
      this._nextNonce = n + 1n;
      return n;
    }
    const n = this._nextNonce;
    this._nextNonce = n + 1n;
    return n;
  }

  async _getRemoteChainId() {
    if (this._remoteChainId !== null) return this._remoteChainId;
    try {
      const hex = await this._rpcCall("eth_chainId", []);
      if (typeof hex === "string" && hex.startsWith("0x")) {
        this._remoteChainId = Number(BigInt(hex));
        return this._remoteChainId;
      }
    } catch {
      // ignore; some nodes don't implement eth_chainId properly
    }
    this._remoteChainId = this.chainId;
    return this._remoteChainId;
  }

  // Backward-compat: older refactors called this._getChainId() directly.
  // Keep as an alias to avoid runtime crashes.
  async _getChainId() {
    // If config provided a chainId, use it. Otherwise ask the node.
    if (typeof this.chainId === "number" && Number.isFinite(this.chainId) && this.chainId > 0) {
      return this.chainId;
    }
    return await this._getRemoteChainId();
  }

  async _sleep(ms) {
    await new Promise((r) => setTimeout(r, ms));
  }

  async _waitForTxInNode(txHash, { attempts = 8, delayMs = 750 } = {}) {
    // Some RPC nodes return a hash immediately but only make the tx queryable a moment later.
    // This helps us distinguish "broadcast ok, just not indexed yet" from "not propagated".
    for (let i = 0; i < attempts; i++) {
      const tx = await this._rpcCall("eth_getTransactionByHash", [txHash]);
      if (tx) return tx;
      await this._sleep(delayMs);
    }
    return null;
  }

  async _waitForPendingNonceBump(address, sentNonce, { attempts = 8, delayMs = 750 } = {}) {
    // Many RPC gateways are flaky about eth_getTransactionByHash for *pending* transactions.
    // However, once a tx is accepted into the node's mempool, eth_getTransactionCount(..., "pending")
    // usually returns the next available nonce (sentNonce + 1).
    for (let i = 0; i < attempts; i++) {
      try {
        const n = await this._getNonce(address);
        if (Number.isFinite(n) && n > sentNonce) return true;
      } catch {
        // ignore and keep trying
      }
      await this._sleep(delayMs);
    }
    return false;
  }

  async waitForReceipt(txHash, { timeoutMs = 120_000, pollMs = 2_000 } = {}) {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      const receipt = await this._rpcCall("eth_getTransactionReceipt", [txHash]);
      if (receipt) return receipt;
      await this._sleep(pollMs);
    }
    return null;
  }

  async _getPostFeeWei() {
    // Cache for 60s; fee rarely changes.
  const now = Date.now();
  if (this._postFeeWei !== null && (now - this._postFeeFetchedAt) < 60_000) return this._postFeeWei;

  const iface = this.contract.interface;
  const data = iface.encodeFunctionData("postFee", []);
  const result = await this._rpcCall("eth_call", [{ to: this.contractAddress, data }, "latest"]);

  // eth_call returns a hex-encoded uint256 (e.g. "0x01"). BigInt can parse that directly.
  const fee = BigInt(result);
  this._postFeeWei = fee;
  this._postFeeFetchedAt = now;
  return fee;
}


  async _ethCall(functionName, args) {
    const data = this.iface.encodeFunctionData(functionName, args);
    const result = await this._rpcCall("eth_call", [{ to: this.contractAddress, data }, "latest"]);
    return this.iface.decodeFunctionResult(functionName, result);
  }

  /**
   * Get all signal ids posted by a trader address.
   * @param {string} trader
   * @returns {Promise<bigint[]>}
   */
  async getTraderSignalIds(trader) {
    const t = String(trader || "").trim();
    if (!t) throw new Error("getTraderSignalIds: missing trader address");

    if (this._traderSignalIdsCache.has(t)) return this._traderSignalIdsCache.get(t);

    const decoded = await this._ethCall("getTraderSignalIds", [t]);
    const ids = (decoded?.[0] ?? []).map((x) => BigInt(x));
    this._traderSignalIdsCache.set(t, ids);
    return ids;
  }

  /**
   * Get a single signal by id.
   * @param {bigint|number|string} id
   * @returns {Promise<{trader:string,strategy:string,asset:string,message:string,target:number,leverage:number,weight:number,timestamp:number}>}
   */
  async getSignalById(id) {
    const key = String(id);
    if (this._signalByIdCache.has(key)) return this._signalByIdCache.get(key);

    const decoded = await this._ethCall("getSignal", [id]);
    const s = decoded?.[0];
    const obj = {
      trader: s?.trader,
      strategy: s?.strategy,
      asset: s?.asset,
      message: s?.message,
      target: Number(s?.target ?? 0),
      leverage: Number(s?.leverage ?? 0),
      weight: Number(s?.weight ?? 0),
      timestamp: Number(s?.timestamp ?? 0)
    };
    this._signalByIdCache.set(key, obj);
    return obj;
  }

  /**
   * Fetch the most recent N on-chain signals posted by this bot for a given strategy name.
   * We search from newest -> oldest and return results in chronological order (oldest -> newest).
   *
   * @param {{strategyName:string, n:number, trader?:string}} p
   * @returns {Promise<Array<{timestamp:number,asset:string,target:number,message:string}>>}
   */
  async getRecentSignalsForStrategy({ strategyName, n, trader }) {
    const want = Math.max(0, Number(n || 0));
    if (!want) return [];

    const strategy = String(strategyName || "").trim();
    if (!strategy) return [];

    const addr = String(trader || this.wallet.address).trim();
    const ids = await this.getTraderSignalIds(addr);
    if (!ids.length) return [];

    const out = [];
    for (let i = ids.length - 1; i >= 0 && out.length < want; i--) {
      const id = ids[i];
      const sig = await this.getSignalById(id);
      if (String(sig.strategy || "").trim() !== strategy) continue;
      out.push({ timestamp: sig.timestamp, asset: sig.asset, target: sig.target, message: sig.message });
    }

    return out.reverse();
  }
  async _getNonce(address) {
    // Prefer "pending" so we don't accidentally reuse a nonce when the node has
    // a pending tx from us already (common cause of silent drops / replacement).
    // Not all RPCs support it, so we fall back.
    try {
      const hex = await this._rpcCall("eth_getTransactionCount", [address, "pending"]);
      return hexToInt(hex);
    } catch (e0) {
      if (!isRpcInvalidRequest(e0)) throw e0;
    }

    // Try standard tag.
    try {
      const hex = await this._rpcCall("eth_getTransactionCount", [address, "latest"]);
      return hexToInt(hex);
    } catch (e1) {
      if (!isRpcInvalidRequest(e1)) throw e1;
    }

    // Some RPCs treat the block tag as optional and reject the 2nd param.
    try {
      const hex = await this._rpcCall("eth_getTransactionCount", [address]);
      return hexToInt(hex);
    } catch (e2) {
      if (!isRpcInvalidRequest(e2)) throw e2;
    }

    // Some RPCs only accept a hex block number, not a tag string.
    const blockHex = await this._rpcCall("eth_blockNumber", []);
    const hex = await this._rpcCall("eth_getTransactionCount", [address, blockHex]);
    return hexToInt(hex);
  }

  async _getSuggestedGasPriceWei() {
    // Standard JSON-RPC method. Returns wei as hex string.
    const hex = await this._rpcCall("eth_gasPrice", []);
    if (typeof hex !== "string" || !hex.startsWith("0x")) {
      throw new Error(`eth_gasPrice: unexpected result: ${String(hex).slice(0, 50)}`);
    }
    return BigInt(hex);
  }

  async _getBalanceWei(address) {
    const hex = await this._rpcCall("eth_getBalance", [address, "latest"]);
    if (typeof hex !== "string" || !hex.startsWith("0x")) {
      throw new Error(`eth_getBalance: unexpected result: ${String(hex).slice(0, 50)}`);
    }
    return BigInt(hex);
  }

  // NOTE: the file previously contained multiple duplicated method definitions
  // (_estimateGas/_getBalanceWei/_detect*1559) due to a bad merge. Those duplicates
  // make it harder to reason about what code is actually running. Keep a single
  // implementation per method.

  async _chooseBaseGasPriceWei() {
    const maxWei = ethers.parseUnits(String(this.maxGasPriceGwei), "gwei");

    // If explicitly configured, use it (still clamp to max).
    if (Number.isFinite(this.gasPriceGwei) && this.gasPriceGwei > 0) {
      const fixed = ethers.parseUnits(String(this.gasPriceGwei), "gwei");
      return clampBigInt(fixed, 1n, maxWei);
    }

    // Otherwise use the RPC suggestion * multiplier (clamped).
    const suggested = await this._getSuggestedGasPriceWei();
    const mul = this.gasPriceMultiplier;
    // Multiply BigInt by a float safely by scaling.
    const scaled = BigInt(Math.max(1, Math.round(mul * 1000)));
    const bumped = (suggested * scaled) / 1000n;
    return clampBigInt(bumped, 1n, maxWei);
  }

  async _detectSupports1559() {
    if (this._supports1559 !== null) return this._supports1559;
    if (this.txTypePref === "legacy") {
      this._supports1559 = false;
      return false;
    }
    if (this.txTypePref === "eip1559") {
      this._supports1559 = true;
      return true;
    }
    // Auto: probe eth_feeHistory (standard EIP-1559 RPC). If it errors, assume legacy.
    try {
      await this._rpcCall("eth_feeHistory", ["0x1", "latest", []]);
      this._supports1559 = true;
      return true;
    } catch {
      this._supports1559 = false;
      return false;
    }
  }

  async _estimateGas(txObj) {
    try {
      const hex = await this._rpcCall("eth_estimateGas", [txObj]);
      if (typeof hex !== "string" || !hex.startsWith("0x")) return null;
      return BigInt(hex);
    } catch {
      return null;
    }
  }

  async _getBaseFeeWei() {
    // Fetch latest baseFeePerGas via eth_feeHistory.
    // Some RPCs accept feeHistory but return baseFeePerGas as an array of hex strings.
    try {
      const fh = await this._rpcCall("eth_feeHistory", ["0x1", "latest", []]);
      const arr = fh?.baseFeePerGas;
      if (Array.isArray(arr) && arr.length > 0 && typeof arr[arr.length - 1] === "string") {
        return BigInt(arr[arr.length - 1]);
      }
    } catch {
      // ignore
    }
    return null;
  }

  _toRpcQuantity(v) {
    // Kasplex's Go-based RPC is strict about hex quantities:
    // - no leading zero digits (e.g., 0x00 is rejected)
    // ethers.toQuantity produces canonical JSON-RPC quantities.
    return ethers.toQuantity(v);
  }

  _isUnderpricedError(err) {
    const msg = String(err?.message || err?.error?.message || "");
    return /underpriced|fee too low|min gas|insufficient fee/i.test(msg);
  }

  
  /**
   * Posts a signal on-chain.
   * @param {object} p
   * @param {string} p.strategy - 1-30 chars
   * @param {string} p.asset - 1-10 chars
   * @param {string} [p.message] - up to 280 chars
   * @param {"Long"|"Short"|0|1} p.target - 0=Long, 1=Short
   * @param {number} [p.leverage] - 1..5
   * @param {number} [p.weight] - 1..100
   */

  async postSignal({ strategy, strategyName, strategy_name, strategyTitle, name, asset, target, comment = "", message, leverage = 1, weight = 1, ...rest }) {
    // Validate inputs early to avoid on-chain reverts and confusing UX.
    const rawStrategy =
      strategy ??
      strategyName ??
      strategy_name ??
      strategyTitle ??
      name ??
      (strategy && typeof strategy === "object" ? (strategy.name ?? strategy.title ?? strategy.id) : undefined) ??
      undefined;
    const strat = String(rawStrategy ?? "").trim();
    if (strat.length < 1 || strat.length > 30) {
      throw new Error(`Invalid strategy: must be 1-30 chars (got ${rawStrategy === null ? "null" : rawStrategy === undefined ? "undefined" : strat.length})`);
    }
    const a = String(asset ?? "").trim();
    if (a.length < 1 || a.length > 10) {
      throw new Error(`Invalid asset: must be 1-10 chars (got ${a.length})`);
    }

    const pickNonEmpty = (...vals) => {
      for (const v of vals) {
        if (v === undefined || v === null) continue;
        const s = String(v).trim();
        if (s) return s;
      }
      return "";
    };

    // Accept multiple legacy/new field names so we never silently drop the expla...
    // Be consistent: the app should pass `message` (preferred) or `comment` (legacy).
    const msg = String(message ?? comment ?? "").trim();
    if (msg.length > 280) {
      throw new Error(`Invalid message: max 280 chars (got ${msg.length})`);
    }

    // Normalize target to enum value 0 (Long) / 1 (Short).
    let targetEnum;
    if (typeof target === "number") {
      targetEnum = target;
    } else if (typeof target === "bigint") {
      targetEnum = Number(target);
    } else {
      const tstr = String(target ?? "").trim().toLowerCase();
      if (tstr === "long" || tstr === "0") targetEnum = 0;
      else if (tstr === "short" || tstr === "1") targetEnum = 1;
      else throw new Error(`Invalid target: expected Long/Short/0/1 (got ${String(target)})`);
    }

    const lev = Number(leverage ?? 1);
    if (!Number.isFinite(lev) || lev < 1 || lev > 5) {
      throw new Error(`Invalid leverage: must be 1-5 (got ${String(leverage)})`);
    }

    const w = Number(weight ?? 1);
    if (!Number.isFinite(w) || w < 1 || w > 100) {
      throw new Error(`Invalid weight: must be 1-100 (got ${String(weight)})`);
    }

    // Encode calldata as postSignal(strategy, asset, message, target, leverage, weight)
    const data = this.iface.encodeFunctionData("postSignal", [strat, a, msg, targetEnum, lev, w]);

    const from = await this.wallet.getAddress();
    const chainId = await this._getChainId();
    // Local nonce allocator: allows multiple txs per run even if `pending`
    // does not reflect freshly-broadcast txs immediately.
    const nonce = await this._allocNonce(from);

    // Gas estimate (with a buffer).
    const est = await this._estimateGas({ from, to: this.contractAddress, data });
    const gasEstimate = est ?? 300_000n;
    const gasLimit = (gasEstimate * 122n) / 100n; // +22% buffer

    // If the contract requires a fee (msg.value), fetch it (best effort).
    let valueWei = 0n;
    try {
      // postFee() selector used in earlier versions.
      const feeHex = await this._rpcCall("eth_call", [
        { to: this.contractAddress, data: "0xfcf7becf" },
        "latest",
      ]);
      if (typeof feeHex === "string" && feeHex.startsWith("0x")) valueWei = BigInt(feeHex);
    } catch {
      // ignore
    }

    const supports1559 = await this._detectSupports1559();

    // Build the transaction request with *minimum reasonable* fees.
    // We do NOT spam rebroadcasts. We return the tx hash immediately after broadcast.
    /** @type {import('ethers').TransactionRequest} */
    const txReq = {
      to: this.contractAddress,
      from,
      data,
      nonce,
      chainId,
      gasLimit,
      value: valueWei,
    };

    if (supports1559) {
      const baseFee = (await this._getBaseFeeWei()) ?? (await this._getSuggestedGasPriceWei());
      // Default priority fee: 1 gwei (override via CHAIN_PRIORITY_FEE_GWEI).
      const prioGwei = Number.isFinite(this.priorityFeeGwei) && this.priorityFeeGwei > 0 ? this.priorityFeeGwei : 1;
      const maxPriorityFeePerGas = ethers.parseUnits(String(prioGwei), "gwei");
      // Keep fees as low as possible: maxFee = baseFee + priority.
      // (You only ever pay baseFee + priority; extra headroom only increases max cap)
      const maxFeePerGas = baseFee + maxPriorityFeePerGas;
      txReq.type = 2;
      txReq.maxFeePerGas = clampBigInt(maxFeePerGas, 1n, ethers.parseUnits(String(this.maxGasPriceGwei), "gwei"));
      txReq.maxPriorityFeePerGas = clampBigInt(maxPriorityFeePerGas, 1n, txReq.maxFeePerGas);
    } else {
      const gasPrice = await this._chooseBaseGasPriceWei();
      txReq.gasPrice = gasPrice;
    }

    // Sign + broadcast.
    const rawTx = await this.wallet.signTransaction(txReq);
    const txHash = await this._rpcCall("eth_sendRawTransaction", [rawTx]);

    // Optional receipt wait (short + bounded). Default: do NOT wait.
    if (!this.waitForReceipt) {
      return { txHash, nonce, chainId };
    }

    const receiptTimeoutSec = Number(this.receiptTimeoutSec ?? 0);
    if (!Number.isFinite(receiptTimeoutSec) || receiptTimeoutSec <= 0) {
      return { txHash, status: "broadcast", nonce, chainId };
    }

    const timeoutMs = Math.max(1_000, receiptTimeoutSec * 1000);
    const pollMs = Math.max(500, this.receiptPollMs ?? 2000);
    const start = Date.now();

    while (Date.now() - start < timeoutMs) {
      const receipt = await this._rpcCall("eth_getTransactionReceipt", [txHash]).catch(() => null);
      if (receipt) return { txHash, nonce, chainId, receipt };
      await this._sleep(pollMs);
    }

    // Still pending: return the hash so the caller can continue.
    return { txHash, nonce, chainId, pending: true };
  }


}
