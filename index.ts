import { createPublicClient, createWalletClient, http, webSocket, encodeFunctionData, getContract, PublicClient, WalletClient, parseAbi, Address, Hex } from 'viem';
import { privateKeyToAccount } from 'viem/accounts';
import { linea } from 'viem/chains';
import { Pool } from 'pg';
import axios from 'axios';
import dotenv from 'dotenv';
import pino from 'pino';

dotenv.config();
const logger = pino({ level: process.env.LOG_LEVEL || 'info', transport: { target: 'pino-pretty', options: { colorize: true, translateTime: true } } });

// ============================================================
// 1. CHAIN CONFIGURATION (Linea only)
// ============================================================
const CHAIN = {
  id: 59144,
  name: 'linea',
  viemChain: linea,
  aavePool: '0xc47b8c00b0f69a36fa203ffeac0334874574a8ac' as Address,
  poolAddressesProvider: '0x89502c3731f69ddc95b65753708a07f8cd0373f4' as Address,
  executor: process.env.EXECUTOR_LINEA || '0x0000000000000000000000000000000000000000',
};

// ============================================================
// 2. DEX FACTORIES & ROUTERS (Linea - all major)
// ============================================================
const FACTORIES: { name: string; address: Address; type: 'v2' | 'v3'; enumerable: boolean; feeTiers?: number[] }[] = [
  { name: 'Lynex', address: '0xBc7695Fd00E3b32D08124b7a4287493aEE99f9ee', type: 'v3', enumerable: true, feeTiers: [100, 500, 3000] },
  { name: 'PancakeSwap V3', address: '0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865', type: 'v3', enumerable: true, feeTiers: [100, 500, 2500, 10000] },
  { name: 'Nile V1', address: '0xAAA32926fcE6bE95ea2c51cB4Fcb60836D320C42', type: 'v2', enumerable: false },
  { name: 'SyncSwap Classic', address: '0x37BAc764494c8db4e54BDE72f6965beA9fa0AC2d', type: 'v2', enumerable: false },
  { name: 'SyncSwap Stable', address: '0xE4CF807E351b56720B17A59094179e7Ed9dD3727', type: 'v2', enumerable: false },
  { name: 'SyncSwap Aqua', address: '0x1080EE857D165186aF7F8d63e8ec510C28A6d1Ea', type: 'v2', enumerable: false },
  { name: 'iZiSwap', address: '0x45e5F26451CDB01B0fA1f8582E0aAD9A6F27C218', type: 'v3', enumerable: true, feeTiers: [100, 500, 3000] },
];

const ROUTERS: Address[] = [
  '0x610D2f07b7EdC67565160F587F37636194C34E74', // Lynex Router
  '0xFE6508f0015C778Bdcc1fB5465bA5ebE224C9912', // PancakeSwap Universal Router
  '0xAAA45c8F5ef92a000a121d102F4e89278a711Faa', // Nile Router
  '0x80e38291e06339d10AAB483C65695D004dBD5C69', // SyncSwap Router v1
  '0xc2a1947d2336b2af74d5813dc9ca6e0c3b3e8a1e', // SyncSwap Router v2
  '0x032b241De86a8660f1Ae0691a4760B426EA246d7', // iZiSwap Router
];

// ============================================================
// 3. AAVE BORROWABLE ASSETS + EXTRA TOKENS (expandable)
// ============================================================
const AAVE_BORROWABLE_ASSETS: Address[] = [
  '0xe5D7c2a44FfDDf6b295A15c148167daaAf5Cf34f', // WETH
  '0x176211869cA2b568f2A7D4EE941E073a821EE1ff', // USDC
  '0xA219439258ca9da29E9Cc4cE5596924745e12B93', // USDT
  '0x3aab2285ddcddad8edf438c1bab47e1a9d05a9b4', // WBTC
  '0xB5beDd42000b71FddE22D3eE8a79Bd49A568fC8F', // wstETH
];

// Add any extra tokens here (e.g., USDC.e, DAI, etc.) to increase detection
const EXTRA_TOKENS: Address[] = [
  // Example: '0x...' – add real token addresses if you have them
];

const ALL_CANDIDATE_TOKENS = [...AAVE_BORROWABLE_ASSETS, ...EXTRA_TOKENS];

// ============================================================
// 4. RPC ENDPOINTS (public + fallbacks)
// ============================================================
const HTTP_RPCS = [
  'https://rpc.linea.build',
  'https://rpc.ankr.com/linea',
  'https://linea.drpc.org',
];
const WS_RPCS = [
  'wss://linea-mainnet.g.alchemy.com/v2/iEikaO2uqERzi7hrxxEVs',
];

// ============================================================
// 5. ABIs (standard + DEX‑specific)
// ============================================================
const ERC20_ABI = parseAbi([
  'function totalSupply() view returns (uint256)',
  'function balanceOf(address) view returns (uint256)',
  'function transfer(address, uint256) returns (bool)',
  'function owner() view returns (address)',
  'function decimals() view returns (uint8)',
  'function mint(address, uint256) returns (bool)',
]);

const PAIR_ABI = parseAbi([
  'function getReserves() view returns (uint112 reserve0, uint112 reserve1, uint32 blockTimestampLast)',
  'function token0() view returns (address)',
  'function token1() view returns (address)',
]);

const UNIV3_POOL_ABI = parseAbi([
  'function slot0() view returns (uint160 sqrtPriceX96, int24 tick, uint16 observationIndex, uint16 observationCardinality, uint16 observationCardinalityNext, uint8 feeProtocol, bool unlocked)',
  'function token0() view returns (address)',
  'function token1() view returns (address)',
  'function liquidity() view returns (uint128)',
]);

const FACTORY_V2_ABI = parseAbi([
  'function allPairsLength() view returns (uint256)',
  'function allPairs(uint256) view returns (address)',
]);

const UNISWAP_V3_FACTORY_ABI = parseAbi([
  'function getPool(address tokenA, address tokenB, uint24 fee) external view returns (address pool)',
]);

const AAVE_POOL_ABI = parseAbi([
  'function getReserveData(address asset) view returns ((uint256 configuration, uint128 liquidityIndex, uint128 currentLiquidityRate, uint128 variableBorrowIndex, uint128 currentVariableBorrowRate, uint128 currentStableBorrowRate, uint40 lastUpdateTimestamp, uint16 id, address aTokenAddress, address stableDebtTokenAddress, address variableDebtTokenAddress, address interestRateStrategyAddress, uint128 accruedToTreasury, uint128 unbacked, uint128 isolationModeTotalDebt))',
  'function flashLoanSimple(address receiver, address asset, uint256 amount, bytes calldata params, uint16 referralCode) external returns (bool)',
]);

const SWAP_EXACT_TOKENS_FOR_TOKENS_ABI = parseAbi([
  'function swapExactTokensForTokens(uint256 amountIn, uint256 amountOutMin, address[] calldata path, address to, uint256 deadline) external returns (uint256[] memory amounts)',
]);

// ============================================================
// 6. RPC Rotator with health checks & rate limiter
// ============================================================
class RPCRotator {
  private urls: string[];
  private index = 0;
  private clients: Map<string, PublicClient> = new Map();
  private callCount = 0;
  private lastReset = Date.now();
  constructor(urls: string[]) { this.urls = [...urls]; }
  async rateLimit() {
    const now = Date.now();
    if (now - this.lastReset > 60000) {
      this.callCount = 0;
      this.lastReset = now;
    }
    this.callCount++;
    if (this.callCount > 25) {
      await new Promise(resolve => setTimeout(resolve, 2000));
      this.callCount = 0;
    }
  }
  getClient() {
    const url = this.urls[this.index % this.urls.length];
    this.index++;
    if (!this.clients.has(url)) this.clients.set(url, createPublicClient({ transport: http(url) }));
    return this.clients.get(url)!;
  }
  async healthCheck() {
    for (let i = 0; i < this.urls.length; i++) {
      const url = this.urls[i];
      try {
        const client = createPublicClient({ transport: http(url) });
        await client.getBlockNumber();
      } catch {
        logger.warn({ url }, 'RPC endpoint dead, removing');
        this.urls.splice(i, 1);
        this.clients.delete(url);
        i--;
      }
    }
  }
}

class WSRotator {
  private urls: string[];
  private index = 0;
  private clients: Map<string, PublicClient> = new Map();
  constructor(urls: string[]) { this.urls = [...urls]; }
  async getClient(maxRetries = 3): Promise<PublicClient> {
    const url = this.urls[this.index % this.urls.length];
    this.index++;
    if (!this.clients.has(url)) {
      const client = createPublicClient({ transport: webSocket(url) });
      this.clients.set(url, client);
      try {
        await client.getBlockNumber();
      } catch (err) {
        logger.error({ err, url }, 'WebSocket connection failed');
        this.clients.delete(url);
        if (maxRetries <= 1) throw new Error('Max WS retries exceeded');
        await new Promise(resolve => setTimeout(resolve, 2000));
        return this.getClient(maxRetries - 1);
      }
    }
    return this.clients.get(url)!;
  }
  async healthCheck() {
    for (let i = 0; i < this.urls.length; i++) {
      const url = this.urls[i];
      try {
        const client = createPublicClient({ transport: webSocket(url) });
        await client.getBlockNumber();
      } catch {
        logger.warn({ url }, 'WebSocket endpoint dead, removing');
        this.urls.splice(i, 1);
        this.clients.delete(url);
        i--;
      }
    }
  }
}

// Global RPC manager (initialized later)
let globalRpcManager: RPCRotator;

// ============================================================
// 7. Database & Telegram
// ============================================================
const pg = new Pool({ connectionString: process.env.DATABASE_URL });
pg.on('error', (err: Error) => logger.error({ err }, 'Postgres error'));

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN!;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID!;
async function sendTelegram(message: string) {
  try {
    await axios.post(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, { chat_id: TELEGRAM_CHAT_ID, text: message });
  } catch (err) { logger.error({ err }, 'Telegram send failed'); }
}

// ============================================================
// 8. Helpers (decimals, honeypot, price) with rate limiting
// ============================================================
const decimalsCache = new Map<string, { decimals: number; timestamp: number }>();
async function getTokenDecimals(client: PublicClient, token: string): Promise<number> {
  await globalRpcManager.rateLimit();
  const key = `${client.transport.url}-${token}`;
  const cached = decimalsCache.get(key);
  if (cached && Date.now() - cached.timestamp < 3600000) return cached.decimals;
  const contract = getContract({ address: token as Address, abi: ERC20_ABI, client });
  const decimals = await contract.read.decimals().catch(() => 18);
  decimalsCache.set(key, { decimals, timestamp: Date.now() });
  return decimals;
}

const BLACKLIST = (process.env.BLACKLISTED_TOKENS || '').split(',').filter(Boolean);
async function isHoneypot(client: PublicClient, token: string, factoryAddress: string): Promise<boolean> {
  await globalRpcManager.rateLimit();
  if (BLACKLIST.includes(token)) return true;
  try {
    const contract = getContract({ address: token as Address, abi: ERC20_ABI, client });
    const mintCall = encodeFunctionData({ abi: ERC20_ABI, functionName: 'mint', args: [factoryAddress as Address, 1n] });
    const mintResult = await client.call({ to: token as Address, data: mintCall }).catch(() => null);
    if (mintResult && !mintResult.data) return true;
    const holder = factoryAddress !== '0x0000000000000000000000000000000000000000' ? factoryAddress : token;
    const balance = await contract.read.balanceOf([holder as Address]).catch(() => 0n);
    if (balance === 0n) return false;
    const amount = balance / 1000n;
    const transferCall = encodeFunctionData({ abi: ERC20_ABI, functionName: 'transfer', args: [holder as Address, amount] });
    const result = await client.call({ to: token as Address, data: transferCall }).catch(() => null);
    if (result && !result.data) return true;
    return false;
  } catch { return true; }
}

let cachedEthPriceUSD = 2100;
let lastEthPriceFetch = 0;
async function getEthPriceUSD(client: PublicClient): Promise<number> {
  await globalRpcManager.rateLimit();
  const now = Date.now();
  if (now - lastEthPriceFetch < 60000) return cachedEthPriceUSD;
  const ethFeed = '0x3c6Cd9Cc7c7a4c2Cf5a82734CD249D7D593354dA' as Address;
  try {
    const aggregator = getContract({ address: ethFeed, abi: parseAbi(['function latestRoundData() view returns (uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound)']), client });
    const data = await aggregator.read.latestRoundData();
    cachedEthPriceUSD = Number(data[1]) / 1e8;
    lastEthPriceFetch = now;
  } catch (err) { logger.warn({ err }, 'Failed to fetch ETH price, using cached'); }
  return cachedEthPriceUSD;
}

async function getUSDPrice(client: PublicClient, tokenAddress: string): Promise<number> {
  await globalRpcManager.rateLimit();
  const chainlinkFeeds: Record<string, string> = {
    '0xe5D7c2a44FfDDf6b295A15c148167daaAf5Cf34f': '0x3c6Cd9Cc7c7a4c2Cf5a82734CD249D7D593354dA',
    '0x176211869cA2b568f2A7D4EE941E073a821EE1ff': '0xAADAa473C1bDF7317ec07c915680Af29DeBfdCb5',
    '0xA219439258ca9da29E9Cc4cE5596924745e12B93': '0xefCA2bbe0EdD0E22b2e0d2F8248E99F4bEf4A7dB',
  };
  if (chainlinkFeeds[tokenAddress]) {
    try {
      const aggregator = getContract({ address: chainlinkFeeds[tokenAddress] as Address, abi: parseAbi(['function latestRoundData() view returns (uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound)']), client });
      const data = await aggregator.read.latestRoundData();
      return Number(data[1]) / 1e8;
    } catch (err) { logger.warn({ err, token: tokenAddress }, 'Chainlink feed failed'); }
  }
  // Stablecoins
  const stableAddrs = ['0x176211869ca2b568f2a7d4ee941e073a821ee1ff', '0xa219439258ca9da29e9cc4ce5596924745e12b93'];
  if (stableAddrs.includes(tokenAddress.toLowerCase())) return 1;
  // WETH
  if (tokenAddress.toLowerCase() === '0xe5d7c2a44fffdf6b295a15c148167daaaf5cf34f'.toLowerCase()) return await getEthPriceUSD(client);
  return 0;
}

// ============================================================
// 9. V3 Pool TVL Helper (improved)
// ============================================================
async function getV3PoolTVL(client: PublicClient, pool: string): Promise<number> {
  await globalRpcManager.rateLimit();
  try {
    const poolContract = getContract({ address: pool as Address, abi: UNIV3_POOL_ABI, client });
    const [liquidity, slot0Data, token0, token1] = await Promise.all([
      poolContract.read.liquidity().catch(() => 0n),
      poolContract.read.slot0().catch(() => null),
      poolContract.read.token0().catch(() => '0x'),
      poolContract.read.token1().catch(() => '0x')
    ]);
    if (liquidity === 0n || !slot0Data) return 1;
    const sqrtPriceX96 = BigInt(slot0Data[0]);
    const price0 = await getUSDPrice(client, token0);
    const price1 = await getUSDPrice(client, token1);
    const price = Number(sqrtPriceX96 * sqrtPriceX96) / Number(2n ** 192n);
    const tvl = Number(liquidity) * Math.sqrt(price) * Math.max(price0, price1) / 1e18;
    return tvl > 50 ? tvl : 1;
  } catch { return 1; }
}

// ============================================================
// 10. Core Math: getAmountOut (V2 & improved V3)
// ============================================================
async function getAmountOut(client: PublicClient, pool: string, type: 'v2' | 'v3', tokenIn: string, amountIn: bigint): Promise<bigint> {
  await globalRpcManager.rateLimit();
  if (type === 'v2') {
    const reserves = await client.readContract({ address: pool as Address, abi: PAIR_ABI, functionName: 'getReserves' }).catch(() => null);
    if (!reserves) return 0n;
    const [reserve0, reserve1] = reserves;
    const token0 = await client.readContract({ address: pool as Address, abi: PAIR_ABI, functionName: 'token0' });
    const isToken0 = token0.toLowerCase() === tokenIn.toLowerCase();
    const [reserveIn, reserveOut] = isToken0 ? [reserve0, reserve1] : [reserve1, reserve0];
    const amountInWithFee = amountIn * 997n;
    const numerator = amountInWithFee * reserveOut;
    const denominator = reserveIn * 1000n + amountInWithFee;
    return numerator / denominator;
  } else {
    const poolContract = getContract({ address: pool as Address, abi: UNIV3_POOL_ABI, client });
    const [slot0Data, token0Addr] = await Promise.all([
      poolContract.read.slot0().catch(() => null),
      poolContract.read.token0().catch(() => '0x')
    ]);
    if (!slot0Data) return 0n;
    const sqrtPriceX96 = BigInt(slot0Data[0]);
    const isToken0 = token0Addr.toLowerCase() === tokenIn.toLowerCase();
    const fee = 997n; // 0.3%
    const price = (sqrtPriceX96 * sqrtPriceX96) / (2n ** 192n);
    let amountOut = isToken0
      ? (amountIn * price * fee) / 1000n
      : (amountIn * (2n ** 192n) * fee) / (sqrtPriceX96 * sqrtPriceX96 * 1000n);
    return amountOut > 0n ? amountOut : 0n;
  }
}

// ============================================================
// 11. Pre‑computed Routes (2‑hop, stored in DB, with cooldown)
// ============================================================
interface SwapStep { fromToken: string; toToken: string; pool: string; type: 'v2' | 'v3'; }
interface PrecomputedRoute {
  borrowToken: string;
  otherToken: string;
  steps: SwapStep[];
  estimatedProfitWei: bigint;
  lastUpdated: number;
}

async function precomputeRoutes(client: PublicClient, chainId: number) {
  await globalRpcManager.rateLimit();
  const poolsRes = await pg.query('SELECT pool_address, token0, token1, type, tvl_usd FROM active_pools WHERE chain_id = $1', [chainId]);
  const pools = poolsRes.rows;
  const tokenToPools = new Map<string, typeof pools>();
  for (const p of pools) {
    for (const t of [p.token0, p.token1]) {
      if (!tokenToPools.has(t)) tokenToPools.set(t, []);
      tokenToPools.get(t)!.push(p);
    }
  }
  for (const borrowToken of ALL_CANDIDATE_TOKENS) {
    const borrowTokenLower = borrowToken.toLowerCase();
    const candidatePools = tokenToPools.get(borrowTokenLower) || [];
    if (candidatePools.length < 2) continue;
    const routes: PrecomputedRoute[] = [];
    for (const poolA of candidatePools) {
      const otherToken = (poolA.token0.toLowerCase() === borrowTokenLower) ? poolA.token1 : poolA.token0;
      const amountOutFirst = await getAmountOut(client, poolA.pool_address, poolA.type, borrowToken, 10n ** 18n);
      if (amountOutFirst === 0n) continue;
      const returnPools = tokenToPools.get(otherToken.toLowerCase()) || [];
      for (const poolB of returnPools) {
        if (poolB.pool_address === poolA.pool_address) continue;
        if (poolB.token0.toLowerCase() !== borrowTokenLower && poolB.token1.toLowerCase() !== borrowTokenLower) continue;
        const amountOutSecond = await getAmountOut(client, poolB.pool_address, poolB.type, otherToken, amountOutFirst);
        if (amountOutSecond === 0n) continue;
        const aaveFee = (10n ** 18n) * 5n / 10000n;
        const profit = amountOutSecond - (10n ** 18n) - aaveFee;
        if (profit > 0n && (poolA.tvl_usd > 200 && poolB.tvl_usd > 200)) {
          routes.push({
            borrowToken: borrowTokenLower,
            otherToken: otherToken.toLowerCase(),
            steps: [
              { fromToken: borrowToken, toToken: otherToken, pool: poolA.pool_address, type: poolA.type },
              { fromToken: otherToken, toToken: borrowToken, pool: poolB.pool_address, type: poolB.type },
            ],
            estimatedProfitWei: profit,
            lastUpdated: Date.now(),
          });
        }
      }
    }
    routes.sort((a, b) => Number(b.estimatedProfitWei - a.estimatedProfitWei));
    const topRoutes = routes.slice(0, 5);
    await pg.query('DELETE FROM arb_routes WHERE borrow_token = $1', [borrowTokenLower]);
    for (const route of topRoutes) {
      await pg.query(
        `INSERT INTO arb_routes (borrow_token, other_token, step1_pool, step1_type, step2_pool, step2_type, estimated_profit_wei, last_updated, cooldown_until)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
        [route.borrowToken, route.otherToken, route.steps[0].pool, route.steps[0].type, route.steps[1].pool, route.steps[1].type, route.estimatedProfitWei.toString(), route.lastUpdated, 0]
      );
    }
    logger.info({ borrowToken, routesCount: topRoutes.length }, 'Precomputed routes');
  }
}

// ============================================================
// 12. Simulation & Execution (dynamic slippage, variable amount)
// ============================================================
const FIXED_GAS_ESTIMATE = 350_000n;
const USE_WHITELISTED_PREMIUM = false;
const FLASHLOAN_FEE_BPS = USE_WHITELISTED_PREMIUM ? 25n : 50n;

async function simulateFlashloan(
  client: PublicClient,
  borrowToken: string,
  borrowAmounts: bigint[],
  steps: SwapStep[],
  gasPrice: bigint,
  poolTVL: number
): Promise<{ profitUSD: number; gasCostUSD: number; bestAmount: bigint }> {
  await globalRpcManager.rateLimit();
  let bestProfitUSD = -Infinity;
  let bestAmount = 0n;
  for (const amount of borrowAmounts) {
    let currentAmount = amount;
    for (const step of steps) {
      const amountOut = await getAmountOut(client, step.pool, step.type, step.fromToken, currentAmount);
      if (amountOut === 0n) break;
      currentAmount = amountOut;
    }
    if (currentAmount === 0n) continue;
    const aaveFee = amount * FLASHLOAN_FEE_BPS / 10000n;
    const profitWei = currentAmount - amount - aaveFee;
    if (profitWei <= 0n) continue;
    const slippage = poolTVL < 1000 ? 0.985 : 0.992;
    const profitAfterSlippage = Number(profitWei) * slippage;
    const borrowDecimals = await getTokenDecimals(client, borrowToken);
    const borrowPrice = await getUSDPrice(client, borrowToken);
    const profitUSD = profitAfterSlippage * borrowPrice / (10 ** borrowDecimals);
    if (profitUSD > bestProfitUSD) {
      bestProfitUSD = profitUSD;
      bestAmount = amount;
    }
  }
  const ethPrice = await getEthPriceUSD(client);
  const gasCostWei = FIXED_GAS_ESTIMATE * gasPrice;
  const gasCostUSD = Number(gasCostWei) * ethPrice / 1e18;
  return { profitUSD: bestProfitUSD, gasCostUSD, bestAmount };
}

const EXECUTOR_PRIVATE_KEYS = [
  process.env.EXECUTOR_PRIVATE_KEY_1,
  process.env.EXECUTOR_PRIVATE_KEY_2,
  process.env.EXECUTOR_PRIVATE_KEY_3,
  process.env.EXECUTOR_PRIVATE_KEY_4,
  process.env.EXECUTOR_PRIVATE_KEY_5,
].filter(Boolean) as string[];
let currentWalletIndex = 0;

function getNextWalletClient(chain: any, rpcUrl: string) {
  if (EXECUTOR_PRIVATE_KEYS.length === 0) return null;
  const key = EXECUTOR_PRIVATE_KEYS[currentWalletIndex % EXECUTOR_PRIVATE_KEYS.length];
  currentWalletIndex++;
  return createWalletClient({
    account: privateKeyToAccount(`0x${key}`),
    transport: http(rpcUrl),
    chain,
  });
}

async function executeFlashloan(route: PrecomputedRoute, borrowAmount: bigint, gasPrice: bigint) {
  const walletClient = getNextWalletClient(CHAIN.viemChain, HTTP_RPCS[0]);
  if (!walletClient) {
    logger.warn('No execution wallet configured, skipping execution');
    return;
  }
  const swapData = encodeFunctionData({
    abi: parseAbi(['function swap(address[] dexes, address[] tokens, uint256[] amounts)']),
    functionName: 'swap',
    args: [
      route.steps.map(s => s.pool as Address),
      route.steps.map(s => s.fromToken as Address),
      [borrowAmount, 0n],
    ],
  });
  const flashloanCall = encodeFunctionData({
    abi: AAVE_POOL_ABI,
    functionName: 'flashLoanSimple',
    args: [CHAIN.executor as Address, route.borrowToken as Address, borrowAmount, swapData, 0],
  });
  const tx = await walletClient.sendTransaction({
    to: CHAIN.aavePool,
    data: flashloanCall,
    gasPrice,
  }).catch(err => { logger.error({ err }, 'Execution failed'); return null; });
  if (tx) {
    logger.info({ txHash: tx }, 'Flashloan executed');
    await sendTelegram(`✅ Executed arbitrage on Linea: ${tx}`);
    await pg.query('INSERT INTO executed_txs (tx_hash, borrow_token, profit_usd, gas_used, block_number) VALUES ($1, $2, $3, $4, $5)', [tx, route.borrowToken, 0, 0, 0]);
  }
}

// ============================================================
// 13. Mempool Monitoring (disabled by default to save RPC)
// ============================================================
async function monitorMempool(wsClient: PublicClient) {
  if (process.env.ENABLE_MEMPOOL !== 'true') {
    logger.info('Mempool monitor disabled');
    return;
  }
  setInterval(async () => {
    await globalRpcManager.rateLimit();
    const block = await wsClient.getBlock({ blockTag: 'pending' }).catch(() => null);
    if (!block || !block.transactions) return;
    for (const txHash of block.transactions) {
      const tx = await wsClient.getTransaction({ hash: txHash }).catch(() => null);
      if (!tx || !tx.to) continue;
      const isRouter = ROUTERS.some(router => router.toLowerCase() === tx.to!.toLowerCase());
      if (!isRouter) continue;
      try {
        const decoded = parseAbi(SWAP_EXACT_TOKENS_FOR_TOKENS_ABI).parseTransaction({ data: tx.input as Hex });
        if (decoded && decoded.functionName === 'swapExactTokensForTokens') {
          const args = decoded.args as any;
          const amountIn = args.amountIn;
          const path = args.path;
          if (path && path.length >= 2) {
            const tokenIn = path[0];
            const tokenOut = path[path.length - 1];
            const isBorrowable = AAVE_BORROWABLE_ASSETS.some(t => t.toLowerCase() === tokenOut.toLowerCase());
            if (isBorrowable && amountIn > 10n ** 17n) {
              logger.info({ txHash, tokenIn, tokenOut, amountIn }, 'Detected large swap, attempting backrun');
              const route = await getBestRoute(tokenOut, tokenIn);
              if (route) {
                const gasPrice = await globalRpcManager.getClient().getGasPrice();
                const sim = await simulateFlashloan(globalRpcManager.getClient(), tokenOut, [amountIn], route.steps, gasPrice, 1000);
                if (sim.profitUSD > 3 && sim.profitUSD > sim.gasCostUSD * 2.5) {
                  await executeFlashloan(route, sim.bestAmount, gasPrice);
                }
              }
            }
          }
        }
      } catch (e) { /* ignore */ }
    }
  }, 5000);
}

async function getBestRoute(borrowToken: string, otherToken: string): Promise<PrecomputedRoute | null> {
  await globalRpcManager.rateLimit();
  const res = await pg.query(
    `SELECT borrow_token, other_token, step1_pool, step1_type, step2_pool, step2_type, estimated_profit_wei, cooldown_until
     FROM arb_routes
     WHERE borrow_token = $1 AND other_token = $2 AND (cooldown_until = 0 OR cooldown_until < $3)`,
    [borrowToken.toLowerCase(), otherToken.toLowerCase(), Date.now()]
  );
  if (res.rows.length === 0) return null;
  const row = res.rows[0];
  return {
    borrowToken: row.borrow_token,
    otherToken: row.other_token,
    steps: [
      { fromToken: row.borrow_token, toToken: row.other_token, pool: row.step1_pool, type: row.step1_type },
      { fromToken: row.other_token, toToken: row.borrow_token, pool: row.step2_pool, type: row.step2_type },
    ],
    estimatedProfitWei: BigInt(row.estimated_profit_wei),
    lastUpdated: 0,
  };
}

// ============================================================
// 14. Pool Loading & Listeners (V2 + V3, with TVL update)
// ============================================================
async function seedV3Pools(client: PublicClient, chainId: number) {
  await globalRpcManager.rateLimit();
  for (const factory of FACTORIES.filter(f => f.type === 'v3' && f.address && f.feeTiers)) {
    const factoryContract = getContract({ address: factory.address, abi: UNISWAP_V3_FACTORY_ABI, client });
    for (const fee of factory.feeTiers!) {
      for (const tokenA of ALL_CANDIDATE_TOKENS) {
        for (const tokenB of ALL_CANDIDATE_TOKENS) {
          if (tokenA === tokenB) continue;
          try {
            const pool = await factoryContract.read.getPool([tokenA, tokenB, fee]);
            if (pool && pool !== '0x0000000000000000000000000000000000000000') {
              await pg.query(
                `INSERT INTO active_pools (chain_id, pool_address, token0, token1, factory, tvl_usd, type, last_scanned_block, cooldown_until, last_tvl_update)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) ON CONFLICT DO NOTHING`,
                [chainId, pool.toLowerCase(), tokenA.toLowerCase(), tokenB.toLowerCase(), factory.address.toLowerCase(), 1, 'v3', 0, 0, 0]
              );
              const tvl = await getV3PoolTVL(client, pool);
              await pg.query(`UPDATE active_pools SET tvl_usd = $1 WHERE pool_address = $2`, [tvl, pool.toLowerCase()]);
            }
          } catch (err) { /* ignore */ }
        }
      }
    }
  }
}

async function loadExistingPools(client: PublicClient, chainId: number) {
  await globalRpcManager.rateLimit();
  for (const factory of FACTORIES) {
    if (factory.type === 'v2' && factory.enumerable) {
      try {
        const contract = getContract({ address: factory.address, abi: FACTORY_V2_ABI, client });
        let length = await contract.read.allPairsLength();
        const batchSize = 50;
        for (let i = 0; i < Number(length); i += batchSize) {
          const addresses = await Promise.all(Array.from({ length: Math.min(batchSize, Number(length) - i) }, (_, j) => contract.read.allPairs([BigInt(i + j)])));
          const poolData = await Promise.all(addresses.map(async pool => {
            const pair = getContract({ address: pool as Address, abi: PAIR_ABI, client });
            const [token0, token1, reserves] = await Promise.all([pair.read.token0().catch(() => '0x'), pair.read.token1().catch(() => '0x'), pair.read.getReserves().catch(() => null)]);
            if (token0 === '0x' || token1 === '0x' || !reserves) return null;
            const decimals0 = await getTokenDecimals(client, token0);
            const decimals1 = await getTokenDecimals(client, token1);
            const price0 = await getUSDPrice(client, token0);
            const price1 = await getUSDPrice(client, token1);
            let tvl = 0;
            if (price0 > 0) tvl += Number(reserves[0]) * price0 / (10 ** decimals0);
            if (price1 > 0) tvl += Number(reserves[1]) * price1 / (10 ** decimals1);
            if (tvl === 0) tvl = 1;
            return { pool: pool as string, token0, token1, tvl, type: 'v2' };
          }));
          
          // FIXED BLOCK: replaced .filter(await ...) with explicit async loop
          for (const data of poolData) {
            if (!data) continue;

            const isBad =
              (await isHoneypot(client, data.token0, factory.address)) ||
              (await isHoneypot(client, data.token1, factory.address));

            if (isBad) continue;

            if (data.tvl >= 50) {
              await pg.query(
                `INSERT INTO active_pools (chain_id, pool_address, token0, token1, factory, tvl_usd, type, last_scanned_block, cooldown_until, last_tvl_update)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) ON CONFLICT DO NOTHING`,
                [chainId, data.pool.toLowerCase(), data.token0.toLowerCase(), data.token1.toLowerCase(), factory.address.toLowerCase(), data.tvl, data.type, 0, 0, 0]
              );
            }
          }
        }
      } catch (err) { logger.error({ err, factory: factory.name }, 'Failed to load pools'); }
    }
  }
  await seedV3Pools(client, chainId);
}

async function refreshPools(client: PublicClient, chainId: number) {
  logger.info('Refreshing pools and routes...');
  await loadExistingPools(client, chainId);
  await new Promise(resolve => setTimeout(resolve, 5000));
  await precomputeRoutes(client, chainId);
}

// ============================================================
// 15. Scanning Loop (using precomputed routes, with limits)
// ============================================================
let lastExecutedBlock = 0;
let dailyProfit = 0;
let lastSummaryDate = new Date().toDateString();
let simulationCount = 0;
let lastSimulationReset = Date.now();

async function scanChain(client: PublicClient, wsClient: PublicClient) {
  await globalRpcManager.rateLimit();
  const currentBlock = await wsClient.getBlockNumber();
  if (Number(currentBlock) <= lastExecutedBlock + 5) return;

  const gasPrice = await client.getGasPrice(); // once per block
  const routesRes = await pg.query('SELECT * FROM arb_routes WHERE cooldown_until = 0 OR cooldown_until < $1', [Date.now()]);
  if (routesRes.rows.length === 0) return;

  let foundOpportunity = false;

  const now = Date.now();
  if (now - lastSimulationReset > 60000) {
    simulationCount = 0;
    lastSimulationReset = now;
  }
  if (simulationCount > 15) {
    logger.info('Simulation limit reached, waiting for next block');
    return;
  }

  for (const routeRow of routesRes.rows) {
    if (foundOpportunity) break;
    const borrowToken = routeRow.borrow_token;
    const borrowAmounts = [10n ** 17n, 5n ** 17n, 10n ** 18n, 5n ** 18n];
    const steps: SwapStep[] = [
      { fromToken: borrowToken, toToken: routeRow.other_token, pool: routeRow.step1_pool, type: routeRow.step1_type },
      { fromToken: routeRow.other_token, toToken: borrowToken, pool: routeRow.step2_pool, type: routeRow.step2_type },
    ];
    const poolTVL = (await pg.query('SELECT tvl_usd FROM active_pools WHERE pool_address = $1', [routeRow.step1_pool])).rows[0]?.tvl_usd || 1000;
    const sim = await simulateFlashloan(client, borrowToken, borrowAmounts, steps, gasPrice, poolTVL);
    simulationCount++;
    const threshold = 3;
    if (sim.profitUSD > threshold && sim.profitUSD > sim.gasCostUSD * 2.5 && sim.bestAmount > 0n) {
      logger.info({ profitUSD: sim.profitUSD, gasUSD: sim.gasCostUSD, route: routeRow, amount: sim.bestAmount }, 'Opportunity found');
      await sendTelegram(`🎯 Opportunity on Linea: $${sim.profitUSD.toFixed(2)} profit (gas $${sim.gasCostUSD.toFixed(2)})`);
      dailyProfit += sim.profitUSD;
      lastExecutedBlock = Number(currentBlock);
      foundOpportunity = true;
      if (EXECUTOR_PRIVATE_KEYS.length > 0 && CHAIN.executor !== '0x0000000000000000000000000000000000000000') {
        const routeObj: PrecomputedRoute = {
          borrowToken: routeRow.borrow_token,
          otherToken: routeRow.other_token,
          steps,
          estimatedProfitWei: BigInt(routeRow.estimated_profit_wei),
          lastUpdated: 0,
        };
        await executeFlashloan(routeObj, sim.bestAmount, gasPrice);
      }
      await pg.query('UPDATE arb_routes SET cooldown_until = $1 WHERE id = $2', [Date.now() + 3600000, routeRow.id]);
    }
  }
  if (!foundOpportunity) {
    await new Promise(resolve => setTimeout(resolve, Math.random() * 2000));
  }
}

// ============================================================
// 16. Main Entry Point
// ============================================================
async function main() {
  await pg.query(`
    CREATE TABLE IF NOT EXISTS active_pools (
      id SERIAL PRIMARY KEY,
      chain_id INTEGER NOT NULL,
      pool_address TEXT NOT NULL,
      token0 TEXT,
      token1 TEXT,
      factory TEXT,
      tvl_usd DECIMAL,
      type TEXT DEFAULT 'v2',
      last_scanned_block INTEGER,
      cooldown_until INTEGER,
      last_tvl_update INTEGER,
      created_at TIMESTAMP DEFAULT NOW(),
      UNIQUE(chain_id, pool_address)
    );
    CREATE TABLE IF NOT EXISTS arb_routes (
      id SERIAL PRIMARY KEY,
      borrow_token TEXT NOT NULL,
      other_token TEXT NOT NULL,
      step1_pool TEXT NOT NULL,
      step1_type TEXT NOT NULL,
      step2_pool TEXT NOT NULL,
      step2_type TEXT NOT NULL,
      estimated_profit_wei TEXT NOT NULL,
      last_updated TIMESTAMP,
      cooldown_until INTEGER DEFAULT 0,
      UNIQUE(borrow_token, other_token)
    );
    CREATE TABLE IF NOT EXISTS executed_txs (
      id SERIAL PRIMARY KEY,
      tx_hash TEXT UNIQUE,
      borrow_token TEXT,
      profit_usd DECIMAL,
      gas_used DECIMAL,
      block_number INTEGER,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);

  // Ensure columns exist for older tables (migration)
  await pg.query(`ALTER TABLE arb_routes ADD COLUMN IF NOT EXISTS cooldown_until INTEGER DEFAULT 0`);
  await pg.query(`ALTER TABLE active_pools ADD COLUMN IF NOT EXISTS tvl_usd DECIMAL DEFAULT 1`);

  const rpcManager = new RPCRotator(HTTP_RPCS);
  globalRpcManager = rpcManager;
  const wsRotator = new WSRotator(WS_RPCS);
  const client = rpcManager.getClient();
  const wsClient = await wsRotator.getClient();

  await loadExistingPools(client, CHAIN.id);
  await precomputeRoutes(client, CHAIN.id);

  setInterval(async () => {
    await refreshPools(client, CHAIN.id);
  }, 30 * 60 * 1000);

  wsClient.watchBlocks({ onBlock: async () => { await scanChain(client, wsClient); } });
  // Mempool monitor disabled by default to save RPC; enable with ENABLE_MEMPOOL=true
  // await monitorMempool(wsClient);

  setInterval(() => {
    rpcManager.healthCheck();
    wsRotator.healthCheck();
  }, 60000);

  setInterval(() => {
    const now = new Date();
    if (now.toDateString() !== lastSummaryDate) {
      sendTelegram(`📊 Daily profit summary: $${dailyProfit.toFixed(2)}`);
      dailyProfit = 0;
      lastSummaryDate = now.toDateString();
    }
  }, 60000);

  setInterval(async () => {
    const msg = `🟢 Linea scanner alive at ${new Date().toISOString()}`;
    logger.info(msg);
    await sendTelegram(msg);
  }, 300000);

  logger.info('Linea arbitrage scanner started (final micro-fixes applied)');
}

main().catch(err => { logger.error({ err }, 'Fatal error'); process.exit(1); });
