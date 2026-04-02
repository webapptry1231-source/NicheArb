import { createPublicClient, createWalletClient, http, webSocket, encodeFunctionData, getContract, PublicClient, WalletClient, parseAbi, defineChain, Address } from 'viem';
import { privateKeyToAccount } from 'viem/accounts';
import { zksync, scroll, linea, base } from 'viem/chains';
import { AaveV3ZkSync, AaveV3Scroll, AaveV3Linea, AaveV3Base } from '@bgd-labs/aave-address-book';
import { Pool } from 'pg';
import axios from 'axios';
import dotenv from 'dotenv';
import pino from 'pino';

dotenv.config();
const logger = pino({ level: process.env.LOG_LEVEL || 'info', transport: { target: 'pino-pretty', options: { colorize: true, translateTime: true } } });

// ----- Custom chains (Monad, Berachain) -----
const monad = defineChain({
  id: 143, name: 'Monad', network: 'monad',
  nativeCurrency: { name: 'MON', symbol: 'MON', decimals: 18 },
  rpcUrls: { default: { http: ['https://rpc.monad.xyz'] }, public: { http: ['https://rpc.monad.xyz'] } }
});
const berachain = defineChain({
  id: 80094, name: 'Berachain', network: 'berachain',
  nativeCurrency: { name: 'BERA', symbol: 'BERA', decimals: 18 },
  rpcUrls: { default: { http: ['https://rpc.berachain.com'] }, public: { http: ['https://rpc.berachain.com'] } }
});

// ----- Chain configuration -----
const CHAINS = [
  { id: 324, name: 'zksync', viemChain: zksync, aavePool: AaveV3ZkSync.POOL, executor: process.env.EXECUTOR_ZKSYNC! },
  { id: 534352, name: 'scroll', viemChain: scroll, aavePool: AaveV3Scroll.POOL, executor: process.env.EXECUTOR_SCROLL! },
  { id: 59144, name: 'linea', viemChain: linea, aavePool: AaveV3Linea.POOL, executor: process.env.EXECUTOR_LINEA! },
  { id: 8453, name: 'base', viemChain: base, aavePool: AaveV3Base.POOL, executor: process.env.EXECUTOR_BASE! },
  { id: 143, name: 'monad', viemChain: monad, aavePool: undefined, executor: process.env.EXECUTOR_MONAD! },
  { id: 80094, name: 'berachain', viemChain: berachain, aavePool: undefined, executor: process.env.EXECUTOR_BERACHAIN! }
];

// ----- Factories (all DEXes, fully populated) -----
const FACTORIES: Record<number, { name: string; address: string; type: 'v2' | 'v3'; enumerable: boolean }[]> = {
  324: [
    { name: 'SyncSwap Classic', address: '0xf2DAd89f2788a8CD54625C60b55cD3d2D0ACa7Cb', type: 'v2', enumerable: true },
    { name: 'SyncSwap Stable', address: '0x5b9f21d407F35b10CbfDDca17D5D84b129356ea3', type: 'v2', enumerable: true },
    { name: 'Uniswap V3', address: '0x8FdA5a7a8dCA67BBcDd10F02Fa0649A937215422', type: 'v3', enumerable: false },
    { name: 'iZiSwap', address: '0x8c7d3063579BdB0b90997e18A770eaE32E1eBb08', type: 'v2', enumerable: true }
  ],
  534352: [
    { name: 'SyncSwap Classic', address: '0x37BAc764494c8db4e54BDE72f6965beA9fa0AC2d', type: 'v2', enumerable: true },
    { name: 'SyncSwap Stable', address: '0xE4CF807E351b56720B17A59094179e7Ed9dD3727', type: 'v2', enumerable: true },
    { name: 'iZiSwap', address: '0x8c7d3063579BdB0b90997e18A770eaE32E1eBb08', type: 'v2', enumerable: true }
  ],
  59144: [
    { name: 'PancakeSwap V3', address: '0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865', type: 'v3', enumerable: false },
    { name: 'Nile V2/V3', address: '0xAAA32926fcE6bE95ea2c51cB4Fcb60836D320C42', type: 'v2', enumerable: true }
  ],
  8453: [
    { name: 'Aerodrome', address: '0x42Bf381a259F9aD132D5f257E0eF5E4c5F0b2BfF', type: 'v2', enumerable: true },
    { name: 'Uniswap V3', address: '0x33128a8fC17869897dcE68Ed026d694621f6FDfD', type: 'v3', enumerable: false }
  ],
  143: [
    { name: 'Kuru', address: '0xd651346d7c789536ebf06dc72aE3C8502cd695CC', type: 'v2', enumerable: false }
  ],
  80094: [
    { name: 'Kodiak (V2)', address: '0x5e705e184d233ff2a7cb1553793464a9d0c3028f', type: 'v2', enumerable: true },
    { name: 'Kodiak (V3)', address: '0xD84CBf0B02636E7f53dB9E5e45A616E05d710990', type: 'v3', enumerable: false }
  ]
};

// ----- Routers (primary multi-hop routers) -----
const ROUTERS: Record<number, string> = {
  324: '0x9B5def958d0f3b6955cBEa4D5B7809b2fb26b059',           // SyncSwap Router V2
  534352: '0x80e38291e06339d10aab483c65695d004dbd5c69',      // SyncSwap Router V1 (Scroll)
  59144: '0xFE6508f0015C778Bdcc1fB5465bA5ebE224C9912',       // PancakeSwap V3 Universal Router
  8453: '0xcf77a3ba9a5ca399b7c97c74d54e5b1beb874e43',        // Aerodrome Router
  143: '0x0d3a1BE29E9dEd63c7a5678b31e847D68F71FFa2',         // Kuru FlowRouter
  80094: '0xe301E48F77963D3F7DbD2a4796962Bd7f3867Fb4'         // Kodiak SwapRouter02
};

// ----- Chainlink feeds (latest addresses, fallback to TWAP) -----
const CHAINLINK_FEEDS: Record<number, Record<string, string>> = {
  324: {
    '0x5aea5775959fbc2557cc8789bc1bf90a239d9a91': '0x6D41d1dc818112880b40e26BD6FD347E41008eDA', // WETH / USD
    '0x3355df6D4c9C3035724Fd0e3914dE96A5a83aaf4': '0xFfD85Ff489e1F138819D238f635c2692ba3c074D'  // USDC / USD
  },
  534352: {
    '0x5aea5775959fbc2557cc8789bc1bf90a239d9a91': '0x6bF14CB0A831078629D993FDeBcB182b21A8774C', // WETH / USD
    '0x3355df6D4c9C3035724Fd0e3914dE96A5a83aaf4': '0x43d12Fb3AfCAd5347fA764EeAB105478337b7200'  // USDC / USD
  },
  59144: {
    '0x5aea5775959fbc2557cc8789bc1bf90a239d9a91': '0x3c6Cd9Cc7c7a4c2Cf5a82734CD249D7D593354dA', // WETH / USD
    '0x3355df6D4c9C3035724Fd0e3914dE96A5a83aaf4': '0xAADAa473C1bDF7317ec07c915680Af29DeBfdCb5'  // USDC / USD
  },
  8453: {
    '0x4200000000000000000000000000000000000006': '0x71041dddad3595F9CEd3DcCFBe3D1F4b0a16Bb70', // WETH / USD
    '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913': '0x7e860098F58bBFC8648a4311b374B1D669a2bc6B'  // USDC / USD
  },
  143: {},
  80094: {}
};

// ----- Uniswap V3 factory addresses (for TWAP) -----
const UNISWAP_V3_FACTORIES: Record<number, string> = {
  324: '0x8FdA5a7a8dCA67BBcDd10F02Fa0649A937215422',
  534352: '0x8c7d3063579BdB0b90997e18A770eaE32E1eBb08', // iZiSwap can be used, but we'll stick to SyncSwap for now
  59144: '0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865', // PancakeSwap V3
  8453: '0x33128a8fC17869897dcE68Ed026d694621f6FDfD',
  143: '',
  80094: ''
};

// ----- Token addresses for reference (WETH, USDC) -----
const WETH_ADDRESSES: Record<number, string> = {
  324: '0x5aea5775959fbc2557cc8789bc1bf90a239d9a91',
  534352: '0x5aea5775959fbc2557cc8789bc1bf90a239d9a91',
  59144: '0x5aea5775959fbc2557cc8789bc1bf90a239d9a91',
  8453: '0x4200000000000000000000000000000000000006',
  143: '',
  80094: ''
};
const USDC_ADDRESSES: Record<number, string> = {
  324: '0x3355df6D4c9C3035724Fd0e3914dE96A5a83aaf4',
  534352: '0x3355df6D4c9C3035724Fd0e3914dE96A5a83aaf4',
  59144: '0x3355df6D4c9C3035724Fd0e3914dE96A5a83aaf4',
  8453: '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913',
  143: '',
  80094: ''
};

// ----- ABIs -----
const ERC20_ABI = parseAbi(['function totalSupply() view returns (uint256)', 'function balanceOf(address) view returns (uint256)', 'function transfer(address, uint256) returns (bool)', 'function owner() view returns (address)', 'function decimals() view returns (uint8)']);
const PAIR_ABI = parseAbi(['function getReserves() view returns (uint112 reserve0, uint112 reserve1, uint32 blockTimestampLast)', 'function token0() view returns (address)', 'function token1() view returns (address)']);
const UNIV3_POOL_ABI = parseAbi(['function slot0() view returns (uint160 sqrtPriceX96, int24 tick, uint16 observationIndex, uint16 observationCardinality, uint16 observationCardinalityNext, uint8 feeProtocol, bool unlocked)', 'function token0() view returns (address)', 'function token1() view returns (address)', 'function observe(uint32[] calldata secondsAgos) external view returns (int56[] memory tickCumulatives, uint160[] memory secondsPerLiquidityCumulativeX128s)']);
const FACTORY_V2_ABI = parseAbi(['function allPairsLength() view returns (uint256)', 'function allPairs(uint256) view returns (address)']);
const AAVE_POOL_ABI = parseAbi(['function getReserveData(address asset) view returns ((uint256 configuration, uint128 liquidityIndex, uint128 currentLiquidityRate, uint128 variableBorrowIndex, uint128 currentVariableBorrowRate, uint128 currentStableBorrowRate, uint40 lastUpdateTimestamp, uint16 id, address aTokenAddress, address stableDebtTokenAddress, address variableDebtTokenAddress, address interestRateStrategyAddress, uint128 accruedToTreasury, uint128 unbacked, uint128 isolationModeTotalDebt))', 'function flashLoanSimple(address receiver, address asset, uint256 amount, bytes calldata params, uint16 referralCode) external returns (bool)']);
const UNISWAP_V3_FACTORY_ABI = parseAbi(['function getPool(address tokenA, address tokenB, uint24 fee) external view returns (address pool)']);

// ----- RPC rotators with health checks -----
class RPCRotator {
  private urls: string[];
  private index = 0;
  private clients: Map<string, PublicClient> = new Map();
  constructor(urls: string[]) { this.urls = [...urls]; }
  getClient() {
    const url = this.urls[this.index % this.urls.length];
    this.index++;
    if (!this.clients.has(url)) this.clients.set(url, createPublicClient({ transport: http(url) }));
    return this.clients.get(url)!;
  }
  getFirstUrl() { return this.urls[0]; }
  async healthCheck() {
    for (let i = 0; i < this.urls.length; i++) {
      const url = this.urls[i];
      try {
        const client = createPublicClient({ transport: http(url) });
        await client.getBlockNumber();
      } catch {
        logger.warn({ url }, 'RPC endpoint dead, removing');
        this.urls.splice(i, 1);
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
        i--;
      }
    }
  }
}

// ----- Database & Telegram -----
const pg = new Pool({ connectionString: process.env.DATABASE_URL });
pg.on('error', (err: Error) => logger.error({ err }, 'Postgres error'));
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN!;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID!;
async function sendTelegram(message: string) {
  try {
    await axios.post(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, { chat_id: TELEGRAM_CHAT_ID, text: message });
  } catch (err) { logger.error({ err }, 'Telegram send failed'); }
}

// ----- Helpers -----
const decimalsCache = new Map<string, number>();
async function getTokenDecimals(client: PublicClient, token: string): Promise<number> {
  const key = `${client.transport.url}-${token}`;
  if (decimalsCache.has(key)) return decimalsCache.get(key)!;
  const contract = getContract({ address: token as Address, abi: ERC20_ABI, client });
  const decimals = await contract.read.decimals().catch(() => 18);
  decimalsCache.set(key, decimals);
  return decimals;
}

const BLACKLIST = (process.env.BLACKLISTED_TOKENS || '').split(',').filter(Boolean);
async function isHoneypot(client: PublicClient, token: string, factoryAddress: string): Promise<boolean> {
  if (BLACKLIST.includes(token)) return true;
  try {
    const contract = getContract({ address: token as Address, abi: ERC20_ABI, client });
    const owner = await contract.read.owner().catch(() => '0x');
    if (owner !== '0x0000000000000000000000000000000000000000' && owner !== '0x' && owner !== '0x0') {
      // non-renounced owner – could be risky, but continue check
    }
    const holder = factoryAddress !== '0x...' && factoryAddress !== '0x0' ? factoryAddress : token;
    const balance = await contract.read.balanceOf([holder as Address]).catch(() => 0n);
    if (balance === 0n) return false;
    const amount = balance / 1000n;
    const transferCall = encodeFunctionData({ abi: ERC20_ABI, functionName: 'transfer', args: [holder as Address, amount] });
    const result = await client.call({ to: token as Address, data: transferCall }).catch(() => null);
    if (result && !result.data) {
      const totalSupplyBefore = await contract.read.totalSupply();
      const totalSupplyAfter = await contract.read.totalSupply();
      if (totalSupplyAfter !== totalSupplyBefore) return true;
      return false;
    }
    return true;
  } catch { return true; }
}

// ----- USD price with TWAP fallback (dynamic pool lookup) -----
const poolCacheTwap = new Map<string, string>(); // key: chainId-tokenA-tokenB-fee -> pool address
async function getUniswapV3Pool(client: PublicClient, chainId: number, tokenA: string, tokenB: string, fee: number = 3000): Promise<string | null> {
  const factory = UNISWAP_V3_FACTORIES[chainId];
  if (!factory) return null;
  const key = `${chainId}-${tokenA}-${tokenB}-${fee}`;
  if (poolCacheTwap.has(key)) return poolCacheTwap.get(key)!;
  const factoryContract = getContract({ address: factory as Address, abi: UNISWAP_V3_FACTORY_ABI, client });
  const pool = await factoryContract.read.getPool([tokenA as Address, tokenB as Address, fee]).catch(() => '0x');
  if (pool === '0x') return null;
  poolCacheTwap.set(key, pool);
  return pool;
}

async function getUSDPrice(client: PublicClient, tokenAddress: string, chainId: number): Promise<number> {
  // Chainlink first
  const feeds = CHAINLINK_FEEDS[chainId];
  if (feeds && feeds[tokenAddress]) {
    try {
      const aggregator = getContract({ address: feeds[tokenAddress] as Address, abi: parseAbi(['function latestRoundData() view returns (uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound)']), client });
      const data = await aggregator.read.latestRoundData();
      return Number(data[1]) / 1e8;
    } catch (err) { logger.warn({ err, token: tokenAddress }, 'Chainlink feed failed'); }
  }
  // Stablecoins
  const stablecoins: Record<number, string[]> = { 324: ['0x3355df6D4c9C3035724Fd0e3914dE96A5a83aaf4'], 534352: ['0x3355df6D4c9C3035724Fd0e3914dE96A5a83aaf4'], 59144: ['0x3355df6D4c9C3035724Fd0e3914dE96A5a83aaf4'], 8453: ['0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'] };
  if (stablecoins[chainId]?.includes(tokenAddress)) return 1;
  // WETH fallback
  const weth: Record<number, string[]> = { 324: ['0x5aea5775959fbc2557cc8789bc1bf90a239d9a91'], 534352: ['0x5aea5775959fbc2557cc8789bc1bf90a239d9a91'], 59144: ['0x5aea5775959fbc2557cc8789bc1bf90a239d9a91'], 8453: ['0x4200000000000000000000000000000000000006'] };
  if (weth[chainId]?.includes(tokenAddress)) return 3000;

  // TWAP using WETH/USDC pool if token is one of them
  const wethAddr = WETH_ADDRESSES[chainId];
  const usdcAddr = USDC_ADDRESSES[chainId];
  if (wethAddr && usdcAddr && (tokenAddress === wethAddr || tokenAddress === usdcAddr)) {
    const pool = await getUniswapV3Pool(client, chainId, wethAddr, usdcAddr);
    if (pool && pool !== '0x') {
      try {
        const poolContract = getContract({ address: pool as Address, abi: UNIV3_POOL_ABI, client });
        const secondsAgo = [1800, 0];
        const [tickCumulatives] = await poolContract.read.observe([secondsAgo as any]);
        const tickCumulativeDelta = tickCumulatives[0] - tickCumulatives[1];
        const timeDelta = 1800;
        const tick = Number(tickCumulativeDelta) / timeDelta;
        const sqrtPriceX96 = Math.floor(Math.sqrt(1.0001 ** tick) * 2 ** 96);
        const price = (sqrtPriceX96 / 2 ** 96) ** 2;
        if (tokenAddress === wethAddr) return price;
        else return 1 / price;
      } catch (err) { logger.warn({ err }, 'TWAP fallback failed'); }
    }
  }
  // If all fails, return 0 (token will be ignored)
  return 0;
}

// ----- Swap amount simulation -----
async function getAmountOut(client: PublicClient, pool: string, type: 'v2' | 'v3', tokenIn: string, amountIn: bigint): Promise<bigint> {
  if (type === 'v2') {
    const reserves = await client.readContract({ address: pool as Address, abi: PAIR_ABI, functionName: 'getReserves' }).catch(() => null);
    if (!reserves) return 0n;
    const [reserve0, reserve1] = reserves;
    const token0 = await client.readContract({ address: pool as Address, abi: PAIR_ABI, functionName: 'token0' });
    const [reserveIn, reserveOut] = token0 === tokenIn ? [reserve0, reserve1] : [reserve1, reserve0];
    const amountOut = (amountIn * reserveOut) / reserveIn;
    return (amountOut * 997n) / 1000n;
  } else {
    const slot0 = await client.readContract({ address: pool as Address, abi: UNIV3_POOL_ABI, functionName: 'slot0' }).catch(() => null);
    if (!slot0) return 0n;
    const sqrtPriceX96 = slot0[0];
    const token0 = await client.readContract({ address: pool as Address, abi: UNIV3_POOL_ABI, functionName: 'token0' });
    const isToken0 = token0 === tokenIn;
    const price = (Number(sqrtPriceX96) / 2 ** 96) ** 2;
    const amountOutFloat = isToken0 ? Number(amountIn) * price : Number(amountIn) / price;
    return BigInt(Math.floor(amountOutFloat * 0.997));
  }
}

// ----- Path finder (2‑hop) -----
interface SwapStep { fromToken: string; toToken: string; pool: string; type: 'v2' | 'v3'; }
async function findFlashloanOpp(client: PublicClient, chainId: number, borrowToken: string, borrowAmount: bigint): Promise<{ steps: SwapStep[]; expectedProfit: bigint } | null> {
  const rows = await pg.query(`SELECT pool_address, token0, token1, type FROM active_pools WHERE chain_id = $1 AND (token0 = $2 OR token1 = $2)`, [chainId, borrowToken]);
  if (rows.rows.length < 2) return null;
  let bestProfit = 0n, bestSteps: SwapStep[] = [];
  for (const a of rows.rows) {
    const otherToken = a.token0 === borrowToken ? a.token1 : a.token0;
    const amountOutFirst = await getAmountOut(client, a.pool_address, a.type, borrowToken, borrowAmount);
    if (amountOutFirst === 0n) continue;
    for (const b of rows.rows) {
      if (a.pool_address === b.pool_address) continue;
      if (b.token0 !== otherToken && b.token1 !== otherToken) continue;
      const amountOutSecond = await getAmountOut(client, b.pool_address, b.type, otherToken, amountOutFirst);
      if (amountOutSecond === 0n) continue;
      const aaveFee = borrowAmount * 5n / 10000n;
      const profit = amountOutSecond - borrowAmount - aaveFee;
      if (profit > bestProfit) {
        bestProfit = profit;
        bestSteps = [
          { fromToken: borrowToken, toToken: otherToken, pool: a.pool_address, type: a.type },
          { fromToken: otherToken, toToken: borrowToken, pool: b.pool_address, type: b.type }
        ];
      }
    }
  }
  return bestProfit > 0n ? { steps: bestSteps, expectedProfit: bestProfit } : null;
}

// ----- Simulation (uses computed profit and gas estimate) -----
async function simulateFlashloan(client: PublicClient, aavePool: string, borrowToken: string, borrowAmount: bigint, steps: SwapStep[], gasPrice: bigint, chainId: number): Promise<{ profitUSD: number; gasCost: bigint }> {
  let currentAmount = borrowAmount;
  for (const step of steps) {
    const amountOut = await getAmountOut(client, step.pool, step.type, step.fromToken, currentAmount);
    if (amountOut === 0n) return { profitUSD: -Infinity, gasCost: 0n };
    currentAmount = amountOut;
  }
  const aaveFee = borrowAmount * 5n / 10000n;
  const profitWei = currentAmount - borrowAmount - aaveFee;
  if (profitWei <= 0n) return { profitUSD: -Infinity, gasCost: 0n };
  const profitAfterSlippage = Number(profitWei) * 0.992; // 0.8% slippage buffer
  const dummyReceiver = '0x0000000000000000000000000000000000000001' as Address;
  const swapData = encodeFunctionData({
    abi: parseAbi(['function swap(address[] dexes, address[] tokens, uint256[] amounts)']),
    functionName: 'swap',
    args: [steps.map(s => s.pool as Address), steps.map(s => s.fromToken as Address), [borrowAmount, ...steps.slice(0, -1).map(() => 0n)]]
  });
  const flashloanCall = encodeFunctionData({ abi: AAVE_POOL_ABI, functionName: 'flashLoanSimple', args: [dummyReceiver, borrowToken as Address, borrowAmount, swapData, 0] });
  const gas = await client.estimateGas({ to: aavePool as Address, data: flashloanCall }).catch(() => null);
  const gasCost = gas ? gas * gasPrice : 0n;
  const price = await getUSDPrice(client, borrowToken, chainId);
  const profitUSD = profitAfterSlippage * price / 1e18;
  return { profitUSD, gasCost };
}

// ----- Pool loading, listener, TVL refresh -----
let poolCache = new Map<number, any[]>();
async function loadExistingPools(chain: typeof CHAINS[0], client: PublicClient) {
  const factories = FACTORIES[chain.id] || [];
  for (const factory of factories) {
    if (!factory.address || factory.address === '0x...' || factory.address === '0x0') continue;
    if (!factory.enumerable) continue;
    try {
      const contract = getContract({ address: factory.address as Address, abi: FACTORY_V2_ABI, client });
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
          const price0 = await getUSDPrice(client, token0, chain.id);
          const price1 = await getUSDPrice(client, token1, chain.id);
          const tvl = (Number(reserves[0]) * price0 / (10 ** decimals0)) + (Number(reserves[1]) * price1 / (10 ** decimals1));
          return { pool: pool as string, token0, token1, tvl, type: 'v2' };
        }));
        for (const data of poolData.filter(d => d && d.tvl >= 800 && !(await isHoneypot(client, d.token0, factory.address) || await isHoneypot(client, d.token1, factory.address)))) {
          await pg.query(`INSERT INTO active_pools (chain_id, pool_address, token0, token1, factory, tvl_usd, type, last_scanned_block, cooldown_until, last_tvl_update) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT DO NOTHING`, [chain.id, data.pool, data.token0, data.token1, factory.address, data.tvl, data.type, 0, 0, 0]);
        }
        logger.info({ chain: chain.name, factory: factory.name, progress: i }, 'Loaded batch');
      }
    } catch (err) { logger.error({ err, factory: factory.address }, 'Failed to load existing pools'); }
  }
}

async function setupPoolListeners(chain: typeof CHAINS[0], wsClient: PublicClient, rpcManager: RPCRotator) {
  const factories = FACTORIES[chain.id] || [];
  for (const factory of factories) {
    if (!factory.address || factory.address === '0x...' || factory.address === '0x0') continue;
    const eventName = factory.type === 'v2' ? 'PairCreated' : 'PoolCreated';
    const eventAbi = { inputs: [{ indexed: true, name: 'token0', type: 'address' }, { indexed: true, name: 'token1', type: 'address' }, { indexed: false, name: 'pool', type: 'address' }, { indexed: false, name: 'index', type: 'uint256' }], name: eventName, type: 'event' };
    wsClient.watchContractEvent({
      address: factory.address as Address, abi: [eventAbi], eventName,
      onLogs: async logs => {
        for (const log of logs) {
          const pool = log.args?.pool, token0 = log.args?.token0, token1 = log.args?.token1;
          if (!pool || !token0 || !token1) continue;
          const client = rpcManager.getClient();
          let tvl = 0;
          if (factory.type === 'v2') {
            const reserves = await client.readContract({ address: pool as Address, abi: PAIR_ABI, functionName: 'getReserves' }).catch(() => null);
            if (!reserves) continue;
            const decimals0 = await getTokenDecimals(client, token0);
            const decimals1 = await getTokenDecimals(client, token1);
            const price0 = await getUSDPrice(client, token0, chain.id);
            const price1 = await getUSDPrice(client, token1, chain.id);
            tvl = (Number(reserves[0]) * price0 / (10 ** decimals0)) + (Number(reserves[1]) * price1 / (10 ** decimals1));
          } else {
            const slot0 = await client.readContract({ address: pool as Address, abi: UNIV3_POOL_ABI, functionName: 'slot0' }).catch(() => null);
            if (!slot0) continue;
            tvl = 1000; // rough estimate
          }
          if (tvl < 800) continue;
          const isHoney = await isHoneypot(client, token0, factory.address) || await isHoneypot(client, token1, factory.address);
          if (isHoney) continue;
          await pg.query(`INSERT INTO active_pools (chain_id, pool_address, token0, token1, factory, tvl_usd, type, last_scanned_block, cooldown_until, last_tvl_update) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT DO NOTHING`, [chain.id, pool, token0, token1, factory.address, tvl, factory.type, 0, 0, 0]);
        }
      }
    });
  }
}

let blockCounter = new Map<number, number>();
let globalLastExecutedBlock = 0;
let dailyProfit = 0;
let lastSummaryDate = new Date().toDateString();

async function scanChain(chain: typeof CHAINS[0], rpcManager: RPCRotator, wsClient: PublicClient) {
  if (!chain.aavePool) return;
  if (!poolCache.has(chain.id)) {
    const rows = await pg.query('SELECT * FROM active_pools WHERE chain_id = $1', [chain.id]);
    poolCache.set(chain.id, rows.rows);
  }
  const pools = poolCache.get(chain.id)!;
  const currentBlock = await wsClient.getBlockNumber();
  if (Number(currentBlock) <= globalLastExecutedBlock + 3) return;
  const eligible = pools.filter(p => Number(currentBlock) > p.cooldown_until);
  if (eligible.length === 0) return;

  const aaveContract = getContract({ address: chain.aavePool as Address, abi: AAVE_POOL_ABI, client: rpcManager.getClient() });
  const tokens = new Set<string>();
  for (const p of eligible) { tokens.add(p.token0); tokens.add(p.token1); }
  const reservesMap = new Map<string, any>();
  await Promise.all(Array.from(tokens).map(async t => {
    const data = await aaveContract.read.getReserveData([t as Address]).catch(() => null);
    if (data) reservesMap.set(t, data);
  }));

  for (const pool of eligible) {
    const r0 = reservesMap.get(pool.token0), r1 = reservesMap.get(pool.token1);
    if (!r0 || !r1) continue;
    const aToken0 = r0.aTokenAddress, aToken1 = r1.aTokenAddress;
    const erc20 = getContract({ address: pool.token0 as Address, abi: ERC20_ABI, client: rpcManager.getClient() });
    let avail0 = await erc20.read.balanceOf([aToken0 as Address]).catch(() => 0n);
    let avail1 = await erc20.read.balanceOf([aToken1 as Address]).catch(() => 0n);
    let amount0 = (avail0 * 8n) / 100n, amount1 = (avail1 * 8n) / 100n;
    if (amount0 === 0n) {
      const reserves = await rpcManager.getClient().readContract({ address: pool.pool_address as Address, abi: PAIR_ABI, functionName: 'getReserves' }).catch(() => null);
      if (reserves) amount0 = reserves[0] / 100n;
    }
    if (amount1 === 0n) {
      const reserves = await rpcManager.getClient().readContract({ address: pool.pool_address as Address, abi: PAIR_ABI, functionName: 'getReserves' }).catch(() => null);
      if (reserves) amount1 = reserves[1] / 100n;
    }
    const opp0 = await findFlashloanOpp(rpcManager.getClient(), chain.id, pool.token0, amount0);
    const opp1 = await findFlashloanOpp(rpcManager.getClient(), chain.id, pool.token1, amount1);
    for (const opp of [opp0, opp1].filter(Boolean)) {
      const gasPrice = await rpcManager.getClient().getGasPrice();
      const sim = await simulateFlashloan(rpcManager.getClient(), chain.aavePool!, opp!.steps[0].fromToken, opp!.steps[0].fromToken === pool.token0 ? amount0 : amount1, opp!.steps, gasPrice, chain.id);
      const threshold = chain.name === 'zksync' || chain.name === 'scroll' ? 3 : 5;
      if (sim.profitUSD > threshold && sim.profitUSD > Number(sim.gasCost) * 3 / 1e18) {
        logger.info({ chain: chain.name, profitUSD: sim.profitUSD, opp: opp!.steps }, 'Opportunity found');
        await sendTelegram(`🎯 Opportunity on ${chain.name}: $${sim.profitUSD.toFixed(2)} profit`);
        // --- EXECUTION DISABLED FOR DRY-RUN ---
        // Uncomment the following block only when a real receiver contract is deployed.
        /*
        if (chain.executor && chain.executor !== '0x0' && chain.executor !== '') {
          const walletClient = createWalletClient({
            account: privateKeyToAccount(`0x${process.env.EXECUTOR_PRIVATE_KEY}`),
            transport: http(rpcManager.getFirstUrl()),
            chain: chain.viemChain,
          });
          const swapData = encodeFunctionData({
            abi: parseAbi(['function swap(address[] dexes, address[] tokens, uint256[] amounts)']),
            functionName: 'swap',
            args: [opp!.steps.map(s => s.pool as Address), opp!.steps.map(s => s.fromToken as Address), [opp!.steps[0].fromToken === pool.token0 ? amount0 : amount1, 0n]]
          });
          const tx = await walletClient.sendTransaction({
            to: chain.executor as Address,
            data: encodeFunctionData({
              abi: parseAbi(['function executeArbitrage(address token, uint256 amount, address[] dexes, bytes calldata swapData) external returns (uint256 profit)']),
              functionName: 'executeArbitrage',
              args: [opp!.steps[0].fromToken as Address, opp!.steps[0].fromToken === pool.token0 ? amount0 : amount1, [ROUTERS[chain.id] as Address], swapData],
            }),
          });
          logger.info({ tx }, 'Transaction sent');
          await sendTelegram(`✅ Executed: ${tx}`);
          await pg.query('UPDATE active_pools SET cooldown_until = $1 WHERE pool_address = $2', [Number(currentBlock) + 2, pool.pool_address]);
          globalLastExecutedBlock = Number(currentBlock);
          dailyProfit += sim.profitUSD;
        } else {
          logger.info('Execution skipped – no executor address configured');
          dailyProfit += sim.profitUSD;
        }
        */
        // For dry-run, just accumulate simulated profit:
        dailyProfit += sim.profitUSD;
      }
    }
  }
  // Refresh TVL every 10 blocks (simplified)
  const cnt = (blockCounter.get(chain.id) || 0) + 1;
  blockCounter.set(chain.id, cnt);
  if (cnt % 10 === 0) {
    for (const pool of pools) {
      await pg.query('UPDATE active_pools SET last_tvl_update = $1 WHERE pool_address = $2', [Date.now(), pool.pool_address]);
    }
  }
}

// ----- Main -----
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
  `);
  const rpcManagers: Record<number, RPCRotator> = {};
  const wsRotators: Record<number, WSRotator> = {};
  for (const chain of CHAINS) {
    const httpUrls: string[] = [], wsUrls: string[] = [];
    for (let i = 1; ; i++) {
      const httpKey = `RPC_${chain.name.toUpperCase()}_HTTP_${i}`;
      const httpUrl = process.env[httpKey];
      if (!httpUrl) break;
      httpUrls.push(httpUrl);
    }
    for (let i = 1; ; i++) {
      const wsKey = `RPC_${chain.name.toUpperCase()}_WS_${i}`;
      const wsUrl = process.env[wsKey];
      if (!wsUrl) break;
      wsUrls.push(wsUrl);
    }
    if (httpUrls.length === 0) { logger.error(`No HTTP RPCs for ${chain.name}, skipping`); continue; }
    rpcManagers[chain.id] = new RPCRotator(httpUrls);
    if (wsUrls.length === 0) wsUrls.push(...httpUrls.map(u => u.replace('http', 'ws')));
    wsRotators[chain.id] = new WSRotator(wsUrls);
    await loadExistingPools(chain, rpcManagers[chain.id].getClient());
    const wsClient = await wsRotators[chain.id].getClient();
    await setupPoolListeners(chain, wsClient, rpcManagers[chain.id]);
  }

  // --- DRY-RUN: Only zkSync, no execution ---
  const dryRunChain = CHAINS.find(c => c.name === 'zksync');
  if (dryRunChain && wsRotators[dryRunChain.id]) {
    const wsClient = await wsRotators[dryRunChain.id].getClient();
    wsClient.watchBlocks({ onBlock: async () => { await scanChain(dryRunChain, rpcManagers[dryRunChain.id], wsClient); } });
    logger.info('Dry-run mode: only zkSync, no actual transactions will be sent.');
  } else {
    logger.error('zkSync not configured, cannot run dry-run');
    return;
  }

  // Health checks every minute
  setInterval(() => {
    Object.values(rpcManagers).forEach(m => m.healthCheck());
    Object.values(wsRotators).forEach(m => m.healthCheck());
  }, 60000);
  // Daily profit summary
  setInterval(() => {
    const now = new Date();
    if (now.toDateString() !== lastSummaryDate) {
      sendTelegram(`📊 Daily profit summary: $${dailyProfit.toFixed(2)}`);
      dailyProfit = 0;
      lastSummaryDate = now.toDateString();
    }
  }, 60000);
  // Alive ping every 5 minutes
  setInterval(async () => {
    const msg = `🟢 Scanner alive at ${new Date().toISOString()}`;
    logger.info(msg);
    await sendTelegram(msg);
  }, 300000);
  logger.info('Scanner started in dry-run mode (zkSync only, no execution).');
}

main().catch(err => { logger.error({ err }, 'Fatal error'); process.exit(1); });
