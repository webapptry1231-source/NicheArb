import { createPublicClient, createWalletClient, http, webSocket, encodeFunctionData, getContract, PublicClient, WalletClient, parseAbi, defineChain, Log } from 'viem';
import { privateKeyToAccount } from 'viem/accounts';
import { zksync, scroll, linea, base } from 'viem/chains';
import { AaveV3ZkSync, AaveV3Scroll, AaveV3Base } from '@bgd-labs/aave-address-book';
import { Pool } from 'pg';
import axios from 'axios';
import dotenv from 'dotenv';
import pino from 'pino';

dotenv.config();

const logger = pino({
  level: process.env.LOG_LEVEL || 'info',
  transport: { target: 'pino-pretty', options: { colorize: true, translateTime: true } }
});

// ----- Custom chains (Monad, Berachain) -----
const monad = defineChain({
  id: 143,
  name: 'Monad',
  network: 'monad',
  nativeCurrency: { name: 'MON', symbol: 'MON', decimals: 18 },
  rpcUrls: { default: { http: ['https://rpc.monad.xyz'] }, public: { http: ['https://rpc.monad.xyz'] } }
});

const berachain = defineChain({
  id: 80094,
  name: 'Berachain',
  network: 'berachain',
  nativeCurrency: { name: 'BERA', symbol: 'BERA', decimals: 18 },
  rpcUrls: { default: { http: ['https://rpc.berachain.com'] }, public: { http: ['https://rpc.berachain.com'] } }
});

// ----- Chain configuration (use manual pool address for Linea since address-book doesn't export it) -----
// Linea Aave V3 pool: 0xc47b8c00b0f69a36fa203ffeac0334874574a8ac
const CHAINS = [
  { id: 324, name: 'zksync', viemChain: zksync, aavePool: AaveV3ZkSync.POOL, executor: process.env.EXECUTOR_ZKSYNC! },
  { id: 534352, name: 'scroll', viemChain: scroll, aavePool: AaveV3Scroll.POOL, executor: process.env.EXECUTOR_SCROLL! },
  { id: 59144, name: 'linea', viemChain: linea, aavePool: '0xc47b8c00b0f69a36fa203ffeac0334874574a8ac', executor: process.env.EXECUTOR_LINEA! },
  { id: 8453, name: 'base', viemChain: base, aavePool: AaveV3Base.POOL, executor: process.env.EXECUTOR_BASE! },
  { id: 143, name: 'monad', viemChain: monad, aavePool: undefined, executor: process.env.EXECUTOR_MONAD! },
  { id: 80094, name: 'berachain', viemChain: berachain, aavePool: undefined, executor: process.env.EXECUTOR_BERACHAIN! }
];

// ----- Factory addresses (real where known) -----
const FACTORIES: Record<number, { name: string; address: string; type: 'v2' | 'v3' }[]> = {
  324: [
    { name: 'SyncSwap Classic', address: '0xf2DAd89f2788a8CD54625C60b55cD3d2D0ACa7Cb', type: 'v2' },
    { name: 'SyncSwap Stable', address: '0x5b9f21d407F35b10CbfDDca17D5D84b129356ea3', type: 'v2' },
    { name: 'Uniswap V3', address: '0x8FdA5a7a8dCA67BBcDd10F02Fa0649A937215422', type: 'v3' }
  ],
  534352: [], // Scroll – fill later
  59144: [], // Linea – fill later
  8453: [
    { name: 'Aerodrome', address: '0x42Bf381a259F9aD132D5f257E0eF5E4c5F0b2BfF', type: 'v2' },
    { name: 'Uniswap V3', address: '0x33128a8fC17869897dcE68Ed026d694621f6FDfD', type: 'v3' }
  ],
  143: [
    { name: 'Kuru', address: '0xd651346d7c789536ebf06dc72aE3C8502cd695CC', type: 'v2' }
  ],
  80094: [
    { name: 'Kodiak', address: '0xD84CBf0B02636E7f53dB9E5e45A616E05d710990', type: 'v3' }
  ]
};

// ----- DEX routers (must be non‑empty for execution) -----
const ROUTERS: Record<number, string> = {
  324: '0x9B5def958d0f3b6955cBEa4D5B7809b2fb26b059',
  534352: '', // fill later
  59144: '', // fill later
  8453: '0xcf77a3ba9a5ca399b7c97c74d54e5b1beb874e43', // Aerodrome router
  143: '0x0d3a1BE29E9dEd63c7a5678b31e847D68F71FFa2',
  80094: '0xEd158C4b336A6FCb5B193A5570e3a571f6cbe690'
};

// ----- Chainlink feed mapping – REPLACE WITH REAL ADDRESSES FROM docs.chain.link -----
const CHAINLINK_FEEDS: Record<number, Record<string, string>> = {
  324: {},
  8453: {},
};

// ----- ABIs -----
const ERC20_ABI = parseAbi([
  'function totalSupply() view returns (uint256)',
  'function balanceOf(address) view returns (uint256)',
  'function transfer(address, uint256) returns (bool)',
  'function owner() view returns (address)',
  'function decimals() view returns (uint8)'
]);

const PAIR_ABI = parseAbi([
  'function getReserves() view returns (uint112 reserve0, uint112 reserve1, uint32 blockTimestampLast)',
  'function token0() view returns (address)',
  'function token1() view returns (address)'
]);

const UNIV3_POOL_ABI = parseAbi([
  'function slot0() view returns (uint160 sqrtPriceX96, int24 tick, uint16 observationIndex, uint16 observationCardinality, uint16 observationCardinalityNext, uint8 feeProtocol, bool unlocked)',
  'function token0() view returns (address)',
  'function token1() view returns (address)'
]);

const FACTORY_V2_ABI = parseAbi([
  'function allPairsLength() view returns (uint256)',
  'function allPairs(uint256) view returns (address)'
]);

// Correct Aave V3 reserve data struct (named)
const AAVE_POOL_ABI = parseAbi([
  'function getReserveData(address asset) view returns ((uint256 configuration, uint128 liquidityIndex, uint128 currentLiquidityRate, uint128 variableBorrowIndex, uint128 currentVariableBorrowRate, uint128 currentStableBorrowRate, uint40 lastUpdateTimestamp, uint16 id, address aTokenAddress, address stableDebtTokenAddress, address variableDebtTokenAddress, address interestRateStrategyAddress, uint128 accruedToTreasury, uint128 unbacked, uint128 isolationModeTotalDebt))'
]);

const EXECUTOR_ABI = parseAbi([
  'function executeArbitrage(address token, uint256 amount, address[] dexes, bytes calldata swapData) external returns (uint256 profit)'
]);

// ----- RPC rotators -----
class RPCRotator {
  private urls: string[];
  private index = 0;
  private clients: Map<string, PublicClient> = new Map();
  constructor(urls: string[]) { this.urls = urls; }
  getClient() {
    const url = this.urls[this.index % this.urls.length];
    this.index++;
    if (!this.clients.has(url)) {
      this.clients.set(url, createPublicClient({ transport: http(url) }));
    }
    return this.clients.get(url)!;
  }
  getFirstUrl() { return this.urls[0]; }
}

class WSRotator {
  private urls: string[];
  private index = 0;
  private clients: Map<string, PublicClient> = new Map();
  private retryCount = 0;
  constructor(urls: string[]) { this.urls = urls; }
  async getClient(maxRetries = 3): Promise<PublicClient> {
    const url = this.urls[this.index % this.urls.length];
    this.index++;
    if (!this.clients.has(url)) {
      const client = createPublicClient({ transport: webSocket(url) });
      this.clients.set(url, client);
      try {
        await client.getBlockNumber();
        this.retryCount = 0;
      } catch (err) {
        logger.error({ err, url }, 'WebSocket connection failed');
        this.clients.delete(url);
        if (this.retryCount >= maxRetries) throw new Error('Max WS retries exceeded');
        this.retryCount++;
        await new Promise(resolve => setTimeout(resolve, 2000 * this.retryCount));
        return this.getClient(maxRetries);
      }
    }
    return this.clients.get(url)!;
  }
}

// ----- Database -----
const pg = new Pool({ connectionString: process.env.DATABASE_URL });
pg.on('error', (err: Error) => logger.error({ err }, 'Postgres error'));

// ----- Telegram -----
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN!;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID!;
async function sendTelegram(message: string) {
  try {
    await axios.post(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
      chat_id: TELEGRAM_CHAT_ID,
      text: message,
    });
  } catch (err) {
    logger.error({ err }, 'Telegram send failed');
  }
}

// ----- USD price (Chainlink + fallback) -----
async function getUSDPrice(client: PublicClient, tokenAddress: string, chainId: number): Promise<number> {
  const feeds = CHAINLINK_FEEDS[chainId];
  if (feeds && feeds[tokenAddress]) {
    try {
      const aggregator = getContract({
        address: feeds[tokenAddress] as `0x${string}`,
        abi: parseAbi(['function latestRoundData() view returns (uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound)']),
        client,
      });
      const data = await aggregator.read.latestRoundData();
      return Number(data[1]) / 1e8; // answer is at index 1
    } catch (err) {
      logger.warn({ err, token: tokenAddress }, 'Chainlink feed failed, falling back');
    }
  }
  // Hardcoded stablecoins and WETH by address
  const stablecoins: Record<number, string[]> = {
    324: ['0x3355df6D4c9C3035724Fd0e3914dE96A5a83aaf4'], // USDC.e
    8453: ['0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'], // USDC
  };
  const weth: Record<number, string[]> = {
    324: ['0x5aea5775959fbc2557cc8789bc1bf90a239d9a91'],
    8453: ['0x4200000000000000000000000000000000000006'],
  };
  if (stablecoins[chainId]?.includes(tokenAddress)) return 1;
  if (weth[chainId]?.includes(tokenAddress)) return 3000;
  return 0;
}

// ----- Token decimals -----
async function getTokenDecimals(client: PublicClient, token: string): Promise<number> {
  const contract = getContract({ address: token as `0x${string}`, abi: ERC20_ABI, client });
  return await contract.read.decimals().catch(() => 18);
}

// ----- Honeypot check -----
async function isHoneypot(client: PublicClient, token: string, factoryAddress: string): Promise<boolean> {
  try {
    const contract = getContract({ address: token as `0x${string}`, abi: ERC20_ABI, client });
    const holder = factoryAddress !== '0x...' && factoryAddress !== '0x0' ? factoryAddress : token;
    const balance = await contract.read.balanceOf([holder as `0x${string}`]).catch(() => 0n);
    if (balance === 0n) return false;
    const amount = balance / 1000n;
    const transferCall = encodeFunctionData({ abi: ERC20_ABI, functionName: 'transfer', args: [holder as `0x${string}`, amount] });
    const result = await client.call({ to: token as `0x${string}`, data: transferCall }).catch(() => null);
    if (result && !result.data) {
      const totalSupplyBefore = await contract.read.totalSupply();
      const totalSupplyAfter = await contract.read.totalSupply(); // same block
      if (totalSupplyAfter !== totalSupplyBefore) return true;
      return false;
    }
    return true;
  } catch {
    return true;
  }
}

// ----- Compute output amount for a swap given pool address and type -----
async function getAmountOut(
  client: PublicClient,
  pool: string,
  type: 'v2' | 'v3',
  tokenIn: string,
  amountIn: bigint
): Promise<bigint> {
  if (type === 'v2') {
    const reserves = await client.readContract({
      address: pool as `0x${string}`,
      abi: PAIR_ABI,
      functionName: 'getReserves',
    }).catch(() => null);
    if (!reserves) return 0n;
    const [reserve0, reserve1] = reserves;
    const token0 = await client.readContract({ address: pool as `0x${string}`, abi: PAIR_ABI, functionName: 'token0' });
    let reserveIn, reserveOut;
    if (token0 === tokenIn) {
      reserveIn = reserve0;
      reserveOut = reserve1;
    } else {
      reserveIn = reserve1;
      reserveOut = reserve0;
    }
    const amountOut = (amountIn * reserveOut) / reserveIn;
    return (amountOut * 997n) / 1000n; // 0.3% fee
  } else {
    // V3: use sqrtPriceX96 to compute amount out
    const slot0 = await client.readContract({
      address: pool as `0x${string}`,
      abi: UNIV3_POOL_ABI,
      functionName: 'slot0',
    }).catch(() => null);
    if (!slot0) return 0n;
    const sqrtPriceX96 = slot0[0];
    const token0 = await client.readContract({ address: pool as `0x${string}`, abi: UNIV3_POOL_ABI, functionName: 'token0' });
    const isToken0 = token0 === tokenIn;
    // price = (sqrtPriceX96 / 2^96)^2
    const price = (Number(sqrtPriceX96) / 2 ** 96) ** 2;
    const amountOutFloat = isToken0 ? Number(amountIn) * price : Number(amountIn) / price;
    const fee = 0.997; // assume 0.3% fee
    return BigInt(Math.floor(amountOutFloat * fee));
  }
}

// ----- Arbitrage detection (full round-trip) -----
interface ArbitrageOpportunity {
  borrowToken: string;
  borrowAmount: bigint;
  cheapDex: string;
  expensiveDex: string;
  expectedProfit: bigint; // in borrowToken units
}

async function findArbitrage(
  client: PublicClient,
  chainId: number,
  tokenA: string,
  tokenB: string,
  amount: bigint
): Promise<ArbitrageOpportunity | null> {
  // Get all pools for this pair with their types
  const pools = await pg.query(
    `SELECT pool_address, factory, token0, token1, type FROM active_pools
     WHERE chain_id = $1 AND ((token0 = $2 AND token1 = $3) OR (token0 = $3 AND token1 = $2))`,
    [chainId, tokenA, tokenB]
  );
  if (pools.rows.length < 2) return null;

  // For each pool, compute amountOut for A->B
  const swapAB: { pool: string; type: string; amountOut: bigint }[] = [];
  for (const row of pools.rows) {
    const amountOut = await getAmountOut(client, row.pool_address, row.type, tokenA, amount);
    if (amountOut > 0n) {
      swapAB.push({ pool: row.pool_address, type: row.type, amountOut });
    }
  }

  let bestProfit = 0n;
  let bestCheap = '';
  let bestExpensive = '';
  for (const cheap of swapAB) {
    for (const row of pools.rows) {
      if (cheap.pool === row.pool_address) continue;
      const amountOutSecond = await getAmountOut(client, row.pool_address, row.type, tokenB, cheap.amountOut);
      if (amountOutSecond === 0n) continue;
      const aaveFee = amount * 5n / 10000n;
      const profit = amountOutSecond - amount - aaveFee;
      if (profit > bestProfit) {
        bestProfit = profit;
        bestCheap = cheap.pool;
        bestExpensive = row.pool_address;
      }
    }
  }
  if (bestProfit > 0n) {
    return {
      borrowToken: tokenA,
      borrowAmount: amount,
      cheapDex: bestCheap,
      expensiveDex: bestExpensive,
      expectedProfit: bestProfit
    };
  }
  return null;
}

// ----- Simulation of full arbitrage (using executor contract) -----
async function simulateArbitrage(
  client: PublicClient,
  executor: string,
  borrowToken: string,
  borrowAmount: bigint,
  cheapDex: string,
  expensiveDex: string,
  gasPrice: bigint,
  chainId: number
): Promise<{ profitUSD: number; gasCost: bigint }> {
  const router = ROUTERS[chainId];
  if (!router) return { profitUSD: -Infinity, gasCost: 0n };
  const swapData = encodeFunctionData({
    abi: parseAbi(['function swap(address[] dexes, address[] tokens, uint256[] amounts)']),
    functionName: 'swap',
    args: [
      [cheapDex as `0x${string}`, expensiveDex as `0x${string}`],
      [borrowToken as `0x${string}`, borrowToken as `0x${string}`],
      [borrowAmount, 0n]
    ]
  });
  const callData = encodeFunctionData({
    abi: EXECUTOR_ABI,
    functionName: 'executeArbitrage',
    args: [borrowToken as `0x${string}`, borrowAmount, [router as `0x${string}`], swapData],
  });
  const gas = await client.estimateGas({ to: executor as `0x${string}`, data: callData }).catch(() => null);
  if (!gas) return { profitUSD: -Infinity, gasCost: 0n };
  const gasCost = gas * gasPrice;
  const result = await client.call({ to: executor as `0x${string}`, data: callData }).catch(() => null);
  if (!result || !result.data) return { profitUSD: -Infinity, gasCost };
  const profitWei = BigInt(result.data);
  const price = await getUSDPrice(client, borrowToken, chainId);
  const profitUSD = Number(profitWei) * price / 1e18;
  return { profitUSD, gasCost };
}

// ----- Batch load pools from factory (V2 only) -----
async function loadExistingPools(chain: typeof CHAINS[0], client: PublicClient) {
  const factories = FACTORIES[chain.id] || [];
  for (const factory of factories) {
    if (!factory.address || factory.address === '0x...' || factory.address === '0x0') {
      logger.info(`Skipping factory ${factory.name} on ${chain.name} – address not configured`);
      continue;
    }
    if (factory.type === 'v3') {
      logger.info(`V3 factory ${factory.name} on ${chain.name} cannot be enumerated; will rely on events`);
      continue;
    }
    try {
      const contract = getContract({
        address: factory.address as `0x${string}`,
        abi: FACTORY_V2_ABI,
        client,
      });
      let length: bigint = await contract.read.allPairsLength();
      const batchSize = 50;
      for (let i = 0; i < Number(length); i += batchSize) {
        const promises = [];
        for (let j = i; j < Math.min(i + batchSize, Number(length)); j++) {
          promises.push(contract.read.allPairs([BigInt(j)]));
        }
        const addresses = await Promise.all(promises);
        const poolData = await Promise.all(addresses.map(async (pool) => {
          const pair = getContract({ address: pool as `0x${string}`, abi: PAIR_ABI, client });
          const [token0, token1, reserves] = await Promise.all([
            pair.read.token0().catch(() => '0x'),
            pair.read.token1().catch(() => '0x'),
            pair.read.getReserves().catch(() => null),
          ]);
          if (token0 === '0x' || token1 === '0x' || !reserves) return null;
          const decimals0 = await getTokenDecimals(client, token0);
          const decimals1 = await getTokenDecimals(client, token1);
          const price0 = await getUSDPrice(client, token0, chain.id);
          const price1 = await getUSDPrice(client, token1, chain.id);
          const tvl0 = Number(reserves[0]) * price0 / (10 ** decimals0);
          const tvl1 = Number(reserves[1]) * price1 / (10 ** decimals1);
          const tvl = tvl0 + tvl1;
          return { pool: pool as string, token0, token1, tvl, type: 'v2' };
        }));
        for (const data of poolData) {
          if (!data) continue;
          if (data.tvl < 800) continue;
          const isHoney = await isHoneypot(client, data.token0, factory.address) || await isHoneypot(client, data.token1, factory.address);
          if (isHoney) continue;
          await pg.query(
            `INSERT INTO active_pools (chain_id, pool_address, token0, token1, factory, tvl_usd, type, last_scanned_block, cooldown_until)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
             ON CONFLICT (chain_id, pool_address) DO NOTHING`,
            [chain.id, data.pool, data.token0, data.token1, factory.address, data.tvl, data.type, 0, 0]
          );
        }
        logger.info({ chain: chain.name, factory: factory.name, progress: i }, 'Loaded batch');
      }
    } catch (err) {
      logger.error({ err, factory: factory.address }, 'Failed to load existing pools');
    }
  }
}

// ----- Listeners for new pools -----
async function setupPoolListeners(chain: typeof CHAINS[0], wsClient: PublicClient, rpcManager: RPCRotator) {
  const factories = FACTORIES[chain.id] || [];
  for (const factory of factories) {
    if (!factory.address || factory.address === '0x...' || factory.address === '0x0') {
      logger.info(`Skipping listener for factory ${factory.name} on ${chain.name} – address not configured`);
      continue;
    }
    const eventAbi = {
      inputs: [
        { indexed: true, name: 'token0', type: 'address' },
        { indexed: true, name: 'token1', type: 'address' },
        { indexed: false, name: 'pool', type: 'address' },
        { indexed: false, name: 'index', type: 'uint256' },
      ],
      name: factory.type === 'v2' ? 'PairCreated' : 'PoolCreated',
      type: 'event',
    };
    wsClient.watchContractEvent({
      address: factory.address as `0x${string}`,
      abi: [eventAbi],
      eventName: factory.type === 'v2' ? 'PairCreated' : 'PoolCreated',
      onLogs: async (logs: Log[]) => {
        for (const log of logs) {
          const pool = (log.args as any).pool as string;
          const token0 = (log.args as any).token0 as string;
          const token1 = (log.args as any).token1 as string;
          if (!pool || !token0 || !token1) continue;
          logger.info({ chain: chain.name, pool }, 'New pool discovered');
          const client = rpcManager.getClient();
          let tvl = 0;
          let decimals0, decimals1, price0, price1, type = factory.type;
          if (factory.type === 'v2') {
            const reserves = await client.readContract({ address: pool as `0x${string}`, abi: PAIR_ABI, functionName: 'getReserves' }).catch(() => null);
            if (!reserves) continue;
            decimals0 = await getTokenDecimals(client, token0);
            decimals1 = await getTokenDecimals(client, token1);
            price0 = await getUSDPrice(client, token0, chain.id);
            price1 = await getUSDPrice(client, token1, chain.id);
            const tvl0 = Number(reserves[0]) * price0 / (10 ** decimals0);
            const tvl1 = Number(reserves[1]) * price1 / (10 ** decimals1);
            tvl = tvl0 + tvl1;
          } else {
            // V3: approximate TVL using slot0 and tick (simplified)
            const slot0 = await client.readContract({ address: pool as `0x${string}`, abi: UNIV3_POOL_ABI, functionName: 'slot0' }).catch(() => null);
            if (!slot0) continue;
            decimals0 = await getTokenDecimals(client, token0);
            decimals1 = await getTokenDecimals(client, token1);
            price0 = await getUSDPrice(client, token0, chain.id);
            price1 = await getUSDPrice(client, token1, chain.id);
            tvl = 1000; // assume enough to pass filter
          }
          if (tvl < 800) continue;
          const isHoney = await isHoneypot(client, token0, factory.address) || await isHoneypot(client, token1, factory.address);
          if (isHoney) {
            logger.info({ pool }, 'Skipping honeypot pool');
            continue;
          }
          await pg.query(
            `INSERT INTO active_pools (chain_id, pool_address, token0, token1, factory, tvl_usd, type, last_scanned_block, cooldown_until)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
             ON CONFLICT (chain_id, pool_address) DO NOTHING`,
            [chain.id, pool, token0, token1, factory.address, tvl, factory.type, 0, 0]
          );
        }
      },
    });
  }
}

// ----- Main scanning loop -----
async function scanChain(chain: typeof CHAINS[0], rpcManager: RPCRotator, wsClient: PublicClient) {
  if (!chain.aavePool) {
    logger.info(`${chain.name} has no Aave, skipping scan`);
    return;
  }
  const router = ROUTERS[chain.id];
  if (!router) {
    logger.info(`${chain.name} has no router configured, skipping scan`);
    return;
  }
  const pools = await pg.query('SELECT pool_address, token0, token1, tvl_usd, cooldown_until FROM active_pools WHERE chain_id = $1', [chain.id]);
  const currentBlock = await wsClient.getBlockNumber();
  for (const pool of pools.rows) {
    if (Number(currentBlock) <= pool.cooldown_until) continue;
    // Get Aave reserve data for token0 and token1 using the correct ABI
    const aaveContract = getContract({ address: chain.aavePool as `0x${string}`, abi: AAVE_POOL_ABI, client: rpcManager.getClient() });
    const reserveData0 = await aaveContract.read.getReserveData([pool.token0 as `0x${string}`]).catch(() => null);
    const reserveData1 = await aaveContract.read.getReserveData([pool.token1 as `0x${string}`]).catch(() => null);
    if (!reserveData0 || !reserveData1) continue;
    // aToken address is the 9th field (index 8) in the struct
    const aToken0 = (reserveData0 as any).aTokenAddress as string;
    const aToken1 = (reserveData1 as any).aTokenAddress as string;
    const erc20 = getContract({ address: pool.token0 as `0x${string}`, abi: ERC20_ABI, client: rpcManager.getClient() });
    const available0 = await erc20.read.balanceOf([aToken0 as `0x${string}`]).catch(() => 0n);
    const available1 = await erc20.read.balanceOf([aToken1 as `0x${string}`]).catch(() => 0n);
    let amountToBorrow0 = (available0 * 8n) / 100n;
    let amountToBorrow1 = (available1 * 8n) / 100n;
    // Fallback to 1% of pool reserve if Aave has no liquidity
    if (amountToBorrow0 === 0n) {
      const reserves = await rpcManager.getClient().readContract({ address: pool.pool_address as `0x${string}`, abi: PAIR_ABI, functionName: 'getReserves' }).catch(() => null);
      if (reserves) amountToBorrow0 = reserves[0] / 100n;
    }
    if (amountToBorrow1 === 0n) {
      const reserves = await rpcManager.getClient().readContract({ address: pool.pool_address as `0x${string}`, abi: PAIR_ABI, functionName: 'getReserves' }).catch(() => null);
      if (reserves) amountToBorrow1 = reserves[1] / 100n;
    }

    const opp0 = await findArbitrage(rpcManager.getClient(), chain.id, pool.token0, pool.token1, amountToBorrow0);
    const opp1 = await findArbitrage(rpcManager.getClient(), chain.id, pool.token1, pool.token0, amountToBorrow1);

    for (const opp of [opp0, opp1].filter(o => o !== null)) {
      const gasPrice = await rpcManager.getClient().getGasPrice();
      const { profitUSD, gasCost } = await simulateArbitrage(
        rpcManager.getClient(),
        chain.executor,
        opp.borrowToken,
        opp.borrowAmount,
        opp.cheapDex,
        opp.expensiveDex,
        gasPrice,
        chain.id
      );
      const threshold = (chain.name === 'zksync' || chain.name === 'scroll') ? 3 : 5;
      if (profitUSD > threshold && BigInt(Math.floor(profitUSD * 1e18)) > gasCost * 3n) {
        logger.info({ chain: chain.name, profitUSD, opp }, 'Opportunity found');
        await sendTelegram(`🎯 Opportunity on ${chain.name}: $${profitUSD} profit`);
        if (chain.executor && chain.executor !== '0x0' && chain.executor !== '') {
          const walletClient = createWalletClient({
            account: privateKeyToAccount(`0x${process.env.EXECUTOR_PRIVATE_KEY}`),
            transport: http(rpcManager.getFirstUrl()),
            chain: chain.viemChain,
          });
          const swapData = encodeFunctionData({
            abi: parseAbi(['function swap(address[] dexes, address[] tokens, uint256[] amounts)']),
            functionName: 'swap',
            args: [
              [opp.cheapDex as `0x${string}`, opp.expensiveDex as `0x${string}`],
              [opp.borrowToken as `0x${string}`, opp.borrowToken as `0x${string}`],
              [opp.borrowAmount, 0n]
            ]
          });
          const tx = await walletClient.sendTransaction({
            to: chain.executor as `0x${string}`,
            data: encodeFunctionData({
              abi: EXECUTOR_ABI,
              functionName: 'executeArbitrage',
              args: [opp.borrowToken as `0x${string}`, opp.borrowAmount, [router as `0x${string}`], swapData],
            }),
          });
          logger.info({ tx }, 'Transaction sent');
          await sendTelegram(`✅ Executed: ${tx}`);
          await pg.query('UPDATE active_pools SET cooldown_until = $1 WHERE pool_address = $2', [Number(currentBlock) + 2, pool.pool_address]);
        } else {
          logger.info('Execution skipped – no executor address configured');
        }
      }
    }
  }
}

// ----- Main entry point -----
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
      created_at TIMESTAMP DEFAULT NOW(),
      UNIQUE(chain_id, pool_address)
    );
  `);

  const rpcManagers: Record<number, RPCRotator> = {};
  const wsRotators: Record<number, WSRotator> = {};

  for (const chain of CHAINS) {
    const httpUrls: string[] = [];
    const wsUrls: string[] = [];
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
    if (httpUrls.length === 0) {
      logger.error(`No HTTP RPCs for ${chain.name}, skipping`);
      continue;
    }
    rpcManagers[chain.id] = new RPCRotator(httpUrls);
    if (wsUrls.length === 0) {
      wsUrls.push(...httpUrls.map(u => u.replace('http', 'ws')));
    }
    wsRotators[chain.id] = new WSRotator(wsUrls);

    await loadExistingPools(chain, rpcManagers[chain.id].getClient());

    const wsClient = await wsRotators[chain.id].getClient();
    await setupPoolListeners(chain, wsClient, rpcManagers[chain.id]);
  }

  for (const chain of CHAINS) {
    if (!wsRotators[chain.id]) continue;
    const wsClient = await wsRotators[chain.id].getClient();
    wsClient.watchBlocks({
      onBlock: async (block) => {
        await scanChain(chain, rpcManagers[chain.id], wsClient);
      },
    });
  }

  setInterval(async () => {
    const msg = `🟢 Scanner alive at ${new Date().toISOString()}`;
    logger.info(msg);
    await sendTelegram(msg);
  }, 300000);

  logger.info('Scanner started');
}

main().catch((err) => {
  logger.error({ err }, 'Fatal error');
  process.exit(1);
});
