from dataclasses import dataclass
from typing import Dict, Optional, List
from cow_amm_trade_envy.models import Pools, BCowPool


@dataclass
class EnvyCalculatorConfig:
    network: str
    gas_cost_estimate: int = 100_000


class PGConfig:
    def __init__(self, user: str, password: str, host: str, port: int, database: str):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database


@dataclass
class DataFetcherConfig:
    network: str
    node_url: str
    min_block: int
    pg_config: PGConfig
    dune_query_settle: int = 4448838
    dune_query_price: int = 4468197
    interval_length_settle: int = 10_000
    interval_length_price: int = 100_000
    backoff_blocks: Dict[str, int] = None
    max_block: Optional[int] = None
    used_pools: Optional[List[BCowPool]] = None

    def __post_init__(self):
        if self.backoff_blocks is None:
            self.backoff_blocks = {"ethereum": 1800}

        if self.network not in self.backoff_blocks:
            raise ValueError(f"Network {self.network} not supported")

        # per default all pools are used
        if self.used_pools is None:
            self.used_pools = Pools().get_pools()


BCOW_FULL_COW_HELPER_ABI = '[{"inputs":[{"internalType":"address","name":"factory_","type":"address"}],"stateMutability":"nonpayable","type":"constructor"},{"inputs":[],"name":"BNum_AddOverflow","type":"error"},{"inputs":[],"name":"BNum_BPowBaseTooHigh","type":"error"},{"inputs":[],"name":"BNum_BPowBaseTooLow","type":"error"},{"inputs":[],"name":"BNum_DivInternal","type":"error"},{"inputs":[],"name":"BNum_DivZero","type":"error"},{"inputs":[],"name":"BNum_MulOverflow","type":"error"},{"inputs":[],"name":"BNum_SubUnderflow","type":"error"},{"inputs":[],"name":"MathOverflowedMulDiv","type":"error"},{"inputs":[],"name":"NoOrder","type":"error"},{"inputs":[],"name":"PoolDoesNotExist","type":"error"},{"inputs":[],"name":"PoolIsClosed","type":"error"},{"inputs":[],"name":"PoolIsPaused","type":"error"},{"inputs":[],"name":"BONE","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"BPOW_PRECISION","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"EXIT_FEE","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"INIT_POOL_SUPPLY","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MAX_BOUND_TOKENS","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MAX_BPOW_BASE","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MAX_FEE","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MAX_IN_RATIO","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MAX_OUT_RATIO","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MAX_TOTAL_WEIGHT","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MAX_WEIGHT","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MIN_BALANCE","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MIN_BOUND_TOKENS","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MIN_BPOW_BASE","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MIN_FEE","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MIN_WEIGHT","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"tokenBalanceIn","type":"uint256"},{"internalType":"uint256","name":"tokenWeightIn","type":"uint256"},{"internalType":"uint256","name":"tokenBalanceOut","type":"uint256"},{"internalType":"uint256","name":"tokenWeightOut","type":"uint256"},{"internalType":"uint256","name":"tokenAmountOut","type":"uint256"},{"internalType":"uint256","name":"swapFee","type":"uint256"}],"name":"calcInGivenOut","outputs":[{"internalType":"uint256","name":"tokenAmountIn","type":"uint256"}],"stateMutability":"pure","type":"function"},{"inputs":[{"internalType":"uint256","name":"tokenBalanceIn","type":"uint256"},{"internalType":"uint256","name":"tokenWeightIn","type":"uint256"},{"internalType":"uint256","name":"tokenBalanceOut","type":"uint256"},{"internalType":"uint256","name":"tokenWeightOut","type":"uint256"},{"internalType":"uint256","name":"tokenAmountIn","type":"uint256"},{"internalType":"uint256","name":"swapFee","type":"uint256"}],"name":"calcOutGivenIn","outputs":[{"internalType":"uint256","name":"tokenAmountOut","type":"uint256"}],"stateMutability":"pure","type":"function"},{"inputs":[{"internalType":"uint256","name":"tokenBalanceIn","type":"uint256"},{"internalType":"uint256","name":"tokenWeightIn","type":"uint256"},{"internalType":"uint256","name":"tokenBalanceOut","type":"uint256"},{"internalType":"uint256","name":"tokenWeightOut","type":"uint256"},{"internalType":"uint256","name":"swapFee","type":"uint256"}],"name":"calcSpotPrice","outputs":[{"internalType":"uint256","name":"spotPrice","type":"uint256"}],"stateMutability":"pure","type":"function"},{"inputs":[],"name":"factory","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"pool","type":"address"},{"internalType":"uint256[]","name":"prices","type":"uint256[]"}],"name":"order","outputs":[{"components":[{"internalType":"contract IERC20","name":"sellToken","type":"address"},{"internalType":"contract IERC20","name":"buyToken","type":"address"},{"internalType":"address","name":"receiver","type":"address"},{"internalType":"uint256","name":"sellAmount","type":"uint256"},{"internalType":"uint256","name":"buyAmount","type":"uint256"},{"internalType":"uint32","name":"validTo","type":"uint32"},{"internalType":"bytes32","name":"appData","type":"bytes32"},{"internalType":"uint256","name":"feeAmount","type":"uint256"},{"internalType":"bytes32","name":"kind","type":"bytes32"},{"internalType":"bool","name":"partiallyFillable","type":"bool"},{"internalType":"bytes32","name":"sellTokenBalance","type":"bytes32"},{"internalType":"bytes32","name":"buyTokenBalance","type":"bytes32"}],"internalType":"struct GPv2Order.Data","name":"order_","type":"tuple"},{"components":[{"internalType":"address","name":"target","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"bytes","name":"callData","type":"bytes"}],"internalType":"struct GPv2Interaction.Data[]","name":"preInteractions","type":"tuple[]"},{"components":[{"internalType":"address","name":"target","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"bytes","name":"callData","type":"bytes"}],"internalType":"struct GPv2Interaction.Data[]","name":"postInteractions","type":"tuple[]"},{"internalType":"bytes","name":"sig","type":"bytes"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"pool","type":"address"}],"name":"tokens","outputs":[{"internalType":"address[]","name":"tokens_","type":"address[]"}],"stateMutability":"view","type":"function"}]'
BCOW_PARTIAL_COW_HELPER_ABI = '[{"inputs":[{"internalType":"address","name":"factory_","type":"address"}],"stateMutability":"nonpayable","type":"constructor"},{"inputs":[],"name":"BNum_AddOverflow","type":"error"},{"inputs":[],"name":"BNum_BPowBaseTooHigh","type":"error"},{"inputs":[],"name":"BNum_BPowBaseTooLow","type":"error"},{"inputs":[],"name":"BNum_DivInternal","type":"error"},{"inputs":[],"name":"BNum_DivZero","type":"error"},{"inputs":[],"name":"BNum_MulOverflow","type":"error"},{"inputs":[],"name":"BNum_SubUnderflow","type":"error"},{"inputs":[],"name":"InvalidToken","type":"error"},{"inputs":[],"name":"NoOrder","type":"error"},{"inputs":[],"name":"PoolDoesNotExist","type":"error"},{"inputs":[],"name":"PoolIsClosed","type":"error"},{"inputs":[],"name":"PoolIsPaused","type":"error"},{"inputs":[],"name":"BONE","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"BPOW_PRECISION","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"EXIT_FEE","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"INIT_POOL_SUPPLY","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MAX_BOUND_TOKENS","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MAX_BPOW_BASE","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MAX_FEE","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MAX_IN_RATIO","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MAX_ORDER_DURATION","outputs":[{"internalType":"uint32","name":"","type":"uint32"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MAX_OUT_RATIO","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MAX_TOTAL_WEIGHT","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MAX_WEIGHT","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MIN_BALANCE","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MIN_BOUND_TOKENS","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MIN_BPOW_BASE","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MIN_FEE","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MIN_WEIGHT","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"tokenBalanceIn","type":"uint256"},{"internalType":"uint256","name":"tokenWeightIn","type":"uint256"},{"internalType":"uint256","name":"tokenBalanceOut","type":"uint256"},{"internalType":"uint256","name":"tokenWeightOut","type":"uint256"},{"internalType":"uint256","name":"tokenAmountOut","type":"uint256"},{"internalType":"uint256","name":"swapFee","type":"uint256"}],"name":"calcInGivenOut","outputs":[{"internalType":"uint256","name":"tokenAmountIn","type":"uint256"}],"stateMutability":"pure","type":"function"},{"inputs":[{"internalType":"uint256","name":"tokenBalanceIn","type":"uint256"},{"internalType":"uint256","name":"tokenWeightIn","type":"uint256"},{"internalType":"uint256","name":"tokenBalanceOut","type":"uint256"},{"internalType":"uint256","name":"tokenWeightOut","type":"uint256"},{"internalType":"uint256","name":"tokenAmountIn","type":"uint256"},{"internalType":"uint256","name":"swapFee","type":"uint256"}],"name":"calcOutGivenIn","outputs":[{"internalType":"uint256","name":"tokenAmountOut","type":"uint256"}],"stateMutability":"pure","type":"function"},{"inputs":[{"internalType":"uint256","name":"tokenBalanceIn","type":"uint256"},{"internalType":"uint256","name":"tokenWeightIn","type":"uint256"},{"internalType":"uint256","name":"tokenBalanceOut","type":"uint256"},{"internalType":"uint256","name":"tokenWeightOut","type":"uint256"},{"internalType":"uint256","name":"swapFee","type":"uint256"}],"name":"calcSpotPrice","outputs":[{"internalType":"uint256","name":"spotPrice","type":"uint256"}],"stateMutability":"pure","type":"function"},{"inputs":[],"name":"factory","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"pool","type":"address"},{"internalType":"uint256[]","name":"prices","type":"uint256[]"}],"name":"order","outputs":[{"components":[{"internalType":"contract IERC20","name":"sellToken","type":"address"},{"internalType":"contract IERC20","name":"buyToken","type":"address"},{"internalType":"address","name":"receiver","type":"address"},{"internalType":"uint256","name":"sellAmount","type":"uint256"},{"internalType":"uint256","name":"buyAmount","type":"uint256"},{"internalType":"uint32","name":"validTo","type":"uint32"},{"internalType":"bytes32","name":"appData","type":"bytes32"},{"internalType":"uint256","name":"feeAmount","type":"uint256"},{"internalType":"bytes32","name":"kind","type":"bytes32"},{"internalType":"bool","name":"partiallyFillable","type":"bool"},{"internalType":"bytes32","name":"sellTokenBalance","type":"bytes32"},{"internalType":"bytes32","name":"buyTokenBalance","type":"bytes32"}],"internalType":"struct GPv2Order.Data","name":"order_","type":"tuple"},{"components":[{"internalType":"address","name":"target","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"bytes","name":"callData","type":"bytes"}],"internalType":"struct GPv2Interaction.Data[]","name":"preInteractions","type":"tuple[]"},{"components":[{"internalType":"address","name":"target","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"bytes","name":"callData","type":"bytes"}],"internalType":"struct GPv2Interaction.Data[]","name":"postInteractions","type":"tuple[]"},{"internalType":"bytes","name":"sig","type":"bytes"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"pool","type":"address"},{"internalType":"address","name":"buyToken","type":"address"},{"internalType":"uint256","name":"buyAmount","type":"uint256"}],"name":"orderFromBuyAmount","outputs":[{"components":[{"internalType":"contract IERC20","name":"sellToken","type":"address"},{"internalType":"contract IERC20","name":"buyToken","type":"address"},{"internalType":"address","name":"receiver","type":"address"},{"internalType":"uint256","name":"sellAmount","type":"uint256"},{"internalType":"uint256","name":"buyAmount","type":"uint256"},{"internalType":"uint32","name":"validTo","type":"uint32"},{"internalType":"bytes32","name":"appData","type":"bytes32"},{"internalType":"uint256","name":"feeAmount","type":"uint256"},{"internalType":"bytes32","name":"kind","type":"bytes32"},{"internalType":"bool","name":"partiallyFillable","type":"bool"},{"internalType":"bytes32","name":"sellTokenBalance","type":"bytes32"},{"internalType":"bytes32","name":"buyTokenBalance","type":"bytes32"}],"internalType":"struct GPv2Order.Data","name":"order_","type":"tuple"},{"components":[{"internalType":"address","name":"target","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"bytes","name":"callData","type":"bytes"}],"internalType":"struct GPv2Interaction.Data[]","name":"preInteractions","type":"tuple[]"},{"components":[{"internalType":"address","name":"target","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"bytes","name":"callData","type":"bytes"}],"internalType":"struct GPv2Interaction.Data[]","name":"postInteractions","type":"tuple[]"},{"internalType":"bytes","name":"sig","type":"bytes"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"pool","type":"address"},{"internalType":"address","name":"sellToken","type":"address"},{"internalType":"uint256","name":"sellAmount","type":"uint256"}],"name":"orderFromSellAmount","outputs":[{"components":[{"internalType":"contract IERC20","name":"sellToken","type":"address"},{"internalType":"contract IERC20","name":"buyToken","type":"address"},{"internalType":"address","name":"receiver","type":"address"},{"internalType":"uint256","name":"sellAmount","type":"uint256"},{"internalType":"uint256","name":"buyAmount","type":"uint256"},{"internalType":"uint32","name":"validTo","type":"uint32"},{"internalType":"bytes32","name":"appData","type":"bytes32"},{"internalType":"uint256","name":"feeAmount","type":"uint256"},{"internalType":"bytes32","name":"kind","type":"bytes32"},{"internalType":"bool","name":"partiallyFillable","type":"bool"},{"internalType":"bytes32","name":"sellTokenBalance","type":"bytes32"},{"internalType":"bytes32","name":"buyTokenBalance","type":"bytes32"}],"internalType":"struct GPv2Order.Data","name":"order_","type":"tuple"},{"components":[{"internalType":"address","name":"target","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"bytes","name":"callData","type":"bytes"}],"internalType":"struct GPv2Interaction.Data[]","name":"preInteractions","type":"tuple[]"},{"components":[{"internalType":"address","name":"target","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"bytes","name":"callData","type":"bytes"}],"internalType":"struct GPv2Interaction.Data[]","name":"postInteractions","type":"tuple[]"},{"internalType":"bytes","name":"sig","type":"bytes"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"pool","type":"address"}],"name":"tokens","outputs":[{"internalType":"address[]","name":"tokens_","type":"address[]"}],"stateMutability":"view","type":"function"}]'
