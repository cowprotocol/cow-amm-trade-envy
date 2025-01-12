from dataclasses import dataclass
from typing import Dict, Optional, List
from cow_amm_trade_envy.models import Pools, BCowPool


@dataclass
class EnvyCalculatorConfig:
    network: str
    gas_cost_estimate: int = 100_000


@dataclass
class DataFetcherConfig:
    network: str
    node_url: str
    min_block: int
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
