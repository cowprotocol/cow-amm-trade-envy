from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class EnvyCalculatorConfig:
    network: str
    db_file: str
    gas_cost_estimate: int = 100_000


@dataclass
class DataFetcherConfig:
    network: str
    db_file: str
    node_url: str
    min_block: int
    dune_query_settle: int = 4448838
    dune_query_price: int = 4468197
    interval_length_settle: int = 10_000
    interval_length_price: int = 100_000
    backoff_blocks: Dict[str, int] = None
    max_block: Optional[int] = None

    def __post_init__(self):
        if self.backoff_blocks is None:
            self.backoff_blocks = {"ethereum": 1800}

        if self.network not in self.backoff_blocks:
            raise ValueError(f"Network {self.network} not supported")
