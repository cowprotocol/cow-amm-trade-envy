from web3 import Web3

from typing import ClassVar, Dict, List, Tuple
from dataclasses import dataclass

w3 = Web3()


@dataclass(frozen=True)
class Contracts:
    HELPER_CONTRACT: ClassVar[str] = "0x3FF0041A614A9E6Bf392cbB961C97DA214E9CB31"
    USDC_WETH_POOL: ClassVar[str] = "0xf08d4dea369c456d26a3168ff0024b904f2d8b91"


@dataclass(frozen=True)
class Token:
    name: str
    address: str
    decimals: int


@dataclass(frozen=True)
class Tokens:
    USDC = Token(
        name="USDC",
        address="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".lower(),
        decimals=6,
    )
    WETH = Token(
        name="WETH",
        address="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".lower(),
        decimals=18,
    )


@dataclass(frozen=True)
class BCowPool:
    ADDRESS: str
    TOKEN0: Token
    TOKEN1: Token

    def checksum(self) -> str:
        return w3.to_checksum_address(self.ADDRESS)


usdc_weth = BCowPool(Contracts.USDC_WETH_POOL, Tokens.USDC, Tokens.WETH)


class Pools:
    USDC_WETH = usdc_weth

    def get_pools(self) -> List[BCowPool]:
        return [self.USDC_WETH]

    def get_supported_pools(self) -> Dict[Tuple[str, str], BCowPool]:
        return {
            (pool.TOKEN0.address, pool.TOKEN1.address): pool
            for pool in self.get_pools()
        }

    def get_token_lookup(self) -> Dict[str, Token]:
        pools = self.get_pools()
        tokens = [pool.TOKEN0 for pool in pools] + [pool.TOKEN1 for pool in pools]
        tokens = list(set(tokens))
        return {token.address: token for token in tokens}


addr_to_token = Pools().get_token_lookup()
SUPPORTED_POOLS = Pools().get_supported_pools()


@dataclass(frozen=True)
class CoWAmmOrderData:
    sellToken: Token
    buyToken: Token
    receiver: str
    sellAmount: int
    buyAmount: int
    validTo: int
    appData: bytes
    feeAmount: int
    kind: bytes
    partiallyFillable: bool
    signature: bytes

    @staticmethod
    def from_order_response(order):
        return CoWAmmOrderData(
            sellToken=addr_to_token[order[0].lower()],
            buyToken=addr_to_token[order[1].lower()],
            receiver=order[2],
            sellAmount=order[3],
            buyAmount=order[4],
            validTo=order[5],
            appData=order[6],
            feeAmount=order[7],
            kind=order[8],
            partiallyFillable=order[9],
            signature=order[10],
        )


@dataclass(frozen=True)
class UCP:
    prices: Dict[str, int]

    def __getitem__(self, token: Token) -> int:
        return self.prices[token.address]

    @classmethod
    def from_lists(cls, tokens: List[Token], prices: List[int]) -> "UCP":
        if len(tokens) != len(prices):
            raise ValueError("Cannot zip different lengths")
        prices = [int(price) for price in prices]
        tokens_prices = dict()
        for token, price in zip(tokens, prices):
            # The first occurence of a price in list is the clearing price
            if token not in tokens_prices:
                tokens_prices[token] = price
        return cls(prices=tokens_prices)


@dataclass(frozen=True)
class Trade:
    buyToken: Token
    sellToken: Token
    buyAmount: int
    sellAmount: int
    buyPrice: int
    sellPrice: int

    def isZeroToOne(self, pool: BCowPool) -> bool:
        return self.buyToken == pool.TOKEN1 and self.sellToken == pool.TOKEN0

    def isOneToZero(self, pool: BCowPool) -> bool:
        return self.buyToken == pool.TOKEN0 and self.sellToken == pool.TOKEN1


@dataclass(frozen=True)
class SettlementTrades:
    trades: List[Trade]

    def __getitem__(self, index):
        return self.trades[index]

    @classmethod
    def eligible_trades_from_lists(
        cls, tokens: list, prices: list, trades: list
    ) -> "SettlementTrades":
        trades_processed = []
        for trade in trades:
            buy_token = tokens[int(trade["buyTokenIndex"])].lower()
            sell_token = tokens[int(trade["sellTokenIndex"])].lower()
            buy_amount = int(trade["buyAmount"])
            sell_amount = int(trade["sellAmount"])
            buy_price = int(prices[int(trade["buyTokenIndex"])])
            sell_price = int(prices[int(trade["sellTokenIndex"])])

            is_supported = (buy_token, sell_token) in SUPPORTED_POOLS or (
                sell_token,
                buy_token,
            ) in SUPPORTED_POOLS
            if not is_supported:
                continue

            trades_processed.append(
                Trade(
                    buyToken=addr_to_token[buy_token],
                    sellToken=addr_to_token[sell_token],
                    buyAmount=buy_amount,
                    sellAmount=sell_amount,
                    buyPrice=buy_price,
                    sellPrice=sell_price,
                )
            )

        return cls(trades=trades_processed)

    def __iter__(self):
        return iter(self.trades)
