import json
from web3 import Web3

from typing import ClassVar, Dict, List
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


USDC = Token(name="USDC", address="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".lower(),
             decimals=6)
WETH = Token(name="WETH", address="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".lower(),
             decimals=18)

class Tokens:
    USDC = USDC
    WETH = WETH

supported_pools = {
    (WETH.address, USDC.address): Contracts.USDC_WETH_POOL
}

def token_factory(address: str) -> Token:
    name_map = {
        "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".lower(): "USDC",
        "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".lower(): "WETH"
    }

    decimal_map = {
        "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".lower(): 6,
        "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".lower(): 18
    }

    return Token(name=name_map[address], address=address, decimals=decimal_map[address])


@dataclass(frozen=True)
class BCowPool:
    ADDRESS: str

    def checksum(self) -> str:
        return w3.to_checksum_address(self.ADDRESS)


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
    def from_order_response(prices: List[int], order):
        print(prices)
        print(order)

        return CoWAmmOrderData(
            sellToken=token_factory(address=order[0].lower()),
            buyToken=token_factory(address=order[1].lower()),
            receiver=order[2],
            sellAmount=order[3],
            buyAmount=order[4],
            validTo=order[5],
            appData=order[6],
            feeAmount=order[7],
            kind=order[8],
            partiallyFillable=order[9],
            signature=order[10]
        )


@dataclass(frozen=True)
class UCP:

    prices: Dict[str, int]

    def __getitem__(self, token: Token) -> int:
        return self.prices[token.address]

    @classmethod
    def from_lists(cls, tokens: List[Token], prices: List[int]) -> 'UCP':
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
    BUY_TOKEN: Token
    SELL_TOKEN: Token
    BUY_AMOUNT: int
    SELL_AMOUNT: int
    BUY_PRICE: int
    SELL_PRICE: int

    def get_limit_price(self):  # todo continue
        price = 10 ** (
                    self.BUY_TOKEN.decimals - self.SELL_TOKEN.decimals) * self.BUY_PRICE / self.SELL_PRICE
        return price


@dataclass(frozen=True)
class SettlementTrades:

    trades: List[Trade]

    @property
    def isWethUsdc(self) -> bool:
        return any((trade.SELL_TOKEN, trade.BUY_TOKEN) == (WETH, USDC) for trade in self.trades)

    @property
    def isUsdcWeth(self) -> bool:
        return any((trade.SELL_TOKEN, trade.BUY_TOKEN) == (USDC, WETH) for trade in self.trades)

    @classmethod
    def from_lists(cls, tokens: str, prices: str, trades: str) -> 'Trades':
        trades = json.loads(trades.replace(' ', ','))
        tokens = tokens.strip('[]').split()
        prices = prices.strip('[]').split()
        trades_processed = []
        for trade in trades:
            buy_token = tokens[int(trade['buyTokenIndex'])].lower()
            sell_token = tokens[int(trade['sellTokenIndex'])].lower()
            buy_amount = int(trade['buyAmount'])
            sell_amount = int(trade['sellAmount'])
            buy_price = int(prices[int(trade['buyTokenIndex'])])
            sell_price = int(prices[int(trade['sellTokenIndex'])])

            is_supported = (buy_token, sell_token) in supported_pools or (sell_token, buy_token) in supported_pools
            if not is_supported:
                continue

            trades_processed.append(Trade(
                BUY_TOKEN=token_factory(address=buy_token),
                SELL_TOKEN=token_factory(address=sell_token),
                BUY_AMOUNT=buy_amount,
                SELL_AMOUNT=sell_amount,
                BUY_PRICE=buy_price,
                SELL_PRICE=sell_price
            ))

        return cls(trades=trades_processed)

    def __iter__(self):
        return iter(self.trades)

