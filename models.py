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
class Tokens:
    USDC: ClassVar[str] = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".lower()
    WETH: ClassVar[str] = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".lower()

@dataclass(frozen=True)
class BCowPool:
    ADDRESS: str

    def checksum(self) -> str:
        return w3.to_checksum_address(self.ADDRESS)

@dataclass(frozen=True)
class CoWAmmOrderData:
    sellToken: str
    buyToken: str
    receiver: str
    sellAmount: int
    buyAmount: int
    validTo: int
    appData: bytes
    feeAmount: int
    kind: bytes
    partiallyFillable: bool
    signature: bytes
    usdc_amount: int
    weth_amount: int
    usdc_price: int
    weth_price: int

    @staticmethod
    def from_order_response(prices: List[str], resp):
        # TODO: Fix sell token & buy token
        if resp[0].lower() == Tokens.USDC:
            usdc_amount = resp[3]
        elif resp[0].lower() == Tokens.WETH:
            weth_amount = resp[3]
        if resp[1].lower() == Tokens.USDC:
            usdc_amount = resp[4]
        elif resp[1].lower() == Tokens.WETH:
            weth_amount = resp[4]
        return CoWAmmOrderData(
            sellToken=resp[0].lower(),
            buyToken=resp[1].lower(),
            receiver=resp[2],
            sellAmount=resp[3],
            buyAmount=resp[4],
            validTo=resp[5],
            appData=resp[6],
            feeAmount=resp[7],
            kind=resp[8],
            partiallyFillable=resp[9],
            signature=resp[10],
            usdc_amount=usdc_amount,
            weth_amount=weth_amount,
            usdc_price=int(prices[0]),
            weth_price=int(prices[1])
        )


@dataclass(frozen=True)
class UCP:
    prices: Dict[str, int]

    def __getitem__(self, address: str) -> int:
        return self.prices[address]

    @classmethod
    def from_lists(cls, tokens: List[str], prices: List[int]) -> 'UCP':
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
    BUY_TOKEN: str
    SELL_TOKEN: str
    BUY_AMOUNT: int
    SELL_AMOUNT: int
    BUY_PRICE: int
    SELL_PRICE: int
    IS_USDC_WETH: bool
    IS_WETH_USDC: bool

@dataclass(frozen=True)
class SettlementTrades:
    trades: List[Trade]

    @property
    def isUsdcWeth(self):
        return any(trade.IS_USDC_WETH for trade in self.trades)

    @property
    def isWethUsdc(self):
        return any(trade.IS_WETH_USDC for trade in self.trades)

    @classmethod
    def from_lists(cls, tokens: str, prices: str, trades: str) -> 'SettlementTrades':
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

            isUsdcWeth = False
            isWethUsdc = False

            # convention sell_buy
            if buy_token == Tokens.USDC and sell_token == Tokens.WETH:
                isUsdcWeth = False
                isWethUsdc = True
            elif buy_token == Tokens.WETH and sell_token == Tokens.USDC:
                isUsdcWeth = True
                isWethUsdc = False

            trades_processed.append(Trade(
                    BUY_TOKEN = buy_token,
                    SELL_TOKEN = sell_token,
                    BUY_AMOUNT = buy_amount,
                    SELL_AMOUNT = sell_amount,
                    BUY_PRICE = buy_price,
                    SELL_PRICE = sell_price,
                    IS_USDC_WETH = isUsdcWeth,
                    IS_WETH_USDC = isWethUsdc
            ))

        return cls(trades=trades_processed)

    def __iter__(self):
        return iter(self.trades)

