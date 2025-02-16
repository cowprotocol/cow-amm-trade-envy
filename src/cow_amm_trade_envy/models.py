from web3 import Web3
from typing import Dict, List, Tuple
from dataclasses import dataclass

w3 = Web3()


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
    BAL = Token(
        name="BAL",
        address="0xba100000625a3754423978a60c9317c58a424e3d".lower(),
        decimals=18,
    )
    UNI = Token(
        name="UNI",
        address="0x1f9840a85d5af5bf1d1762f925bdaddc4201f984".lower(),
        decimals=18,
    )
    COW = Token(
        name="COW",
        address="0xdef1ca1fb7fbcdc777520aa7f396b4e015f497ab".lower(),
        decimals=18,
    )
    wstETH = Token(
        name="wstETH",
        address="0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0".lower(),
        decimals=18,
    )
    DOG = Token(
        name="DOG",
        address="0xbaac2b4491727d78d2b78815144570b9f2fe8899".lower(),
        decimals=18,
    )

    tokens = [USDC, WETH, BAL, UNI, COW, wstETH, DOG]
    native = WETH


@dataclass(frozen=True)
class BCowPool:
    NAME: str
    ADDRESS: str
    TOKEN0: Token
    TOKEN1: Token
    creation_block: int

    @property
    def checksum_address(self) -> str:
        return w3.to_checksum_address(self.ADDRESS)

    @property
    def first_block_active(self) -> int:
        return self.creation_block + 1

    def __str__(self):
        return self.checksum_address


@dataclass(frozen=True)
class Pools:
    pools = [
        BCowPool(
            "USDC-WETH",
            "0xf08d4dea369c456d26a3168ff0024b904f2d8b91",
            Tokens.USDC,
            Tokens.WETH,
            20476566,
        ),
        BCowPool(
            "BAL-WETH",
            "0xf8f5b88328dff3d19e5f4f11a9700293ac8f638f",
            Tokens.BAL,
            Tokens.WETH,
            20479347,
        ),
        BCowPool(
            "WETH-UNI",
            "0xa81b22966f1841e383e69393175e2cc65f0a8854",
            Tokens.WETH,
            Tokens.UNI,
            21105545,
        ),
        BCowPool(
            "COW-wstETH",
            "0x9bd702e05b9c97e4a4a3e47df1e0fe7a0c26d2f1",
            Tokens.COW,
            Tokens.wstETH,
            20522025,
        ),
        BCowPool(
            "wstETH-DOG",
            "0x9d0e8cdf137976e03ef92ede4c30648d05e25285",
            Tokens.wstETH,
            Tokens.DOG,
            20587725,
        ),
    ]

    def get_pools(self) -> List[BCowPool]:
        return self.pools

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

    def get_pool_lookup(self) -> Dict[Tuple[str, str], BCowPool]:
        return {
            (pool.TOKEN0.address, pool.TOKEN1.address): pool
            for pool in self.get_pools()
        }

    def get_fitting_pool(self, trade: "Trade") -> BCowPool:
        lookup = self.get_pool_lookup()

        if (trade.buyToken.address, trade.sellToken.address) in lookup:
            return lookup[(trade.buyToken.address, trade.sellToken.address)]
        elif (trade.sellToken.address, trade.buyToken.address) in lookup:
            return lookup[(trade.sellToken.address, trade.buyToken.address)]
        else:
            raise ValueError("Trade not supported by any pool")

    def pair_is_supported(self, buy_token: str, sell_token: str) -> bool:
        lookup = self.get_pool_lookup()
        return (buy_token, sell_token) in lookup or (sell_token, buy_token) in lookup

    def __getitem__(self, key: Tuple[str, str]) -> BCowPool:
        lookup = self.get_pool_lookup()
        if key in lookup:
            return lookup[key]
        elif (key[1], key[0]) in lookup:
            return lookup[(key[1], key[0])]
        else:
            raise KeyError("Pool not found")

    def get_name_from_address(self, address: str) -> str:
        for pool in self.get_pools():
            if pool.ADDRESS == address:
                return pool.NAME
        raise ValueError(f"Pool with address {address} not found")


addr_to_token = Pools().get_token_lookup()


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
    def from_lists(cls, tokens: List[Token], prices: List[int], n_trades: int) -> "UCP":
        if len(tokens) != len(prices):
            raise ValueError("Cannot zip different lengths")
        prices = [int(price) for price in prices]
        tokens_prices = dict()

        # this is to make sure that no trade data is included in the UCPs
        tokens_with_prices = list(zip(tokens, prices))[: -2 * n_trades]

        for token, price in tokens_with_prices:
            # The first occurence of a price in list is the clearing price
            # further below is just trade data - not UCPs
            if token not in tokens_prices:
                tokens_prices[token] = price

        # dicts are ordered
        # fix problem that arises from the fact that the coin of the blockchain, called
        # 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee in the price sometimes represents
        # the wrapped native coin.

        # Note: substituting price of wrapped native with the price of the native coin
        # leads to unrealistic envy prices

        # Therefore, we only substitute the wrapped price if it doesnt occur in the UCPs
        # (and only in the trade data)
        coin = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
        if coin in tokens_prices and Tokens.native.address not in tokens_prices:
            tokens_prices[Tokens.native.address] = tokens_prices[coin]

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
        return self.sellToken == pool.TOKEN0 and self.buyToken == pool.TOKEN1

    def isOneToZero(self, pool: BCowPool) -> bool:
        return self.sellToken == pool.TOKEN1 and self.buyToken == pool.TOKEN0


def trades_from_lists(
    tokens: list, prices: list, trades: list, block_num: int
) -> List["Trade"]:
    trades_processed = []
    for trade in trades:
        buy_token = tokens[int(trade["buyTokenIndex"])].lower()
        sell_token = tokens[int(trade["sellTokenIndex"])].lower()
        buy_amount = int(trade["buyAmount"])
        sell_amount = int(trade["sellAmount"])
        buy_price = int(prices[int(trade["buyTokenIndex"])])
        sell_price = int(prices[int(trade["sellTokenIndex"])])

        is_supported = Pools().pair_is_supported(buy_token, sell_token)
        if not is_supported:
            trades_processed.append(None)
            continue

        pool = Pools()[(buy_token, sell_token)]
        if block_num < pool.first_block_active:  # trade before pool creation
            trades_processed.append(None)
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

    return trades_processed
