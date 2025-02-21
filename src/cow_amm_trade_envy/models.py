from web3 import Web3
from typing import Dict, List, Tuple, ClassVar
from dataclasses import dataclass

w3 = Web3()


@dataclass(frozen=True)
class Token:
    name: str
    address: str
    decimals: int


@dataclass(frozen=True)
class Tokens:
    tokens: ClassVar[List[Token]]
    native: ClassVar[Token]


@dataclass(frozen=True)
class EthereumTokens(Tokens):
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


class GnosisTokens(Tokens):
    WETH = Token(
        name="WETH",
        address="0x6a023ccd1ff6f2045c3309768ead9e68f978f6e1".lower(),
        decimals=18,
    )
    GNO = Token(
        name="GNO",
        address="0x9c58bacc331c9aa871afd802db6379a98e80cedb".lower(),
        decimals=18,
    )
    sDAI = Token(
        name="sDAI",
        address="0xaf204776c7245bF4147c2612BF6e5972Ee483701".lower(),
        decimals=18,
    )

    wstETH = Token(
        name="wstETH",
        address="0x6C76971f98945AE98dD7d4DFcA8711ebea946eA6".lower(),
        decimals=18,
    )

    SAFE = Token(
        name="SAFE",
        address="0x4d18815D14fe5c3304e87B3FA18318baa5c23820".lower(),
        decimals=18,
    )

    OLAS = Token(
        name="OLAS",
        address="0xcE11e14225575945b8E6Dc0D4F2dD4C570f79d9f".lower(),
        decimals=18,
    )

    COW = Token(
        name="COW",
        address="0x177127622c4A00F3d409B75571e12cB3c8973d3c".lower(),
        decimals=18,
    )

    WxDai = Token(
        name="WxDai",
        address="0xe91d153e0b41518a2ce8dd3d7944fa863463a97d".lower(),
        decimals=18,
    )

    tokens = [WETH, GNO, sDAI, wstETH, SAFE, OLAS, COW]
    native = WxDai


def tokens_factory(network: str) -> Tokens:
    if network == "ethereum":
        return EthereumTokens()
    elif network == "gnosis":
        return GnosisTokens()
    else:
        raise ValueError("Network not supported")


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


class Pools:
    def __init__(self, pools: List[BCowPool]):
        self.pools = pools

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


EthereumPools = Pools(
    [
        BCowPool(
            "USDC-WETH",
            "0xf08d4dea369c456d26a3168ff0024b904f2d8b91",
            EthereumTokens.USDC,
            EthereumTokens.WETH,
            20476566,
        ),
        BCowPool(
            "BAL-WETH",
            "0xf8f5b88328dff3d19e5f4f11a9700293ac8f638f",
            EthereumTokens.BAL,
            EthereumTokens.WETH,
            20479347,
        ),
        BCowPool(
            "WETH-UNI",
            "0xa81b22966f1841e383e69393175e2cc65f0a8854",
            EthereumTokens.WETH,
            EthereumTokens.UNI,
            21105545,
        ),
        BCowPool(
            "COW-wstETH",
            "0x9bd702e05b9c97e4a4a3e47df1e0fe7a0c26d2f1",
            EthereumTokens.COW,
            EthereumTokens.wstETH,
            20522025,
        ),
        BCowPool(
            "wstETH-DOG",
            "0x9d0e8cdf137976e03ef92ede4c30648d05e25285",
            EthereumTokens.wstETH,
            EthereumTokens.DOG,
            20587725,
        ),
    ]
)

GnosisPools = Pools(
    [
        BCowPool(
            "WETH-GNO",
            "0x079d2094e16210c42457438195042898a3cff72d",
            GnosisTokens.WETH,
            GnosisTokens.GNO,
            35382680,
        ),
        BCowPool(
            "wstETH- sDAI",
            "0x5089007dec8e93f891dcb908c9e2af8d9dedb72e",
            GnosisTokens.wstETH,
            GnosisTokens.sDAI,
            35388314,
        ),
        BCowPool(
            "GNO-SAFE",
            "0xad58d2bc841cb8e4f8717cb21e3fb6c95dcbc286",
            GnosisTokens.GNO,
            GnosisTokens.SAFE,
            35388198,
        ),
        BCowPool(
            "GNO-OLAS",
            "0xd7f99b1cda3eecf6b6eaa8a61ed21d061e745400",
            GnosisTokens.GNO,
            GnosisTokens.OLAS,
            35388100,
        ),
        BCowPool(
            "GNO-COW",
            "0x71663f74490673706d7b8860b7d02b7c76160bae",
            GnosisTokens.GNO,
            GnosisTokens.COW,
            35388008,
        ),
    ]
)


def pools_factory(network: str) -> Pools:
    if network == "ethereum":
        return EthereumPools
    elif network == "gnosis":
        return GnosisPools
    else:
        raise ValueError("Network not supported")


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
    def from_order_response(order, network: str):
        addr_to_token = pools_factory(network).get_token_lookup()

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
    def from_lists(
        cls, tokens: List[Token], prices: List[int], n_trades: int, native_address: str
    ) -> "UCP":
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
        if coin in tokens_prices and native_address not in tokens_prices:
            tokens_prices[native_address] = tokens_prices[coin]

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
    tokens: list, prices: list, trades: list, block_num: int, network: str
) -> List["Trade"]:
    addr_to_token = pools_factory(network).get_token_lookup()

    NetworkPools = pools_factory(network)

    trades_processed = []
    for trade in trades:
        buy_token = tokens[int(trade["buyTokenIndex"])].lower()
        sell_token = tokens[int(trade["sellTokenIndex"])].lower()
        buy_amount = int(trade["buyAmount"])
        sell_amount = int(trade["sellAmount"])
        buy_price = int(prices[int(trade["buyTokenIndex"])])
        sell_price = int(prices[int(trade["sellTokenIndex"])])

        is_supported = NetworkPools.pair_is_supported(buy_token, sell_token)
        if not is_supported:
            trades_processed.append(None)
            continue

        pool = NetworkPools[(buy_token, sell_token)]
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
