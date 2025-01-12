import pandas as pd
from cow_amm_trade_envy.envy_calculation import (
    TradeEnvyCalculator,
)
from cow_amm_trade_envy.configs import EnvyCalculatorConfig, DataFetcherConfig
from io import StringIO
import os
from dotenv import load_dotenv

from cow_amm_trade_envy.datasources import DataFetcher

load_dotenv()


# first timeframe for tests
config = EnvyCalculatorConfig(network="ethereum")
dfc = DataFetcherConfig(
    "ethereum",
    node_url=os.getenv("NODE_URL"),
    min_block=20842476,
    max_block=20842716,
)
tec = TradeEnvyCalculator(config, dfc)
data_fetcher = DataFetcher(dfc)

# second time frame for tests
config2 = EnvyCalculatorConfig(network="ethereum")
dfc2 = DataFetcherConfig(
    "ethereum",
    node_url=os.getenv("NODE_URL"),
    min_block=20 * 10**6,
    max_block=20 * 10**6 + 100,
)
tec2 = TradeEnvyCalculator(config2, dfc2)
data_fetcher2 = DataFetcher(dfc2)


def get_row_from_string(row_str):
    data_row = pd.read_csv(StringIO(row_str), header=None)

    row_as_dict = {
        "call_tx_hash": data_row[0][0],
        "call_block_number": int(data_row[1][0]),
        "gas_price": int(data_row[2][0]),
        "tokens": data_row[3][0],
        "clearing_prices": data_row[4][0],
        "trades": data_row[5][0],
    }
    return row_as_dict


def test_populate1():
    """
    Not really a test, just a way to populate the database with data
    """
    data_fetcher.populate_settlement_and_price()
    data_fetcher2.populate_settlement_and_price()


def test_calc_envy1():
    data_row_str = '0x9a9d29cf57eec2c3ac8e5cf4e1984ddf30eb9b708af5380c350dd395b60da747,20842716,22512333999,[0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2],[675702271894863831239753728 1802379882825404416 1882610587446793687 1795746521597806075 1882610587446793687 408804721466143689 1095366441],"[{""sellTokenIndex"":3,""buyTokenIndex"":4,""receiver"":""0xba3eff3dd2b4442b6c141c5008260dc18824f2f1"",""sellAmount"":8347127038529613057,""buyAmount"":7959236045049141939,""validTo"":1727451436,""appData"":""0x924f5e36ae70c8cdad505bc4807be76099024df2520e89d14478e42c52084441"",""feeAmount"":0,""flags"":2,""executedAmount"":1882610587446793687,""signature"":""0x42f03167a200d107ece818f3f73ba5d5166698550cfea474a445065149e2eb1971664e5961d90d4bd21130c4c039801b3763aa3ae3a725049e1d3474407e701e1c""} {""sellTokenIndex"":5,""buyTokenIndex"":6,""receiver"":""0x0000000000000000000000000000000000000000"",""sellAmount"":1095366441,""buyAmount"":408422025712910370,""validTo"":1727449487,""appData"":""0x362e5182440b52aa8fffe70a251550fbbcbca424740fe5a14f59bf0c1b06fe1d"",""feeAmount"":0,""flags"":66,""executedAmount"":1095366441,""signature"":""0xf08d4dea369c456d26a3168ff0024b904f2d8b91000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc20000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004149f72900000000000000000000000000000000000000000000000005ab01ab211040220000000000000000000000000000000000000000000000000000000066f6c98f362e5182440b52aa8fffe70a251550fbbcbca424740fe5a14f59bf0c1b06fe1d0000000000000000000000000000000000000000000000000000000000000000f3b277728b3fee749481eb3e0b3b48980dbbab78658fc419025cb16eee34677500000000000000000000000000000000000000000000000000000000000000015a28e9363bb942b639270062aa6bb295f434bcdfc42c97267bf003f272060dc95a28e9363bb942b639270062aa6bb295f434bcdfc42c97267bf003f272060dc9""}]"'

    row_as_series = get_row_from_string(data_row_str)
    result = tec.calc_envy_per_settlement(row_as_series)
    print(result)
    assert result == []


def test_calc_envy2():
    data_row_str = '0x36ade13a244741d6b0de1133ac7a4203a816fb9de6c780767648179547ac25d2,20842704,18884935879,[0x4104b135dbc9609fc1a9490e61369036497660c8 0x4c9edd5852cd905f086c759e8383e09bff1e68b3 0x9d39a5de30e57443bff2a8307a4256c8797a3497 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 0xdac17f958d2ee523a2206206994597c13d831ec7 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48 0xdac17f958d2ee523a2206206994597c13d831ec7 0x4c9edd5852cd905f086c759e8383e09bff1e68b3 0x9d39a5de30e57443bff2a8307a4256c8797a3497 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 0x4104b135dbc9609fc1a9490e61369036497660c8],[3601938712054330286 16810796960059929659 18446744073709551616 16868235400140385204116331308895 44586737895357512086485 16872030776699377561091098976055 32617326123 32640364989 65745999728263775336556 72176200608570553663488 4906726922711791030124 400000000000000000],"[{""sellTokenIndex"":6,""buyTokenIndex"":7,""receiver"":""0x016c6d9d43a6fc4a34409fc40fc49ca74b465303"",""sellAmount"":32640364989,""buyAmount"":32454958994,""validTo"":1727450762,""appData"":""0x71a155fce0334e246225b90bf0938d8b83974fa0eb7b8734389f87c27aa734b9"",""feeAmount"":0,""flags"":0,""executedAmount"":32640364989,""signature"":""0x464af6b3d006515660a20fdef4ad3d666c1b7031117bf3d46a8c92f07d57103419a56e5037501877b1b9c7db933fd8bd1c2d86b019148b2ca482e184b8164c6d1b""} {""sellTokenIndex"":8,""buyTokenIndex"":9,""receiver"":""0x3af4a49c8e2fcaf33fd3389543b80d320fcc9091"",""sellAmount"":500000000000000000000000,""buyAmount"":455373406193078324225865,""validTo"":1727702333,""appData"":""0x5c15afad771ff6aa7f8d884957934b8a3b33b6015db0631f88764ba607e9ee44"",""feeAmount"":0,""flags"":2,""executedAmount"":72176200608570553663488,""signature"":""0x77f866dd3c785cdf885e02bd50b62c64165e09e06003ace92d931d71066b09a5776fcb7a404542aa1ca15d307e8f49118e9df4ee20f818711277b11a3db67c531c""} {""sellTokenIndex"":10,""buyTokenIndex"":11,""receiver"":""0xfad85cfb8ba2288df114d4327cd218d04c7d015c"",""sellAmount"":400000000000000000,""buyAmount"":4881532747332483418091,""validTo"":1727450820,""appData"":""0xa517cb7620afde39bad1ba35d81e31fd9dc181d82d983482c8dcae31b0901cd2"",""feeAmount"":0,""flags"":0,""executedAmount"":400000000000000000,""signature"":""0xf94bae3a864b123c2f20afce62560298302c4c415d1e48eeb15dc5f8d7a2e4316000d5eab1501540b82995c6715f0aa9afd7252c3bd638124e4e011d3e08379f1b""}]"'

    row_as_series = get_row_from_string(data_row_str)
    result = tec.calc_envy_per_settlement(row_as_series)
    assert result == []


def test_calc_envy3():
    data_row_str = '0xb63483e4eb331b1475a80c594d83524a316dc17fa0c1125c4505ce128a369a26,20842479,24742315967,[0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2],[3735232874593773216 9964452107 3735232874593773216 10000000000],"[{""sellTokenIndex"":2,""buyTokenIndex"":3,""receiver"":""0xa0b23e0f09b70828574eb5c0e9ab4d95d929df47"",""sellAmount"":10000000000,""buyAmount"":3734607607620223402,""validTo"":1727448113,""appData"":""0x49ef2624996389aa2969212e43f6700e25469d78c9af4e6d715fd717b4fa5e00"",""feeAmount"":0,""flags"":0,""executedAmount"":10000000000,""signature"":""0x4553dcb388578f8202352ac3e5aed9f64a46ecfd57b799f4eab98d6f4f15353e2825a3e5bca2f9daba0d614a960a08a28e680ba073621867912b15a094f3493f1c""}]"'
    row_as_series = get_row_from_string(data_row_str)
    result = tec.calc_envy_per_settlement(row_as_series)

    len(result) == 1
    result = result[0]
    assert result["trade_envy"] == -2311335713655648 * 1e-18
    assert result["pool"] == "0xf08d4dea369c456d26a3168ff0024b904f2d8b91"


def test_calc_envy_partial1():
    pass
