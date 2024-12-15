# Trade Envy Calculation

We define the envy of a trade as the surplus missed out on by not choosing a CoW AMM minus
an estimated transaction cost of 100k gas at the current gas price of the trade.

### Usage

To calculate the envy over all the settlements in the input file:
```bash
make calc
```

To generate the report in the outputs directory:
```bash
make report-render
```


To have dagster run (for subsequent inspection of the assets):
```bash
make dagster-run
```

### Notes
1. Note that currently for the test set the envy is still zero

2. I don't understand yet why for this settlement https://etherscan.io/tx/0x9a9d29cf57eec2c3ac8e5cf4e1984ddf30eb9b708af5380c350dd395b60da747#eventlog (the first one in the dune data csv) 
the price in the price vector of this USDC-ETH trade is (USDC, ETH) [1802379882825404416 675702271894863831239753728], the price in the helper-recommendation is [2840561070 1062484954236092448]
but it used a CoW AMM in the background with price vector [1095366441 408804721466143689].
The first price is better than the second, the second price is better than the third. Since the trade seems to be fully covered by a CoW AMM, I dont understand yet why the UCP is better than the CoW AMM prices.
