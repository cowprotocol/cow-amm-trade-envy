# Trade Envy Calculation

We define the envy of a trade as the surplus missed out on by not choosing a CoW AMM minus
an estimated transaction cost of 100k gas at the current gas price of the trade.

### Method

The pipeline queries data, calculates envy, and visualizes it in a html file. The process goes as follows:

1. Query all settlements for the given timeframe (`time_start`, `time_end` parameters) and for the given pools (`used_pool_names` parameter)
2. For each settlement get the trades made in it
3. For each pool that the envy is calculated for (given in a parameter), query the prices of the tokens from dune for later envy conversion
4. For each trade, calculate the envy
   -  Query from the helper contract if and with which amount the CoWAMM would want to execute a CoW
   -  Check if the trade aligns with the CoWAMM recommendation, if not there is no envy
   -  In case the helper-recommended CoW cant be filled completely, get the price via another helper (if it hasnt been deployed at that time, just use the same price but a fraction of the amount for the calculation)
   -  Calculate the envy as the difference between the CoW price and the trade price minus the estimated transaction cost 
   -  Convert the envy to ETH using prices from dune (for example a COW-USDC pair trade only allows easy representation of the surplus in USDC or COW, but needs price data to convert to ETH)
5. Query logs from node to find out if the CoWAMM was already used in the trade
6. Visualize the envy and envy of settlements that already use the correct CoWAMM in output/trade_envy_report.hmtl


The code is written so that additional runs will add data on top of the database, so that if
a given blockrange of price or settlement data has already been ingested, no more queries to dune or the node have to be made.
Note that if the minimum time parameter is before the highest currently ingested block, it will be ignored and the highest block will be used as the start block


### Usage

The Makefile serves as a showcase of how to use the commands

To run the pipeline for a week for the USDC-WETH pair:
```bash
uv run src/cow_amm_trade_envy/main.py --used_pool_names "['USDC-WETH']" --time_start '2025-01-04 00:00:00' --time_end '2025-01-11 23:59:59'
```

To use all pools omit the `--used_pool_names` argument
```bash
uv run src/cow_amm_trade_envy/main.py --time_start '2025-01-04 00:00:00' --time_end '2025-01-11 23:59:59'
```

### TODOs

- add more chains (Maybe take the Pools class and adapt the pools for each chain
  - Make node url adaptable
  - Add network to caching key for good measure
- integrate into GH actions

### Notes

- is it safe enough to assume the sender of the settlement is the solver or do I have to check that in the logs? (Settlement)
- Keep an eye on: Point queries of helper calls to postgres could be much slower in production when the DB isnt on the same machine