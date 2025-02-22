# Trade Envy Calculation

We define the envy of a trade as the surplus missed out on by not choosing a CoW AMM minus
an estimated transaction cost of 100k gas at the current gas price of the trade. Envy is denominated in the native token of the chain the trade is made on.

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

The program uses the `seed_min_block_number` to start populating the database. This is the first block data is being ingested for, all the following ingests have to add continuously on top.
Also using for example a `time_start` parameter only sets the most fitting block number as a `seed_min_block_number`. Changing it requires deletion of the database and the parameter is only relevant for the very first ingest because additional ingests query the top block ingested.

The Makefile serves as a showcase of how to use the commands

To run the pipeline for a week for the USDC-WETH pair:
```bash
uv run src/cow_amm_trade_envy/main.py --used_pool_names "['USDC-WETH']" --time_start '2025-01-04 00:00:00' --time_end '2025-01-11 23:59:59'
```

To use all pools omit the `--used_pool_names` argument
```bash
uv run src/cow_amm_trade_envy/main.py --time_start '2025-01-04 00:00:00' --time_end '2025-01-11 23:59:59'
```

To use docker to update the database for Ethereum and Gnosis and upload the data to Dune:
```bash
make update-and-sync
```

### TODOs

- add more chains (Maybe take the Pools class and adapt the pools for each chain
  - Make node url adaptable
  - Add network to caching key for good measure
- integrate into GH actions

### Notes

- Since Dune doesn't ingest prices in real time we have backed off the highest block for 6h worth of blocks. If a higher block is specified for ingestion it will be set to the highest block minus 6h.
- Keep an eye on: Point queries of helper calls to postgres could be much slower in production when the DB isnt on the same machine
- If you run the pipeline on top of existing data in the database, an incremental update will be made
  - When a settlement has been run, its tx_hash will be saved with a dummy value for trade_index envy etc, the same settlement will be skipped in future runs
  - This means that you have to delete the envy table if you want to ingest Pools that you havent ingested before
  - 