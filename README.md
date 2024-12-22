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

### TODOs


- query more dune data for a larger test
- query price data for underfilled CoWs
- find out why some pools dont allow calling of the helper function
- add more pools
- price conversion for non-eth pools
- more insights in the dashboard
- add more chains (Maybe take the Pools class and adapt the pools for each chain
  - Make node url adaptable
  - Add network to caching key for good measure
- dockerize
- integrate into GH actions
