"""
So that the tests can run, data must be in the database.
This script creates test data for the database.

Additional data will be downloaded and stored in the database as a side-effect
of running the tests, but the dune queries are separate from that.
"""

import os
import sys

from cow_amm_trade_envy.datasources import populate_settlement_and_price_by_blockrange

# populate tests 1
DB_TEST = "data_test1.duckdb"
left, right = 20826055, 20842716
populate_settlement_and_price_by_blockrange("ethereum", left, right)