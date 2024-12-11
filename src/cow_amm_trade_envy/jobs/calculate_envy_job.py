from dagster import job
from cow_amm_trade_envy.assets.assets import (
    dune_settlement_data,
)


@job
def envy_job():
    run_dune_settlement_data = dune_settlement_data()
