from dagster import job
from cow_amm_trade_envy.assets.assets import (
    run_download_dune,
)


@job
def envy_job():
    download_dune = run_download_dune()
