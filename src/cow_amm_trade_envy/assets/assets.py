from dagster import asset
from cow_amm_trade_envy.render_report import render_report
from cow_amm_trade_envy.envy_calculation import create_envy_data


@asset
def dune_settlement_data():
    pass  # todo


@asset(deps=[dune_settlement_data])
def envy_calculation():
    infile = "data/cow_amm_ucp.csv"  # todo
    outfile = "data/cow_amm_missed_surplus.csv"  # todo
    create_envy_data(infile, outfile)


@asset(deps=[envy_calculation])
def report():
    render_report()
