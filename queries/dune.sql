----------------- settlement query
WITH gas_data AS (
    SELECT hash, gas_price, "from" as solver
    FROM {{network}}.transactions
    WHERE block_number >= {{start_block}}
    AND block_number <= {{end_block}}
)
SELECT GP.*, GD.gas_price, GD.solver
FROM gnosis_protocol_v2_{{network}}.GPv2Settlement_call_settle GP
JOIN gas_data GD ON GP.call_tx_hash = GD.hash
WHERE GP.call_block_number >= {{start_block}}
AND GP.call_block_number <= {{end_block}}


----------------- price query
WITH gas_data AS (
    SELECT hash, gas_price, "from" as solver
    FROM {{network}}.transactions
    WHERE block_number >= {{start_block}}
    AND block_number <= {{end_block}}
)
SELECT GP.contract_address, GP.call_success, GP.call_tx_hash,
       GP.call_trace_address, GP.call_block_time, GP.call_block_number,
       GP.clearingPrices as clearing_prices, GP.interactions, GP.tokens, GP.trades,
       GD.gas_price, GD.solver
FROM gnosis_protocol_v2_{{network}}.GPv2Settlement_call_settle GP
JOIN gas_data GD ON GP.call_tx_hash = GD.hash
WHERE GP.call_block_number >= {{start_block}}
AND GP.call_block_number <= {{end_block}}


----------------- block time
WITH RecentBlock AS (
    SELECT max(number) AS block_number
    FROM {{network}}.blocks
    WHERE time <= TIMESTAMP '{{blocktime}}'
)
SELECT block_number
FROM RecentBlock;