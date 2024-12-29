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



WITH block_range AS (
    SELECT
        MIN(time) AS start_time,
        MAX(time) AS end_time
    FROM
        {{network}}.blocks
    WHERE
        number IN ({{start_block}}, {{end_block}})
),
price_data AS (
    SELECT
        AVG(price) AS price,
        DATE_TRUNC('minute', timestamp) AS minute
    FROM
        prices.minute
    WHERE
        blockchain = '{{network}}'
        AND contract_address = {{contract_address}}
        AND timestamp >= DATE_TRUNC('minute', (SELECT start_time FROM block_range))
        AND timestamp <= DATE_TRUNC('minute', (SELECT end_time FROM block_range))
        AND year >= EXTRACT(YEAR FROM (SELECT start_time FROM block_range))
        AND year <= EXTRACT(YEAR FROM (SELECT end_time FROM block_range))
    GROUP BY
        DATE_TRUNC('minute', timestamp)
),
block_data AS (
    SELECT
        number,
        time,
        DATE_TRUNC('minute', time) AS minute
    FROM
        {{network}}.blocks
    WHERE
        number >= {{start_block}}
        AND number <= {{end_block}}
)
SELECT
    b.number AS block_number,
    p.price AS price
FROM
    block_data AS b
LEFT JOIN
    price_data AS p
ON
    b.minute = p.minute
ORDER BY
    block_number