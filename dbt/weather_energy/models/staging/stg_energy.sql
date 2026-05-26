-- Standardized Ethereum pricing data for downstream analytics.
-- Produces a daily-grain staging model aligned with weather datasets.

with source as (

    select * from raw_energy

),

cleaned as (

    select
        energy_date         as date,
        series_name         as series_name,
        price_value         as price_usd,
        price_unit          as price_unit,
        source_api          as source_api,
        ingested_at         as ingested_at

    from source

    where series_name = 'Ethereum USD Price'

)

select * from cleaned
