-- mart_weather_energy.sql
-- This is the analytical mart that powers my dashboard.
-- Joins daily Johannesburg weather with Ethereum price on date.
-- Adds a rainy day flag which is the core variable in our correlation analysis.
-- Adds a price_change_usd column showing day-over-day ETH movement.
-- This is the table that answers the actual project question asking below,
-- "Does rainy weather in Johannesburg correlate with Ethereum price?"

with weather as (

    select * from {{ ref('stg_weather') }}

),

energy as (

    select * from {{ ref('stg_energy') }}

),

joined as (

    select
        -- Date
        w.date                                              as weather_date,

        -- Weather conditions
        w.city,
        w.temp_celsius,
        w.feels_like_celsius,
        w.humidity_pct,
        w.wind_speed_ms,
        w.weather_condition,
        w.weather_description,
        w.precipitation_mm,

        -- Rainy day flag, this is my core analytical variable
        -- Threshold of 1mm is the meteorological definition of a rainy day
        case
            when w.precipitation_mm >= 1.0 then true
            else false
        end                                                 as rainy_day,

        -- Rainfall intensity category for richer dashboard filtering
        case
            when w.precipitation_mm = 0        then 'Dry'
            when w.precipitation_mm < 1        then 'Trace'
            when w.precipitation_mm < 5        then 'Light Rain'
            when w.precipitation_mm < 20       then 'Moderate Rain'
            else                                    'Heavy Rain'
        end                                                 as rainfall_category,

        -- Ethereum pricing attributes
        e.price_usd                                         as eth_price_usd,
        e.series_name,
        e.source_api,

        -- Day to day price movement
        e.price_usd - lag(e.price_usd)
            over (order by w.date)                          as eth_price_change_usd,

        -- Percentage change
        round(
            (e.price_usd - lag(e.price_usd) over (order by w.date))
            / nullif(lag(e.price_usd) over (order by w.date), 0) * 100,
            2
        )                                                   as eth_price_change_pct

    from weather w
    inner join energy e
        on w.date = e.date

)

select * from joined
order by weather_date desc
