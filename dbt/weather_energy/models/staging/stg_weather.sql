-- stg_weather.sql
-- Cleaning and standardises raw weather data.
-- Renames columns consistently, and we I have gone with snake_case.
-- Casts types explicitly so downstream models can trust the schema.
-- One row per city per day which is the same grain as the source table.

with source as (

    select * from raw_weather

),

cleaned as (

    select
        weather_date                                    as date,
        city                                            as city,
        country                                         as country,
        temp_celsius                                    as temp_celsius,
        feels_like                                      as feels_like_celsius,
        humidity_pct                                    as humidity_pct,
        wind_speed_ms                                   as wind_speed_ms,
        weather_main                                    as weather_condition,
        weather_desc                                    as weather_description,
        coalesce(precipitation_mm, 0)                   as precipitation_mm,
        ingested_at                                     as ingested_at

    from source

)

select * from cleaned
