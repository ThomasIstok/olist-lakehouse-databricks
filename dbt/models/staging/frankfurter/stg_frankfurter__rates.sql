{#
    Staging model for daily FX rates (ECB via Frankfurter API).
    Grain: one row per business day and currency pair. Weekends/holidays are missing by design
    (ECB publishes no rates) - they are filled in int_fx_rates__daily.
    No deduplication needed: bronze is written with MERGE on the same key (uniqueness is tested instead).
#}

with source as (

    select * from {{ source('frankfurter', 'frankfurter_rates') }}

),

renamed as (

    select
        cast(rate_date as date)           as rate_date,
        upper(trim(base_currency))        as base_currency,   -- BRL
        upper(trim(quote_currency))       as quote_currency,  -- EUR, CZK, USD

        -- decimal: rates are multiplied with money amounts (decimal) -> exact result
        cast(rate as decimal(18, 6))      as fx_rate,

        -- audit columns
        _ingested_at,
        _source                           as _source_url,
        _batch_id

    from source

)

select * from renamed