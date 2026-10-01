{#
    Daily FX rates in wide format. Grain: one row per calendar day.
    Source int_fx_rates__daily is long (day x currency) and already forward-filled for
    weekends/holidays (no look-ahead). Pivoting here lets every fact get all rates
    with ONE join on the order date instead of one join per currency.
    1 BRL = brl_to_xxx units of the target currency.
#}

with rates as (

    select calendar_date, quote_currency, fx_rate
    from {{ ref('int_fx_rates__daily') }}
    where base_currency = 'BRL'

)

select
    calendar_date,
    -- pivot: one column per target currency (max() just picks the single value per day)
    max(case when quote_currency = 'EUR' then fx_rate end)   as brl_to_eur,
    max(case when quote_currency = 'CZK' then fx_rate end)   as brl_to_czk,
    max(case when quote_currency = 'USD' then fx_rate end)   as brl_to_usd
from rates
group by calendar_date