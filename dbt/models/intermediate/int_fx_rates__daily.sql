{#
    FX rates for EVERY calendar day (weekends and holidays filled).
    Rule: use the last PUBLISHED rate (forward fill). A Saturday order gets Friday's rate -
    never the following Monday's, which did not exist yet at order time (look-ahead bias).
#}

with rates as (

    select rate_date, base_currency, quote_currency, fx_rate
    from {{ ref('stg_frankfurter__rates') }}

),

-- Calendar from the first to the last published date (no hardcoded dates -> grows with new data)
calendar as (

    select explode(sequence(min(rate_date), max(rate_date), interval 1 day)) as calendar_date
    from rates

),

-- Every currency pair x every calendar day
grid as (

    select c.calendar_date, p.base_currency, p.quote_currency
    from calendar c
    cross join (select distinct base_currency, quote_currency from rates) p

),

joined as (

    select g.calendar_date, g.base_currency, g.quote_currency, r.fx_rate, r.rate_date
    from grid g
    left join rates r
        on  r.rate_date      = g.calendar_date
        and r.base_currency  = g.base_currency
        and r.quote_currency = g.quote_currency

)

select
    calendar_date,
    base_currency,
    quote_currency,

    -- forward fill: last non-null rate up to and including this day
    last_value(fx_rate, true) over (
        partition by base_currency, quote_currency
        order by calendar_date
        rows between unbounded preceding and current row
    )                                   as fx_rate,

    -- which publication day the rate comes from (transparency for filled days)
    last_value(rate_date, true) over (
        partition by base_currency, quote_currency
        order by calendar_date
        rows between unbounded preceding and current row
    )                                   as rate_published_date,

    rate_date is null                   as is_filled   -- true for weekends/holidays

from joined