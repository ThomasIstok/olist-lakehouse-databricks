{#
    Daily FX rates for the Sales Overview report (rate trend chart).
    Grain: one row per calendar day. Filtered with the project vars, the same single definition
    of the analysis period the calendar uses (no hardcoded dates, no join needed).
#}

select
    calendar_date,        -- -> dim_calendar
    brl_to_eur,
    brl_to_czk,
    brl_to_usd
from {{ ref('fx_rate_daily') }}
where calendar_date between '{{ var("analysis_start_date") }}' and '{{ var("analysis_end_date") }}'