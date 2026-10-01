{#
    Calendar dimension for the Sales Overview report.
    NOT filtered to the analysis period: Power BI time intelligence (YTD, YoY) needs full years.
    The analysis period is filtered in the facts instead.
#}

select
    calendar_date,
    date_key,
    year,
    quarter,
    year_quarter,
    month,
    month_name,
    year_month,
    week_of_year,
    day_of_week,
    day_name,
    is_weekend,
    is_in_analysis_period
from {{ ref('calendar') }}