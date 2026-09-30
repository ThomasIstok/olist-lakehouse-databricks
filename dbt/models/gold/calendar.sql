{#
    Date dimension. Grain: one row per calendar day.
    Range: full calendar years covering all orders (Power BI time intelligence needs whole years),
    derived from the data -> extends automatically when a new year of orders arrives.
#}

with bounds as (

    select
        make_date(year(min(ordered_at)), 1, 1)    as start_date,   -- 1 Jan of the first year with orders
        make_date(year(max(ordered_at)), 12, 31)  as end_date      -- 31 Dec of the last year with orders
    from {{ ref('stg_olist__orders') }}

),

-- one row per day between start and end (days without orders are included on purpose)
spine as (

    select explode(sequence(start_date, end_date, interval 1 day)) as calendar_date
    from bounds

)

select
    calendar_date,
    cast(date_format(calendar_date, 'yyyyMMdd') as int)       as date_key,       -- 20170115: compact key for BI
    year(calendar_date)                                       as year,
    quarter(calendar_date)                                    as quarter,
    concat(year(calendar_date), '-Q', quarter(calendar_date)) as year_quarter,   -- 2017-Q1
    month(calendar_date)                                      as month,
    date_format(calendar_date, 'MMMM')                        as month_name,     -- January
    date_format(calendar_date, 'yyyy-MM')                     as year_month,     -- 2017-01
    weekofyear(calendar_date)                                 as week_of_year,
    weekday(calendar_date) + 1                                as day_of_week,    -- 1 = Monday ... 7 = Sunday (ISO)
    date_format(calendar_date, 'EEEE')                        as day_name,       -- Monday
    weekday(calendar_date) >= 5                               as is_weekend,     -- Saturday / Sunday

    -- reliable data period from vars in dbt_project.yml; marts filter on this flag with WHERE
    calendar_date between '{{ var("analysis_start_date") }}'
                      and '{{ var("analysis_end_date") }}'    as is_in_analysis_period

from spine