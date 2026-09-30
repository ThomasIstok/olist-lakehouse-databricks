{#
    Product dimension. Grain: one row per product.
    All category logic lives here (single source of truth):
      - product without category         -> unknown
      - category missing in translations -> fixed via seed category_translation_fixes
      - category_group (report level)    -> manual mapping via seed category_groups
#}

with products as (

    select * from {{ ref('stg_olist__products') }}

),

-- source translations first (priority 1), manual fixes only as fallback (priority 2)
translations as (

    select product_category_name, product_category_name_en, 1 as priority
    from {{ ref('stg_olist__category_translation') }}

    union all

    select product_category_name, product_category_name_en, 2 as priority
    from {{ ref('category_translation_fixes') }}

),

-- keep exactly one translation per category: if the source ever adds a category we fixed
-- manually, the source wins and the join cannot duplicate products (no fan-out)
translations_deduplicated as (

    select *
    from translations
    qualify row_number() over (partition by product_category_name order by priority) = 1

),

groups as (

    select * from {{ ref('category_groups') }}

),

categorised as (

    select
        p.*,
        coalesce(p.product_category_name, 'unknown')                             as category_name_pt,
        -- English name; falls back to Portuguese if a translation were still missing
        coalesce(t.product_category_name_en, p.product_category_name, 'unknown') as category_name_en
    from products p
    left join translations_deduplicated t on t.product_category_name = p.product_category_name

)

select
    c.product_id,

    -- category: technical names, readable label and report group
    c.category_name_pt,
    c.category_name_en                                                   as category_name,
    initcap(replace(c.category_name_en, '_', ' '))                       as category_label,   -- bed_bath_table -> Bed Bath Table
    -- NULL here = a category missing in the seed -> caught by a not_null test (then add it to the CSV)
    case
        when c.category_name_en = 'unknown' then 'Unknown'
        else g.category_group
    end                                                                  as category_group,

    -- readable name for visuals: source names are anonymised -> derived label, nothing invented
    concat(initcap(replace(c.category_name_en, '_', ' ')), ' · ', left(c.product_id, 6)) as product_display_name,

    -- physical attributes
    c.product_weight_g,
    c.product_length_cm,
    c.product_height_cm,
    c.product_width_cm,
    c.product_length_cm * c.product_height_cm * c.product_width_cm       as product_volume_cm3,  -- for freight analysis

    -- listing metadata
    c.product_name_length,
    c.product_description_length,
    c.product_photo_count

from categorised c
left join groups g on g.category_name = c.category_name_en