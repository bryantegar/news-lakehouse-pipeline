with bounds as (
    select
        min(published_date) as min_date,
        max(published_date) as max_date
    from {{ ref('int_articles_deduped') }}
),

spine as (
    select generate_series(
        (select min_date from bounds),
        (select max_date from bounds),
        interval '1 day'
    )::date as date_day
)

select
    date_day,
    extract(year from date_day)::int as year,
    extract(month from date_day)::int as month,
    extract(day from date_day)::int as day,
    extract(dow from date_day)::int as day_of_week,
    to_char(date_day, 'Day') as day_name
from spine
