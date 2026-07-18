-- Intermediate: dedupe (keep latest version per article_id) and
-- derive a couple of business fields used by more than one mart.

with staged as (
    select * from {{ ref('stg_articles') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by article_id order by updated_at desc
        ) as rn
    from staged
)

select
    article_id,
    title,
    content,
    author_id,
    author_name,
    category,
    published_at,
    created_at,
    updated_at,
    date_trunc('day', published_at) as published_date,
    length(content) as content_length
from ranked
where rn = 1
