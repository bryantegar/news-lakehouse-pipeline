-- Staging: 1:1 with the raw landing table, light renaming/casting only.
-- No business logic here — that belongs in intermediate/marts.

with source as (
    select * from {{ source('news_raw', 'articles_raw') }}
)

select
    id                  as article_id,
    trim(title)         as title,
    content,
    author_id,
    author_name,
    nullif(category, 'UNKNOWN') as category,
    published_at,
    created_at,
    updated_at,
    deleted_at,
    is_hard_deleted,
    _loaded_at
from source
where is_hard_deleted is not true
