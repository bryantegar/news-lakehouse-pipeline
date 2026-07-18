-- Intermediate: per-author rollup, reused by dim_author and by
-- any future "top authors" mart without recomputing the aggregation.

select
    author_id,
    max(author_name) as author_name,  -- author_name can be null on some rows; take any non-null
    count(*) as total_articles,
    min(published_date) as first_published_date,
    max(published_date) as last_published_date
from {{ ref('int_articles_deduped') }}
group by author_id
