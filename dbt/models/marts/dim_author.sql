select
    author_id,
    author_name,
    total_articles,
    first_published_date,
    last_published_date
from {{ ref('int_author_activity') }}
