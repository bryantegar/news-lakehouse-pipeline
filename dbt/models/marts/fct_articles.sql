select
    a.article_id,
    a.author_id,
    a.published_date as date_day,
    a.category,
    a.content_length,
    a.title
from {{ ref('int_articles_deduped') }} a
