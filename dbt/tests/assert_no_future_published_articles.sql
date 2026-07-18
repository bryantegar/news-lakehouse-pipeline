-- Fails if any article claims to be published in the future — a
-- classic "accuracy" dimension violation (SELECT ... WHERE
-- shipped_date < order_date pattern from the DQ material, adapted).

select article_id, published_at
from {{ ref('int_articles_deduped') }}
where published_at > now()
