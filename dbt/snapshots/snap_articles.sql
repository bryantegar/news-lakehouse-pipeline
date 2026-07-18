{#
    SCD Type 2 on articles: kumparan sometimes re-titles or re-categorizes
    a story after publishing. This snapshot keeps the full history instead
    of overwriting it — closing the "Slowly Changing Dimension" gap from
    the DWH Modeling material (int_articles_deduped intentionally only
    keeps the latest version; this is where history lives instead).

    Runs AFTER `dbt run` in the DAG (see news_ingestion_hourly.py) since
    it snapshots stg_articles, a view that `dbt run` has to create first.
#}
{% snapshot snap_articles %}

{{
    config(
        target_schema='news_intermediate',
        unique_key='article_id',
        strategy='check',
        check_cols=['title', 'category'],
    )
}}

select
    article_id,
    title,
    category,
    author_id,
    updated_at
from {{ ref('stg_articles') }}

{% endsnapshot %}
