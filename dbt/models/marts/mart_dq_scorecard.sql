-- Exposes the PySpark-computed DQ scorecard (news_dwh.dq_scorecard) as a
-- mart, matching the "Data Quality Scorecard" dashboard shape from the
-- bootcamp material: one row per run, 6 dimension scores + overall.

select
    run_at,
    data_source,
    table_name,
    completeness,
    accuracy,
    consistency,
    timeliness,
    validity,
    uniqueness,
    overall_score
from {{ source('news_dwh', 'dq_scorecard') }}
