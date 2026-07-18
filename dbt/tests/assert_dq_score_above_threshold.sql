-- Fails the run if the latest DQ scorecard overall_score drops below 0.85.
-- This turns the "Cost of Poor Data Quality" slide into an actual CI gate:
-- a bad batch fails `dbt test` instead of silently reaching the marts.

select *
from {{ ref('mart_dq_scorecard') }}
where run_at = (select max(run_at) from {{ ref('mart_dq_scorecard') }})
  and overall_score < 0.85
