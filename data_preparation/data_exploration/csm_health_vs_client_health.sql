----------------------------------
-- CSM Health vs. Client Health --
----------------------------------

WITH
	usage_rollup_scrubbed AS (
		SELECT *
		FROM mv_monthly_usage_rollup
		WHERE DATE_PART('month', start_date) NOT IN (6, 7, 8, 12)
		  AND atd_database IS NOT NULL
	)

SELECT
	start_date,
	atd_database,
	csm_health,
	atd_client_health_score_band

FROM usage_rollup_scrubbed
;