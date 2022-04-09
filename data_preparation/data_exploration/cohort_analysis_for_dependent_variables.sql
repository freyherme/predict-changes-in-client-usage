/*
COHORT ANALYSIS:
    - Cohort analysis is a technique for identifying & comparing possible features for the modeling stage.
    - Data can be used to generate easy to understand data visualizations demonstrating how specific
      client behaviors and demographics correlate with overall client health.
    - Data visualizations from this data is used routinely used in high-level strategic meetings with product
      managers and business executives.
*/

WITH
	usage_atd_monthly_scrubbed AS (

		SELECT *
		FROM usage_atd_monthly
		WHERE date_range_type = 'month'
		  AND DATE_PART('month', start_date) NOT IN (6, 7, 8, 12)
		  AND sf_an IS NOT NULL

	),

	usage_rollup_scrubbed AS (
		SELECT *
		FROM mv_monthly_usage_rollup
		WHERE DATE_PART('month', start_date) NOT IN (6, 7, 8, 12)
		  AND atd_database IS NOT NULL
		  AND atd_users_count > 0
	),

	zd_rollup AS (
		-- Monthly ZD Ticket Count

		SELECT
			u.sf_an,
			zo.name,
			COUNT(t.ticket_id) AS ticket_count,
			u.start_date

		FROM zd_tickets t
		JOIN zd_orgs zo ON t.org_id = zo.org_id
			-- AND zo.dna
			AND zo.ie_salesforce_id IS NOT NULL
			AND zo.ie_salesforce_id != ''
			AND DATE_TRUNC('month', t.created_at) IN (
				SELECT start_date
				FROM usage_rollup_scrubbed
			)
		RIGHT JOIN usage_rollup_scrubbed u
				   ON zo.ie_salesforce_id = u.sf_an
					   AND u.start_date = DATE_TRUNC('month', t.created_at)
		GROUP BY sf_an, name, u.start_date

	),

	record_count AS (
		SELECT COUNT(*) AS row_count
		FROM usage_rollup_scrubbed
	),

	cr_rate AS (

		WITH
			cr_buckets AS (
				SELECT
					ur.sf_an,
					ur.start_date,
					ur.atd_client_health_score,
					user_created_custom_reports / atd_users_count::NUMERIC AS rate,
					RANK()
					OVER (ORDER BY user_created_custom_reports / atd_users_count::NUMERIC) AS rank,
					FLOOR(RANK()
					OVER (ORDER BY user_created_custom_reports / atd_users_count::NUMERIC DESC) /
					row_count::numeric * 10)  AS bucket
				FROM usage_rollup_scrubbed ur
				JOIN record_count ON TRUE

			)

-- 		SELECT * FROM cr_buckets

		SELECT
			'Custom Reports p/ User' AS cohort,

			-- Calculate rank of cr p/user rate.
			-- Multiply by 10 and divide by client count to create 10 buckets per month if possible
			bucket,
			'[' || MIN(ROUND(rate, 3)) || ', ' || MAX(ROUND(rate, 3)) || ']' AS range,
			AVG(atd_client_health_score) AS avg_hs,
			COUNT(*) AS n

		FROM cr_buckets

		GROUP BY bucket
		ORDER BY bucket DESC

	),

	sa_rate AS (

		WITH
			sa_buckets AS (
				SELECT
					ur.sf_an,
					ur.start_date,
					ur.atd_client_health_score,
					um.summary_asmt_created / atd_users_count::NUMERIC AS rate,
					RANK()
					OVER (ORDER BY um.summary_asmt_created / atd_users_count::NUMERIC) AS rank,
					FLOOR(RANK()
					OVER (ORDER BY um.summary_asmt_created / atd_users_count::NUMERIC DESC) /
					row_count::numeric * 10)  AS bucket
				FROM usage_rollup_scrubbed ur
				JOIN usage_atd_monthly_scrubbed um
					ON ur.start_date = um.start_date
					AND ur.sf_an = um.sf_an
				JOIN record_count ON TRUE

			)

-- 		SELECT * FROM cr_buckets

		SELECT
			'Summary Asmts p/ User' AS cohort,

			-- Calculate rank of cr p/user rate.
			-- Multiply by 10 and divide by client count to create 10 buckets per month if possible
			bucket,
			'[' || MIN(ROUND(rate, 3)) || ', ' || MAX(ROUND(rate, 3)) || ']' AS range,
			AVG(atd_client_health_score) AS avg_hs,
			COUNT(*) AS n

		FROM sa_buckets

		GROUP BY bucket
		ORDER BY bucket DESC

	),

	student_count AS (

		WITH
			student_count_buckets AS (
				SELECT
					ur.sf_an,
					ur.start_date,
					ur.atd_client_health_score,
					atd_student_count AS student_count,
					FLOOR(RANK()
					OVER (ORDER BY atd_student_count DESC) /
					row_count::numeric *10) AS bucket
				FROM usage_rollup_scrubbed ur
				JOIN record_count ON TRUE

			)

		SELECT
			'Student Count' AS cohort,
			bucket,
			'[' || MIN(ROUND(student_count, 0)) || ', ' || MAX(ROUND(student_count, 0)) || ']' AS range,
			AVG(atd_client_health_score) AS avg_hs,
			COUNT(*) AS n

		FROM student_count_buckets

		GROUP BY bucket
		ORDER BY bucket DESC

	),

	zd_tix_rate AS (

		WITH
			zd_buckets AS (
				SELECT
					zd.sf_an,
					zd.start_date,
					ur.atd_client_health_score,
					zd.ticket_count / atd_users_count::NUMERIC AS rate,
					FLOOR(RANK()
					OVER (ORDER BY zd.ticket_count / atd_users_count::NUMERIC DESC) /
					row_count::numeric * 10)  AS bucket
				FROM zd_rollup zd
				JOIN usage_rollup_scrubbed ur
					ON ur.start_date = zd.start_date
					AND ur.sf_an = zd.sf_an
				JOIN record_count ON TRUE

			)

		SELECT
			'ZD Tickets p/ User' AS cohort,

			-- Calculate rank of cr p/user rate.
			-- Multiply by 10 and divide by client count to create 10 buckets per month if possible
			bucket,
			'[' || MIN(ROUND(rate, 3)) || ', ' || MAX(ROUND(rate, 3)) || ']' AS range,
			AVG(atd_client_health_score) AS avg_hs,
			COUNT(*) AS n

		FROM zd_buckets

		GROUP BY bucket
		ORDER BY bucket DESC

	),

	flex_created_rate AS (

		WITH
			flex_created_buckets AS (
				SELECT
					ur.sf_an,
					ur.start_date,
					ur.atd_client_health_score,
					asmt_created_flex / atd_users_count::NUMERIC AS rate,
					RANK()
					OVER (ORDER BY asmt_created_flex / atd_users_count::NUMERIC) AS rank,
					FLOOR(RANK()
					OVER (ORDER BY asmt_created_flex / atd_users_count::NUMERIC DESC) /
					row_count::numeric * 10)  AS bucket
				FROM usage_rollup_scrubbed ur
				JOIN record_count ON TRUE

			)

		SELECT
			'Flex Asmts Created p/ User' AS cohort,
			bucket,
			'[' || MIN(ROUND(rate, 3)) || ', ' || MAX(ROUND(rate, 3)) || ']' AS range,
			AVG(atd_client_health_score) AS avg_hs,
			COUNT(*) AS n

		FROM flex_created_buckets

		GROUP BY bucket
		ORDER BY bucket DESC

	),

	flex_admin_rate AS (

		WITH
			flex_admin_buckets AS (
				SELECT
					ur.sf_an,
					ur.start_date,
					ur.atd_client_health_score,
					asmt_admin_flex / atd_users_count::NUMERIC AS rate,
					RANK()
					OVER (ORDER BY asmt_admin_flex / atd_users_count::NUMERIC) AS rank,
					FLOOR(RANK()
					OVER (ORDER BY asmt_admin_flex / atd_users_count::NUMERIC DESC) /
					row_count::numeric * 10)  AS bucket
				FROM usage_rollup_scrubbed ur
				JOIN record_count ON TRUE

			)

		SELECT
			'Flex Asmts Administered p/ User' AS cohort,
			bucket,
			'[' || MIN(ROUND(rate, 3)) || ', ' || MAX(ROUND(rate, 3)) || ']' AS range,
			AVG(atd_client_health_score) AS avg_hs,
			COUNT(*) AS n

		FROM flex_admin_buckets

		GROUP BY bucket
		ORDER BY bucket DESC

	),
	
	ib_created_rate AS (

		WITH
			ib_created_buckets AS (
				SELECT
					ur.sf_an,
					ur.start_date,
					ur.atd_client_health_score,
					asmt_created_ib / atd_users_count::NUMERIC AS rate,
					RANK()
					OVER (ORDER BY asmt_created_ib / atd_users_count::NUMERIC) AS rank,
					FLOOR(RANK()
					OVER (ORDER BY asmt_created_ib / atd_users_count::NUMERIC DESC) /
					row_count::numeric * 10)  AS bucket
				FROM usage_rollup_scrubbed ur
				JOIN record_count ON TRUE

			)

		SELECT
			'IB Asmts Created p/ User' AS cohort,
			bucket,
			'[' || MIN(ROUND(rate, 3)) || ', ' || MAX(ROUND(rate, 3)) || ']' AS range,
			AVG(atd_client_health_score) AS avg_hs,
			COUNT(*) AS n

		FROM ib_created_buckets

		GROUP BY bucket
		ORDER BY bucket DESC

	),

	ib_admin_rate AS (

		WITH
			ib_admin_buckets AS (
				SELECT
					ur.sf_an,
					ur.start_date,
					ur.atd_client_health_score,
					asmt_admin_ib / atd_users_count::NUMERIC AS rate,
					RANK()
					OVER (ORDER BY asmt_admin_ib / atd_users_count::NUMERIC) AS rank,
					FLOOR(RANK()
					OVER (ORDER BY asmt_admin_ib / atd_users_count::NUMERIC DESC) /
					row_count::numeric * 10)  AS bucket
				FROM usage_rollup_scrubbed ur
				JOIN record_count ON TRUE

			)

		SELECT
			'IB Asmts Administered p/ User' AS cohort,
			bucket,
			'[' || MIN(ROUND(rate, 3)) || ', ' || MAX(ROUND(rate, 3)) || ']' AS range,
			AVG(atd_client_health_score) AS avg_hs,
			COUNT(*) AS n

		FROM ib_admin_buckets

		GROUP BY bucket
		ORDER BY bucket DESC

	),
	
	flex_created_score AS (

		WITH
			flex_created_buckets AS (
				SELECT
					ur.sf_an,
					ur.start_date,
					ur.atd_client_health_score,
					asmt_created_flex_score  AS rate,
					RANK()
					OVER (ORDER BY asmt_created_flex_score ) AS rank,
					FLOOR(RANK()
					OVER (ORDER BY asmt_created_flex_score  DESC) /
					row_count::numeric * 10)  AS bucket
				FROM usage_rollup_scrubbed ur
				JOIN record_count ON TRUE

			)

		SELECT
			'Flex Asmts Created Score' AS cohort,
			bucket,
			'[' || MIN(ROUND(rate, 3)) || ', ' || MAX(ROUND(rate, 3)) || ']' AS range,
			AVG(atd_client_health_score) AS avg_hs,
			COUNT(*) AS n

		FROM flex_created_buckets

		GROUP BY bucket
		ORDER BY bucket DESC

	),

	flex_admin_score AS (

		WITH
			flex_admin_buckets AS (
				SELECT
					ur.sf_an,
					ur.start_date,
					ur.atd_client_health_score,
					asmt_admin_flex_score  AS rate,
					RANK()
					OVER (ORDER BY asmt_admin_flex_score ) AS rank,
					FLOOR(RANK()
					OVER (ORDER BY asmt_admin_flex_score  DESC) /
					row_count::numeric * 10)  AS bucket
				FROM usage_rollup_scrubbed ur
				JOIN record_count ON TRUE

			)

		SELECT
			'Flex Asmts Administered Score' AS cohort,
			bucket,
			'[' || MIN(ROUND(rate, 3)) || ', ' || MAX(ROUND(rate, 3)) || ']' AS range,
			AVG(atd_client_health_score) AS avg_hs,
			COUNT(*) AS n

		FROM flex_admin_buckets

		GROUP BY bucket
		ORDER BY bucket DESC

	),
	
	ib_created_score AS (

		WITH
			ib_created_buckets AS (
				SELECT
					ur.sf_an,
					ur.start_date,
					ur.atd_client_health_score,
					asmt_created_ib_score  AS rate,
					RANK()
					OVER (ORDER BY asmt_created_ib_score ) AS rank,
					FLOOR(RANK()
					OVER (ORDER BY asmt_created_ib_score  DESC) /
					row_count::numeric * 10)  AS bucket
				FROM usage_rollup_scrubbed ur
				JOIN record_count ON TRUE

			)

		SELECT
			'IB Asmts Created Score' AS cohort,
			bucket,
			'[' || MIN(ROUND(rate, 3)) || ', ' || MAX(ROUND(rate, 3)) || ']' AS range,
			AVG(atd_client_health_score) AS avg_hs,
			COUNT(*) AS n

		FROM ib_created_buckets

		GROUP BY bucket
		ORDER BY bucket DESC

	),

	ib_admin_score AS (

		WITH
			ib_admin_buckets AS (
				SELECT
					ur.sf_an,
					ur.start_date,
					ur.atd_client_health_score,
					asmt_admin_ib_score  AS rate,
					RANK()
					OVER (ORDER BY asmt_admin_ib_score ) AS rank,
					FLOOR(RANK()
					OVER (ORDER BY asmt_admin_ib_score  DESC) /
					row_count::numeric * 10)  AS bucket
				FROM usage_rollup_scrubbed ur
				JOIN record_count ON TRUE

			)

		SELECT
			'IB Asmts Administered Score' AS cohort,
			bucket,
			'[' || MIN(ROUND(rate, 3)) || ', ' || MAX(ROUND(rate, 3)) || ']' AS range,
			AVG(atd_client_health_score) AS avg_hs,
			COUNT(*) AS n

		FROM ib_admin_buckets

		GROUP BY bucket
		ORDER BY bucket DESC

	)

SELECT *, SUM(n) OVER (PARTITION BY TRUE) AS total_n
FROM ib_admin_score
UNION ALL
SELECT *, SUM(n) OVER (PARTITION BY TRUE) AS total_n
FROM ib_created_score
UNION ALL 
SELECT *, SUM(n) OVER (PARTITION BY TRUE) AS total_n
FROM flex_admin_score
UNION ALL
SELECT *, SUM(n) OVER (PARTITION BY TRUE) AS total_n
FROM flex_created_score
UNION ALL
SELECT *, SUM(n) OVER (PARTITION BY TRUE) AS total_n
FROM ib_admin_rate
UNION ALL
SELECT *, SUM(n) OVER (PARTITION BY TRUE) AS total_n
FROM ib_created_rate
UNION ALL 
SELECT *, SUM(n) OVER (PARTITION BY TRUE) AS total_n
FROM flex_admin_rate
UNION ALL
SELECT *, SUM(n) OVER (PARTITION BY TRUE) AS total_n
FROM flex_created_rate
UNION ALL
SELECT *, SUM(n) OVER (PARTITION BY TRUE) AS total_n
FROM zd_tix_rate
UNION ALL
SELECT *, SUM(n) OVER (PARTITION BY TRUE) AS total_n
FROM student_count
UNION ALL
SELECT *, SUM(n) OVER (PARTITION BY TRUE) AS total_n
FROM cr_rate
UNION ALL
SELECT *, SUM(n) OVER (PARTITION BY TRUE) AS total_n
FROM sa_rate
