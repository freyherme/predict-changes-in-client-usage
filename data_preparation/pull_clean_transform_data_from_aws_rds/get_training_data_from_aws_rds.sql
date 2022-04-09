/*

SUMMARY:
The purpose of this query is to pull training data from the AWS RDS Postgres data store.

1. Engineer target variable (hs_did_change)
    - hs_did_change is a boolean field indicating if client's health status did change in the 3 month window
2. Pull dependent variables available in AWS RDS
    - Data from numerous sources (Salesforce, client production databases, application logs, etc.) are being stored in
      AWS RDS DB.
    - This training data will be combined with additional features that are engineered downstream to generate the
      full training dataset.

*/

WITH

	client_tenure AS (
        -- Client tenure -> calculates long a client has had a continuous subscription at a a given point in time.
		WITH
			RECURSIVE
			reporting_periods AS (
				SELECT
					d::date AS month_start,
					(d + '1 month'::interval - '1 day'::interval)::date month_end
				FROM GENERATE_SERIES(
						(DATE_TRUNC('month', CURRENT_DATE)::date - INTERVAL '5 years')::date,
						(DATE_TRUNC('month', CURRENT_DATE::date) + INTERVAL '1 month' - INTERVAL '1 day')::date,
						'1 month'::interval
					) AS d
			),

			date_range AS (
				SELECT month_end AS calc_date
				FROM reporting_periods
			),

			earlier_starts AS (
				SELECT
					account_number,
					account_name,
					MIN(start_date) AS start_date,
					ARRAY_AGG(DISTINCT product_name) AS products
				FROM sfdc_subscriptions
				INNER JOIN date_range
						   ON start_date <= calc_date
							   AND (end_date > calc_date OR end_date IS NULL)
				WHERE product_group = 'DnA'
				  AND product_name ILIKE '%DnA%'
				  AND (product_name ILIKE '%Rate%' OR product_name ILIKE '%License%')
				GROUP BY account_number, account_name

				UNION

				SELECT
					s.account_number,
					s.account_name,
					s.start_date,
					ARRAY [product_name] AS products

				FROM sfdc_subscriptions s
				INNER JOIN earlier_starts e
						   ON s.account_number = e.account_number
							   AND s.start_date < e.start_date
							   AND s.end_date >= (e.start_date - 90)
				WHERE product_group = 'DnA'
				  AND product_name ILIKE '%DnA%'
				  AND (product_name ILIKE '%Rate%' OR product_name ILIKE '%License%')
			)

		SELECT
			account_number,
			account_name,
			MIN(start_date) AS earliest_start,
			calc_date AS as_of_date,
			calc_date - MIN(start_date) AS subscriber_tenure_days

		FROM earlier_starts
		CROSS JOIN date_range
		GROUP BY account_number, account_name, calc_date
		ORDER BY account_number

	),

	usage_rollup_scrubbed AS (
		SELECT *
		FROM mv_monthly_usage_rollup
		-- DATA CLEANING:
		-- Removing June, July, August, and December from training data
		--      Since schools are not in session during large portions of these months, client health
		--      calculations are invalid.  The data for these months is noise.
		WHERE DATE_PART('month', start_date) NOT IN (6, 7, 8, 12)
		  AND atd_database IS NOT NULL
	),

	hs_lag AS (

		SELECT
			sf_an,
			start_date,
			-- 1 lag
			MAX(ur.start_date) OVER (
				PARTITION BY sf_an ORDER BY start_date ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
				) hs_1_lag_date,
			MAX(ur.atd_client_health_score_band) OVER (
				PARTITION BY sf_an ORDER BY start_date ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
				) hs_1_lag_band,
			MAX(ur.atd_client_health_score) OVER (
				PARTITION BY sf_an ORDER BY start_date ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
				) hs_1_lag,

			-- 4 lag
			MAX(ur.start_date) OVER (
				PARTITION BY sf_an ORDER BY start_date ROWS BETWEEN 4 PRECEDING AND 4 PRECEDING
				) hs_4_lag_date,
			MAX(ur.atd_client_health_score_band) OVER (
				PARTITION BY sf_an ORDER BY start_date ROWS BETWEEN 4 PRECEDING AND 4 PRECEDING
				) hs_4_lag_band,
			MAX(ur.atd_client_health_score) OVER (
				PARTITION BY sf_an ORDER BY start_date ROWS BETWEEN 4 PRECEDING AND 4 PRECEDING
				) hs_4_lag,

			-- 5 lag
			MAX(ur.start_date) OVER (
				PARTITION BY sf_an ORDER BY start_date ROWS BETWEEN 5 PRECEDING AND 5 PRECEDING
				) hs_5_lag_date,
			MAX(ur.atd_client_health_score_band) OVER (
				PARTITION BY sf_an ORDER BY start_date ROWS BETWEEN 5 PRECEDING AND 5 PRECEDING
				) hs_5_lag_band,
			MAX(ur.atd_client_health_score) OVER (
				PARTITION BY sf_an ORDER BY start_date ROWS BETWEEN 5 PRECEDING AND 5 PRECEDING
				) hs_5_lag,

			-- 6 lag
			MAX(ur.start_date) OVER (
				PARTITION BY sf_an ORDER BY start_date ROWS BETWEEN 6 PRECEDING AND 6 PRECEDING
				) hs_6_lag_date,
			MAX(ur.atd_client_health_score_band) OVER (
				PARTITION BY sf_an ORDER BY start_date ROWS BETWEEN 6 PRECEDING AND 6 PRECEDING
				) hs_6_lag_band,
			MAX(ur.atd_client_health_score) OVER (
				PARTITION BY sf_an ORDER BY start_date ROWS BETWEEN 6 PRECEDING AND 6 PRECEDING
				) hs_6_lag,

			-- 7 lag
			MAX(ur.start_date) OVER (
				PARTITION BY sf_an ORDER BY start_date ROWS BETWEEN 7 PRECEDING AND 7 PRECEDING
				) hs_7_lag_date,
			MAX(ur.atd_client_health_score_band) OVER (
				PARTITION BY sf_an ORDER BY start_date ROWS BETWEEN 7 PRECEDING AND 7 PRECEDING
				) hs_7_lag_band,
			MAX(ur.atd_client_health_score) OVER (
				PARTITION BY sf_an ORDER BY start_date ROWS BETWEEN 7 PRECEDING AND 7 PRECEDING
				) hs_7_lag,

			-- 8 lag (one year)
			MAX(ur.start_date) OVER (
				PARTITION BY sf_an ORDER BY start_date ROWS BETWEEN 8 PRECEDING AND 8 PRECEDING
				) hs_1year_lag_date,
			MAX(ur.atd_client_health_score_band) OVER (
				PARTITION BY sf_an ORDER BY start_date ROWS BETWEEN 8 PRECEDING AND 8 PRECEDING
				) hs_1year_lag_band,
			MAX(ur.atd_client_health_score) OVER (
				PARTITION BY sf_an ORDER BY start_date ROWS BETWEEN 8 PRECEDING AND 8 PRECEDING
				) hs_1year_lag

		FROM usage_rollup_scrubbed ur
		ORDER BY sf_an, start_date
	),

	client_health_3month_window AS (
		SELECT
			hs.sf_an,
			hs.atd_database,
-- 		mr.atd_client_health_score,
			hs.atd_client_health_score_band AS hs_band,
			CASE
				WHEN hs.atd_client_health_score_band IN ('Red', 'Yellow') THEN 1
				WHEN hs.atd_client_health_score_band = 'Green'            THEN 2
				END AS band_number,
			hs.start_date,
			MIN(CASE
				WHEN hs2.atd_client_health_score_band IN ('Red', 'Yellow') THEN 1
				WHEN hs2.atd_client_health_score_band = 'Green'            THEN 2
				END
				) AS interval_min_band_number,
			MAX(CASE
				WHEN hs2.atd_client_health_score_band IN ('Red', 'Yellow') THEN 1
				WHEN hs2.atd_client_health_score_band = 'Green'            THEN 2
				END
				) AS interval_max_band_number

		FROM usage_rollup_scrubbed hs
		LEFT JOIN usage_rollup_scrubbed hs2
				  ON hs2.start_date <= (hs.start_date + '3 months'::interval)
					  AND hs2.start_date > hs.start_date
					  AND hs.atd_database = hs2.atd_database
		GROUP BY
			hs.sf_an,
			hs.atd_database,
			hs.atd_client_health_score,
			hs.atd_client_health_score_band,
			hs.start_date
		ORDER BY atd_database, start_date
	),

	hs_did_change AS (

		SELECT *,
			   CASE WHEN band_number = 2
						THEN (
					   CASE
						   WHEN interval_min_band_number IS NULL       THEN NULL
						   WHEN band_number = interval_min_band_number THEN FALSE
						   ELSE TRUE
						   END
					   )
					ELSE (
						CASE
							WHEN interval_max_band_number IS NULL       THEN NULL
							WHEN band_number = interval_max_band_number THEN FALSE
							ELSE TRUE
							END
						)
					END
				   AS hs_did_change,
			   CASE WHEN band_number = 2
						THEN (
					   CASE
						   WHEN interval_min_band_number IS NULL       THEN NULL
						   WHEN band_number = interval_min_band_number THEN 'Stayed Green'
						   ELSE 'Green to Red/Yellow'
						   END
					   )
					ELSE (
						CASE
							WHEN interval_max_band_number IS NULL       THEN NULL
							WHEN band_number = interval_max_band_number THEN 'Stayed Red/Yellow'
							ELSE 'Red/Yellow to Green'
							END
						)
					END
				   AS hs_change_type
		FROM client_health_3month_window

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

	)


SELECT
	hs_did_change.start_date,
	hs_did_change.sf_an,
	hs_did_change.atd_database,
	hs_did_change.hs_band,
	hs_did_change.band_number,
	hs_did_change.hs_did_change,
	hs_did_change.hs_change_type,
	CASE WHEN hs_change_type = 'Stayed Green'        THEN 4
		 WHEN hs_change_type = 'Red/Yellow to Green' THEN 3
		 WHEN hs_change_type = 'Green to Red/Yellow' THEN 2
		 WHEN hs_change_type = 'Stayed Red/Yellow'   THEN 1
		 END AS change_type_order,
	usage.atd_client_health_score,
	usage.atd_users_login_percent,
	usage.atd_students_assessed_percent,
	usage.atd_feature_adoption_score,
	usage.asmt_admin_flex,
	usage.asmt_admin_ib,
	usage.user_created_custom_reports,
	usage.state,
	usage.csm_name,
	usage2.teachers_login_percent,
	usage2.asmt_admin_inspect_prebuilt,
	usage2.summary_asmt_created,
	usage2.tile_layouts_created_modified,
	usage2.integration_educlimber,
	usage2.integration_fast,
	usage2.integration_google_classroom,
	usage2.integration_pra,
	zd_rollup.ticket_count,
	sfdc.arr_dna,
	sfdc.has_ise,
	client_tenure.subscriber_tenure_days,
	hs_lag.hs_1_lag_band,
	hs_lag.hs_4_lag_band,
	hs_lag.hs_5_lag_band,
	hs_lag.hs_6_lag_band,
	hs_lag.hs_7_lag_band,
	hs_lag.hs_1year_lag_band

FROM hs_did_change
LEFT JOIN usage_rollup_scrubbed usage
		  ON usage.sf_an = hs_did_change.sf_an
			  AND usage.start_date = hs_did_change.start_date
LEFT JOIN usage_atd_monthly usage2
		  ON usage2.sf_an = hs_did_change.sf_an
			  AND usage2.start_date = hs_did_change.start_date
LEFT JOIN zd_rollup
		  ON zd_rollup.sf_an = hs_did_change.sf_an
			  AND zd_rollup.start_date = hs_did_change.start_date
LEFT JOIN sfdc
		  ON sfdc.account_number = hs_did_change.sf_an
LEFT JOIN client_tenure
		  ON client_tenure.account_number = hs_did_change.sf_an
		  AND client_tenure.as_of_date = (
		  	DATE_TRUNC('month', hs_did_change.start_date) +
		  	INTERVAL '1 month'
		  	- INTERVAL '1 day'
		  )::date
LEFT JOIN hs_lag
		  ON hs_lag.sf_an = hs_did_change.sf_an
		  AND hs_lag.start_date = hs_did_change.start_date

ORDER BY subscriber_tenure_days DESC
;


