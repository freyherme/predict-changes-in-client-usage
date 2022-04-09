/*
SUMMARY:
- Combines data from AWS RDS with features engineered downstream to generate full training dataset for modeling
- Transforms data for modeling
- Performs additional data cleaning
- Stores training data as materialized view
*/


DROP MATERIALIZED VIEW IF EXISTS client_health_full_training_data_mv;
CREATE MATERIALIZED VIEW client_health_full_training_data_mv AS
(

SELECT
	aws.start_date,

    ----------------------------
	-- Identification Fields  --
	----------------------------
	aws.sf_an,
	aws.atd_database,


	----------------------------
	--    Target Variable     --
	----------------------------
	aws.hs_did_change,


	--------------------------------
	-- AWS Categorical Predictors --
	--------------------------------
	aws.state,
	aws.csm_name,

	------------------------------
	-- AWS Numeric Predictors   --
	------------------------------
	CASE WHEN aws.hs_band IN ('Red', 'Yellow') THEN 0
		 WHEN aws.hs_band = 'Green'            THEN 1
		 END AS hs_band,
	CASE WHEN aws.hs_1_lag_band IN ('Red', 'Yellow') THEN 0
		 WHEN aws.hs_1_lag_band = 'Green'            THEN 1
		 END AS hs_1_lag_band,
	CASE WHEN aws.hs_4_lag_band IN ('Red', 'Yellow') THEN 0
		 WHEN aws.hs_4_lag_band = 'Green'            THEN 1
		 END AS hs_4_lag_band,
	CASE WHEN aws.hs_5_lag_band IN ('Red', 'Yellow') THEN 0
		 WHEN aws.hs_5_lag_band = 'Green'            THEN 1
		 END AS hs_5_lag_band,
	CASE WHEN aws.hs_6_lag_band IN ('Red', 'Yellow') THEN 0
		 WHEN aws.hs_6_lag_band = 'Green'            THEN 1
		 END AS hs_6_lag_band,
	CASE WHEN aws.hs_7_lag_band IN ('Red', 'Yellow') THEN 0
		 WHEN aws.hs_7_lag_band = 'Green'            THEN 1
		 END AS hs_7_lag_band,
	CASE WHEN aws.hs_1year_lag_band IN ('Red', 'Yellow') THEN 0
		 WHEN aws.hs_1year_lag_band = 'Green'            THEN 1
		 END AS hs_1year_lag_band,
	aws.change_type_order,
	aws.atd_client_health_score,
	aws.atd_users_login_percent,
	aws.atd_students_assessed_percent,
	aws.atd_feature_adoption_score,
	aws.asmt_admin_flex,
	aws.asmt_admin_ib,
	aws.user_created_custom_reports,
	aws.teachers_login_percent,
	aws.asmt_admin_inspect_prebuilt,
	aws.summary_asmt_created,
	aws.tile_layouts_created_modified,
	aws.integration_educlimber,
	aws.integration_fast,
	aws.integration_google_classroom,
	aws.integration_pra,
	aws.ticket_count,
	aws.arr_dna,
	aws.has_ise,
	aws.subscriber_tenure_days,

    -------------------------
	-- Engineered Features --
    -------------------------
	ca.cumulative_common_assessment_count_per_ay,
	sys_admin.sa_tenure_in_days,
	matrix.times_accessed AS matrix_times_accessed,
	matrix.count_of_distinct_users AS matrix_distinct_users,
	site_asmt_ovr.times_accessed AS site_asmt_ovr_times_accessed,
	site_asmt_ovr.count_of_distinct_users AS site_asmt_ovr_distinct_users,
	site_peer_comp.times_accessed AS site_peer_comp_times_accessed,
	site_peer_comp.count_of_distinct_users AS site_peer_comp_distinct_users,
	mltp_asmt_smry.times_accessed AS mltp_asmt_smry_times_accessed,
	mltp_asmt_smry.count_of_distinct_users AS mltp_asmt_smry_distinct_users,
	rsp_freq.times_accessed AS rsp_freq_times_accessed,
	rsp_freq.count_of_distinct_users AS rsp_freq_distinct_users,
	skills_letter.times_accessed AS skills_letter_times_accessed,
	skills_letter.count_of_distinct_users AS skills_letter_distinct_users

FROM aws_talend_training_data aws
LEFT JOIN (
	SELECT DISTINCT ON (sf_an, client, month_start) *
	FROM dna_common_assessments
) ca
		  ON ca.sf_an = aws.sf_an
			  AND ca.client = aws.atd_database
			  AND ca.month_start::date = aws.start_date
LEFT JOIN (
	SELECT DISTINCT ON (sf_an, client, month_start) *
	FROM all_dna_system_admin_tenure
) sys_admin
		  ON sys_admin.sf_an = aws.sf_an
			  AND sys_admin.client = aws.atd_database
			  AND sys_admin.month_start::date = aws.start_date
LEFT JOIN (
	SELECT DISTINCT ON (sf_an, client, month_start) *
	FROM all_dna_prebuilt_report_usage
	WHERE title = 'Assessment Matrix Report'
	ORDER BY month_start
) matrix
		  ON matrix.sf_an = aws.sf_an
			  AND matrix.client = aws.atd_database
			  AND matrix.month_start = aws.start_date
LEFT JOIN (
	SELECT DISTINCT ON (sf_an, client, month_start) *
	FROM all_dna_prebuilt_report_usage
	WHERE title = 'Site Assessment Overview'
	ORDER BY month_start
) site_asmt_ovr
		  ON site_asmt_ovr.sf_an = aws.sf_an
			  AND site_asmt_ovr.client = aws.atd_database
			  AND site_asmt_ovr.month_start = aws.start_date
LEFT JOIN (
	SELECT DISTINCT ON (sf_an, client, month_start) *
	FROM all_dna_prebuilt_report_usage
	WHERE title = 'Assessment Site Peer Comparison'
	ORDER BY month_start
) site_peer_comp
		  ON site_peer_comp.sf_an = aws.sf_an
			  AND site_peer_comp.client = aws.atd_database
			  AND site_peer_comp.month_start = aws.start_date
LEFT JOIN (
	SELECT DISTINCT ON (sf_an, client, month_start) *
	FROM all_dna_prebuilt_report_usage
	WHERE title = 'Multiple Assessment Summary Report'
	ORDER BY month_start
) mltp_asmt_smry
		  ON mltp_asmt_smry.sf_an = aws.sf_an
			  AND mltp_asmt_smry.client = aws.atd_database
			  AND mltp_asmt_smry.month_start = aws.start_date
LEFT JOIN (
	SELECT DISTINCT ON (sf_an, client, month_start) *
	FROM all_dna_prebuilt_report_usage
	WHERE title = 'Assessment Response Frequency'
	ORDER BY month_start
) rsp_freq
		  ON rsp_freq.sf_an = aws.sf_an
			  AND rsp_freq.client = aws.atd_database
			  AND rsp_freq.month_start = aws.start_date
LEFT JOIN (
	SELECT DISTINCT ON (sf_an, client, month_start) *
	FROM all_dna_prebuilt_report_usage
	WHERE title = 'Skills Assessment Parent Letter'
	ORDER BY month_start
) skills_letter
		  ON skills_letter.sf_an = aws.sf_an
			  AND skills_letter.client = aws.atd_database
			  AND skills_letter.month_start = aws.start_date

WHERE aws.atd_database NOT ILIKE '%candidate%'

	)
;