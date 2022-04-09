/*
 SUMMARY:
 Calculate client tenure.
 - Client tenure -> how long has the client had a continuous subscription?
*/

WITH client_tenure AS (

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

)

SELECT * FROM client_tenure

--ORDER BY subscriber_tenure_days DESC

;

