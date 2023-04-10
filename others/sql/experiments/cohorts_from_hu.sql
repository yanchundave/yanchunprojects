-- this script will be used to generate cohorts to upload to amplitude to test
-- status will be as of 2/15/2023
SET DATE_OF_INTEREST = '2023-03-01';
-- SET DATE_OF_INTEREST = '2022-11-01';

-- cohort 1
CREATE OR REPLACE TABLE DBT.DEV_HU_PUBLIC.cb_relaunch_cohort1 AS
SELECT
      DISTINCT n.user_id
  FROM ANALYTIC_DB.DBT_MARTS.NEW_USER_REATTRIBUTION n
  LEFT JOIN overdraft.overdraft_overdraft.account ec ON n.user_id = ec.dave_user_id AND ec.created::date <= $DATE_OF_INTEREST
  WHERE n.PV_TS::date BETWEEN '2022-06-16' AND $DATE_OF_INTEREST -- after EC full launch
  	AND n.first_advance_timestamp IS NULL -- no advacne taken
  	-- AND (n.first_advance_timestamp IS NULL OR n.first_advance_timestamp::date > $DATE_OF_INTEREST) -- no advacne taken
    AND ec.dave_user_id IS NULL -- no EC account
;

-- 1,786,259
SELECT COUNT(*) FROM DBT.DEV_HU_PUBLIC.cb_relaunch_cohort1;


-- upload to amplitude 1,786,045
SELECT * FROM DBT.DEV_HU_PUBLIC.cb_relaunch_cohort1;



-- cohort 2
-- DROP VIEW DBT.DEV_HU_PUBLIC.cb_relaunch_cohort2;
CREATE OR REPLACE TABLE DBT.DEV_HU_PUBLIC.cb_relaunch_cohort2 AS
SELECT
    seg.user_id
    -- seg.user_label,
    -- seg.has_outstanding_advance
FROM dbt.adv_churn_marts.fct_adv_segment seg
INNER JOIN overdraft.overdraft_overdraft.account ec ON seg.user_id = ec.dave_user_id -- has EC account
WHERE seg.user_label IN ('deep dormant user') --, 'light dormant user')
    AND seg.date_of_interest = $DATE_OF_INTEREST
    AND seg.has_outstanding_advance = 0
    AND ec.created::date <= seg.date_of_interest
;

-- 166,752
SELECT COUNT(*) FROM DBT.DEV_HU_PUBLIC.cb_relaunch_cohort2;


SELECT * FROM DBT.DEV_HU_PUBLIC.cb_relaunch_cohort2;



-- cohort 3
-- update on 3/29/2023: not in cohort 2; remove 19,946 users
CREATE OR REPLACE TABLE DBT.DEV_HU_PUBLIC.cb_relaunch_cohort3 AS
WITH cb_opt_in AS (
    SELECT
        user_id,
        DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', client_event_time::timestamp_ntz)) AS enrollment_date_pt
    FROM ANALYTIC_DB.DBT_marts.amplitude_dao
    WHERE
      enrollment_date_pt >= '2022-06-15' -- CB launched on 6/15
      AND session_id <> -1 -- logic from fct_user_sessions: valid session
      AND TRY_TO_NUMBER(user_id) IS NOT NULL -- logic from fct_user_sessions: numeric user_id
      AND event_type in ('credit builder enrollment success screen viewed', 'credit builder enrollment success from legacy home') -- CB enrollment success page
)

SELECT distinct cb_opt_in.user_id
FROM cb_opt_in
LEFT JOIN DBT.DEV_HU_PUBLIC.cb_relaunch_cohort2 c2 ON cb_opt_in.user_id = c2.user_id
WHERE c2.user_id IS NULL -- not in cohort 2
;

-- after 3/29 update: 111,566
-- 131,512
SELECT COUNT(*) FROM DBT.DEV_HU_PUBLIC.cb_relaunch_cohort3;

SELECT * FROM DBT.DEV_HU_PUBLIC.cb_relaunch_cohort3;


-- cohort 4
CREATE OR REPLACE TABLE DBT.DEV_HU_PUBLIC.cb_relaunch_cohort4 AS
WITH cb_opt_in AS
(SELECT
        user_id,
        DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', client_event_time::timestamp_ntz)) AS enrollment_date_pt
    FROM ANALYTIC_DB.DBT_marts.amplitude_dao
    WHERE
      enrollment_date_pt >= '2022-06-15' -- CB launched on 6/15
      AND session_id <> -1 -- logic from fct_user_sessions: valid session
      AND TRY_TO_NUMBER(user_id) IS NOT NULL -- logic from fct_user_sessions: numeric user_id
      AND event_type in ('credit builder enrollment success screen viewed', 'credit builder enrollment success from legacy home') -- CB enrollment success page
)

SELECT
        seg.user_id,
        seg.has_outstanding_advance
FROM dbt.adv_churn_marts.fct_adv_segment seg
INNER JOIN overdraft.overdraft_overdraft.account ec ON seg.user_id = ec.dave_user_id -- has EC account
LEFT JOIN cb_opt_in cb ON seg.user_id = cb.user_id
WHERE seg.user_label NOT IN ('deep dormant user', 'light dormant user') -- active users then
    AND seg.date_of_interest = $DATE_OF_INTEREST
    AND cb.user_id IS NULL -- didn't enroll
    AND seg.has_outstanding_advance = 0 -- no outstanding
    AND ec.created::date <= seg.date_of_interest
;

-- 734,338
SELECT COUNT(*), COUNT(DISTINCT user_id) FROM DBT.DEV_HU_PUBLIC.cb_relaunch_cohort4;

SELECT user_id FROM DBT.DEV_HU_PUBLIC.cb_relaunch_cohort4;


SELECT COUNT(*) AS cnt
FROM DBT.DEV_HU_PUBLIC.cb_relaunch_cohort2 c2
INNER JOIN DBT.DEV_HU_PUBLIC.cb_relaunch_cohort3 c3 ON c2.user_id = c3.user_id
;