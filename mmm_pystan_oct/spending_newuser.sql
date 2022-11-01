-- Daily PV
SELECT 
    LEFT(convert_timezone('UTC','America/Los_Angeles',PV_TS::timestamp_ntz) , 10) AS date, 
    SUM(case when PV_TS is not null then 1 else 0 end) AS PV
FROM ANALYTIC_DB.DBT_MARTS.NEW_USER_REATTRIBUTION
WHERE to_date(convert_timezone('UTC','America/Los_Angeles',PV_TS::timestamp_ntz)) >='2021-01-03'
AND to_date(convert_timezone('UTC','America/Los_Angeles',PV_TS::timestamp_ntz))!='2022-02-03'
GROUP BY 1

-- DAILY SPENDING 
SELECT 
    LEFT (date_trunc('day',SPEND_DATE_PACIFIC_TIME), 10) AS date,
    network,
    SUM(spend) AS Spend
FROM ANALYTIC_DB.DBT_MARTS.MARKETING_SPEND
WHERE SPEND_DATE_PACIFIC_TIME >='2021-01-03'
    AND to_date(convert_timezone('UTC','America/Los_Angeles',PV_TS::timestamp_ntz))!='2021-03-31'
    AND SPEND_DATE_PACIFIC_TIME !='2022-02-03'
GROUP BY 1, 2
