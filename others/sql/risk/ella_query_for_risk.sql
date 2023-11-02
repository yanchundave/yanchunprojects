WITH EARLIEST_ACH_RETURN AS (
SELECT
  ba.DAVE_USER_ID,
  ba.CREATED AS BOD_CREATED_DATE,
  act.id AS ACH_ID,
  at.NAME as Account_Type,
  act.AMOUNT as ACH_Return_Amount,
  act.CREATED as ACH_Transfer_Initiated,
  act.COMPLETED_AT as ACH_Transfer_Completed_At,
  act.RETURNED_AT as ACH_Transfer_Returned_At,
  act.RETURN_CODE as ACH_Transfer_Return_Code,
  i.DISPLAY_NAME as External_Bank_Account,
  CASE
    WHEN act.RETURN_CODE = 'R01' THEN 'INSUFFICIENT FUNDS'
    WHEN act.RETURN_CODE = 'R02' THEN 'ACCOUNT CLOSED'
    WHEN act.RETURN_CODE = 'R03' THEN 'NO ACCOUNT/UNABLE TO LOCATE ACCOUNT'
    WHEN act.RETURN_CODE = 'R04' THEN 'INVALID ACCOUNT NUMBER'
    WHEN act.RETURN_CODE = 'R05' THEN 'Unauthorized Consumer Debit using Corporate SEC Code'
    WHEN act.RETURN_CODE = 'R06' THEN 'RETURNED PER ODFIs REQUEST'
    WHEN act.RETURN_CODE = 'R07' THEN 'AUTHORIZATION REVOKED BY CUSTOMER'
    WHEN act.RETURN_CODE = 'R08' THEN 'PAYMENT STOPPED'
    WHEN act.RETURN_CODE = 'R09' THEN 'UNCOLLECTED FUNDS'
    WHEN act.RETURN_CODE = 'R10' THEN 'CUSTOMER ADVISES NOT AUTHORIZED'
    WHEN act.RETURN_CODE = 'R11' THEN 'CHECK SAFEKEEPING ENTRY RETURN'
    WHEN act.RETURN_CODE = 'R15' THEN 'BENEFICIARY DECEASED'
    WHEN act.RETURN_CODE = 'R16' THEN 'ACCOUNT FROZEN'
    WHEN act.RETURN_CODE = 'R17' THEN 'FILE RECORD EDIT CRITERIA'
    WHEN act.RETURN_CODE = 'R20' THEN 'NON-TRANSACTION ACCOUNT'
    WHEN act.RETURN_CODE = 'R29' THEN 'CORPORATE CUSTOMER ADVISES NOT AUTHORIZED'
    ELSE NULL END AS "ACH Return Reason"
FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_BANK_BANKING.ACH_TRANSFER act
LEFT JOIN APPLICATION_DB.GOOGLE_CLOUD_MYSQL_BANK_BANKING.ACH_ACCOUNT aca on aca.id = act.ACH_ACCOUNT_ID
LEFT JOIN APPLICATION_DB.GOOGLE_CLOUD_MYSQL_BANK_BANKING.ACCOUNT ba on ba.ID = aca.ACCOUNT_ID
LEFT JOIN APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.BANK_ACCOUNT eba ON aca.EXTERNAL_BANK_ACCOUNT_ID = eba.ID
LEFT JOIN APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.INSTITUTION i ON i.id = eba.institution_id
LEFT JOIN APPLICATION_DB.GOOGLE_CLOUD_MYSQL_BANK_BANKING.ACCOUNT_TYPE at on at.id = ba.ACCOUNT_TYPE_ID
LEFT JOIN APPLICATION_DB.GOOGLE_CLOUD_MYSQL_BANK_BANKING.ACCOUNT_STATUS st on st.id = ba.ACCOUNT_STATUS_ID
WHERE (RETURNED_AT IS NOT NULL OR RETURN_CODE IS NOT NULL)
  AND amount > 100
  and to_date(Act.created) >= '2023-01-01'
  and "ACH Return Reason" = 'CUSTOMER ADVISES NOT AUTHORIZED'
  and Account_Type <> 'Extra Cash Account'
  QUALIFY ROW_NUMBER () OVER (PARTITION BY DAVE_USER_ID ORDER BY ACH_Transfer_Initiated) = 1
  )

  , EC_CHARGEBACK AS
(SELECT DISTINCT original_transaction_id,
        ORIGINAL_CREATION_DATE,
        EXCEPTION_DATE,
        DAVE_USER_ID,
        l.DAVEUSERID,
        overdraft_ID,
       EXCEPTION_SETTLED_AMOUNT,
       ORIGINAL_SETTLED_AMOUNT,
       DAVE_INTERNAL_CB_STATUS,
       CASE WHEN EXCEPTION_CODE IN ('10.1','10.2','10.3','10.4', '10.5') THEN 'VISA Fraud'
		        WHEN EXCEPTION_CODE IN ('4871', '4870', '4863', '4849', '4837','4840', '4863', '4868' , '4540') THEN 'Mastercard Fraud'
		        WHEN EXCEPTION_CODE IN ('00' , '17','40', '60', 'S5') THEN 'STAR Fraud'
		        WHEN EXCEPTION_CODE IN ('57' , '0.0','56') THEN 'Accel Fraud'
		        WHEN EXCEPTION_CODE IN ('UA01' , 'UA02','UA05', 'UA06') THEN 'Discover Fraud'
		        WHEN EXCEPTION_CODE IN ('F10' , 'F14','F24', 'F29', 'F30', 'F31') THEN 'Amex Fraud'
		        ELSE 'No Fraud'
		   END as Fraud_type
FROM ANALYTIC_DB.DBT_MARTS.CHARGEBACK_TRANSACTIONS  c
JOIN DAVE.LOOMIS.TRANSACTION l ON c.ORIGINAL_TRANSACTION_ID = l.EXTERNALID
join OVERDRAFT.OVERDRAFT_OVERDRAFT.SETTLEMENT s on l.REFERENCEID = s.id
JOIN OVERDRAFT.OVERDRAFT_OVERDRAFT.OVERDRAFT o on s.OVERDRAFT_ID = o.id
JOIN OVERDRAFT.OVERDRAFT_OVERDRAFT.ACCOUNT A ON A.ID = o.Account_id
and MID IN (0006, 0008)
and EXCEPTION_CODE in ('10.1','10.2','10.3','10.4', '10.5',
'4871', '4870', '4863', '4849', '4837','4840', '4863', '4868' , '4540',
'00' , '17','40', '60', 'S5',
'57' , '0.0','56',
'UA01' , 'UA02','UA05', 'UA06',
'F10' , 'F14','F24', 'F29', 'F30', 'F31'
)
)

, EC_CHARGEBACK_CNT AS (
SELECT DAVE_USER_ID, COUNT(*) AS EC_FRAUD_CB_CNT
FROM EC_CHARGEBACK
GROUP BY 1
)

, Mapped_Dispute AS (
SELECT
DISPUTE_CASE,
DISPUTE_ID,
XID,
BAL_ID,
CH_NOTICE_DATE,
LTR_DATE,
ERROR_DESCRIPTION,
TRANSACTION_TYPE,
FOLLOWUP_DATE,
REG_E,
MERCHANT_NAME,
SETTLE_DATE,
DISPUTE_AMOUNT,
PC_DATE,
PC_AMT,
PC_LETTER_DATE,
PC_REVERSAL_DATE,
CH_LIABILITY_AMT,
RESOLUTION,
DATE_COMPLETE,
CURRENT_STATUS,
MCC,
CB_DATE,
CB_AMT,
CB_REASON_CODE,
SEC_PRESENTMENT_DATE,
PRE_ARB_DATE,
ENDING_STATUS,
FINAL_STATUS_DATE,
ASSOCIATION,
AUTHORIZATION_CODE,
AUTH_DATE
FROM RISK.RISK_PROD.TRANSACTION_DISPUTE OLD
LEFT JOIN RISK.RISK_DEV.DISPUTE_ID_MAPPING R ON R.OLD_CBD_ID = OLD.DISPUTE_ID
WHERE R.OLD_CBD_ID IS NULL
--AND PRN = '269105657853'

UNION

SELECT
CASE WHEN NEW.DISPUTE_CASE IS NOT NULL THEN NEW.DISPUTE_CASE END AS DISPUTE_CASE,
CASE WHEN NEW.DISPUTE_ID IS NOT NULL THEN NEW.DISPUTE_ID END AS DISPUTE_ID,
CASE WHEN NEW.XID IS NOT NULL THEN NEW.XID END AS XID,
CASE WHEN NEW.BAL_ID IS NOT NULL THEN NEW.BAL_ID END AS BAL_ID,
CASE WHEN NEW.CH_NOTICE_DATE IS NOT NULL THEN NEW.CH_NOTICE_DATE END AS CH_NOTICE_DATE,
CASE WHEN NEW.LTR_DATE IS NOT NULL THEN NEW.LTR_DATE END AS LTR_DATE,
CASE WHEN NEW.ERROR_DESCRIPTION IS NOT NULL THEN NEW.ERROR_DESCRIPTION END AS ERROR_DESCRIPTION,
CASE WHEN NEW.TRANSACTION_TYPE IS NOT NULL THEN NEW.TRANSACTION_TYPE END AS TRANSACTION_TYPE,
CASE WHEN NEW.FOLLOWUP_DATE IS NOT NULL THEN NEW.FOLLOWUP_DATE END AS FOLLOWUP_DATE,
CASE WHEN NEW.REG_E IS NOT NULL THEN NEW.REG_E END AS REG_E,
CASE WHEN NEW.MERCHANT_NAME IS NOT NULL THEN NEW.MERCHANT_NAME END AS MERCHANT_NAME,
CASE WHEN NEW.SETTLE_DATE IS NOT NULL THEN NEW.SETTLE_DATE END AS SETTLE_DATE,
CASE WHEN NEW.DISPUTE_AMOUNT IS NOT NULL THEN NEW.DISPUTE_AMOUNT END AS DISPUTE_AMOUNT,
CASE WHEN NEW.PC_DATE IS NOT NULL THEN NEW.PC_DATE END AS PC_DATE,
CASE WHEN NEW.PC_AMT IS NOT NULL THEN NEW.PC_AMT END AS PC_AMT,
CASE WHEN NEW.PC_LETTER_DATE IS NOT NULL THEN NEW.PC_LETTER_DATE END AS PC_LETTER_DATE,
CASE WHEN NEW.PC_REVERSAL_DATE IS NOT NULL THEN NEW.PC_REVERSAL_DATE END AS PC_REVERSAL_DATE,
CASE WHEN NEW.CH_LIABILITY_AMT IS NOT NULL THEN NEW.CH_LIABILITY_AMT END AS CH_LIABILITY_AMT,
CASE WHEN NEW.RESOLUTION IS NOT NULL THEN NEW.RESOLUTION END AS RESOLUTION,
CASE WHEN NEW.DATE_COMPLETE IS NOT NULL THEN NEW.DATE_COMPLETE END AS DATE_COMPLETE,
CASE WHEN NEW.CURRENT_STATUS IS NOT NULL THEN NEW.CURRENT_STATUS END AS CURRENT_STATUS,
CASE WHEN NEW.MCC IS NOT NULL THEN NEW.MCC END AS MCC,
CASE WHEN NEW.CB_DATE IS NOT NULL THEN NEW.CB_DATE END AS CB_DATE,
CASE WHEN NEW.CB_AMT IS NOT NULL THEN NEW.CB_AMT END AS CB_AMT,
CASE WHEN NEW.CB_REASON_CODE IS NOT NULL THEN NEW.CB_REASON_CODE END AS CB_REASON_CODE,
CASE WHEN NEW.SEC_PRESENTMENT_DATE IS NOT NULL THEN NEW.SEC_PRESENTMENT_DATE END AS SEC_PRESENTMENT_DAET,
CASE WHEN NEW.PRE_ARB_DATE IS NOT NULL THEN NEW.PRE_ARB_DATE END AS PRE_ARB_DATE,
CASE WHEN NEW.ENDING_STATUS IS NOT NULL THEN NEW.ENDING_STATUS END AS ENDING_STATUS,
CASE WHEN NEW.FINAL_STATUS_DATE IS NOT NULL THEN NEW.FINAL_STATUS_DATE END AS FINAL_STATUS_DATE,
CASE WHEN NEW.ASSOCIATION IS NOT NULL THEN NEW.ASSOCIATION END AS ASSOCIATION,
CASE WHEN NEW.AUTHORIZATION_CODE IS NOT NULL THEN NEW.AUTHORIZATION_CODE END AS AUTHORIZATION_CODE,
CASE WHEN NEW.AUTH_DATE IS NOT NULL THEN NEW.AUTH_DATE END AS AUTH_DATE
FROM RISK.RISK_PROD.TRANSACTION_DISPUTE OLD
JOIN RISK.RISK_DEV.DISPUTE_ID_MAPPING R ON R.OLD_CBD_ID = OLD.DISPUTE_ID
JOIN RISK.RISK_PROD.TRANSACTION_DISPUTE NEW ON R.NEW_CBD_ID = NEW.DISPUTE_ID
)

, dispute_dr as (
 SELECT t.id, DAVE_USER_ID, T.description, MERCHANT_NAME, TD.DISPUTE_AMOUNT, FINAL_STATUS_DATE,
 CASE WHEN TD.MCC IS NULL THEN T.MERCHANT_CATEGORY_CODE ELSE TD.MCC END AS DISPUTE_MCC, ENDING_STATUS
  FROM Mapped_Dispute td
  JOIN APPLICATION_DB.GOOGLE_CLOUD_MYSQL_BANK_BANKING.TRANSACTION T on t.external_id = td.AUTHORIZATION_CODE
  JOIN APPLICATION_DB.GOOGLE_CLOUD_MYSQL_BANK_BANKING.PAYMENT_REFERENCE P ON P.ID = T.PAYMENT_REFERENCE_ID AND P.EXTERNAL_ID = TD.XID
  JOIN APPLICATION_DB.GOOGLE_CLOUD_MYSQL_BANK_BANKING.ACCOUNT A ON A.ID = P.ACCOUNT_ID
  JOIN APPLICATION_DB.GOOGLE_CLOUD_MYSQL_BANK_BANKING.ACCOUNT_TYPE acct_type ON a.account_type_id = acct_type.id
)

, dispute_dr_user as (
  SELECT DAVE_USER_ID, COUNT(DISTINCT ID) AS DISPUTE_CNT
    FROM dispute_dr
    GROUP BY 1
)

, _fraud_invs_ato_raw as
(select id as invs_id,
        dave_user_id as user_id,
        status as invs_status,
        created as invs_ts,
        updated as invs_updated_ts,
        completed as invs_completed_ts
 from   dave.identity.fraud_investigation
 ),

_fraud_invs_ato_raw_enriched as
(select i.*,
        il.fraud_investigation_label_id as invs_fraud_label_id,
        il_name.value as invs_fraud_label_name,
        case when lower(il_name.value) like '%account%take%over' then 'ATO' else null end as invs_ato_result
 from   _fraud_invs_ato_raw as i
 left join dave.identity.fraud_investigation_fraud_investigation_label as il
        on i.invs_id = il.fraud_investigation_id
 left join dave.identity.fraud_investigation_label as il_name
        on il.fraud_investigation_label_id = il_name.id
 ),

_fraud_invs_ato as
(select invs_id,
        user_id,
        invs_ts,
        invs_completed_ts,
        invs_fraud_label_name,
        invs_ato_result
 from   _fraud_invs_ato_raw_enriched
 where  True
   and  invs_ato_result = 'ATO'
 qualify row_number() over(partition by user_id order by invs_completed_ts desc) = 1
 )

, SOCURE_RESPONSE AS (
select DAVE_USER_ID, sr.created
, convert_timezone('UTC','America/Los_Angeles', sr.created::timestamp_ntz) AS Created_PST
, REFERENCE_ID AS SocureTransaction_ID
, PARSE_JSON(sr.RESPONSE) :fraud:scores[0].score::decimal(4,3) AS Sigma_Score
,PARSE_JSON(sr.RESPONSE) :decision:modelName::string AS Socure_Decision_Model
,PARSE_JSON(sr.RESPONSE) :decision:modelVersion::string AS Socure_Decision_Version
,PARSE_JSON(sr.RESPONSE) :decision:value::string AS KYC_Socure_Decision

,PARSE_JSON(sr.RESPONSE) :fraud:reasonCodes::string AS Fraud_Reason_Codes
,PARSE_JSON(sr.RESPONSE) :fraud:scores[0]:name::string AS Fraud_Score_Name
,PARSE_JSON(sr.RESPONSE) :fraud:scores[0]:score::decimal(4,3) AS Fraud_Score
,PARSE_JSON(sr.RESPONSE) :fraud:scores[0]:version::string AS Fraud_version

,PARSE_JSON(sr.RESPONSE) :kyc:reasonCodes::string AS KYC_Reason_Codes
,PARSE_JSON(sr.RESPONSE) :kyc:fieldValidations:firstName::decimal(4,3) AS KYC_First_Name
,PARSE_JSON(sr.RESPONSE) :kyc:fieldValidations:surName::decimal(4,3) AS KYC_Surname
,PARSE_JSON(sr.RESPONSE) :kyc:fieldValidations:streetAddress::decimal(4,3) AS KYC_Street_Address
,PARSE_JSON(sr.RESPONSE) :kyc:fieldValidations:city::decimal(4,3) AS KYC_City
,PARSE_JSON(sr.RESPONSE) :kyc:fieldValidations:state::decimal(4,3) AS KYC_State
,PARSE_JSON(sr.RESPONSE) :kyc:fieldValidations:zip::decimal(4,3) AS KYC_Zip
,PARSE_JSON(sr.RESPONSE) :kyc:fieldValidations:mobileNumber::decimal(4,3) AS KYC_Mobile_Number
,PARSE_JSON(sr.RESPONSE) :kyc:fieldValidations:dob::decimal(4,3) AS KYC_DOB
,PARSE_JSON(sr.RESPONSE) :kyc:fieldValidations:ssn::decimal(4,3) AS KYC_SSN

,PARSE_JSON(sr.RESPONSE) :nameAddressCorrelation:reasonCodes::string AS NameAddressCorrelation_ReasonCodes
,PARSE_JSON(sr.RESPONSE) :nameAddressCorrelation:score::decimal(4,3) AS NameAddressCorrelation_score

,PARSE_JSON(sr.RESPONSE) :nameEmailCorrelation:reasonCodes::string AS NameEmailCorrelation_ReasonCodes
,PARSE_JSON(sr.RESPONSE) :nameEmailCorrelation:score::decimal(4,3) AS NameEmailCorrelation_score

,PARSE_JSON(sr.RESPONSE) :namePhoneCorrelation:reasonCodes::string AS NamePhoneCorrelation_ReasonCodes
,PARSE_JSON(sr.RESPONSE) :namePhoneCorrelation:score::decimal(4,3) AS NamePhoneCorrelation_score

,PARSE_JSON(sr.RESPONSE) :addressRisk:reasonCodes::string AS addressRisk_ReasonCodes
,PARSE_JSON(sr.RESPONSE) :addressRisk:score::decimal(4,3) AS addressRisk_score

,PARSE_JSON(sr.RESPONSE) :emailRisk:reasonCodes::string AS emailRisk_ReasonCodes
,PARSE_JSON(sr.RESPONSE) :emailRisk:score::decimal(4,3) AS emailRisk_score

,PARSE_JSON(sr.RESPONSE) :phoneRisk:reasonCodes::string AS phoneRisk_ReasonCodes
,PARSE_JSON(sr.RESPONSE) :phoneRisk:score::decimal(4,3) AS phoneRisk_score

,PARSE_JSON(sr.RESPONSE) :deviceRisk:reasonCodes::string AS deviceRisk_ReasonCodes
,PARSE_JSON(sr.RESPONSE) :deviceRisk:score::decimal(4,3) AS deviceRisk_score

,PARSE_JSON(sr.RESPONSE) :globalWatchlist:matches::string AS globalWatchlist
,PARSE_JSON(sr.RESPONSE) :globalWatchlist:reasonCodes::string AS globalWatchlist_ReasonCodes

,PARSE_JSON(sr.RESPONSE) :deviceData:geolocation:ipGeolocation:city::string AS deviceData_city
,PARSE_JSON(sr.RESPONSE) :deviceData:geolocation:ipGeolocation:coordinates::string AS deviceData_coordinates
,PARSE_JSON(sr.RESPONSE) :deviceData:geolocation:ipGeolocation:country::string AS deviceData_country
,PARSE_JSON(sr.RESPONSE) :deviceData:geolocation:ipGeolocation:state::string AS deviceData_state
,PARSE_JSON(sr.RESPONSE) :deviceData:geolocation:ipGeolocation:zip::string AS deviceData_zip
,PARSE_JSON(sr.RESPONSE) :deviceData:geolocation:gpsGeolocation::string AS device_gpsgeolocation

,PARSE_JSON(sr.RESPONSE) :deviceData:information:deviceManufacturer::string AS device_manufacturer
,PARSE_JSON(sr.RESPONSE) :deviceData:information:deviceModelNumber::string AS device_modelnumber
,PARSE_JSON(sr.RESPONSE) :deviceData:information:operatingSystem::string AS device_operating_system
,PARSE_JSON(sr.RESPONSE) :deviceData:information:operatingSystemVersion::string AS device_operating_version

,PARSE_JSON(sr.RESPONSE) :deviceData:velocityMetrics:historicalCount:email:uniqueCount::integer AS device_email_unique_Count
,PARSE_JSON(sr.RESPONSE) :deviceData:velocityMetrics:historicalCount:email:uniqueSharePercent::integer AS device_email_unique_sharepercent
,PARSE_JSON(sr.RESPONSE) :deviceData:velocityMetrics:historicalCount:firstName:uniqueCount::integer AS device_firstname_unique_Count
,PARSE_JSON(sr.RESPONSE) :deviceData:velocityMetrics:historicalCount:firstName:uniqueSharePercent::integer AS device_firstname_unique_sharepercent
,PARSE_JSON(sr.RESPONSE) :deviceData:velocityMetrics:historicalCount:mobileNumber:uniqueCount::integer AS device_mobilenumber_unique_Count
,PARSE_JSON(sr.RESPONSE) :deviceData:velocityMetrics:historicalCount:mobileNumber:uniqueSharePercent::integer AS device_mobilenumber_unique_sharepercent
,PARSE_JSON(sr.RESPONSE) :deviceData:velocityMetrics:historicalCount:surName:uniqueCount::integer AS device_surname_unique_Count
,PARSE_JSON(sr.RESPONSE) :deviceData:velocityMetrics:historicalCount:surname:uniqueSharePercent::integer AS device_surname_unique_sharepercent

,PARSE_JSON(sr.RESPONSE) :documentVerification:decision:name::string AS document_verification_decision_name
,PARSE_JSON(sr.RESPONSE) :documentVerification:decision:value::string AS document_verification_decision_value

,PARSE_JSON(sr.RESPONSE) :documentVerification:documentType:type::string as document_verification_document_type
,PARSE_JSON(sr.RESPONSE) :documentVerification:documentType:country::string as document_verification_document_type_country
,PARSE_JSON(sr.RESPONSE) :documentVerification:documentType:state::string as document_verification_document_type_state
,PARSE_JSON(sr.RESPONSE) :documentVerification:reasonCodes::string as document_verification_reasoncodes

FROM RISK.IDENTITY.SOCURE_RESPONSE sr
)

, LATEST_KYC AS (
SELECT *
FROM SOCURE_RESPONSE
WHERE Sigma_Score IS NOT NULL
QUALIFY ROW_NUMBER () OVER (PARTITION BY DAVE_USER_ID ORDER BY CREATED_PST DESC) = 1
)

, LATEST_DOCV AS (
SELECT *
FROM SOCURE_RESPONSE
WHERE document_verification_decision_value IS NOT NULL
QUALIFY ROW_NUMBER () OVER (PARTITION BY DAVE_USER_ID ORDER BY CREATED_PST DESC) = 1
)

, SPEND_ACCOUNT_OPEN AS (
SELECT DAVE_USER_ID, A.NAME AS ACCOUNT_NAME, A.CREATED AS ACCOUNT_CREATION_DATE, PRODUCT_CATEGORY, AT.CODE
FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_BANK_BANKING.ACCOUNT A
JOIN APPLICATION_DB.GOOGLE_CLOUD_MYSQL_BANK_BANKING.ACCOUNT_TYPE AT ON AT.ID = A.ACCOUNT_TYPE_ID
WHERE CODE = 'CHECKING'
QUALIFY ROW_NUMBER() OVER (PARTITION BY DAVE_USER_ID ORDER BY A.CREATED) = 1
)

, GOAL_ACCOUNT_OPEN AS (
SELECT DAVE_USER_ID, A.NAME AS ACCOUNT_NAME, A.CREATED AS ACCOUNT_CREATION_DATE, PRODUCT_CATEGORY, AT.CODE
FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_BANK_BANKING.ACCOUNT A
JOIN APPLICATION_DB.GOOGLE_CLOUD_MYSQL_BANK_BANKING.ACCOUNT_TYPE AT ON AT.ID = A.ACCOUNT_TYPE_ID
WHERE CODE = 'GOAL'
QUALIFY ROW_NUMBER() OVER (PARTITION BY DAVE_USER_ID ORDER BY A.CREATED) = 1
)

, driver_raw as (
SELECT U.ID AS DAVE_USER_ID, u.CREATED AS USER_CREATION_DATE, FRAUD, U.DELETED AS DAVE_ACCOUNT_DELETED_DATE
, CASE WHEN E.DAVE_USER_ID IS NOT NULL THEN 'true' ELSE 'false' END AS FRAUD_ACH_RETURN
, SA.ACCOUNT_CREATION_DATE AS SPEND_CREATED_DATE
, G.ACCOUNT_CREATION_DATE AS GOAL_CREATED_DATE
, CASE WHEN SPEND_CREATED_DATE < COALESCE(GOAL_CREATED_DATE,SPEND_CREATED_DATE) THEN SPEND_CREATED_DATE ELSE COALESCE(GOAL_CREATED_DATE,SPEND_CREATED_DATE) END AS EARLIEST_BOD_DATE
, DATEDIFF('DAY', EARLIEST_BOD_DATE, ACH_Transfer_Initiated) AS DAYS_ACH_INITIATED_ACCOUNT_CREATION
, Account_Type, ACH_Return_Amount, ACH_Transfer_Initiated, ACH_Transfer_Returned_At, ACH_Transfer_Return_Code, External_Bank_Account
, CASE WHEN (FRAUD = 'true' OR EC.DAVE_USER_ID IS NOT NULL OR DR.DAVE_USER_ID IS NOT NULL OR ATO.USER_ID IS NOT NULL) THEN 'true' ELSE 'false' END AS BAD_ACTOR
, FLAT.CREATED AS KYC_RUN_DATE
, LK.KYC_SOCURE_DECISION AS KYC_SOCURE_DECISION
, LD.document_verification_decision_value AS DOCV_SOCURE_DECISION
, R208
, I566
, I625
, R662
, R655
, I428
, R606
, R659
, R610
, R559
, R604
, R608
, R571
, R006
, R572
, R653
, R561
, R402
, R658
, R551
, R660
, R605
, R657
, R601
, R642
, R619
, R566
, R207
, R618
, R616
, R211
, R607
, R210
, I414
, I402
, R720
, R650
, R646
, R633
, R410
, R409
, R944
, R631
, R623
, R574
, R408
, R209
, I127
, R980
, R632
, R573
, R569
, R568
, R217
, I912
, R919
, R721
, R705
, R702
, R701
, R661
, R656
, R654
, R644
, R622
, R567
, R113
, R005
, I920
, I907
, S.NameEmailCorrelation_score AS NameEmailCorrelation_score_V2
, S.emailRisk_score AS emailRisk_score_V2
, S.NamePhoneCorrelation_score AS NamePhoneCorrelation_score_V2
, S.addressRisk_score AS addressRisk_score_V2
, S.phoneRisk_score AS phoneRisk_score_V2
FROM  APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.USER U
LEFT JOIN EC_CHARGEBACK_CNT EC ON U.ID = EC.DAVE_USER_ID
LEFT JOIN dispute_dr_user DR ON U.ID = DR.DAVE_USER_ID
LEFT JOIN _fraud_invs_ato ATO ON U.ID = ATO.USER_ID
LEFT JOIN LATEST_KYC LK ON U.ID = LK.DAVE_USER_ID
LEFT JOIN LATEST_DOCV LD ON U.ID = LD.DAVE_USER_ID
LEFT JOIN RISK.DBT_PROD_MARTS.FCT_SOCURE_KYC_USER_FLAT FLAT ON FLAT.DAVE_USER_ID = U.ID
LEFT JOIN SOCURE_RESPONSE S ON FLAT.ORIGINAL_TRANSACTION_ID = S.SocureTransaction_ID
LEFT JOIN SPEND_ACCOUNT_OPEN SA ON SA.DAVE_USER_ID = U.ID
LEFT JOIN GOAL_ACCOUNT_OPEN G ON G.DAVE_USER_ID = U.ID
LEFT JOIN EARLIEST_ACH_RETURN E ON U.ID = E.DAVE_USER_ID
WHERE U.CREATED >= '2023-01-01'
)

, driver_rule_test as (
SELECT DAVE_USER_ID
, USER_CREATION_DATE
, FRAUD
, DAVE_ACCOUNT_DELETED_DATE
, FRAUD_ACH_RETURN
, SPEND_CREATED_DATE
, GOAL_CREATED_DATE
, EARLIEST_BOD_DATE
, DAYS_ACH_INITIATED_ACCOUNT_CREATION
, Account_Type
, ACH_Return_Amount
, ACH_Transfer_Initiated
, ACH_Transfer_Returned_At
, ACH_Transfer_Return_Code
, External_Bank_Account
, BAD_ACTOR
, KYC_RUN_DATE
, KYC_SOCURE_DECISION
, DOCV_SOCURE_DECISION
, R208
, I566
, I625
, R662
, R655
, I428
, R606
, R659
, R610
, R559
, R604
, R608
, R571
, R006
, R572
, R653
, R561
, R402
, R658
, R551
, R660
, R605
, R657
, R601
, R642
, R619
, R566
, R207
, R618
, R616
, R211
, R607
, R210
, I414
, I402
, R720
, R650
, R646
, R633
, R410
, R409
, R944
, R631
, R623
, R574
, R408
, R209
, I127
, R980
, R632
, R573
, R569
, R568
, R217
, I912
, R919
, R721
, R705
, R702
, R701
, R661
, R656
, R654
, R644
, R622
, R567
, R113
, R005
, I920
, I907
, NameEmailCorrelation_score_V2
, emailRisk_score_V2
, NamePhoneCorrelation_score_V2
, addressRisk_score_V2
, phoneRisk_score_V2
FROM driver_raw
)

SELECT *
FROM driver_rule_test
WHERE FRAUD_ACH_RETURN = 'true'
order by dave_user_id