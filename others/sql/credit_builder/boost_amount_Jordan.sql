SELECT
  od.id,
  od.created,
  od.approved_amount,
  dis.amount AS `disburse_amount`,
  dis.TOTAL_DISBURSED_AMOUNT,
  ds.name AS overdraft_status,
  dis.promo_id,
  pp.reference_id as promo_redemption_reference_id,
  json_extract_path_text(pp.snapshot, 'data.redemptionAmount') AS boost_amount,
  --pp.AMOUNT AS `promo_amount`
  pp.ID as papi_redemption_id,
  pp.STATUS AS papi_status,
  pp.USER_ID
FROM
  OVERDRAFT.OVERDRAFT_OVERDRAFT.PROMOTION odp -- Overdraft's table for collecting earned 'coupons'
  -- you can use the source of truth table from papi, that's the official record for the promotion
  JOIN MARKETING_DB.GOOGLE_CLOUD_MYSQL_PROMOTIONS.REFERRER_REDEMPTION pp ON (LOWER(pp.REFERENCE_ID) = odp.PROMO_ID) -- for status of redemption
  LEFT JOIN OVERDRAFT.OVERDRAFT_OVERDRAFT.DISBURSEMENT dis ON (odp.PROMO_ID = dis.promo_id)
  LEFT JOIN OVERDRAFT.OVERDRAFT_OVERDRAFT.DISBURSEMENT_STATUS ds on (dis.DISBURSEMENT_STATUS_ID = ds.ID)
  LEFT JOIN OVERDRAFT.OVERDRAFT_OVERDRAFT.OVERDRAFT od ON (dis.overdraft_id = od.id)


WHERE
  dis.promo_id IS NOT NULL
ORDER BY
  status DESC
LIMIT 100


----Non-referral

SELECT
  od.id,
  od.created,
  od.approved_amount,
  dis.amount AS `disburse_amount`,
  dis.TOTAL_DISBURSED_AMOUNT,
  ds.name AS overdraft_status,
  dis.promo_id,
  pp.reference_id as promo_redemption_reference_id,
  json_extract_path_text(pp.snapshot, 'data.redemptionAmount') AS boost_amount,
  --pp.AMOUNT AS `promo_amount`
  pp.ID as papi_redemption_id,
  pp.STATUS AS papi_status,
  pp.USER_ID
FROM
  OVERDRAFT.OVERDRAFT_OVERDRAFT.PROMOTION odp -- Overdraft's table for collecting earned 'coupons'
  -- you can use the source of truth table from papi, that's the official record for the promotion
  JOIN MARKETING_DB.GOOGLE_CLOUD_MYSQL_PROMOTIONS.PROMO_REDEMPTION pp ON (LOWER(pp.REFERENCE_ID) = odp.PROMO_ID) -- for status of redemption
  LEFT JOIN OVERDRAFT.OVERDRAFT_OVERDRAFT.DISBURSEMENT dis ON (odp.PROMO_ID = dis.promo_id)
  LEFT JOIN OVERDRAFT.OVERDRAFT_OVERDRAFT.DISBURSEMENT_STATUS ds on (dis.DISBURSEMENT_STATUS_ID = ds.ID)
  LEFT JOIN OVERDRAFT.OVERDRAFT_OVERDRAFT.OVERDRAFT od ON (dis.overdraft_id = od.id)


WHERE
  dis.promo_id IS NOT NULL
ORDER BY
  status DESC
LIMIT 100