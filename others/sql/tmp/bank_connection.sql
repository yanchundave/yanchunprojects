
select count(distinct user_id)
 from APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.BANK_CONNECTION
 WHERE deleted IS NULL
    AND _fivetran_deleted = false
    AND banking_data_source = 'PLAID'
    and has_valid_credentials