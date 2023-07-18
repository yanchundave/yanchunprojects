SELECT
        a.dave_user_id AS user_id,
        a.id AS extra_cash_account_id,
        c.PRN AS Customer_Account_Number

    FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_BANK_BANKING.ACCOUNT a
    INNER JOIN APPLICATION_DB.GOOGLE_CLOUD_MYSQL_BANK_BANKING.ACCOUNT_TYPE att
        ON a.account_type_id = att.id AND att.name = 'Extra Cash Account'
    LEFT JOIN APPLICATION_DB.GOOGLE_CLOUD_MYSQL_BANK_BANKING.ACCOUNT_STATUS acs
        ON a.account_status_id = acs.id
    LEFT JOIN APPLICATION_DB.GOOGLE_CLOUD_MYSQL_BANK_BANKING.PAYMENT_REFERENCE pr
        ON a.id = pr.account_id
    LEFT JOIN finance.galileo.customer c
        ON APPLICATION_DB.GOOGLE_CLOUD_MYSQL_BANK_BANKING.BANKING_HASH(c.prn) = pr.account_number
    LEFT JOIN OVERDRAFT.OVERDRAFT_OVERDRAFT.ACCOUNT oa
        ON a.id = oa.extra_cash_account_id
    LEFT JOIN APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.USER ON a.dave_user_id = dave_user.id


    banking.account  a
join banking.payment_reference pr on pr.account_id = a.id
join finance.galileo.customer c on banking_hash(c.prn) = pr.account_number