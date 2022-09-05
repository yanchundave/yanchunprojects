"""
numeric_columns = [
   'PV_TENURE',
   'BC_TENURE',
    'ADVANCE_TAKEN_USER',
    'EVENT_TENURE',
    'BOD_ACCOUNT_OPEN_USER',
    'CARD_OPEN_TENURE',
    'BOD_DIRECT_DEPOSIT_USER',
    'ONE_DAVE_NEW_MEMBER',
    'IS_NEW_USER',
    'MOST_RECENT_REQUEST_DECLINE',
    'MAX_APPROVED_AMOUNT',
    'APPROVED_AMOUNT_DECREASE',
    'REQUEST_COUNT',
    'APPROVED_COUNT',
    'TAKEOUT_COUNT',
    'HAS_VALID_CREDENTIALS',
    'HAS_TRANSACTIONS',
    'REQUEST_BANK_COUNT',
    'APPROVED_BANK_COUNT',
    'TAKEOUT_BANK_COUNT',
    'DAYS_SINCE_LAST_ACTIVE',
    'DAYS_SINCE_FIRST_ACTIVE',
    'ADVANCE_TAKEN_AMOUNT',
    'sine_month',
    'cosine_month',
    'frequency'	,
    'T'	,
    'recency'	,
    'age'	,
    'monetary'	,
    'timediff_std'	,
    'revenue_std'	,
    'lastquarter_session'	,
    'sessiontotal'	,
    'lastsession'	,
    'activemonth'	,
    'session_std'
]
"""
"""
T , BC_TENURE, EVENT_TENURE have strong correlation  so remove BC_TENUE AND EVENT_TENURE
frequency and TAKEOUT_COUNT has strong correlation so remove TAKEOUT_COUNT
AFTER REMOVING THESE FEATURES, R-SQUARE DROP FROM 0.374 TO 0.371
"""
numeric_columns = [

    'BOD_ACCOUNT_OPEN_USER',

    'BOD_DIRECT_DEPOSIT_USER',

    'IS_NEW_USER',
    'MOST_RECENT_REQUEST_DECLINE',
    'MAX_APPROVED_AMOUNT',

    'HAS_VALID_CREDENTIALS',

    'APPROVED_BANK_COUNT',

    'ADVANCE_TAKEN_AMOUNT',
    'sine_month',
    'cosine_month',
    'frequency'	,
    'T'	,
    'recency'	,

    'monetary'	,
    'timediff_std'	,
    'revenue_std'	,
    'lastquarter_session'	,
    'sessiontotal'	,
    'lastsession'	,
    'activemonth'	,
    'session_std'
]
"""
categorical_columns = [
  'PLATFORM',
    'ATTRIBUTION',
    'BANK_CATEGORY',
    'CHANNEL'
]
"""
categorical_columns = None

jing_columns = ['userid', 'PV_TENURE', 'BC_TENURE', 'PLATFORM', 'ATTRIBUTION',
       'ADVANCE_TAKEN_USER', 'TOTAL_DAVE_REVENUE', 'ADVANCE_TENURE',
       'LATEST_ADVANCE_TENURE', 'EVENT_TENURE', 'BOD_ACCOUNT_OPEN_USER',
       'CARD_OPEN_TENURE', 'BOD_DIRECT_DEPOSIT_USER', 'ONE_DAVE_NEW_MEMBER',
       'IS_NEW_USER', 'MOST_RECENT_REQUEST_DECLINE', 'MAX_APPROVED_AMOUNT',
       'APPROVED_AMOUNT_DECREASE', 'REQUEST_COUNT', 'APPROVED_COUNT',
       'TAKEOUT_COUNT', 'BANK_CATEGORY', 'HAS_VALID_CREDENTIALS',
       'HAS_TRANSACTIONS', 'REQUEST_BANK_COUNT', 'APPROVED_BANK_COUNT',
       'TAKEOUT_BANK_COUNT', 'DAYS_SINCE_LAST_ACTIVE',
       'DAYS_SINCE_FIRST_ACTIVE', 'ADVANCE_TAKEN_AMOUNT', 'CHURN',
       'CHURN_DATE', 'CURRENT_BALANCE', 'AVAILABLE_BALANCE']


datafile_path = "/Users/yanchunyang/Documents/datafiles/ltv_ml/"