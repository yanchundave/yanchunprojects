numeric_features = [
    'BOD_ACCOUNT_OPEN_USER',
    'BOD_DIRECT_DEPOSIT_USER',
    'MOST_RECENT_REQUEST_DECLINE',
    'HAS_VALID_CREDENTIALS',
    'LAST_MAX_APPROVED_AMOUNT',
    'APPROVED_BANK_COUNT',
    'SINE_MONTH',
    'COSINE_MONTH',
    'FREQUENCY'	,
    'T'	,
    'RECENCY'	,
    'MONETARY'	,
    'TIMEDIFF_STD'	,
    'MONETARY_STD'	,
    'SESSIONTOTAL'	,
    'ACTIVEMONTH'	,
    'SESSION_STD'
]

category_features = [
    'PLATFORM',
    'ATTRIBUTION',
    'NETWORK',
    'BANK_CATEGORY'
]
y_column = ['REVENUE']

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
"""
numeric_columns = [

    'BOD_ACCOUNT_OPEN_USER',

    'BOD_DIRECT_DEPOSIT_USER',

    'MOST_RECENT_REQUEST_DECLINE',
    'LAST_MAX_APPROVED_AMOUNT',

    'HAS_VALID_CREDENTIALS',
    'ADVANCE_TAKEN_AMOUNT',
    'sine_month',
    'cosine_month',
    'FREQUENCY'	,
    'T'	,
    'RECENCY'	,

    'MONETARY'	,
    'timediff_std'	,
    'revenue_std'	,
    'sessiontotal'	,
    'activemonth'	,
    'session_std'
]

categorical_columns = ['PLATFORM', 'BANK_CATEGORY', 'ATTRIBUTION', 'NETWORK']
"""
datafile_path = "/Users/yanchunyang/Documents/datafiles/ltv_ml/"

