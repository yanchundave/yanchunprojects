numeric_features = [
    'BOD_ACCOUNT_OPEN_USER',
    'BOD_DIRECT_DEPOSIT_USER',
    'MOST_RECENT_REQUEST_DECLINE',
    'ADVANCE_TAKEN_AMOUNT',
    'APPROVED_BANK_COUNT',
    'FREQUENCY',
    'T',
    'RECENCY',
    'MONETARY',
    'ACTIVESESSION',
    'HAS_VALID_CREDENTIALS',
    'SINE_MONTH',
    'COS_MONTH'

]

category_features = [
    'PLATFORM',
    'ATTRIBUTION',
    'NETWORK',
   'BANK_CATEGORY'
]
y_column = ['REVENUE']

channels = ['Adwords', 'Apple Search Ads', 'Facebook', 'Organic', 'Referral', 'Snapchat', 'bytedanceglobal_int']

datafile_path = "/Users/yanchunyang/Documents/datafiles/ltv_ml/"