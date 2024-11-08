import os
import logging


log = logging.getLogger(os.path.basename(__file__))
logging.basicConfig(level=logging.INFO)

# y_column = ["REALREVENUE"]
numeric_features = [
    "BOD_ACCOUNT_OPEN_USER",
    "BOD_DIRECT_DEPOSIT_USER",
    "MOST_RECENT_REQUEST_DECLINE",
    "HAS_VALID_CREDENTIALS",
    "ADVANCE_TAKEN_AMOUNT",
    "APPROVED_BANK_COUNT",
    "SINE_MONTH",
    "COSINE_MONTH",
    "FREQUENCY",
    "T",
    "RECENCY",
    "MONETARY",
    "TIMEDIFF_STD",
    "MONETARY_STD",
    "SESSIONTOTAL",
    "ACTIVEMONTH",
    "SESSION_STD",
]

category_features = ["PLATFORM", "ATTRIBUTION", "NETWORK", "BANK_CATEGORY"]

channels = [
    "Adwords",
    "Apple Search Ads",
    "Facebook",
    "Organic",
    "Referral",
    "Snapchat",
    "bytedanceglobal_int",
]
