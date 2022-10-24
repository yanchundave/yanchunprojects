import logging
import os


log = logging.getLogger(os.path.basename(__file__))
logging.basicConfig(level=logging.INFO)

# y_column = ["REVENUE"]
numeric_features = [
    "BOD_ACCOUNT_OPEN_USER",
    "BOD_DIRECT_DEPOSIT_USER",
    "MOST_RECENT_REQUEST_DECLINE",
    "ADVANCE_TAKEN_AMOUNT",
    "APPROVED_BANK_COUNT",
    "FREQUENCY",
    "T",
    "RECENCY",
    "MONETARY",
    "ACTIVESESSION",
    "HAS_VALID_CREDENTIALS",
    "SINE_MONTH",
    "COS_MONTH",
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
