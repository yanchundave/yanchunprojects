from datetime import datetime

from pyparsing import common_html_entity

ORIGIN_DATE = '2021-01-01'
END_DATE_1 = '2022-10-16'
END_DATE_2 = '2022-09-16'

FLAG = 1 # flag = 1 means dependent variable is user, flag == 0 then dependent variable is revenue
y_constant = 1000 if FLAG == 1 else 100000
END_DATE = END_DATE_1 if FLAG==1 else END_DATE_2

common_path = "/Users/yanchunyang/Documents/datafiles/pystan/"
datafile_path = common_path + "user/" if FLAG==1 else common_path + "revenue/"

date_diff = (datetime.strptime(END_DATE, '%Y-%m-%d') - datetime.strptime(ORIGIN_DATE, '%Y-%m-%d')).days
#media_list = ['Adwords_Android', 'Adwords_iOS', 'Apple_Search_Ads_iOS', 'Facebook_Android', 'Facebook_iOS',
#'Snapchat_Android', 'Snapchat_iOS', 'bytedanceglobal_int_Android', 'bytedanceglobal_int_iOS', 'unknown', 'TV']

LTV_VALUE = 115


sample_size = 1000



#channel_list = ['Adwords', 'Apple', 'Facebook', 'Snapchat', 'bytedanceglobal', 'unknown', 'TV']

#user_type = 'advance'