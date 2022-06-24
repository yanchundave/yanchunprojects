from datetime import datetime

from pyparsing import common_html_entity

origin_date = '2022-01-01'
end_date_1 = '2022-06-22'
end_date_2 = '2022-05-22'

flag = 0 # flag = 1 means dependent variable is user, flag == 0 then dependent variable is revenue
y_constant = 1000 if flag == 1 else 100000
end_date = end_date_1 if flag==1 else end_date_2

common_path = "/Users/yanchunyang/Documents/datafiles/pystan/"
datafile_path = common_path + "user/" if flag==1 else common_path + "revenue/"

date_diff = (datetime.strptime(end_date, '%Y-%m-%d') - datetime.strptime(origin_date, '%Y-%m-%d')).days
media_list = ['Adwords_Android', 'Adwords_iOS', 'Apple_Search_Ads_iOS', 'Facebook_Android', 'Facebook_iOS', 'Snapchat_Android', 'Snapchat_iOS', 'bytedanceglobal_int_Android', 'bytedanceglobal_int_iOS', 'unknown', 'TV']

L = 14  #carryover period
T = 7   #moving average period
Kb = 7 #basic variable
Km = len(media_list) #platform information
Kl = date_diff - T + 1  #total data points - T


sample_size = 1000
M = Kl

#response_file = "platform_user.csv"
response_file = "platform_user_advance.csv" if flag == 1 else "total_revenue.csv"
independent_file = "channel_spending_raw.csv"

platform_list = ['iOS', 'Android']
channel_list = ['Adwords', 'Apple', 'Facebook', 'Snapchat', 'bytedanceglobal', 'unknown', 'TV']

user_type = 'advance'