datafile_path = "/Users/yanchunyang/Documents/datafiles/pystan/"
L = 14  #carryover period
T = 7   #moving average period
Kb = 7 #basic variable
Km = 11 #platform information
Kl = 148  #total data points
"""
media_list = ['Adwords_Android', 'Adwords_iOS', 'Apple_Search_Ads_iOS', 'Facebook_Android', 
'Facebook_iOS', 'Reddit_Android', 'Reddit_iOS', 'Snapchat_Android', 'Snapchat_iOS', 'Taboola_Android', 'Taboola_iOS', 
'bytedanceglobal_int_Android', 'bytedanceglobal_int_iOS', 'unknown', 'TV']
"""
media_list = ['Adwords_Android', 'Adwords_iOS', 'Apple_Search_Ads_iOS', 'Facebook_Android', 'Facebook_iOS', 'Snapchat_Android', 'Snapchat_iOS', 'bytedanceglobal_int_Android', 'bytedanceglobal_int_iOS', 'unknown', 'TV']
sample_size = 1000
M = 148
origin_date = '2022-01-01'
flag = 1
y_constant = 1000 if flag == 1 else 100000