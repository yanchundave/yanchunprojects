import numpy as np 
import pandas as pd
from datetime import datetime, timedelta 
from udf import *
from cnn_mix_ann import *
from global_variable import *
import pickle
import tensorflow as tf
from tensorflow.keras.callbacks import ModelCheckpoint
cp = ModelCheckpoint('model/', save_best_only=True)
ann_columns = ['userid', 'PV_TENURE', 'BC_TENURE', 'PLATFORM', 'ATTRIBUTION',
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
numerical_columns = [
    'PV_TENURE',
    'BC_TENURE',
    'ADVANCE_TAKEN_USER',
    'TOTAL_DAVE_REVENUE',
    'ADVANCE_TENURE',
    'LATEST_ADVANCE_TENURE',
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
    'ADVANCE_TAKEN_AMOUNT'  
    ]
categorical_columns = ['BANK_CATEGORY', 'PLATFORM', 'ATTRIBUTION']

def daydiff(a, b):
    a_time = datetime.strptime(a, '%Y-%m-%d')
    b_time = datetime.strptime(b, '%Y-%m-%d')
    c = b_time - a_time
    return int(c.days)

def get_bucket(x):
    if x == 0:
        return 0
    elif x > 0 and x <= 10:
        return 1
    elif x>10 and x <=20:
        return 2
    elif x>20 and x<= 30:
        return 3
    elif x > 30 and x<=40:
        return 4
    elif x>40 and x <=50:
        return 5
    else: 
        return 6

def generate_events(df_event):
    df_event['datediff'] = df_event.apply(lambda x: daydiff(x['start_date'], x['event_date']), axis=1)
    df1 = df_event.pivot(index=['userid', 'event_type'], columns=['datediff'], values=['event_volume']).reset_index()
    df1.columns = ['userid', 'event_type'] + [item[1] for item in df1.columns[2:]]
    df2 = df1.loc[~df1['event_type'].str.contains("\]"),:].sort_values(by=['userid', 'event_type'])
    df2 = df2.fillna(0)

    event_types = list(set(df2['event_type']))
    df_eventname = pd.DataFrame(event_types, columns=['event_type'])
    df_eventname = df_eventname.sort_values(by=['event_type'])

    df_user = pd.DataFrame(list(set(df2['userid'])), columns=['userid'])
    df_head = pd.merge(df_user, df_eventname, how='cross')

    df3 = pd.merge(df_head, df2, on=['userid', 'event_type'], how='left')
    df3 = df3.fillna(0)
    return df3, df_user, df_eventname


def obtain_ann_data(df_user):   
    with open(datafile_path + "df_ann.pk", 'rb') as f:
        df = pickle.load(f)
    df = df.drop_duplicates(['userid'])
    df_common = pd.merge(df_user, df, on=['userid'], how='left')
    x, x_name = feature_clean(df_common, numerical_columns, categorical_columns)
    return x, x_name

def obtain_data():
    
    event_str = """
    select USER_ID, start_date, event_date, event_type, event_volume from DBT.DEV_YANCHUN_PUBLIC.CNN_EVENT;
    """
    revenue_str = """
    with user_revenue as 
    (
    select USER_ID, SUM(REVENUE) as total_revenue
    from DBT.DEV_YANCHUN_PUBLIC.USER_TRANSACTION_2022
    WHERE date(TRANS_TIME) >= date('2022-01-01') and date(TRANS_TIME) < DATE('2022-07-01')
    group by USER_ID
    ),
    selected_user AS 
    (
    SELECT USER_ID, COUNT(*) AS total
    from DBT.DEV_YANCHUN_PUBLIC.CNN_EVENT
    group by USER_ID
    )
    SELECT a.USER_ID, b.total_revenue
    FROM selected_user a 
    join user_revenue b
    on a.USER_ID = b.USER_ID
    """
    jing_query = """
    select * from DBT.DEV_JGAN_PUBLIC.USER_DATA_2022 WHERE ADVANCE_TAKEN_USER=1
    """
    

    revenue_result = read_from_database(revenue_str)
    event_result = read_from_database(event_str)
    ann_result = read_from_database(jing_query)
    
    df_revenue = pd.DataFrame(revenue_result, columns=['userid', 'total_revenue'])
    df_event = pd.DataFrame(event_result, columns=['userid', 'start_date', 'event_date', 'event_type', 'event_volume'])
    df_ann = pd.DataFrame(ann_result, columns = ann_columns)

    with open(datafile_path + "df_revenue.pk", 'wb') as f:
        pickle.dump(df_revenue, f)

    with open(datafile_path + "df_event.pk", 'wb') as f:
        pickle.dump(df_event, f)

    with open(datafile_path + "df_ann.pk", 'wb') as f:
        pickle.dump(df_ann, f)

def train_model():
    with open(datafile_path + "df_revenue.pk", 'rb') as f:
        df_revenue = pickle.load(f)

    with open(datafile_path + "df_event.pk", 'rb') as f:
        df_event = pickle.load(f)

    print("read data successful")
    print("df_revenue shape is "+ str(df_revenue.shape))
    print("df_event shape is " + str(df_event.shape))

    df_event_update , df_user, df_eventname= generate_events(df_event)

    print("cnn data is ready")
    print("df_event_upate shape is " + str(df_event_update.shape))
    print("df_user shape is " + str(df_user.shape))
    print("df_eventname shape is " + str(df_eventname.shape))

    df_event_update = df_event_update.sort_values(by=['userid'])
    df_user_revenue = pd.merge(df_user, df_revenue, on=['userid'], how='left').sort_values(by=['userid'])
    df_user_revenue = df_user_revenue.fillna(0)
    df_user_revenue['revenue_bucket'] = df_user_revenue['total_revenue'].apply(lambda x: get_bucket(x))
    df_user_revenue = df_user_revenue.fillna(0)

    a3d = df_event_update.loc[:,[0, 1, 2, 3, 4, 5, 6, 7]].to_numpy().reshape(df_user.shape[0], df_eventname.shape[0], 8)
    ay = df_user_revenue.loc[:, ['revenue_bucket']].to_numpy().reshape(-1, 1)
    a3x = np.clip(a3d, 0, 100)/100
    a3x_expand = np.expand_dims(a3x, axis=-1)

    print("cnn input shape is")
    print(a3x_expand.shape)

    ann_data, ann_data_name = obtain_ann_data(df_user)
    print(ann_data_name)
    print("ann input shape is ")
    print(ann_data.shape)

    train_x, test_x, train_xx, test_xx, train_y, test_y = split_train_test(a3x_expand, ay, 0.2, ann_data)

    cnn_input = train_x.shape[1:]
    cnn_output = 7
    ann_input = (ann_data.shape[1],)
    ann_output = 7
    final_output = 7

    model = create_model(cnn_input, cnn_output, ann_input, ann_output, final_output)
    model.compile(optimizer='adam',
              loss=tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True),
              metrics=['accuracy'])
    
    model.fit(x=[train_x, train_xx], y=train_y, validation_data=([test_x, test_xx], test_y), epochs=10, callbacks=[cp])
    predict_y = model.predict([test_x, test_xx])

    return predict_y


def main():
    #obtain_data()
    prediction = train_model()
    print(prediction[0:10])
    print("Done")


if __name__ == '__main__':
    main()

