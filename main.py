import glob
import json
import os
import pandas as pd
#import streamlit as st
from sqlalchemy import create_engine


def aggregated_transaction_push():
    os.chdir('PhonePePulseData/data/Transaction/aggregated')
    f = ''
    df = []
    for filename in glob.glob('*.json'):
        with open(filename) as f:
            g = json.loads(f.read())
            e = pd.json_normalize(g)
        e.drop(['success', 'code', 'responseTimestamp', 'data.from', 'data.to'], axis=1, inplace=True)
        for each in e['data.transactionData']:
            for i in each:
                f = i['name']
                for j in i['paymentInstruments']:
                    df.append({'Name': f, 'Count': j['count'], 'Amount': j['amount']})

    df = pd.DataFrame(df)
    print(df)
    return (df)


def push_to_sql(df):
    engine = create_engine(
        "mysql+mysqldb://root:asdfghjkl$12@localhost:3306/mydb")
    df.to_sql(name='aggregatedtransaction', con=engine, if_exists='append', chunksize=1000, index=False)


#opt = str(st.sidebar.selectbox(label='select', options=[' ', 'aggregated', 'map']))
#opt2 = str(st.sidebar.selectbox(label='select', options=[' ', 'transaction', 'user']))
# elif opt == 'aggregated' and opt2 == 'user':
#     os.chdir('PhonePePulseData/data/aggregated/user')
# elif opt == 'map' and opt2 == 'transaction':
#     os.chdir('PhonePePulseData/data/map/transaction')
# elif opt == 'map' and opt2 == 'user':
#     os.chdir('PhonePePulseData/data/map/user')
# elif opt == 'top' and opt2 == 'transaction':
#     os.chdir('PhonePePulseData/data/top/transaction')
# elif opt == 'map' and opt2 == 'user':
#     os.chdir('PhonePePulseData/data/top/user')

#if st.button(label='Upload'):
df = aggregated_transaction_push()
push_to_sql(df)
    #st.write(df.head())

