import glob
import json
import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine
from geopy import Nominatim


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
    return (df)


def aggregated__transaction_push_to_sql(df):
    engine = create_engine(
        "mysql+mysqldb://root:asdfghjkl$12@localhost:3306/mydb")
    df.to_sql(name='aggregatedtransaction', con=engine, if_exists='append', chunksize=1000, index=False)


def map_transaction_push():
    os.chdir('PhonePePulseData/data/Transaction/map')
    f = ''
    df = []
    for filename in glob.glob('*.json'):
        with open(filename) as f:
            g = json.loads(f.read())
            e = pd.json_normalize(g)
        e.drop(['success', 'code'], axis=1, inplace=True)
        for each in e['data.hoverDataList']:
            for i in each:
                f = i['name']
                for j in i['metric']:
                    df.append({'Name': f, 'Count': j['count'], 'Amount': j['amount']})

    df = pd.DataFrame(df)
    return (df)


def map_transaction_on_map(d):
    a = d['Name'].unique()
    map = []
    for each in a:
        geolocator = Nominatim(user_agent='MyApp')
        location = geolocator.geocode(each)
        map.append({'lat': location.latitude, 'lon': location.longitude})
    df_map = pd.DataFrame(map)
    return df_map


def map_transaction_push_to_sql(df):
    engine = create_engine(
        "mysql+mysqldb://root:asdfghjkl$12@localhost:3306/mydb")
    df.to_sql(name='map_transaction', con=engine, if_exists='append', chunksize=1000, index=False)


def aggregated_user_push():
    os.chdir('PhonePePulseData/data/User/Aggregated')
    df = []
    for filename in glob.glob('*.json'):
        with open(filename) as f:
            g = json.loads(f.read())
            e = pd.json_normalize(g)
        e.drop(['success', 'code'], axis=1, inplace=True)
        for each in e['data.usersByDevice']:
            if each is not None:
                for j in each:
                    df.append({'Brand': j['brand'], 'Count': j['count'], 'Percentage': j['percentage']})
        df2 = pd.DataFrame(df)
        return df2


def aggregated_user_push_to_sql(df):
    engine = create_engine(
        "mysql+mysqldb://root:asdfghjkl$12@localhost:3306/mydb")
    df.to_sql(name='aggregated_user', con=engine, if_exists='append', chunksize=1000, index=False)


opt = str(st.sidebar.selectbox(label='select', options=[' ', 'aggregated', 'map']))
opt2 = str(st.sidebar.selectbox(label='select', options=[' ', 'transaction', 'user']))
upload = st.sidebar.button(label='Upload')
if opt == 'aggregated' and opt2 == 'transaction' and upload:
    df = aggregated_transaction_push()
    st.write(df.head())
    aggregated__transaction_push_to_sql(df)
    st.write('SQL upload successful')

elif opt == 'aggregated' and opt2 == 'user' and upload:
    d = aggregated_user_push()
    st.write('Push successful')
    aggregated_user_push_to_sql(d)
    st.write('SQL upload successful')

elif opt == 'map' and opt2 == 'transaction' and upload:
    df = map_transaction_push()
    st.write('Push successful')
    map_transaction_push_to_sql(df)
    st.write('SQL upload successful')
    df_map = map_transaction_on_map(df)
    st.map(df_map)

elif opt == 'map' and opt2 == 'user' and upload:
    pass
elif opt == 'top' and opt2 == 'transaction':
    os.chdir('PhonePePulseData/data/top/transaction')
elif opt == 'map' and opt2 == 'user':
    os.chdir('PhonePePulseData/data/top/user')
