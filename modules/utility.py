import json
from datetime import date, timedelta
from google.oauth2 import service_account
import requests
import pandas as pd
from googleapiclient.discovery import build
from modules.sheets import *

def get_gcp_credentials():
    """creating credentials object for authentication"""
    print('getting credientials')
    #service_acc = open('ssh/service-account.json', 'r').read()
    service_acc = '''
            {
          "type": "service_account",
          "project_id": "brewcrew-27806",
          "private_key_id": "33cafe4a571892d0b5debdd5d9616c550f66ffbb",
          "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQC3LpeVqxW6hJSU\ntFYfc+tyVhDwOwCNDDg0HnFmmAe/Unvy1fUGN2SVS/joN9+LDtlWHREFncddVauP\nxLR8dkgnvIk2vbtyKLDU/sOZBnK9eB6NEsFhtdVcFE/StqEo6aNPPOAcXj2kx0JO\nNRDj+IAToFzIJOiobO3D17orTLRn8o+ThGCDmiqVjXGPYXejkaYv8JIb6Y+OWMur\n9OvNQ1yHM/DPNxVjkyQUtnlACKEjoZkJrInv5zrGSACf8vRCeEcXYJWW7WvE3RYX\n/+DscfNlLHiJO/lcIfQQjY/cUTdKpcgbhyHIbb8rjT7VrPdSfHZdZNugX++kjeAa\n2xyBZQmlAgMBAAECggEACopvUD5NMfOWxdckt4rzlZuMk6Oe3KmYgwNCHO21pcA+\nKqiTgL087UxhsSHPRrte1+eJ8bafthK2cb+Xb9ax6ztxnTPzboEXP9e9yKHy2008\nsNB5+UCTMPO9mM9H3HNT9ud2sUXeVTY0GLemq6pChclDXYRQ26mSUE6AbiUWon6H\n6meGZ9Vz858jvbZYcwAPq5i0vY7dWAmE5POJqZbeZymnmqeTsaMb4of90TAWI94t\njExuveCDyM8ppj0xKWGUdVdaajmJQhN9KerHP8cGrkjh1FDU5j7peq0bJDkUMzsk\nCuhlNmhwKCwEeBNt13Y3RzGJ8yDRM7mXwbMejl3vsQKBgQDi8UlFKtvZW8ZP17L6\n06MJPGn/3vDGd/lwIUiJDkTTrdBwR5cYR+lbcIy2AQvCmIv+AgFsIW0JFin/HiJN\n/QsB32gqb41SV6Bbwe6EGUZdjYjRlBUBP3hNOgxGk+qJBs4I9xh0ch6L6+d8rPNq\nwhSpGokT37mh0lQKx9YzOyoXXQKBgQDOouzQ7Aa6x4veDxwTEk9kXviU0ovFcI2/\n0aZzEyI951W3BOz0zOi7nukaRZcfpYx2yCF62pO9xCV8uOnLtx2Eplm2UXSFqMnI\n6jdX7BN9ULdutspQ9HRb9NfZoEXL4AfnGYqOx8FO1fazLzbvfTOschV7nB8aVwrn\nh/oLN45+6QKBgCBG4v3kZ5pVdDIc4obMHO8OWaEbqzD/FUn+YdukXOOiX/foJrQ8\nXhM0Qb8NDQgjPB3nWR5w7poRPe3b8lMXPz6H8L/AV+AzOEp0naDKG3jozWHJZudj\niI+rRt2u4feVMS6TPjYpmPCbn7PawVAYxGl8raxW8Ib0bCGQZIWqz4S9AoGAMCVa\nFFvKPZ7dAgJ3QiJUCUjvlo1wL7LH9Fe+9rUQl2fdWuV06J0N5RljqtFoIuGJ1utO\nkbd4EREwwty67NJfuFaiNI7KHZJ0YbHg70V8TQiPlOZWQYKT0hE8ceKcIKK0dK5I\n0HfOnLXR43Iu1KJRde6m/sQjoxmA2u2d3xs0wnECgYAdDL3ApdDozbquiCGu5CQs\nJhmMb+MdAB84QsQw9Ec/9d4PkiygPHv9xY39qJRlJavk2ga6TBsN3KKlxxMG3cG6\nhqzRh5+RX+WkNcwEGaCi0reT1KgKNUH4mUOoHaYRZkLih0eUPvDIq9xB7eL4+j4b\nPW5Ae3nOPNi2jR2BdLqPtA==\n-----END PRIVATE KEY-----\n",
          "client_email": "google-sheet@brewcrew-27806.iam.gserviceaccount.com",
          "client_id": "103289036546748045690",
          "auth_uri": "https://accounts.google.com/o/oauth2/auth",
          "token_uri": "https://oauth2.googleapis.com/token",
          "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
          "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/google-sheet%40brewcrew-27806.iam.gserviceaccount.com"
        }
        '''
    credentials = service_account.Credentials.from_service_account_info(
        json.loads(service_acc, strict=False),
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return credentials

def calculate_start_and_end_date(days_ago):
    """calculate min and max date using days"""
    max_date = (date.today()).strftime("%Y-%m-%d")
    min_date = (date.today() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
    return min_date, max_date

def transform_data(**kwargs):
    
    data = kwargs['ti'].xcom_pull(key='api_data' , task_ids = 'get_api_data')
    complete_date = pd.DataFrame()
    for comp_data in data:
        try:
            for doc in comp_data['hits']['hits'][:5]:
                    doc_updated = doc['_source']
                    doc_updated.update({'_id':doc['_id']})
                    # print()
                    # print(doc_updated)
                    complete_date = complete_date.append(doc_updated, ignore_index=True)
        except:
            pass
    transformed_data = pd.DataFrame()
    transformed_data['Product'] = complete_date['product']
    transformed_data['Issue'] = complete_date['issue']
    transformed_data['Sub_product'] = complete_date['sub_product']
    transformed_data['Complaint_id'] = complete_date['complaint_id']
    transformed_data['Submitted_via'] = complete_date['submitted_via']
    transformed_data['Company'] = complete_date['company']
    transformed_data['Date_received'] = complete_date['date_received']
    transformed_data['Month_year'] = pd.to_datetime(transformed_data['Date_received']).dt.to_period('M')
    transformed_data['Company_Response'] = complete_date['company_response']
    transformed_data['State'] = complete_date['state']
    transformed_data['Sub_issue'] = complete_date['sub_issue']
    transformed_data['Timely'] = complete_date['timely']
    td = transformed_data.groupby(['Product','Issue','Sub_product','Submitted_via','Company','Date_received','Month_year','State', 'Sub_issue','Company_Response'],as_index=True).size().reset_index(name='Complaint_id')
    td = td.drop(['Month_year'] , axis = 1) 
    kwargs['ti'].xcom_push(key='transformed_data' , value = td)

def get_all_state_data(**kwargs):
    url = 'https://gist.githubusercontent.com/mshafrir/2646763/raw/8b0dbb93521f5d6889502305335104218454c2bf/states_hash.json'
    response = requests.get(url = url)
    response = response.json()
    keys = response.keys()
    complete_date = []
    count = 0
    for key in keys:
        if count == 10:
            break
        data = get_data_from_api(days_ago=365 , state=key)
        complete_date.append(data)
        count += 1
    
    
    kwargs['ti'].xcom_push(key='api_data' , value = complete_date)
    return 'success'

def get_data_from_api(date_received_min='', date_received_max='', days_ago=20, state='WA'):
    """this will get the required data from api"""
    if any([date_received_min == '' , date_received_max=='']) and days_ago == 0:
        return 'missing parameters'

    server_endpoint = 'https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/'
    headers = {
    'accept': 'application/json',
    }
    if days_ago > 0:
        date_received_min, date_received_max = calculate_start_and_end_date(days_ago=days_ago)
   
    params = {
        'field': 'complaint_what_happened',
        # 'size': '100',
        'date_received_min': date_received_min,
        'date_received_max':  date_received_max,
        'state': state
    }
    response = requests.get(url=server_endpoint, headers=headers, params=params)
    print(response)
    data = response.json()
    #complete_date = pd.DataFrame()
    # try:
    #     for doc in data['hits']['hits'][:]:
    #         doc_updated = doc['_source']
    #         doc_updated.update({'_id':doc['_id']})
    #         # print()
    #         # print(doc_updated)
    #         complete_date = complete_date.append(doc_updated, ignore_index=True)
    # except:
    #     return pd.DataFrame()
    return data

def upload_data_to_sheet(**kwargs):
    
    data = kwargs['ti'].xcom_pull(key='transformed_data' , task_ids = 'transform_api_data')
    
    credentials = get_gcp_credentials()
    sheet_client = build(
        'sheets', 'v4', 
        credentials=credentials,
        cache_discovery=False)
    range_name = 'Sheet1'
    sheet_id = '19YPF4fZxCAU4ghhTrWH1rTAUvYu_4lfZedGdKC31BTg'
    
    columns = list(data.columns)
    values = [list(i) for i in data.itertuples(index=False)]
    results = get_sheet_data(sheet_client, sheet_id, range_name)
    try:
        results['values']
    except:
        # if data is not present then adding the column as well.
        values.insert(0, columns)
    update_results = update_sheet_data(sheet_client, sheet_id, values, range_name)
    print(update_results)
    
