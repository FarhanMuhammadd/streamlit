import streamlit as st
import pandas as pd
from googleapiclient.discovery import build
import json
from google.oauth2 import service_account
import plotly.express as px 


st.set_page_config(page_title="Dashboard", page_icon=":bar_chart:", layout="wide")

credentials =  service_acc = '''
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
sheet_client = build(
    'sheets', 'v4', 
    credentials=credentials,
    cache_discovery=False)
range_name = 'Sheet1'
sheet_id = '19YPF4fZxCAU4ghhTrWH1rTAUvYu_4lfZedGdKC31BTg'

#st.title(':bar_chart: Dashboard')
#st.markdown('##')

try:
    results = sheet_client.spreadsheets().values().get(
        spreadsheetId=sheet_id, range=range_name).execute()
        # print('{0} cells updated.'.format(result.get('updatedCells')))
except Exception as e:
        print("exception in get_set_jobid: ",str(e))

df = pd.DataFrame(results['values'][1:] , columns = results['values'][0])


st.sidebar.header('Please Filter Here:')
state = st.sidebar.multiselect("Select the state", options = df['State'].unique(),default = df['State'].unique())


df_selection = df.query("State == @state")
st.header('DataFrame:')
st.dataframe(df_selection)




st.header('KPIs:')
total_compalints = pd.to_numeric(df_selection['Complaint_id']).sum()
total_complaints_with_closed_status = pd.to_numeric(df_selection.loc[df_selection['Company_Response'] == 'Closed with explanation', 'Complaint_id']).sum()
total_complaints_with_in_progress = pd.to_numeric(df_selection.loc[df_selection['Company_Response'] == 'In progress', 'Complaint_id']).sum()
df_selection['Complaint_id'] = pd.to_numeric(df_selection['Complaint_id'])
dd = df_selection.groupby('Product')['Complaint_id'].sum()    
dd1 = df_selection.groupby('Date_received')['Complaint_id'].sum()   
dd2 = df_selection.groupby('Submitted_via')['Complaint_id'].sum()  
fig = px.pie(dd2 , values = dd2.values , names = dd2.index)
with st.container():
    col2 , col3 ,col4= st.columns(3)
    with col2:
        st.text('Total Complaints')
        st.text(total_compalints)
    
    with col3:
        st.text('Complaints Closed Status')
        st.text(total_complaints_with_closed_status)
    
    with col4:
        st.text('Complaints In Progress Status')
        st.text(total_complaints_with_in_progress)


with st.container():
    col1,col5 = st.columns(2)
    with col1:
        st.text('Complaints by Product')
        st.bar_chart(dd)
    with col5:
        st.text('Complaints by Date')
        st.line_chart(dd1)
            
            
with st.container():
    st.text('Complaints Submitted Via')
    st.plotly_chart(fig)
  
    
    
            
            
            
            
