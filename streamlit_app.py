import os
import streamlit as st
from google.cloud import bigquery
import pandas as pd
import os
import base64
import json
from google.oauth2 import service_account


# Function to get credentials
def get_credentials():
    encoded_credentials = st.secrets['GOOGLE_CREDENTIALS']
    if not encoded_credentials:
        raise EnvironmentError("Missing GOOGLE_CREDENTIALS in environment variables.")
    json_creds = base64.b64decode(encoded_credentials).decode()
    credentials_dict = json.loads(json_creds)
    return service_account.Credentials.from_service_account_info(credentials_dict)

# Initialize BigQuery client with credentials
credentials = get_credentials()

# Initialize BigQuery client
project = 'peachy-268419'
client = bigquery.Client(project=project, credentials=credentials)

import nps
import referrals

# Streamlit interface
start_date = st.sidebar.date_input("Start Date", value=pd.to_datetime("2023-01-01"))
end_date = st.sidebar.date_input("End Date", value=pd.to_datetime("2024-12-31"))

nps.run(client, start_date, end_date)
referrals.run(client, start_date, end_date)