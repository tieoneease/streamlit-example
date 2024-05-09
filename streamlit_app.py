import os
import streamlit as st
from google.cloud import bigquery
import pandas as pd
import os
import base64
import json
from google.oauth2 import service_account

# Retrieve the encoded JSON from the environment variable
encoded_credentials = os.getenv('GOOGLE_CREDENTIALS')
if not encoded_credentials:
    raise EnvironmentError("Missing ENCODED_GOOGLE_CREDENTIALS in environment variables.")


# Decode the JSON back to its original format
json_creds = base64.b64decode(encoded_credentials).decode()

# Convert the JSON string back to a dictionary
credentials_dict = json.loads(json_creds)

# Create credentials from the service account dictionary
credentials = service_account.Credentials.from_service_account_info(credentials_dict)

import nps
import referrals

# Initialize BigQuery client
project = 'peachy-268419'
client = bigquery.Client(project=project, credentials=credentials)

# Streamlit interface
start_date = st.sidebar.date_input("Start Date", value=pd.to_datetime("2023-01-01"))
end_date = st.sidebar.date_input("End Date", value=pd.to_datetime("2024-12-31"))

nps.run(client, start_date, end_date)
referrals.run(client, start_date, end_date)