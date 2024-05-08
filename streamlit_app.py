import os
import streamlit as st
from google.cloud import bigquery
import pandas as pd
import nps
import referrals

# Set the Google Cloud credentials environment variable
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service-account.json"

# Initialize BigQuery client
project = 'peachy-268419'
client = bigquery.Client(project=project)

# Streamlit interface
start_date = st.sidebar.date_input("Start Date", value=pd.to_datetime("2023-01-01"))
end_date = st.sidebar.date_input("End Date", value=pd.to_datetime("2024-12-31"))

nps.run(client, start_date, end_date)
referrals.run(client, start_date, end_date)