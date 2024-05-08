import os
import streamlit as st
from google.cloud import bigquery
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import nps

# Set the Google Cloud credentials environment variable
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service-account.json"

# Initialize BigQuery client
project = 'peachy-268419'
client = bigquery.Client(project=project)
