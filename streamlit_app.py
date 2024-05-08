import os
import streamlit as st
from google.cloud import bigquery
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt

# Set the Google Cloud credentials environment variable
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service-account.json"

# Initialize BigQuery client
project = 'peachy-268419'
client = bigquery.Client(project=project)

def fetch_data(start_date, end_date):
    query = f"""
    SELECT
        EXTRACT(YEAR FROM appointment_date) AS year,
        EXTRACT(QUARTER FROM appointment_date) AS quarter,
        surveyName,
        CASE
            WHEN service_booked IS NULL OR service_booked LIKE '%Dose Adjustment%' THEN 'Other'
            WHEN service_booked LIKE '%Treatment%' THEN 'Returning'
            WHEN service_booked LIKE '%Consultation%' THEN 'New'
        END AS service_type,
        score,
        COUNT(*) as total_responses
    FROM
        `treatment_analytics.mv_responses_appointments`
    WHERE
        appointment_date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY
        year, quarter, surveyName, service_type, score
    """
    query_job = client.query(query)
    return query_job.to_dataframe()

def calculate_nps(data):
    data['category'] = data['score'].apply(lambda x: 'Promoter' if x >= 9 else ('Detractor' if x <= 6 else 'Passive'))
    summary = data.groupby(['year', 'quarter', 'surveyName', 'service_type', 'category']).agg({'total_responses': 'sum'}).reset_index()
    pivot = summary.pivot(index=['year', 'quarter', 'surveyName', 'service_type'], columns='category', values='total_responses').fillna(0)
    pivot['NPS'] = ((pivot['Promoter'] - pivot['Detractor']) / (pivot['Promoter'] + pivot['Passive'] + pivot['Detractor'])) * 100
    pivot.reset_index(inplace=True)
    pivot['Quarter'] = pivot.apply(lambda row: f"{row['year']}-Q{row['quarter']}", axis=1)
    return pivot

# Streamlit UI
st.title("NPS Dashboard")
start_date = st.date_input("Start Date", datetime.now().replace(year=datetime.now().year-1, month=1, day=1))
end_date = st.date_input("End Date", datetime.now())

if st.button("Calculate NPS"):
    data = fetch_data(start_date, end_date)
    if not data.empty:
        nps_data = calculate_nps(data)
        fig, ax = plt.subplots()
        surveys = [('14 Day Post Treatment', 'Results NPS'), ('Post Appointment', 'Visit NPS')]
        service_types = ['Other', 'Returning', 'New']
        for survey_label, survey_title in surveys:
            for service_type in service_types:
                service_data = nps_data[(nps_data['surveyName'] == survey_label) & (nps_data['service_type'] == service_type)]
                if not service_data.empty:
                    ax.plot(service_data['Quarter'], service_data['NPS'], label=f"{survey_title} - {service_type}")
        ax.set_xticks(nps_data['Quarter'].unique())
        ax.set_xticklabels(nps_data['Quarter'].unique(), rotation=45)
        ax.set_xlabel('Quarter')
        ax.set_ylabel('NPS')
        ax.set_title('NPS Trend by Quarter, Survey, and Service Type')
        ax.legend()
        st.pyplot(fig)
    else:
        st.write("No data available for the selected date range.")
