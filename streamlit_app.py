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

def fetch_appointments_data(start_date, end_date):
    query = f"""
    SELECT
        EXTRACT(YEAR FROM appointment_date) AS year,
        EXTRACT(QUARTER FROM appointment_date) AS quarter,
        CASE
            WHEN service_finalized IS NULL OR service_finalized LIKE '%Dose Adjustment%' THEN 'Other'
            WHEN service_finalized LIKE '%Treatment%' THEN 'Returning'
            WHEN service_finalized LIKE '%Consultation%' THEN 'New'
        END AS service_type,
        appointment_id,
        appointment_date,
        location,
        firebase_practitioner_name
    FROM
        `firebase.appointments`
    WHERE
        appointment_date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY
        year, quarter, service_type, appointment_id, appointment_date, location, firebase_practitioner_name
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

def fetch_combined_data(start_date, end_date):
    # Fetch survey data with service type categorized
    survey_query = f"""
    SELECT
        a.appointment_id,
        CASE
            WHEN a.service_booked IS NULL OR a.service_booked LIKE '%Dose Adjustment%' THEN 'Other'
            WHEN a.service_booked LIKE '%Treatment%' THEN 'Returning'
            WHEN a.service_booked LIKE '%Consultation%' THEN 'New'
        END AS service_type,
        a.score
    FROM
        `treatment_analytics.mv_responses_appointments` a
    WHERE
        a.appointment_date BETWEEN '{start_date}' AND '{end_date}'
    """

    # Fetch appointment data with service type categorized
    appointment_query = f"""
    SELECT
        b.appointment_id,
        CASE
            WHEN b.service_finalized IS NULL OR b.service_finalized LIKE '%Dose Adjustment%' THEN 'Other'
            WHEN b.service_finalized LIKE '%Treatment%' THEN 'Returning'
            WHEN b.service_finalized LIKE '%Consultation%' THEN 'New'
        END AS service_type
    FROM
        `firebase.appointments` b
    WHERE
        b.appointment_date BETWEEN '{start_date}' AND '{end_date}'
    """

    survey_data = client.query(survey_query).to_dataframe()
    appointment_data = client.query(appointment_query).to_dataframe()

    # Join on appointment_id
    combined_data = pd.merge(survey_data, appointment_data, on='appointment_id', suffixes=('_survey', '_appointment'))
    return combined_data

def calculate_percentages(data):
    # Count the number of surveys and appointments by service type
    survey_counts = data.groupby('service_type_survey').size().reset_index(name='survey_count')
    appointment_counts = data.groupby('service_type_appointment').size().reset_index(name='appointment_count')

    # Calculate percentages
    combined_counts = pd.merge(survey_counts, appointment_counts, left_on='service_type_survey', right_on='service_type_appointment')
    combined_counts['percentage'] = (combined_counts['survey_count'] / combined_counts['appointment_count']) * 100
    return combined_counts

def plot_survey_counts(data):
    result_data = calculate_percentages(data)
    fig, ax1 = plt.subplots()

    # Plotting
    colors = ['tab:blue', 'tab:orange']
    ax1.set_xlabel('Service Type')
    ax1.set_ylabel('Counts', color='tab:red')
    ax1.bar(result_data['service_type_survey'], result_data['survey_count'], color='tab:red')
    ax1.tick_params(axis='y', labelcolor='tab:red')

    ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis
    ax2.set_ylabel('Percentage', color='tab:blue')
    ax2.plot(result_data['service_type_survey'], result_data['percentage'], color='tab:blue')
    ax2.tick_params(axis='y', labelcolor='tab:blue')

    plt.title('Survey Response Counts and Percentages by Service Type')
    st.pyplot(fig)

def plot_nps(data):
    fig, ax = plt.subplots()
    surveys = [('14 Day Post Treatment', 'Results NPS'), ('Post Appointment', 'Visit NPS')]
    service_types = ['Other', 'Returning', 'New']
    for survey_label, survey_title in surveys:
        for service_type in service_types:
            service_data = data[(data['surveyName'] == survey_label) & (data['service_type'] == service_type)]
            if not service_data.empty:
                ax.plot(service_data['Quarter'], service_data['NPS'], label=f"{survey_title} - {service_type}")
    ax.set_xticks(data['Quarter'].unique())
    ax.set_xticklabels(data['Quarter'].unique(), rotation=45)
    ax.set_xlabel('Quarter')
    ax.set_ylabel('NPS')
    ax.set_title('NPS Trend by Quarter, Survey, and Service Type')
    ax.legend()
    st.pyplot(fig)

# Streamlit UI
st.title("NPS Dashboard")
start_date = st.date_input("Start Date", datetime.now().replace(year=datetime.now().year-1, month=1, day=1))
end_date = st.date_input("End Date", datetime.now())

# Automatically load and display NPS plot
nps_data = fetch_data(start_date, end_date)
if not nps_data.empty:
    plot_nps(calculate_nps(nps_data))
else:
    st.write("No NPS data available for the selected date range.")

# Automatically load and display survey count and percentage plot
survey_data = fetch_combined_data(start_date, end_date)
if not survey_data.empty:
    plot_survey_counts(calculate_percentages(survey_data))
else:
    st.write("No survey data available for the selected date range.")