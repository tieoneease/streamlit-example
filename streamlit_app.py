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

def fetch_appointment_data(start_date, end_date):
    query = f"""
    SELECT
        appointment_id,
        EXTRACT(YEAR FROM appointment_date) AS year,
        EXTRACT(QUARTER FROM appointment_date) AS quarter,
        CASE
            WHEN service_finalized IS NULL OR service_finalized LIKE '%Dose Adjustment%' THEN 'Other'
            WHEN service_finalized LIKE '%Treatment%' THEN 'Returning'
            WHEN service_finalized LIKE '%Consultation%' THEN 'New'
        END AS service_type
    FROM
        `firebase.appointments`
    WHERE
        appointment_date BETWEEN '{start_date}' AND '{end_date}'
    """
    return client.query(query).to_dataframe()

def fetch_survey_data(start_date, end_date):
    survey_query = f"""
    SELECT
        appointment_id,
        EXTRACT(YEAR FROM appointment_date) AS year,
        EXTRACT(QUARTER FROM appointment_date) AS quarter,
        CASE
            WHEN service_booked IS NULL OR service_booked LIKE '%Dose Adjustment%' THEN 'Other'
            WHEN service_booked LIKE '%Treatment%' THEN 'Returning'
            WHEN service_booked LIKE '%Consultation%' THEN 'New'
        END AS service_type
    FROM
        `treatment_analytics.mv_responses_appointments`
    WHERE
        appointment_date BETWEEN '{start_date}' AND '{end_date}'
    """
    return client.query(survey_query).to_dataframe()

def calculate_response_ratios(appointments_data, survey_data):
    # Print columns for debugging
    print("Appointments columns:", appointments_data.columns)  # Debugging line
    print("Survey columns:", survey_data.columns)  # Debugging line

    # Merge data while specifying suffixes to resolve column name conflicts
    combined_data = pd.merge(
        appointments_data,
        survey_data,
        on='appointment_id',
        suffixes=('_appt', '_survey')
    )

    # Ensure 'year', 'quarter', and 'service_type' are aligned before grouping
    # Assuming 'service_type_appt' and 'service_type_survey' are always the same, we can simplify:
    combined_data['service_type'] = combined_data['service_type_appt']
    combined_data['year'] = combined_data['year_appt']
    combined_data['quarter'] = combined_data['quarter_appt']

    print("Combined columns:", combined_data.columns)  # Debugging line

    # Group by year, quarter, and service type to summarize
    grouped = combined_data.groupby(['year', 'quarter', 'service_type']).agg(
        total_appointments=pd.NamedAgg(column='appointment_id', aggfunc='nunique'),
        survey_responses=pd.NamedAgg(column='appointment_id', aggfunc='nunique')
    ).reset_index()

    # Calculate the response ratio
    grouped['response_ratio'] = (grouped['survey_responses'] / grouped['total_appointments']) * 100

    return grouped

def plot_response_trends(data):
    fig, ax = plt.subplots()

    # Prepare data
    quarter_labels = sorted(data['quarter_label'].unique())  # Sorted and unique quarters
    quarter_indices = {q: i for i, q in enumerate(quarter_labels)}  # Mapping quarters to indices

    # Plotting logic with numeric x-axis
    if 'New' in data['service_type'].values:
        new_client_data = data[data['service_type'] == 'New']
        new_x = [quarter_indices[q] for q in new_client_data['quarter_label']]
        ax.plot(new_x, new_client_data['response_ratio'], label='New Clients Responses (%)', marker='o', linestyle='-', color='blue')

    if 'Returning' in data['service_type'].values:
        returning_client_data = data[data['service_type'] == 'Returning']
        returning_x = [quarter_indices[q] for q in returning_client_data['quarter_label']]
        ax.plot(returning_x, returning_client_data['response_ratio'], label='Returning Clients Responses (%)', marker='s', linestyle='--', color='green')

    # Setting labels and title
    ax.set_xlabel('Quarter')
    ax.set_ylabel('Response Percentage')
    ax.set_title('Survey Response Percentage by Quarter and Client Type')
    ax.legend()

    # Setting x-ticks and labels
    ax.set_xticks(range(len(quarter_labels)))
    ax.set_xticklabels(quarter_labels, rotation=45)

    st.pyplot(fig)

# Streamlit UI
st.title("Responses Dashboard")
start_date = st.date_input("Start Date", datetime.now().replace(year=datetime.now().year-1, month=1, day=1))
end_date = st.date_input("End Date", datetime.now())

# Automatically load and display survey count and percentage plot
appointment_data = fetch_appointment_data(start_date, end_date)
survey_data = fetch_survey_data(start_date, end_date)
response_data = calculate_response_ratios(appointment_data, survey_data)
if not response_data.empty:
    response_data['quarter_label'] = response_data.apply(lambda row: f"{row['year']}-Q{row['quarter']}", axis=1)
    plot_response_trends(response_data)
else:
    st.write("No survey response data available for the selected date range.")