import streamlit as st
import matplotlib.pyplot as plt
import pandas as pd
import os
import streamlit as st
from google.cloud import bigquery
from datetime import datetime

filename = 'results_nps_old.csv'

def read_results_nps(file):
    try:
        # Attempt to read the CSV file with UTF-16 encoding
        df = pd.read_csv(file, encoding='utf-16')
        # Rename columns to match the existing data processing logic
        df.rename(columns={
            'NPS': 'score',
            'service name': 'service_booked',
            'start at': 'appointment_date'
        }, inplace=True)

        # Convert 'appointment_date' from ISO timestamp to datetime, converting to UTC
        df['appointment_date'] = pd.to_datetime(df['appointment_date'], errors='coerce', utc=True)
        df = df.dropna(subset=['appointment_date'])

        # Convert 'score' to numeric and drop rows where 'score' is empty
        df['score'] = pd.to_numeric(df['score'], errors='coerce')
        df = df.dropna(subset=['score'])

        # Derive 'service_type' based on 'service_booked'
        def classify_service(service_booked):
            if service_booked is None or 'Dose Adjustment' in service_booked:
                return 'Other'
            elif 'Treatment' in service_booked:
                return 'Returning'
            elif 'Consultation' in service_booked:
                return 'New'
            return 'Other'  # Default case if no conditions are met

        df['service_type'] = df['service_booked'].apply(classify_service)
        # Extract year and quarter from 'appointment_date'
        df['year'] = pd.DatetimeIndex(df['appointment_date']).year
        df['quarter'] = pd.DatetimeIndex(df['appointment_date']).quarter

        # Group and calculate total responses as in your existing fetch_data function
        grouped = df.groupby(['year', 'quarter', 'service_type', 'score']).size().reset_index(name='total_responses')

        # Assume all responses are part of 'Results NPS' survey since the dataset only contains Results NPS
        grouped['surveyName'] = '14 Day Post Treatment'
        return grouped
    except Exception as e:
        print("Error reading file:", e)

def fetch_data(client, start_date, end_date):
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

def plot_nps(data):
    st.title("Quarterly Trends: Results NPS")
    fig, ax = plt.subplots()
    surveys = [('14 Day Post Treatment', 'Results NPS')]
    #surveys = [('14 Day Post Treatment', 'Results NPS'), ('Post Appointment', 'Visit NPS')]
    service_types = ['Returning', 'New']
    #service_types = ['Other', 'Returning', 'New']
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

def merge_data(old, new):
    # Merge the two datasets on common columns
    merged_data = pd.merge(old, new, on=['year', 'quarter', 'surveyName', 'service_type'], how='outer')

    # You might need to fill NaN values after merging if there are mismatches
    merged_data.fillna(0, inplace=True)
    return merged_data

def run(client, start_date, end_date):
    old = calculate_nps(fetch_data(client, start_date, end_date))
    new = read_results_nps(filename)
    merged = merge_data(old, new)
    calculated =calculate_nps(merged)
    plot_nps(calculated)