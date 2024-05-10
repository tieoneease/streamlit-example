import pandas as pd
import streamlit as st
import plotly.graph_objects as go

filename = 'results_nps_old.csv'

def debug_data(old, new):
    print("Old Data Sample:")
    print(old.head())
    print("Old Data Types:")
    print(old.dtypes)

    print("New Data Sample:")
    print(new.head())
    print("New Data Types:")
    print(new.dtypes)


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
        df['category'] = df['score'].apply(lambda x: 'Promoter' if x >= 9 else ('Detractor' if x <= 6 else 'Passive'))
        df.drop(columns=['score'])

        # Group and calculate total responses as in your existing fetch_data function
        grouped = df.groupby(['year', 'quarter', 'service_type', 'category']).size().reset_index(name='total_responses')

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
        CASE 
            WHEN score >= 9 THEN 'Promoter'
            WHEN score <= 6 THEN 'Detractor'
            ELSE 'Passive'
        END AS category,
        COUNT(*) as total_responses
    FROM
        `treatment_analytics.mv_responses_appointments`
    WHERE
        appointment_date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY
        year, quarter, surveyName, service_type, category
    """
    query_job = client.query(query)
    data = query_job.to_dataframe()
    if 'total_responses' not in data.columns:
        raise ValueError("Failed to retrieve 'total_responses' column. Check SQL query and output.")
    return data

def fetch_total_appointments(client, start_date, end_date):
    query = f"""
    SELECT
        EXTRACT(YEAR FROM appointment_date) AS year,
        EXTRACT(QUARTER FROM appointment_date) AS quarter,
        CASE
            WHEN service_booked LIKE '%Treatment%' THEN 'Returning'
            WHEN service_booked LIKE '%Consultation%' THEN 'New'
            ELSE 'Other'
        END AS service_type,
        COUNT(*) AS total_appointments
    FROM
        `firebase.appointments`
    WHERE
        appointment_date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY
        year, quarter, service_type
    """
    query_job = client.query(query)
    return query_job.to_dataframe()

def calculate_nps(data):
    pivot = data.pivot(index=['year', 'quarter', 'surveyName', 'service_type'], columns='category', values='total_responses').fillna(0)
    pivot['NPS'] = ((pivot['Promoter'] - pivot['Detractor']) / (pivot['Promoter'] + pivot['Passive'] + pivot['Detractor'])) * 100
    pivot.reset_index(inplace=True)
    # Standardize quarter formatting
    pivot['Quarter'] = pivot.apply(lambda row: f"{int(row['quarter'])}Q{int(row['year']) % 100:02d}", axis=1)
    return pivot

def calculate_total_responses(data):
    # Sum up all responses by year and quarter
    total_by_quarter = data.groupby(['year', 'quarter']).agg({'total_responses': 'sum'}).reset_index()
    # Standardize quarter formatting
    total_by_quarter['Quarter'] = total_by_quarter.apply(lambda row: f"{int(row['quarter'])}Q{int(row['year']) % 100:02d}", axis=1)
    return total_by_quarter

def plot_nps(data):
    st.title("Quarterly Trends: Results NPS")
    fig = go.Figure()

    surveys = [('14 Day Post Treatment', 'Results NPS')]
    service_types = ['Returning', 'New']
    for survey_label, survey_title in surveys:
        for service_type in service_types:
            service_data = data[(data['surveyName'] == survey_label) & (data['service_type'] == service_type)]
            if not service_data.empty:
                fig.add_trace(go.Scatter(
                    x=service_data['Quarter'],
                    y=service_data['NPS'],
                    mode='lines+markers',
                    name=f"{survey_title} - {service_type}"
                ))

    fig.update_layout(
        title='NPS Trend by Quarter, Survey, and Service Type',
        xaxis_title='Quarter',
        yaxis_title='NPS',
        legend_title='Service Type',
        xaxis=dict(tickmode='array', tickvals=service_data['Quarter'], ticktext=service_data['Quarter'])
    )
    st.plotly_chart(fig)

def plot_total_responses(data):
    st.title("Total Responses Over Time")
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=data['Quarter'],
        y=data['total_responses'],
        mode='lines+markers',
        name='Total Responses'
    ))

    fig.update_layout(
        title='Total Scored Responses by Quarter',
        xaxis_title='Quarter',
        yaxis_title='Total Responses',
        legend_title='Response Type',
        xaxis=dict(tickmode='array', tickvals=data['Quarter'], ticktext=data['Quarter'])
    )
    st.plotly_chart(fig)

def merge_data(old, new):
    # Merge the two datasets on common columns and sum the total_responses from both
    merged_data = pd.merge(old, new, on=['year', 'quarter', 'surveyName', 'service_type', 'category'], how='outer', suffixes=('_old', '_new'))

    # Instead of using inplace=True in an assignment chain, directly assign the filled values to the columns
    merged_data['total_responses_old'] = merged_data['total_responses_old'].fillna(0)
    merged_data['total_responses_new'] = merged_data['total_responses_new'].fillna(0)

    # Sum the responses from both datasets
    merged_data['total_responses'] = merged_data['total_responses_old'] + merged_data['total_responses_new']

    # Drop the original total_responses columns as they are no longer needed
    merged_data.drop(columns=['total_responses_old', 'total_responses_new'], inplace=True)

    # Fill NaN values in the merged DataFrame to ensure no missing data issues
    merged_data.fillna(0, inplace=True)

    return merged_data

def calculate_response_percentages(total_responses, total_appointments):
    # Merge the response data with appointment totals
    merged_data = pd.merge(total_responses, total_appointments, on=['year', 'quarter', 'service_type'], how='left')
    
    # Sum up responses and calculate the percentages for each group
    aggregated_data = merged_data.groupby(['year', 'quarter', 'service_type']).agg(
        total_responses=pd.NamedAgg(column='total_responses', aggfunc='sum'),
        total_appointments=pd.NamedAgg(column='total_appointments', aggfunc='sum')
    ).reset_index()

    # Calculate response percentages
    aggregated_data['response_percentage'] = (aggregated_data['total_responses'] / aggregated_data['total_appointments']) * 100
    aggregated_data['Quarter'] = aggregated_data.apply(lambda row: f"{int(row['quarter'])}Q{int(row['year']) % 100:02d}", axis=1)
    
    return aggregated_data


def plot_response_rates(data):
    st.title("Response Rates as % of Total Appointments")
    fig = go.Figure()

    for service_type in ['Returning', 'New']:
        service_data = data[data['service_type'] == service_type]
        if not service_data.empty:
            fig.add_trace(go.Scatter(
                x=service_data['Quarter'],
                y=service_data['response_percentage'],
                mode='lines+markers',  # Change from bars to line with markers
                name=f'{service_type} Clients',
                text=service_data['response_percentage'].apply(lambda x: f'{x:.2f}%'),
                textposition='top center'  # Adjust text position for clarity
            ))

    fig.update_layout(
        title='Quarterly Response Rates by Client Type',
        xaxis_title='Quarter',
        yaxis_title='Response Rate (%)',
        legend_title='Client Type',
        xaxis=dict(tickmode='array', tickvals=service_data['Quarter'], ticktext=service_data['Quarter'])
    )
    st.plotly_chart(fig)

def run(client, start_date, end_date):
    # Fetch and process the data from BigQuery
    new_data = fetch_data(client, start_date, end_date)

    # Read and process the CSV data
    old_data = read_results_nps(filename)

    debug_data(old_data, new_data)

    # Merge both data sources
    merged_data = merge_data(old_data, new_data)

    # Calculate NPS from the merged data
    nps_data = calculate_nps(merged_data)
    plot_nps(nps_data)

    # Fetch and process appointment data
    total_appointments = fetch_total_appointments(client, start_date, end_date)

    print(merged_data.dtypes)
    print(total_appointments.dtypes)
    print(total_appointments.head())
    # Calculate response percentages
    response_percentages = calculate_response_percentages(merged_data, total_appointments)
    plot_response_rates(response_percentages)
