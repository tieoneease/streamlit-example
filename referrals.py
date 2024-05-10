import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery

def get_appointments_data(client, start_date, end_date):
    query = f"""
    SELECT
      EXTRACT(YEAR FROM a.appointment_date) AS year,
      EXTRACT(QUARTER FROM a.appointment_date) AS quarter,
      COUNT(*) AS total_appointments,
      COUNT(CASE WHEN a.service_booked LIKE '%Consultation%' OR a.service_booked LIKE '%Analysis%' THEN 1 END) AS total_new_appointments,
      COUNT(CASE WHEN (a.service_booked LIKE '%Consultation%' OR a.service_booked LIKE '%Analysis%') AND p.referred_by IS NOT NULL THEN 1 END) AS total_new_referrals,
      100.0 * COUNT(CASE WHEN (a.service_booked LIKE '%Consultation%' OR a.service_booked LIKE '%Analysis%') AND p.referred_by IS NOT NULL THEN 1 END) / 
      COUNT(CASE WHEN a.service_booked LIKE '%Consultation%' OR a.service_booked LIKE '%Analysis%' THEN 1 END) AS percent_new_referrals_of_new_appointments
    FROM
      `firebase.appointments` AS a
    LEFT JOIN
      `firebase.patients` AS p ON a.pacient_firebase_id = p.firebase_id
    WHERE
      a.appointment_date BETWEEN '{start_date}' AND '{end_date}'
      AND a.service_finalized = 'Wrinkle Relaxer'
    GROUP BY
      year, quarter
    ORDER BY
      year, quarter
    """
    query_job = client.query(query)
    df = query_job.to_dataframe()
    df['referral_to_new_ratio'] = df['total_new_referrals'] / df['total_new_appointments']
    return df

def run(client, start_date, end_date):
    data = get_appointments_data(client, start_date, end_date)
    # Format 'quarter_year' to the desired format like '1Q23', ensuring both 'quarter' and 'year' are treated as integers
    data['quarter_year'] = data.apply(lambda row: f"{int(row['quarter'])}Q{int(row['year']) % 100:02d}", axis=1)
    data['percent_new_referrals_of_new_appointments'] = data['percent_new_referrals_of_new_appointments'].map("{:.2f}%".format)
    
    # Creating the Plotly Graph Object figure
    fig = go.Figure()

    # Adding total new treatments bar (formerly appointments)
    fig.add_trace(go.Bar(
        x=data['quarter_year'],
        y=data['total_new_appointments'] - data['total_new_referrals'],
        name='New Treatments',
        hovertemplate='New Treatments: %{y}<extra></extra>'
    ))

    # Adding total new referral treatments bar (formerly referral appointments)
    fig.add_trace(go.Bar(
        x=data['quarter_year'],
        y=data['total_new_referrals'],
        name='New Referral Treatments',
        text=data['percent_new_referrals_of_new_appointments'],
        hovertemplate='New Referral Treatments: %{y}<br>Percent of New: %{text}<extra></extra>'
    ))

    # Add a secondary y-axis for the ratio
    fig.add_trace(go.Scatter(
        x=data['quarter_year'],
        y=data['referral_to_new_ratio'],
        name='Referral to New Treatment Ratio',
        mode='lines+markers',
        yaxis='y2',
        line=dict(color='red', width=2),
        hovertemplate='Ratio: %{y:.2f}<extra></extra>'
    ))

    # Update layout for stacked bar and secondary y-axis
    fig.update_layout(
        barmode='stack',
        title_text="Quarterly Trends: New and New Referral Treatments",
        yaxis=dict(title="Number of Treatments"),
        xaxis=dict(title="Quarter-Year", type='category'),
        yaxis2=dict(
            title="Ratio of New Referrals to New Treatments",
            overlaying='y',
            side='right',
            range=[0, 1.2],
            showgrid=False,
            color='red'
        )
    )

    st.plotly_chart(fig)
