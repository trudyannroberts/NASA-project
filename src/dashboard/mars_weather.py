import streamlit as st
import plotly.express as px
import pandas as pd
import sys
import os
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.append(PROJECT_ROOT)
from src.db.db_mars_weather import fetch_mars_weather

def show_mars_weather():
    mars_weather_rows = fetch_mars_weather()
    df = pd.DataFrame(
        [row[:5] for row in mars_weather_rows],
        columns=['sol', 'date', 'max_temp', 'min_temp', 'avg_temp']
    )
    df_melted = df.melt(
        id_vars='date',
        value_vars=['max_temp', 'min_temp', 'avg_temp'],
        var_name='Temperature Type',
        value_name='Temperature (°C)'
    )
    legend_mapping = {
        'max_temp': 'Maximum Temperature',
        'min_temp': 'Minimum Temperature',
        'avg_temp': 'Average Temperature'
    }
    df_melted['Temperature Type'] = df_melted['Temperature Type'].map(legend_mapping)

    fig = px.line(
        df_melted,
        x='date',
        y='Temperature (°C)',
        color='Temperature Type',
        labels={'date': 'Date'}
    )
    fig.update_layout(hovermode="x")
    return st.plotly_chart(fig)