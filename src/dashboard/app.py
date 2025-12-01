import sys
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT))

import streamlit as st
import plotly.express as px
import pandas as pd

from src.db.db_mars_weather import fetch_mars_weather
from src.db.db_neo import fetch_neo

def mars_weather_dash():
    mars_weather_rows = fetch_mars_weather()
    df = pd.DataFrame(
        [row[:5] for row in mars_weather_rows],
        columns=['sol', 'date', 'max_temp', 'min_temp', 'avg_temp']
    )

    # Melt to long form for clean legend
    df_melted = df.melt(
        id_vars='date',
        value_vars=['max_temp', 'min_temp', 'avg_temp'],
        var_name='Temperature Type',
        value_name='Temperature (°C)'
    )

    # Map nicer legend labels
    legend_mapping = {
        'max_temp': 'Maximum Temperature',
        'min_temp': 'Minimum Temperature',
        'avg_temp': 'Average Temperature'
    }
    df_melted['Temperature Type'] = df_melted['Temperature Type'].map(legend_mapping)

    # Plot
    fig = px.line(
        df_melted,
        x='date',
        y='Temperature (°C)',
        color='Temperature Type',
        labels={'date': 'Date'}
    )
    fig.update_layout(hovermode="x")
    st.plotly_chart(fig)

def neo_dash():
    neo_rows = fetch_neo()
    df = pd.DataFrame(
        [row[1:7] for row in neo_rows],
        columns=['name', 'min_diameter', 'max_diameter', 'is_potential_hazard', 'close_approach_date', 'miss_distance_km']
    )



mars_weather_dash()


