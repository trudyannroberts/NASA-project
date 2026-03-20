import streamlit as st
import plotly.graph_objects as go
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

    fig = go.Figure()

    # Min temp bar
    fig.add_trace(go.Bar(
        x=df['date'],
        y=df['min_temp'],
        name='Min Temperature',
        marker_color='steelblue',
        opacity=0.7
    ))

    # Max temp bar
    fig.add_trace(go.Bar(
        x=df['date'],
        y=df['max_temp'],
        name='Max Temperature',
        marker_color='lightblue',
        opacity=0.7
    ))

    # Avg temp line
    fig.add_trace(go.Scatter(
        x=df['date'],
        y=df['avg_temp'],
        name='Avg Temperature',
        mode='lines+markers',
        line=dict(color='gold', width=2),
        marker=dict(size=4)
    ))

    fig.update_layout(
        barmode='group',
        xaxis_title='Date',
        yaxis_title='Temperature (°C)',
        yaxis=dict(autorange="reversed"),
        hovermode='x unified',
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
    )

    return st.plotly_chart(fig, use_container_width=True)