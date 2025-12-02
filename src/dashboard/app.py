import streamlit as st
import sys
import os
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.append(PROJECT_ROOT)
from src.dashboard import mars_weather, neo, picture


col1, col2 = st.columns(2)


st.set_page_config(
    page_title="NASA Dashboard",
    layout="wide",
    initial_sidebar_state="auto"
)

with col1:
    mars_weather.show_mars_weather()

with col2:
    neo.show_neo()
picture.show_picture()


