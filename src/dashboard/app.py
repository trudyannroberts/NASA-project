import streamlit as st
import sys
import os
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.append(PROJECT_ROOT)
from src.dashboard import mars_weather, neo, picture


st.set_page_config(page_title="NASA Dashboard", layout="wide")
title = st.title("NASA Dashboard")
title.markdown(
    "<h1 style='text-align: center; color: #1f77b4; padding: 10px;'>NASA Dashboard</h1>",
    unsafe_allow_html=True
)
# Top section: Picture of the day
with st.container():
    st.markdown("## Picture of the Day")
    picture.show_picture()

# Middle section: Mars weather
with st.container():
    st.markdown("## Mars Weather")
    mars_weather.show_mars_weather()

# Bottom section: Near Earth Objects
with st.container():
    st.markdown("## Near Earth Objects")
    neo.show_neo()