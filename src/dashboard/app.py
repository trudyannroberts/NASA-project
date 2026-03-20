import streamlit as st
import sys
import os
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.append(PROJECT_ROOT)
from src.dashboard import mars_weather, neo, picture


st.set_page_config(page_title="Explore Space", layout="wide")

st.markdown("""
    <style>
        .block-container {
            max-width: 1100px;
            margin: auto;
            padding-left: 2rem;
            padding-right: 2rem;
        }
    </style>
""", unsafe_allow_html=True)

st.markdown(
    "<h1 style='text-align: center; color: #1f77b4; padding: 10px;'>Explore Space</h1>",
    unsafe_allow_html=True
)

pic_tab, neo_tab, mars_tab = st.tabs(["Picture of the day", "Near Earth Objects", "Mars Weather"])

with pic_tab:
    st.markdown("## Picture of the Day")
    picture.show_picture()
with mars_tab:
    # Middle section: Mars weather
    with st.container():
        st.markdown("## Mars Weather")
        mars_weather.show_mars_weather()
with neo_tab:
    # Bottom section: Near Earth Objects
    with st.container():
        st.markdown("## Near Earth Objects")
        neo.show_neo()