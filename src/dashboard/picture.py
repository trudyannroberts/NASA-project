import streamlit as st
import sys
import os
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.append(PROJECT_ROOT)
from src.db.db_picture import fetch_picture


def show_picture():
    st.title("Picture of the day")

    pic_row = fetch_picture()
    if not pic_row:
        st.warning("No picture available")
        return

    date, description, copyright, url = pic_row[0]

    st.subheader(date)
    st.image(url, use_container_width=True)
    st.caption(description)
    if copyright:
        st.caption(f"© {copyright}")
