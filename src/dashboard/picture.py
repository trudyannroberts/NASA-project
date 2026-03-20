import streamlit as st
import sys
import os
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.append(PROJECT_ROOT)
from src.db.db_picture import fetch_picture, fetch_all_pictures


def show_picture():
    all_pics = fetch_all_pictures()
    if not all_pics:
        st.warning("No picture available")
        return

    # Default to the last item (today's date)
    if "current_index" not in st.session_state:
        st.session_state["current_index"] = len(all_pics) - 1

    def next_item():
        if st.session_state["current_index"] < len(all_pics) - 1:
            st.session_state["current_index"] += 1

    def prev_item():
        if st.session_state["current_index"] > 0:
            st.session_state["current_index"] -= 1

    current_item = all_pics[st.session_state["current_index"]]

    date = current_item[0].strftime("%B %d, %Y")
    description = current_item[1]
    copyright = current_item[2]
    url = current_item[3]

    st.subheader(date)

    st.image(url, use_container_width=True)
    st.caption(description)
    if copyright:
        st.caption(f"© {copyright}")

    # Navigation row
    col1, col2, col3 = st.columns([1, 7, 1])
    with col1:
        st.button("◀ Previous", on_click=prev_item,
                  disabled=st.session_state["current_index"] == 0)
    with col2:
        st.markdown(
            f"<p style='text-align:center'>{st.session_state['current_index'] + 1} / {len(all_pics)}</p>",
            unsafe_allow_html=True
        )
    with col3:
        st.button("Next ▶", on_click=next_item,
                  disabled=st.session_state["current_index"] == len(all_pics) - 1)