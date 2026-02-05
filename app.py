import streamlit as st

st.title("RADIO TEST")

st.radio(
    "라디오 보이면 성공",
    ["A", "B"],
    horizontal=True
)
