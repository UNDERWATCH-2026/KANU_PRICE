import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import re, io, os
from datetime import datetime, timedelta
from supabase import create_client
from openai import OpenAI

# =========================
# Supabase
# =========================
import os
from supabase import create_client

SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# =========================
# GPT (fallback only)
# =========================
from openai import OpenAI

client = None

if "OPENAI_API_KEY" in st.secrets:
    client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])

# =========================
# 기본 설정
# =========================
st.set_page_config(layout="wide")
st.title("Capsule Price Intelligence")

# =========================
# 공통 함수
# =========================
def format_price(v):
    if v is None:
        return "-"
    return f"{int(v):,}"

def kpi(label, value, key):
    clicked = st.session_state.get("event_filter") == key
    if st.button(f"{label}\n{value}", key=key):
        st.session_state["event_filter"] = None if clicked else key

# =========================
# GPT fallback 파서
# =========================
def gpt_parse_query(text):
    prompt = f"""
가격 조회 조건을 JSON으로만 반환.
설명 금지.

keys:
products
event_types
start_date
end_date

문장:
{text}
"""
    try:
        res = client.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0,
            max_tokens=100,
            messages=[{"role":"user","content":prompt}]
        )
        import json
        return json.loads(res.choices[0].message.content)
    except:
        return {}

# =========================
# Regex + GPT 파서
# =========================
def parse_query(text, df):

    result = {}

    if "할인" in text:
        result["event_types"] = ["DISCOUNT_START", "DISCOUNT_END"]

    if "정상가" in text:
        result["event_types"] = ["NORMAL_UP", "NORMAL_DOWN"]

    if "판매가" in text:
        result["event_types"] = ["SALE_UP", "SALE_DOWN"]

    if "지난달" in text:
        today = datetime.today()
        first = today.replace(day=1) - timedelta(days=1)
        result["start_date"] = first.replace(day=1)
        result["end_date"] = first

    # 제품명 자동 매칭
    found = [
        p for p in df["product_name"].unique()
        if p.lower() in text.lower()
    ]
    if found:
        result["products"] = found

    # fallback
    if not result:
        result.update(gpt_parse_query(text))

    return result

# =========================
# 상단 필터
# =========================
col1, col2 = st.columns(2)

with col1:
    product_input = st.text_input("제품명 (쉼표로 여러 개 가능)")

with col2:
    date_range = st.date_input("기간 선택", value=[])

query_text = st.text_input("💬 자연어 질문")

# =========================
# 데이터 조회
# =========================
if product_input:

    products = [p.strip() for p in product_input.split(",")]

    query = supabase.table("product_price_events_enriched").select("*")

    for p in products:
        query = query.ilike("product_name", f"%{p}%")

    res = query.execute()
    df = pd.DataFrame(res.data)

    if df.empty:
        st.warning("데이터 없음")
        st.stop()

    df["event_date"] = pd.to_datetime(df["event_date"])

    # ----------------------
    # 자연어 필터
    # ----------------------
    if query_text:
        parsed = parse_query(query_text, df)

        if "products" in parsed:
            df = df[df["product_name"].isin(parsed["products"])]

        if "event_types" in parsed:
            df = df[df["price_event_type"].isin(parsed["event_types"])]

        if "start_date" in parsed:
            df = df[df["event_date"] >= pd.to_datetime(parsed["start_date"])]

        if "end_date" in parsed:
            df = df[df["event_date"] <= pd.to_datetime(parsed["end_date"])]

    # ----------------------
    # 기간 필터
    # ----------------------
    if len(date_range) == 2:
        start, end = date_range
        df = df[(df["event_date"] >= pd.to_datetime(start)) &
                (df["event_date"] <= pd.to_datetime(end))]

    # =========================
    # KPI
    # =========================
    c1, c2, c3, c4 = st.columns(4)

    with c1:
        kpi("할인 시작", (df.price_event_type=="DISCOUNT_START").sum(), "DISCOUNT_START")

    with c2:
        kpi("할인 종료", (df.price_event_type=="DISCOUNT_END").sum(), "DISCOUNT_END")

    with c3:
        kpi("정상가 변동",
            df.price_event_type.isin(["NORMAL_UP","NORMAL_DOWN"]).sum(),
            "NORMAL")

    with c4:
        kpi("판매가 변동",
            df.price_event_type.isin(["SALE_UP","SALE_DOWN"]).sum(),
            "SALE")

    if st.session_state.get("event_filter"):
        ef = st.session_state["event_filter"]

        if ef == "NORMAL":
            df = df[df.price_event_type.isin(["NORMAL_UP","NORMAL_DOWN"])]
        elif ef == "SALE":
            df = df[df.price_event_type.isin(["SALE_UP","SALE_DOWN"])]
        else:
            df = df[df.price_event_type == ef]

    st.divider()

    # =========================
    # 📈 비교 차트 + 할인 shading
    # =========================
    if st.toggle("📈 제품 비교 차트"):

        fig = go.Figure()
        colors = px.colors.qualitative.Set2

        for i,(product,g) in enumerate(df.groupby("product_name")):

            g = g.sort_values("event_date")
            color = colors[i%len(colors)]

            fig.add_trace(
                go.Scatter(
                    x=g["event_date"],
                    y=g["current_unit_price"],
                    mode="lines+markers",
                    name=product,
                    line=dict(color=color,width=3)
                )
            )

            discount_start=None
            for _,r in g.iterrows():

                if r["price_event_type"]=="DISCOUNT_START":
                    discount_start=r["event_date"]

                if r["price_event_type"]=="DISCOUNT_END" and discount_start:
                    fig.add_vrect(
                        x0=discount_start,
                        x1=r["event_date"],
                        fillcolor=color,
                        opacity=0.12,
                        layer="below",
                        line_width=0
                    )
                    discount_start=None

        fig.update_layout(height=450, hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # =========================
    # 📦 제품 히스토리
    # =========================
    st.subheader("제품 히스토리")

    for product,g in df.sort_values("event_date").groupby("product_name"):
        st.markdown(f"### {product}")
        for _,r in g.iterrows():
            st.markdown(
                f"- {r['event_date'].date()} | {r['price_event_type']} | {format_price(r['current_unit_price'])}원/개"
            )

# =========================
# 📥 주차 리포트 다운로드
# =========================
st.divider()

if st.button("📥 전체 제품 주차 리포트 Excel 다운로드"):

    res = supabase.table("weekly_price_summary").select("*").execute()
    df = pd.DataFrame(res.data)

    df["행사여부"] = df["has_discount"].map({True:"행사",False:"-"})
    df["행사기간"] = df.apply(
        lambda r: f"{r['discount_start']} ~ {r['discount_end']}"
        if r["has_discount"] else "-", axis=1
    )

    df = df.rename(columns={
        "brand":"제조사",
        "category1_raw":"카테고리1",
        "category2_raw":"카테고리2",
        "product_name":"제품명",
        "normal_price":"정상가",
        "week_start":"주차"
    })

    output = io.BytesIO()
    df.to_excel(output, index=False)

    st.download_button(
        "엑셀 다운로드",
        output.getvalue(),
        "weekly_price_report.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

