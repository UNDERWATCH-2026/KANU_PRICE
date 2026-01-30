import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from supabase import create_client
from openai import OpenAI

# =========================
# Secrets
# =========================
SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

client = None
if "OPENAI_API_KEY" in st.secrets:
    client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])


# =========================
# UI 설정
# =========================
st.set_page_config(layout="wide")
st.title("Capsule Price Intelligence")

# =========================
# 이벤트 한글 매핑
# =========================
EVENT_LABEL = {
    "DISCOUNT_START": "할인 시작",
    "DISCOUNT_END": "할인 종료",
    "NORMAL_UP": "정상가 인상",
    "NORMAL_DOWN": "정상가 인하",
    "SALE_UP": "할인가 인상",
    "SALE_DOWN": "할인가 인하",
    "NEW": "신제품 출시",
    "OUT_OF_STOCK": "품절",
    "RESTOCK": "재입고"
}


# =========================
# 유틸
# =========================
def format_price(v):
    if v is None:
        return "-"
    return f"{int(v):,}"


def kpi(label, value):
    st.metric(label, value)


# =========================
# 입력 영역
# =========================
c1, c2 = st.columns([3,2])

with c1:
    product_input = st.text_input("제품명 (쉼표로 여러 개 가능)", "쥬시")

with c2:
    date_range = st.date_input("기간 선택", [])


products = [p.strip() for p in product_input.split(",") if p.strip()]


# =========================
# 데이터 조회
# =========================
if products:

    # ---------- 가격 ----------
    price_res = (
        supabase.table("product_price_events_enriched")
        .select("*")
        .execute()
    )

    price_df = pd.DataFrame(price_res.data)

    # ---------- presence ----------
    pres_res = (
        supabase.table("product_presence_events")
        .select("*")
        .execute()
    )

    pres_df = pd.DataFrame(pres_res.data)


    # =========================
    # 필터
    # =========================
    price_df = price_df[price_df.product_name.str.contains("|".join(products))]
    pres_df = pres_df[pres_df.product_name.str.contains("|".join(products))]

    if len(date_range) == 2:
        start, end = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
        price_df = price_df[(price_df.event_date >= start) & (price_df.event_date <= end)]
        pres_df = pres_df[(pres_df.event_date >= start) & (pres_df.event_date <= end)]


    # =========================
    # KPI 계산
    # =========================
    discount_start = (price_df.price_event_type == "DISCOUNT_START").sum()
    discount_end = (price_df.price_event_type == "DISCOUNT_END").sum()
    normal_change = price_df.price_event_type.isin(["NORMAL_UP","NORMAL_DOWN"]).sum()
    sale_change = price_df.price_event_type.isin(["SALE_UP","SALE_DOWN"]).sum()

    new_cnt = (pres_df.event_type == "NEW").sum()
    oos_cnt = (pres_df.event_type == "OUT_OF_STOCK").sum()
    restock_cnt = (pres_df.event_type == "RESTOCK").sum()


    cols = st.columns(7)

    with cols[0]: kpi("할인 시작", discount_start)
    with cols[1]: kpi("할인 종료", discount_end)
    with cols[2]: kpi("정상가 변동", normal_change)
    with cols[3]: kpi("할인가 변동", sale_change)
    with cols[4]: kpi("신제품 출시", new_cnt)
    with cols[5]: kpi("품절", oos_cnt)
    with cols[6]: kpi("재입고", restock_cnt)


    # =========================
    # 차트 (단가 비교)
    # =========================
    st.subheader("📈 단가 추이")

    fig = go.Figure()

    for p in products:
        sub = price_df[price_df.product_name == p]
        fig.add_trace(
            go.Scatter(
                x=sub.event_date,
                y=sub.current_unit_price,
                name=p
            )
        )

    st.plotly_chart(fig, use_container_width=True)


    # =========================
    # 타임라인
    # =========================
    st.subheader("📜 이벤트 히스토리")

    pres_df["price_event_type"] = pres_df["event_type"]
    pres_df["current_unit_price"] = None

    merged = pd.concat([price_df, pres_df], ignore_index=True)
    merged = merged.sort_values("event_date")

    for _, r in merged.iterrows():
        label = EVENT_LABEL.get(r["price_event_type"], r["price_event_type"])

        unit = ""
        if pd.notna(r["current_unit_price"]):
            unit = f" | {format_price(r['current_unit_price'])}원/개"

        st.write(f"{r['event_date']} · {r['product_name']} · {label}{unit}")
