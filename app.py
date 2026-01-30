import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from supabase import create_client

# =====================================================
# 🔧 기본 설정
# =====================================================
st.set_page_config(layout="wide")
st.title("Capsule Price Intelligence")

SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# =====================================================
# 📌 유틸
# =====================================================
def safe_str(x):
    if pd.isna(x):
        return ""
    return str(x)


def format_price(v):
    if pd.isna(v):
        return "-"
    return f"{int(v):,}"


# =====================================================
# 📌 이벤트 매핑 + 아이콘
# =====================================================
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

EVENT_ICON = {
    "DISCOUNT_START": "💙",
    "DISCOUNT_END": "💙",
    "NORMAL_UP": "📈",
    "NORMAL_DOWN": "📉",
    "SALE_UP": "📈",
    "SALE_DOWN": "📉",
    "NEW": "🆕",
    "OUT_OF_STOCK": "⛔",
    "RESTOCK": "🔄"
}


# =====================================================
# 🔍 검색 FORM (버튼 + 엔터 지원)
# =====================================================
with st.form("search_form"):

    c1, c2, c3 = st.columns([4, 2, 1])

    with c1:
        product_input = st.text_input(
            "제품 키워드 (쉼표 가능)",
            placeholder="예: 스노우, 쥬시"
        )

    with c2:
        date_range = st.date_input("기간 선택", [])

    with c3:
        submitted = st.form_submit_button("조회하기", use_container_width=True)

if not submitted:
    st.stop()

keywords = [p.strip() for p in product_input.split(",") if p.strip()]

if not keywords:
    st.info("제품 키워드를 입력하세요.")
    st.stop()


# =====================================================
# 📦 제품 메타 조회 → 후보 선택
# =====================================================
meta_res = supabase.table("filter_products").select("*").execute()
meta_df = pd.DataFrame(meta_res.data or [])

meta_df["product_name"] = meta_df["product_name"].astype(str)

mask = meta_df["product_name"].apply(
    lambda x: any(k.lower() in safe_str(x).lower() for k in keywords)
)

meta_df = meta_df[mask]

if len(meta_df) == 0:
    st.warning("검색 결과 없음")
    st.stop()


st.subheader("📦 조회할 제품 선택")

selected_products = []

groups = meta_df.groupby(["brand", "category1", "category2"])

for (b, c1, c2), g in groups:
    with st.expander(f"{b} / {c1} / {c2} ({len(g)})"):
        for name in g["product_name"]:
            if st.checkbox(name, key=name):
                selected_products.append(name)

if not selected_products:
    st.stop()


# =====================================================
# 📊 가격 이벤트 조회
# =====================================================
price_res = supabase.table("product_price_events_enriched").select("*").execute()
price_df = pd.DataFrame(price_res.data or [])

price_df = price_df.reindex(columns=[
    "product_name", "event_date", "price_event_type", "current_unit_price"
])

price_df["event_date"] = pd.to_datetime(price_df["event_date"])
price_df["current_unit_price"] = pd.to_numeric(price_df["current_unit_price"], errors="coerce")

price_df = price_df[price_df["product_name"].isin(selected_products)]


# =====================================================
# 📊 presence 이벤트
# =====================================================
pres_res = supabase.table("product_presence_events").select("*").execute()
pres_df = pd.DataFrame(pres_res.data or [])

pres_df = pres_df.reindex(columns=["product_name", "event_date", "event_type"])
pres_df["event_date"] = pd.to_datetime(pres_df["event_date"])
pres_df = pres_df[pres_df["product_name"].isin(selected_products)]


# =====================================================
# 📅 날짜 필터
# =====================================================
if len(date_range) == 2:
    s, e = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
    price_df = price_df[(price_df.event_date >= s) & (price_df.event_date <= e)]
    pres_df = pres_df[(pres_df.event_date >= s) & (pres_df.event_date <= e)]


# =====================================================
# 📌 KPI 카드
# =====================================================
st.divider()

def kpi(label, value, icon):
    st.metric(f"{icon} {label}", int(value))

cols = st.columns(7)

with cols[0]: kpi("할인 시작", (price_df.price_event_type=="DISCOUNT_START").sum(), "💙")
with cols[1]: kpi("할인 종료", (price_df.price_event_type=="DISCOUNT_END").sum(), "💙")
with cols[2]: kpi("정상가 변동", price_df.price_event_type.isin(["NORMAL_UP","NORMAL_DOWN"]).sum(), "📈")
with cols[3]: kpi("할인가 변동", price_df.price_event_type.isin(["SALE_UP","SALE_DOWN"]).sum(), "📉")
with cols[4]: kpi("신제품", (pres_df.event_type=="NEW").sum(), "🆕")
with cols[5]: kpi("품절", (pres_df.event_type=="OUT_OF_STOCK").sum(), "⛔")
with cols[6]: kpi("재입고", (pres_df.event_type=="RESTOCK").sum(), "🔄")


# =====================================================
# 📈 단가 차트 + 할인 shading
# =====================================================
st.subheader("📈 단가 추이 (원/개)")

fig = go.Figure()

for name in selected_products:

    sub = price_df[price_df.product_name == name].sort_values("event_date")

    if len(sub) == 0:
        continue

    fig.add_trace(go.Scatter(
        x=sub["event_date"],
        y=sub["current_unit_price"],
        mode="lines+markers",
        name=name
    ))

    start = None

    for _, r in sub.iterrows():
        if r["price_event_type"] == "DISCOUNT_START":
            start = r["event_date"]
        elif r["price_event_type"] == "DISCOUNT_END" and start:
            fig.add_vrect(x0=start, x1=r["event_date"], fillcolor="lightblue", opacity=0.25)
            start = None

fig.update_layout(
    height=450,
    xaxis=dict(type="date", dtick="D1"),
    yaxis=dict(title="원/개", tickformat=","),
    legend_title="제품"
)

st.plotly_chart(fig, use_container_width=True)


# =====================================================
# 📜 이벤트 히스토리 (제품별 카드)
# =====================================================
st.subheader("📜 이벤트 히스토리")

pres_df["price_event_type"] = pres_df["event_type"]
pres_df["current_unit_price"] = None

merged = pd.concat([price_df, pres_df])

for product, g in merged.groupby("product_name"):

    st.markdown(f"### 📦 {product}")
    g = g.sort_values("event_date")

    for _, r in g.iterrows():
        icon = EVENT_ICON.get(r["price_event_type"], "•")
        label = EVENT_LABEL.get(r["price_event_type"], r["price_event_type"])
        price = f" | {format_price(r['current_unit_price'])}원/개" if pd.notna(r["current_unit_price"]) else ""
        st.write(f"{icon} {r['event_date'].date()} · {label}{price}")
