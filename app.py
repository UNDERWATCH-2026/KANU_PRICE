import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from supabase import create_client

# =====================================================
# Supabase
# =====================================================
SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

st.set_page_config(layout="wide")
st.title("Capsule Price Intelligence")

# =====================================================
# 이벤트 한글/아이콘
# =====================================================
EVENT_LABEL = {
    "DISCOUNT_START": "💸 할인 시작",
    "DISCOUNT_END": "🔚 할인 종료",
    "NORMAL_UP": "⬆ 정상가 인상",
    "NORMAL_DOWN": "⬇ 정상가 인하",
    "SALE_UP": "🔺 할인가 인상",
    "SALE_DOWN": "🔻 할인가 인하",
    "NEW": "🆕 신제품 출시",
    "OUT_OF_STOCK": "❌ 품절",
    "RESTOCK": "♻ 재입고"
}

# =====================================================
# 유틸
# =====================================================
def format_price(v):
    if v is None or pd.isna(v):
        return "-"
    return f"{int(v):,}"

def kpi_card(label, value, key):
    if "event_filter" not in st.session_state:
        st.session_state.event_filter = None

    active = st.session_state.event_filter == key

    if st.button(
        f"{label}\n{int(value)}",
        use_container_width=True,
        key=f"kpi_{key}"
    ):
        st.session_state.event_filter = None if active else key


# =====================================================
# 1️⃣ 검색 키워드
# =====================================================
st.subheader("🔍 제품 검색")

col1, col2 = st.columns([3,1])

with col1:
    keyword_input = st.text_input(
        "제품명 키워드 (쉼표 가능)",
        placeholder="예: 스노우, 쥬시"
    )

with col2:
    run_btn = st.button("조회하기", type="primary")

if not run_btn:
    st.stop()

keywords = [k.strip() for k in keyword_input.split(",") if k.strip()]


# =====================================================
# 2️⃣ 제품 메타 (브랜드 그룹 선택)
# =====================================================
meta_res = supabase.table("filter_products").select(
    "brand, category1_raw, category2_raw, product_name"
).execute()

meta_df = pd.DataFrame(meta_res.data or [])

if keywords:
    mask = meta_df["product_name"].apply(
        lambda x: any(k.lower() in x.lower() for k in keywords)
    )
    meta_df = meta_df[mask]

st.subheader("📦 조회할 제품 선택")

selected_products = []

for brand, bdf in meta_df.groupby("brand"):

    with st.expander(f"🏷️ {brand}"):

        for cat1, c1df in bdf.groupby("category1_raw"):

            with st.expander(f"📂 {cat1}"):

                for cat2, c2df in c1df.groupby("category2_raw"):

                    st.markdown(f"**{cat2}**")

                    for p in sorted(c2df["product_name"].unique()):

                        if st.checkbox(p, key=f"chk_{p}"):
                            selected_products.append(p)

if not selected_products:
    st.info("제품을 선택하세요.")
    st.stop()


# =====================================================
# 3️⃣ 가격 데이터
# =====================================================
price_res = supabase.table("product_price_events_enriched").select("*").execute()
price_df = pd.DataFrame(price_res.data or [])

price_df["event_date"] = pd.to_datetime(price_df["event_date"], errors="coerce")
price_df["current_unit_price"] = pd.to_numeric(
    price_df["current_unit_price"], errors="coerce"
)

pattern = "|".join(selected_products)
price_df = price_df[price_df["product_name"].str.contains(pattern, na=False)]


# =====================================================
# 4️⃣ presence 데이터
# =====================================================
pres_res = supabase.table("product_presence_events").select("*").execute()
pres_df = pd.DataFrame(pres_res.data or [])

pres_df["event_date"] = pd.to_datetime(pres_df["event_date"], errors="coerce")
pres_df = pres_df[pres_df["product_name"].str.contains(pattern, na=False)]


# =====================================================
# 5️⃣ KPI
# =====================================================
st.divider()

cols = st.columns(7)

with cols[0]: kpi_card("💸 할인 시작", (price_df.price_event_type=="DISCOUNT_START").sum(), "DISCOUNT_START")
with cols[1]: kpi_card("🔚 할인 종료", (price_df.price_event_type=="DISCOUNT_END").sum(), "DISCOUNT_END")
with cols[2]: kpi_card("⬆ 정상가 변동", price_df.price_event_type.isin(["NORMAL_UP","NORMAL_DOWN"]).sum(), "NORMAL")
with cols[3]: kpi_card("🔺 할인가 변동", price_df.price_event_type.isin(["SALE_UP","SALE_DOWN"]).sum(), "SALE")
with cols[4]: kpi_card("🆕 신제품", (pres_df.event_type=="NEW").sum(), "NEW")
with cols[5]: kpi_card("❌ 품절", (pres_df.event_type=="OUT_OF_STOCK").sum(), "OUT_OF_STOCK")
with cols[6]: kpi_card("♻ 재입고", (pres_df.event_type=="RESTOCK").sum(), "RESTOCK")


# =====================================================
# 6️⃣ 단가 차트
# =====================================================
st.divider()
st.subheader("📈 단가 추이 (원/개)")

fig = go.Figure()

for p in selected_products:

    sub = price_df[price_df.product_name==p].sort_values("event_date")

    if len(sub)==0:
        continue

    fig.add_trace(go.Scatter(
        x=sub["event_date"],
        y=sub["current_unit_price"],
        mode="lines+markers",
        name=p
    ))

    # 할인 shading
    start=None
    for _, r in sub.iterrows():

        if r.price_event_type=="DISCOUNT_START":
            start=r.event_date

        elif r.price_event_type=="DISCOUNT_END" and start:
            fig.add_vrect(x0=start, x1=r.event_date,
                          fillcolor="lightblue", opacity=0.25,
                          layer="below", line_width=0)
            start=None

fig.update_layout(
    height=450,
    xaxis=dict(type="date", dtick="D1", title="날짜"),
    yaxis=dict(title="원/개", tickformat=","),
    legend_title="제품"
)

st.plotly_chart(fig, use_container_width=True)


# =====================================================
# 7️⃣ 이벤트 히스토리 (제품별 카드)
# =====================================================
st.divider()
st.subheader("📜 이벤트 히스토리")

pres_df["price_event_type"]=pres_df["event_type"]
pres_df["current_unit_price"]=None

merged=pd.concat([price_df, pres_df]).sort_values("event_date")

for product, g in merged.groupby("product_name"):

    st.markdown(f"### 📦 {product}")

    for _, r in g.iterrows():

        label = EVENT_LABEL.get(r["price_event_type"], r["price_event_type"])

        price=""
        if pd.notna(r["current_unit_price"]):
            price=f" | {format_price(r['current_unit_price'])}원/개"

        st.write(f"{r['event_date'].date()} · {label}{price}")
