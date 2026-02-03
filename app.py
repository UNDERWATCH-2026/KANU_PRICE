import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from supabase import create_client

# =====================================================
# 🔧 기본 설정
# =====================================================
st.set_page_config(layout="wide")
st.title("☕ Capsule Price Intelligence")

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
    return f"{int(float(v)):,}"

# =====================================================
# 🧠 세션 초기화
# =====================================================
if "selected_products" not in st.session_state:
    st.session_state.selected_products = set()

# =====================================================
# 🚀 데이터 로드
# =====================================================
@st.cache_data(ttl=300)
def load_price_events():
    res = supabase.table("product_price_events_enriched").select(
        "product_name, event_date, price_event_type, current_unit_price"
    ).execute()
    return pd.DataFrame(res.data or [])

@st.cache_data(ttl=300)
def load_filter_products():
    res = supabase.table("filter_products").select(
        "brand, category1_raw, category2_raw, product_name, "
        "product_name_norm, intensity, capsule_weight_g, capsule_count, "
        "brew_type, brew_type_kr"
    ).execute()
    return pd.DataFrame(res.data or [])

price_all = load_price_events()
meta_all = load_filter_products()

if price_all.empty or meta_all.empty:
    st.warning("필수 데이터가 없습니다.")
    st.stop()

# =====================================================
# 🗑️ 전체 초기화
# =====================================================
if st.button("🗑️ 전체 삭제"):
    st.session_state.selected_products = set()
    st.rerun()

# =====================================================
# 🔍 조회 조건 UI
# =====================================================
st.subheader("🔍 조회 조건")

c1, c2, c3, c4 = st.columns(4)

with c1:
    brand = st.selectbox(
        "브랜드",
        ["(전체)"] + sorted(meta_all["brand"].dropna().unique().tolist())
    )

with c2:
    brew = st.selectbox(
        "추출 타입 (brew)",
        ["(전체)"] + sorted(
            meta_all["brew_type_kr"]
            .dropna()
            .unique()
            .tolist()
        )
    )

with c3:
    cat1_candidates = meta_all["category1_raw"].dropna().unique()
    category1 = st.selectbox(
        "카테고리 1",
        ["(전체)"] + sorted(cat1_candidates.tolist())
    )

with c4:
    if category1 != "(전체)":
        cat2_candidates = meta_all[
            meta_all["category1_raw"] == category1
        ]["category2_raw"].dropna().unique()
    else:
        cat2_candidates = meta_all["category2_raw"].dropna().unique()

    category2 = st.selectbox(
        "카테고리 2",
        ["(전체)"] + sorted(cat2_candidates.tolist())
    )

# =====================================================
# 🔎 후보 풀 생성
#   - brand OR brew_type
#   - category는 AND
# =====================================================
mask_or = pd.Series(False, index=meta_all.index)

if brand != "(전체)":
    mask_or |= meta_all["brand"] == brand

if brew != "(전체)":
    mask_or |= (
        meta_all["brew_type_kr"].str.contains(brew, na=False)
        | meta_all["brew_type"].str.contains(brew, na=False)
    )

# OR 조건이 하나도 없으면 전체 허용
if brand == "(전체)" and brew == "(전체)":
    mask_or |= True

mask_and = pd.Series(True, index=meta_all.index)

if category1 != "(전체)":
    mask_and &= meta_all["category1_raw"] == category1

if category2 != "(전체)":
    mask_and &= meta_all["category2_raw"] == category2

candidates_df = meta_all[mask_or & mask_and]

if candidates_df.empty:
    st.warning("조건에 맞는 제품이 없습니다.")
    st.stop()

# =====================================================
# 📦 제품 선택
# =====================================================
st.subheader("📦 비교할 제품 선택")

def toggle_product(name):
    if name in st.session_state.selected_products:
        st.session_state.selected_products.remove(name)
    else:
        st.session_state.selected_products.add(name)

for name in sorted(candidates_df["product_name"].unique()):
    st.checkbox(
        name,
        value=name in st.session_state.selected_products,
        key=f"chk_{name}",
        on_change=toggle_product,
        args=(name,)
    )

selected_products = list(st.session_state.selected_products)

if not selected_products:
    st.info("제품을 선택하세요.")
    st.stop()

# =====================================================
# 📊 이벤트 데이터 필터
# =====================================================
price_df = price_all[
    price_all["product_name"].isin(selected_products)
].copy()

price_df["event_date"] = pd.to_datetime(price_df["event_date"])

# =====================================================
# 📌 KPI
# =====================================================
st.divider()

cols = st.columns(4)
with cols[0]:
    st.metric("할인 시작", (price_df.price_event_type=="DISCOUNT_START").sum())
with cols[1]:
    st.metric("할인 종료", (price_df.price_event_type=="DISCOUNT_END").sum())
with cols[2]:
    st.metric(
        "정상가 변동",
        price_df.price_event_type.isin(["NORMAL_UP","NORMAL_DOWN"]).sum()
    )
with cols[3]:
    st.metric(
        "할인가 변동",
        price_df.price_event_type.isin(["SALE_UP","SALE_DOWN"]).sum()
    )

# =====================================================
# 📈 가격 차트
# =====================================================
st.subheader("📈 단가 추이")

fig = go.Figure()

for name in selected_products:
    sub = price_df[price_df.product_name == name].sort_values("event_date")
    fig.add_trace(go.Scatter(
        x=sub.event_date,
        y=sub.current_unit_price,
        mode="lines+markers",
        name=name
    ))

fig.update_layout(height=450)
st.plotly_chart(fig, use_container_width=True)

# =====================================================
# 📜 이벤트 히스토리
# =====================================================
st.subheader("📜 이벤트 히스토리")

for product, g in price_df.groupby("product_name"):
    st.markdown(f"### 📦 {product}")
    for _, r in g.sort_values("event_date").iterrows():
        st.write(
            f"{r.event_date.date()} · {r.price_event_type} | "
            f"{format_price(r.current_unit_price)}원"
        )
