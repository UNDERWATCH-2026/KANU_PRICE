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
def format_price(v):
    if pd.isna(v):
        return "-"
    return f"{int(float(v)):,}"

def clear_checkbox_state(prefixes=("chk_",)):
    for k in list(st.session_state.keys()):
        if any(k.startswith(p) for p in prefixes):
            del st.session_state[k]

# =====================================================
# 🧠 세션 초기화
# =====================================================
if "active_mode" not in st.session_state:
    st.session_state.active_mode = "제품명 입력"

if "selected_products" not in st.session_state:
    st.session_state.selected_products = set()

if "confirmed_products" not in st.session_state:
    st.session_state.confirmed_products = set()

if "product_search_keywords" not in st.session_state:
    st.session_state.product_search_keywords = []

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

# 문자열 컬럼 안전 처리
for col in ["brand","category1_raw","category2_raw","product_name","brew_type","brew_type_kr"]:
    meta_all[col] = meta_all[col].astype(str)

# =====================================================
# 🗑️ 전체 초기화
# =====================================================
if st.button("🗑️ 전체 삭제"):
    st.session_state.selected_products = set()
    st.session_state.confirmed_products = set()
    st.session_state.product_search_keywords = []
    clear_checkbox_state()
    st.rerun()

# =====================================================
# 🔎 조회 기준 (최상단)
# =====================================================
st.subheader("🔎 조회 기준")

search_mode = st.radio(
    "조회 기준 선택",
    ["제품명 입력", "브랜드/카테고리 선택"],
    horizontal=True
)

st.caption("※ 조회 기준을 변경하면 현재 선택된 제품은 초기화됩니다.")

# =====================================================
# 🔁 모드 전환 감지 → 초기화
# =====================================================
if search_mode != st.session_state.active_mode:
    st.session_state.active_mode = search_mode
    st.session_state.selected_products = set()
    st.session_state.confirmed_products = set()
    st.session_state.product_search_keywords = []
    clear_checkbox_state()
    st.rerun()

# =====================================================
# 🔍 조회 조건 UI
# =====================================================
candidates_df = pd.DataFrame()

# -----------------------------------------------------
# A) 제품명 입력 모드 (누적 검색)
# -----------------------------------------------------
if search_mode == "제품명 입력":
    c1, c2, c3 = st.columns([6, 2, 2])

    with c1:
        product_input = st.text_input(
            "제품명 키워드 입력 (추가 검색 가능)",
            placeholder="예: 쥬시, 스노우, 도쿄"
        )

    with c2:
        if st.button("🔍 검색 추가", use_container_width=True):
            kw = product_input.strip()
            if kw and kw not in st.session_state.product_search_keywords:
                st.session_state.product_search_keywords.append(kw)
                st.rerun()

    with c3:
        if st.button("🧹 검색어 비우기", use_container_width=True):
            st.session_state.product_search_keywords = []
            clear_checkbox_state()
            st.rerun()

    if st.session_state.product_search_keywords:
        st.caption("현재 검색어: " + ", ".join(st.session_state.product_search_keywords))

        mask = pd.Series(False, index=meta_all.index)
        for kw in st.session_state.product_search_keywords:
            mask |= meta_all["product_name"].str.contains(kw, case=False, na=False)
        candidates_df = meta_all[mask]

# -----------------------------------------------------
# B) 브랜드/카테고리 선택 모드
# -----------------------------------------------------
else:
    c1, c2, c3, c4 = st.columns(4)

    with c1:
        brand = st.selectbox(
            "브랜드",
            ["(전체)"] + sorted(meta_all["brand"].unique())
        )

    meta_brand = meta_all if brand == "(전체)" else meta_all[meta_all["brand"] == brand]

    with c2:
        category1 = st.selectbox(
            "카테고리 1",
            ["(전체)"] + sorted(meta_brand["category1_raw"].unique())
        )

    meta_cat1 = meta_brand if category1 == "(전체)" else meta_brand[meta_brand["category1_raw"] == category1]

    with c3:
        category2 = st.selectbox(
            "카테고리 2",
            ["(전체)"] + sorted(meta_cat1["category2_raw"].unique())
        )

    with c4:
        brew = st.selectbox(
            "추출타입",
            ["(전체)"] + sorted(meta_all["brew_type_kr"].unique())
        )

    # OR 조건
    mask_or = pd.Series(False, index=meta_all.index)
    if brand != "(전체)":
        mask_or |= meta_all["brand"] == brand
    if brew != "(전체)":
        mask_or |= (
            meta_all["brew_type_kr"].str.contains(brew, case=False, na=False) |
            meta_all["brew_type"].str.contains(brew, case=False, na=False)
        )
    if brand == "(전체)" and brew == "(전체)":
        mask_or |= True

    # AND 조건
    mask_and = pd.Series(True, index=meta_all.index)
    if category1 != "(전체)":
        mask_and &= meta_all["category1_raw"] == category1
    if category2 != "(전체)":
        mask_and &= meta_all["category2_raw"] == category2

    candidates_df = meta_all[mask_or & mask_and]

# =====================================================
# 📦 제품 선택
# =====================================================
if candidates_df.empty:
    st.warning("조건에 맞는 제품이 없습니다.")
    st.stop()

st.subheader("📦 비교할 제품 선택")
st.caption("※ 제품을 선택한 뒤 ‘조회하기’를 눌러야 결과가 적용됩니다.")

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

if not st.session_state.selected_products:
    st.info("제품을 선택하세요.")
    st.stop()

# =====================================================
# 🔎 조회하기 (확정)
# =====================================================
if st.button("🔎 조회하기", use_container_width=True):
    st.session_state.confirmed_products = set(st.session_state.selected_products)
    st.toast("조회 조건이 적용되었습니다.")

applied_products = list(st.session_state.confirmed_products)
if not applied_products:
    st.info("조회하기 버튼을 눌러 결과를 적용하세요.")
    st.stop()

# =====================================================
# 📊 이벤트 데이터 필터
# =====================================================
price_df = price_all[price_all["product_name"].isin(applied_products)].copy()
price_df["event_date"] = pd.to_datetime(price_df["event_date"])

# =====================================================
# 📌 KPI
# =====================================================
st.divider()
cols = st.columns(4)

with cols[0]:
    st.metric("할인 시작", (price_df.price_event_type == "DISCOUNT_START").sum())
with cols[1]:
    st.metric("할인 종료", (price_df.price_event_type == "DISCOUNT_END").sum())
with cols[2]:
    st.metric("정상가 변동", price_df.price_event_type.isin(["NORMAL_UP","NORMAL_DOWN"]).sum())
with cols[3]:
    st.metric("할인가 변동", price_df.price_event_type.isin(["SALE_UP","SALE_DOWN"]).sum())

# =====================================================
# 📈 가격 차트
# =====================================================
st.subheader("📈 단가 추이")
fig = go.Figure()

for name in applied_products:
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
