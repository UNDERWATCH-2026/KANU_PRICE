import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from supabase import create_client
import re

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
    try:
        return f"{int(float(v)):,}"
    except Exception:
        return safe_str(v)

def highlight_keywords(text: str, keywords: list[str]) -> str:
    """
    product_name 표시용 하이라이트 (데이터 로직에는 영향 없음)
    """
    if not text or not keywords:
        return safe_str(text)

    ks = [k.strip() for k in keywords if k and k.strip()]
    if not ks:
        return safe_str(text)

    escaped = [re.escape(k) for k in ks]
    pattern = re.compile(rf"({'|'.join(escaped)})", re.IGNORECASE)

    return pattern.sub(
        r"<span style='background-color:#FFF3B0; font-weight:700; padding:0 2px; border-radius:3px;'>\1</span>",
        safe_str(text),
    )

# =====================================================
# 🧠 세션 초기화
# =====================================================
if "selected_products" not in st.session_state:
    st.session_state.selected_products = set()

# =====================================================
# 🚀 가격 이벤트만 사용 (🔥 핵심 단일 소스)
# =====================================================
@st.cache_data(ttl=300)
def load_price_events():
    res = supabase.table("product_price_events_enriched").select(
        "product_name, event_date, price_event_type, current_unit_price"
    ).execute()
    return pd.DataFrame(res.data or [])

price_all = load_price_events()

if price_all.empty:
    st.warning("가격 이벤트 데이터가 없습니다.")
    st.stop()

# =====================================================
# 📦 제품 후보 생성 (이벤트가 존재하는 제품만)
# =====================================================
meta_df = price_all[["product_name"]].drop_duplicates().copy()
meta_df["product_name"] = meta_df["product_name"].astype(str)

# =====================================================
# 🗑️ 전체 삭제 (검색/선택 초기화)
# =====================================================
top_c1, top_c2 = st.columns([1, 9])
with top_c1:
    if st.button("🗑️ 전체 삭제", use_container_width=True):
        st.session_state.base_keywords = ""
        st.session_state.refine_keywords = ""
        st.session_state.selected_products = set()
        # 체크박스 키는 제품명 기반이라 rerun으로 충분
        st.rerun()

with top_c2:
    st.caption("※ 1차 검색으로 후보를 만든 뒤, 체크박스로 선택하고, 2차 검색으로 선택된 상품 내에서만 추가 필터링합니다.")

# =====================================================
# 🔍 검색 FORM (Enter 지원) - 2단 검색
#   1) base_keywords: 후보 생성
#   2) refine_keywords: 선택된 상품 내 추가 필터
# =====================================================
with st.form("search_form", clear_on_submit=False):
    c1, c2, c3, c4 = st.columns([4, 4, 2, 1])

    with c1:
        base_input = st.text_input(
            "1차 검색 (후보 생성) · 쉼표 가능",
            placeholder="예: 스노우, 쥬시",
            key="base_keywords",
        )

    with c2:
        refine_input = st.text_input(
            "2차 검색 (선택된 상품 내 추가 필터) · 쉼표 가능",
            placeholder="예: 도쿄",
            key="refine_keywords",
        )

    with c3:
        date_range = st.date_input("기간 선택", [], key="date_range")

    with c4:
        submitted = st.form_submit_button("조회하기", use_container_width=True)

# 제출 전에는 화면을 더 진행하지 않음 (초기 로딩 stop)
if not submitted:
    st.stop()

base_keywords = [p.strip() for p in safe_str(st.session_state.base_keywords).split(",") if p.strip()]
refine_keywords = [p.strip() for p in safe_str(st.session_state.refine_keywords).split(",") if p.strip()]

if not base_keywords:
    st.info("1차 검색 키워드를 입력하세요.")
    st.stop()

# =====================================================
# 🔎 1차 후보 필터 (product_name 포함 검색)
# =====================================================
mask = meta_df["product_name"].apply(
    lambda x: any(k.lower() in safe_str(x).lower() for k in base_keywords)
)
candidates_df = meta_df[mask].copy()

if candidates_df.empty:
    st.warning("1차 검색 결과 없음 (후보가 없습니다).")
    st.stop()

# =====================================================
# 📦 제품 선택 (체크박스 + 키워드 하이라이트)
# =====================================================
st.subheader("📦 조회할 제품 선택")

def toggle_product(name: str):
    if name in st.session_state.selected_products:
        st.session_state.selected_products.remove(name)
    else:
        st.session_state.selected_products.add(name)

# 체크박스는 라벨 스타일링이 어려워, 좌/우 컬럼으로 분리해서 우측에 하이라이트 표시
for name in candidates_df["product_name"]:
    checked = name in st.session_state.selected_products
    highlighted = highlight_keywords(name, base_keywords)

    col_chk, col_txt = st.columns([0.06, 0.94], vertical_alignment="center")
    with col_chk:
        st.checkbox(
            "",
            value=checked,
            key=f"chk_{name}",
            on_change=toggle_product,
            args=(name,),
        )
    with col_txt:
        st.markdown(highlighted, unsafe_allow_html=True)

selected_products = list(st.session_state.selected_products)

if len(selected_products) == 0:
    st.info("제품을 선택하세요.")
    st.stop()

# =====================================================
# 🔎 2차 추가 필터 (선택된 상품 내에서만)
# =====================================================
if refine_keywords:
    filtered_selected = [
        p for p in selected_products
        if any(k.lower() in safe_str(p).lower() for k in refine_keywords)
    ]
else:
    filtered_selected = selected_products

if len(filtered_selected) == 0:
    st.warning("2차 검색 조건으로 남는 선택 상품이 없습니다. (2차 검색어를 지우거나 다시 선택하세요.)")
    st.stop()

# 현재 적용 필터 요약
active_filters = [f"1차: {', '.join(base_keywords)}"]
if refine_keywords:
    active_filters.append(f"2차: {', '.join(refine_keywords)}")
st.caption("적용 중인 필터 · " + " / ".join(active_filters))

# =====================================================
# 📊 이벤트 필터링
# =====================================================
price_df = price_all[price_all["product_name"].isin(filtered_selected)].copy()
price_df["event_date"] = pd.to_datetime(price_df["event_date"], errors="coerce")

if len(date_range) == 2:
    s, e = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
    price_df = price_df[(price_df.event_date >= s) & (price_df.event_date <= e)]

if price_df.empty:
    st.warning("선택/기간 조건에서 이벤트 데이터가 없습니다.")
    st.stop()

# =====================================================
# 📌 KPI (가격 이벤트 전용)
# =====================================================
st.divider()

def kpi(label, value, icon):
    st.metric(f"{icon} {label}", int(value))

cols = st.columns(4)

with cols[0]:
    kpi("할인 시작", (price_df.price_event_type == "DISCOUNT_START").sum(), "💙")
with cols[1]:
    kpi("할인 종료", (price_df.price_event_type == "DISCOUNT_END").sum(), "💙")
with cols[2]:
    kpi("정상가 변동", price_df.price_event_type.isin(["NORMAL_UP", "NORMAL_DOWN"]).sum(), "📈")
with cols[3]:
    kpi("할인가 변동", price_df.price_event_type.isin(["SALE_UP", "SALE_DOWN"]).sum(), "📉")

# =====================================================
# 📈 가격 차트
# =====================================================
st.subheader("📈 단가 추이 (원/개)")

fig = go.Figure()

for name in filtered_selected:
    sub = price_df[price_df.product_name == name].sort_values("event_date")
    if sub.empty:
        continue

    fig.add_trace(go.Scatter(
        x=sub["event_date"],
        y=sub["current_unit_price"],
        mode="lines+markers",
        name=name
    ))

    # 할인 구간 음영
    start = None
    for _, r in sub.iterrows():
        if r["price_event_type"] == "DISCOUNT_START":
            start = r["event_date"]
        elif r["price_event_type"] == "DISCOUNT_END" and start is not None:
            fig.add_vrect(
                x0=start, x1=r["event_date"],
                fillcolor="lightblue",
                opacity=0.25,
                line_width=0
            )
            start = None

fig.update_layout(height=450)
st.plotly_chart(fig, use_container_width=True)

# =====================================================
# 📜 이벤트 히스토리
# =====================================================
st.subheader("📜 이벤트 히스토리")

for product, g in price_df.groupby("product_name"):
    st.markdown(f"### 📦 {product}")
    g = g.sort_values("event_date")

    for _, r in g.iterrows():
        price = format_price(r["current_unit_price"])
        dt = r["event_date"]
        dt_str = dt.date() if pd.notna(dt) else "-"
        st.write(f"{dt_str} · {r['price_event_type']} | {price}원")
