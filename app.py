import streamlit as st
import pandas as pd
from supabase import create_client

# =========================
# 0️⃣ 기본 설정
# =========================
st.set_page_config(page_title="Capsule Price Intelligence", layout="wide")

# =========================
# 1️⃣ Supabase 설정
# =========================
SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_ANON_KEY"]
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# =========================
# 2️⃣ 데이터 로딩
# =========================
@st.cache_data(ttl=300)
def load_product_summary():
    base_cols = [
        "product_key", "brand", "category1", "category2", "product_name",
        "current_price", "is_discount",
        "first_seen_date", "last_seen_date", "event_count",
        "product_event_status", "is_new_product"
    ]

    try:
        res = supabase.table("product_price_summary").select(", ".join(base_cols + ["brew_type"])).execute()
        return pd.DataFrame(res.data)
    except Exception:
        res = supabase.table("product_price_summary").select(", ".join(base_cols)).execute()
        return pd.DataFrame(res.data)

@st.cache_data(ttl=300)
def load_events(product_key: str):
    res = (
        supabase.table("product_all_events")
        .select("event_date, event_type, price")
        .eq("product_key", product_key)
        .order("event_date", desc=True)
        .execute()
    )
    return pd.DataFrame(res.data)

# =========================
# 3️⃣ 유틸
# =========================
def _norm_series(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str)

def options_from(df: pd.DataFrame, col: str):
    if col not in df.columns:
        return []
    vals = df[col].dropna().astype(str)
    vals = [v.strip() for v in vals.tolist() if v.strip()]
    return sorted(list(dict.fromkeys(vals)))

# =========================
# 4️⃣ 세션 상태 초기화
# =========================
if "selected_products" not in st.session_state:
    st.session_state.selected_products = set()

if "keyword_searches" not in st.session_state:
    st.session_state.keyword_searches = []

if "active_mode" not in st.session_state:
    st.session_state.active_mode = "키워드 검색"

if "show_results" not in st.session_state:
    st.session_state.show_results = False

# =========================
# 5️⃣ 메인 UI
# =========================
st.title("☕ Capsule Price Intelligence")

# =========================
# 검색 모드 선택 (⚠️ 항상 가장 먼저 렌더링)
# =========================
st.subheader("🔎 조회 기준")
search_mode = st.radio(
    "검색 방식 선택",
    ["키워드 검색", "필터 선택 (브랜드/카테고리)"],
    horizontal=True
)

st.caption("※ 조회 기준을 변경하면 현재 선택된 제품/검색 상태가 초기화됩니다.")

# 모드 변경 감지 → 초기화
if search_mode != st.session_state.active_mode:
    st.session_state.active_mode = search_mode
    st.session_state.selected_products = set()
    st.session_state.keyword_searches = []
    st.session_state.show_results = False
    st.rerun()

st.divider()

# =========================
# 데이터 로딩
# =========================
df_all = load_product_summary()
if df_all.empty:
    st.warning("아직 집계된 제품 데이터가 없습니다. 데이터 수집/집계 이후 이용 가능합니다.")
    st.stop()

# =========================
# 상단 버튼: 조회하기 + 전체 삭제
# =========================
col_query, col_delete = st.columns([1, 1])
with col_query:
    if st.button("📊 조회하기", type="primary", use_container_width=True):
        st.session_state.show_results = True

with col_delete:
    if st.button("🗑️ 전체 삭제", use_container_width=True):
        st.session_state.selected_products = set()
        st.session_state.keyword_searches = []
        st.session_state.show_results = False
        st.rerun()

st.divider()

# =========================
# 6️⃣ 조회 조건 UI
# =========================
st.subheader("🔍 조회 조건")

candidates_df = pd.DataFrame()

# ----- A) 키워드 검색 모드 -----
if search_mode == "키워드 검색":
    col_input, col_add, col_clear = st.columns([6, 2, 2])
    
    with col_input:
        keyword_input = st.text_input(
            "제품명 키워드 입력",
            placeholder="예: 쥬시, 스노우, 도쿄",
            label_visibility="collapsed"
        )
    
    with col_add:
        if st.button("🔍 검색 추가", use_container_width=True):
            kw = keyword_input.strip()
            if kw and kw not in st.session_state.keyword_searches:
                st.session_state.keyword_searches.append(kw)
                st.rerun()
    
    with col_clear:
        if st.button("🧹 검색어 비우기", use_container_width=True):
            st.session_state.keyword_searches = []
            st.session_state.selected_products = set()
            st.session_state.show_results = False
            st.rerun()
    
    # 현재 검색어 표시
    if st.session_state.keyword_searches:
        st.caption("**현재 검색어:** " + ", ".join(st.session_state.keyword_searches))
    else:
        st.info("제품명 키워드를 입력하고 '검색 추가'를 클릭하세요.")
    
    # 후보 생성: 키워드 OR 조건
    if st.session_state.keyword_searches:
        mask = pd.Series(False, index=df_all.index)
        for kw in st.session_state.keyword_searches:
            mask |= _norm_series(df_all["product_name"]).str.contains(kw, case=False, na=False)
        candidates_df = df_all[mask].copy()
    else:
        candidates_df = pd.DataFrame()

# ----- B) 필터 선택 모드 -----
else:
    col1, col2, col3, col4 = st.columns(4)
    
    # 1) 브랜드
    with col1:
        brand_opts = options_from(df_all, "brand")
        sel_brand = st.selectbox(
            "브랜드",
            options=["(전체)"] + brand_opts,
            index=0,
            key="filter_brand"
        )
    
    # 브랜드로 필터링
    df_after_brand = df_all.copy()
    if sel_brand != "(전체)":
        df_after_brand = df_after_brand[_norm_series(df_after_brand["brand"]) == sel_brand]
    
    # 2) 카테고리1 (브랜드 범위로 제한)
    with col2:
        cat1_opts = options_from(df_after_brand, "category1")
        sel_cat1 = st.selectbox(
            "카테고리1",
            options=["(전체)"] + cat1_opts,
            index=0,
            key="filter_cat1"
        )
    
    # 브랜드 + 카테고리1로 필터링
    df_after_cat1 = df_after_brand.copy()
    if sel_cat1 != "(전체)":
        df_after_cat1 = df_after_cat1[_norm_series(df_after_cat1["category1"]) == sel_cat1]
    
    # 3) 카테고리2 (브랜드 + 카테고리1 범위로 제한)
    with col3:
        cat2_opts = options_from(df_after_cat1, "category2")
        sel_cat2 = st.selectbox(
            "카테고리2",
            options=["(전체)"] + cat2_opts,
            index=0,
            key="filter_cat2"
        )
    
    # 브랜드 + 카테고리1 + 카테고리2로 필터링
    df_after_cat2 = df_after_cat1.copy()
    if sel_cat2 != "(전체)":
        df_after_cat2 = df_after_cat2[_norm_series(df_after_cat2["category2"]) == sel_cat2]
    
    # 4) Brew Type (OR 조건 - 독립적)
    with col4:
        if "brew_type" in df_all.columns:
            brew_opts = options_from(df_all, "brew_type")
            sel_brew = st.selectbox(
                "Brew Type",
                options=["(전체)"] + brew_opts,
                index=0,
                key="filter_brew"
            )
        else:
            sel_brew = "(전체)"
            st.caption("※ Brew Type 없음")
    
    # 최종 필터링: (브랜드 AND 카테고리1 AND 카테고리2) OR Brew Type
    candidates_df = df_after_cat2.copy()
    
    # Brew Type이 선택되면 OR 조건으로 추가
    if sel_brew != "(전체)" and "brew_type" in df_all.columns:
        brew_mask = _norm_series(df_all["brew_type"]) == sel_brew
        candidates_df = pd.concat([candidates_df, df_all[brew_mask]], ignore_index=True).drop_duplicates(subset=["product_key"])

# =========================
# 7️⃣ 후보 없음 처리
# =========================
if candidates_df.empty:
    st.warning("조건에 맞는 제품이 없습니다.")
    st.stop()

# =========================
# 8️⃣ 제품 선택 (체크박스)
# =========================
st.subheader("📦 비교할 제품 선택")

def toggle_product(product_name):
    if product_name in st.session_state.selected_products:
        st.session_state.selected_products.remove(product_name)
    else:
        st.session_state.selected_products.add(product_name)

# 제품명 목록 (선택된 제품 유지) - 가로로 5개씩 배열
product_list = sorted(candidates_df["product_name"].unique().tolist())
cols_per_row = 5
num_rows = (len(product_list) + cols_per_row - 1) // cols_per_row

for row_idx in range(num_rows):
    cols = st.columns(cols_per_row)
    for col_idx in range(cols_per_row):
        product_idx = row_idx * cols_per_row + col_idx
        if product_idx < len(product_list):
            product_name = product_list[product_idx]
            is_checked = product_name in st.session_state.selected_products
            
            with cols[col_idx]:
                st.checkbox(
                    product_name,
                    value=is_checked,
                    key=f"chk_{product_name}",
                    on_change=toggle_product,
                    args=(product_name,)
                )

selected_products = list(st.session_state.selected_products)

if not selected_products:
    st.info("제품을 선택하세요.")
    st.stop()

# =========================
# 9️⃣ 결과 조회 안내
# =========================
if not st.session_state.show_results:
    st.info("위에서 제품을 선택하고 '조회하기' 버튼을 클릭하세요.")
    st.stop()

# =========================
# 🔟 결과 표시
# =========================
st.divider()
st.subheader(f"📊 조회 결과 ({len(selected_products)}개 제품)")

for product_name in selected_products:
    product = df_all[df_all["product_name"] == product_name].iloc[0]

    st.markdown(f"### {product['product_name']}")

    col1, col2, col3, col4 = st.columns(4)

    # 1️⃣ 개당 가격 (소수점 1자리)
    with col1:
        price = product.get("current_unit_price")

        if price is not None and pd.notna(price):
            st.metric("개당 가격", f"{float(price):,.1f}원")
        else:
            st.metric("개당 가격", "–")

    # 2️⃣ 할인 여부
    with col2:
        if bool(product.get("is_discount", False)):
            st.success("✅ 할인 중")
        else:
            st.info("정상가")

    # 3️⃣ 신제품 / 관측 시작일
    with col3:
        if bool(product.get("is_new_product", False)):
            st.warning("🆕 신제품")
        else:
            st.caption(f"관측 시작일\n{product['first_seen_date']}")

    # 4️⃣ 마지막 관측일
    with col4:
        st.caption(f"마지막 관측일\n{product['last_seen_date']}")

    # =========================
    # 상태 메시지
    # =========================
    if product["product_event_status"] == "NO_EVENT_STABLE":
        st.info(f"📊 가격 변동 없음 ({product['first_seen_date']} 이후)")
    else:
        st.success(f"📈 가격 이벤트 {product['event_count']}건 발생")

    # =========================
    # 이벤트 타임라인
    # =========================
    if int(product["event_count"]) > 0:
        with st.expander(f"📅 이벤트 히스토리 ({product['event_count']}건)"):
            df_events = load_events(product["product_key"])

            if not df_events.empty:
                df_events["event_date"] = pd.to_datetime(
                    df_events["event_date"]
                ).dt.date

                st.dataframe(
                    df_events,
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.caption("이벤트 데이터가 없습니다.")

    st.divider()

