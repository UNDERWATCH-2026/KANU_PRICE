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
    # brew_type이 summary에 있으면 같이 가져오고, 없으면 기존 컬럼만 가져오기
    base_cols = [
        "product_key", "brand", "category1", "category2", "product_name",
        "current_price", "is_discount",
        "first_seen_date", "last_seen_date", "event_count",
        "product_event_status", "is_new_product"
    ]

    # 우선 brew_type 포함 시도 → 실패하면 제외 재시도 (PostgREST는 없는 컬럼 select 시 에러)
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
    # None/NaN 안전하게 문자열화
    return s.fillna("").astype(str)

def filter_by_keyword(df: pd.DataFrame, keyword: str) -> pd.DataFrame:
    if not keyword:
        return df.iloc[0:0]
    q = keyword.lower()
    return df[_norm_series(df["product_name"]).str.lower().str.contains(q)]

def options_from(df: pd.DataFrame, col: str):
    if col not in df.columns:
        return []
    vals = df[col].dropna().astype(str)
    vals = [v.strip() for v in vals.tolist() if v.strip()]
    return sorted(list(dict.fromkeys(vals)))  # unique + preserve-ish

# =========================
# 4️⃣ 메인 UI
# =========================
st.title("☕ Capsule Price Intelligence")

df_all = load_product_summary()
if df_all.empty:
    st.error("product_price_summary에서 데이터를 불러오지 못했습니다. (RLS/권한/뷰 데이터 확인 필요)")
    st.stop()

st.subheader("🔍 제품 검색")

search_mode = st.radio(
    "검색 방식 선택",
    ["제품명으로 검색", "필터로 검색(브랜드/카테고리/브루타입)"],
    horizontal=True
)

selected_product_name = None
filtered_df = pd.DataFrame()

# =========================
# 4-A) 제품명 검색 모드
# =========================
if search_mode == "제품명으로 검색":
    keyword = st.text_input("제품명 키워드", placeholder="예: 네스프레소 아르페지오, 카누 다크, 디카페인")
    filtered_df = filter_by_keyword(df_all, keyword)

    selected_product_name = st.selectbox(
        "검색 결과(제품 선택)",
        options=filtered_df["product_name"].tolist(),
        index=None,
        placeholder="키워드를 입력하면 제품이 나타납니다."
    )

# =========================
# 4-B) 연쇄 드롭다운 필터 모드
# =========================
else:
    # 1) 브랜드
    brand_opts = options_from(df_all, "brand")
    sel_brand = st.selectbox("브랜드", options=brand_opts, index=None, placeholder="브랜드 선택")

    df1 = df_all.copy()
    if sel_brand:
        df1 = df1[_norm_series(df1["brand"]) == sel_brand]

    # 2) 카테고리1 (선택한 브랜드 범위로 제한)
    cat1_opts = options_from(df1, "category1")
    sel_cat1 = st.selectbox("카테고리1", options=cat1_opts, index=None, placeholder="카테고리1 선택")

    df2 = df1.copy()
    if sel_cat1:
        df2 = df2[_norm_series(df2["category1"]) == sel_cat1]

    # 3) 카테고리2 (브랜드+카테고리1 범위로 제한)
    cat2_opts = options_from(df2, "category2")
    sel_cat2 = st.selectbox("카테고리2", options=cat2_opts, index=None, placeholder="카테고리2 선택")

    df3 = df2.copy()
    if sel_cat2:
        df3 = df3[_norm_series(df3["category2"]) == sel_cat2]

    # 4) 브루타입 (있을 때만 표시)
    if "brew_type" in df_all.columns:
        brew_opts = options_from(df3, "brew_type")
        sel_brew = st.selectbox("Brew type", options=brew_opts, index=None, placeholder="브루타입 선택")
        df4 = df3.copy()
        if sel_brew:
            df4 = df4[_norm_series(df4["brew_type"]) == sel_brew]
    else:
        sel_brew = None
        df4 = df3
        st.caption("※ brew_type 컬럼이 product_price_summary에 없어 브루타입 필터는 숨김 처리했습니다.")

    filtered_df = df4

    # 최종 제품 선택(항상 제품 단위)
    selected_product_name = st.selectbox(
        "필터 결과(제품 선택)",
        options=filtered_df["product_name"].tolist(),
        index=None,
        placeholder="필터를 선택하면 해당 제품 목록이 나타납니다."
    )

# =========================
# 5️⃣ 결과 카드 + 이벤트
# =========================
if selected_product_name:
    product = df_all[df_all["product_name"] == selected_product_name].iloc[0]

    st.divider()
    st.subheader(product["product_name"])

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        try:
            st.metric("현재 가격", f"{int(product['current_price']):,}원")
        except Exception:
            st.metric("현재 가격", f"{product['current_price']}")

    with col2:
        if bool(product["is_discount"]):
            st.success("할인 중")
        else:
            st.info("정상가")

    with col3:
        if bool(product["is_new_product"]):
            st.warning("신제품")
        else:
            st.caption(f"관측 시작일\n{product['first_seen_date']}")

    with col4:
        st.caption(f"마지막 관측일\n{product['last_seen_date']}")

    # 상태 메시지
    if product["product_event_status"] == "NO_EVENT_STABLE":
        st.info(f"📊 가격 변동 없음 ({product['first_seen_date']} 이후)")
    else:
        st.success(f"📈 가격 이벤트 {product['event_count']}건 발생")

    # 이벤트 타임라인
    if int(product["event_count"]) > 0:
        st.subheader("📅 가격 이벤트 타임라인")
        df_events = load_events(product["product_key"])
        if not df_events.empty:
            df_events["event_date"] = pd.to_datetime(df_events["event_date"]).dt.date
            st.dataframe(df_events, use_container_width=True, hide_index=True)
        else:
            st.caption("이벤트 데이터가 없습니다.")
else:
    # 모드별 안내
    if search_mode == "제품명으로 검색":
        st.info("⬆️ 제품명 키워드를 입력하면 결과가 나타납니다.")
    else:
        st.info("⬆️ 브랜드/카테고리/브루타입을 선택하면 결과가 나타납니다.")
