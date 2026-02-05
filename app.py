import streamlit as st
import pandas as pd
from supabase import create_client

# =========================
# 0️⃣ 기본 설정
# =========================
st.set_page_config(
    page_title="Capsule Price Intelligence",
    layout="wide"
)

# =========================
# 1️⃣ Supabase 설정
# =========================
SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_ANON_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# =========================
# 2️⃣ 데이터 로딩 함수
# =========================
@st.cache_data(ttl=300)
def load_product_summary():
    res = supabase.table("product_price_summary").select(
        "product_key, brand, category1, category2, product_name, "
        "current_price, is_discount, "
        "first_seen_date, last_seen_date, event_count, "
        "product_event_status, is_new_product"
    ).execute()
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
# 3️⃣ 검색 필터 함수
# =========================
def filter_products(df: pd.DataFrame, query: str):
    if not query:
        return df

    q = query.lower()
    return df[
        df["product_name"].str.lower().str.contains(q)
        | df["brand"].str.lower().str.contains(q)
        | df["category1"].str.lower().str.contains(q)
        | df["category2"].str.lower().str.contains(q)
    ]


# =========================
# 4️⃣ 메인 UI
# =========================
st.title("☕ Capsule Price Intelligence")

df_all = load_product_summary()

# --- 검색 영역 ---
st.subheader("🔍 제품 검색")

query = st.text_input(
    "제품명 / 브랜드 / 카테고리 검색",
    placeholder="예: 카누 다크, 바리스타, 디카페인"
)

df_filtered = filter_products(df_all, query)

selected_product_name = st.selectbox(
    "조회할 제품 선택",
    options=df_filtered["product_name"].tolist(),
    index=None,
    placeholder="검색 후 제품을 선택하세요"
)

# =========================
# 5️⃣ 결과 카드
# =========================
if selected_product_name:
    product = df_all[df_all["product_name"] == selected_product_name].iloc[0]

    st.divider()
    st.subheader(product["product_name"])

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("현재 가격", f"{int(product['current_price']):,}원")

    with col2:
        if product["is_discount"]:
            st.success("할인 중")
        else:
            st.info("정상가")

    with col3:
        if product["is_new_product"]:
            st.warning("신제품")
        else:
            st.caption(f"관측 시작일\n{product['first_seen_date']}")

    with col4:
        st.caption(f"마지막 관측일\n{product['last_seen_date']}")

    # =========================
    # 6️⃣ 상태 메시지 (핵심 UX)
    # =========================
    if product["product_event_status"] == "NO_EVENT_STABLE":
        st.info(
            f"📊 가격 변동 없음 "
            f"({product['first_seen_date']} 이후)"
        )
    else:
        st.success(
            f"📈 가격 이벤트 {product['event_count']}건 발생"
        )

    # =========================
    # 7️⃣ 이벤트 타임라인
    # =========================
    if product["event_count"] > 0:
        st.subheader("📅 가격 이벤트 타임라인")

        df_events = load_events(product["product_key"])

        if not df_events.empty:
            df_events_display = df_events.copy()
            df_events_display["event_date"] = pd.to_datetime(
                df_events_display["event_date"]
            ).dt.date

            st.dataframe(
                df_events_display,
                use_container_width=True,
                hide_index=True
            )
        else:
            st.caption("이벤트 데이터가 없습니다.")

else:
    st.info("⬆️ 상단에서 제품을 검색하고 선택하세요.")

