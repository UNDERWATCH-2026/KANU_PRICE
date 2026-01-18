import streamlit as st
import pandas as pd
import psycopg2

# =========================
# 1. Supabase DB 연결 정보
# =========================
# ⚠️ 아래 DB_PASSWORD만 실제 비밀번호로 교체하세요
DB_PASSWORD = "KANUPRICE2026!"

DATABASE_URL = (
    f"postgresql://postgres:{DB_PASSWORD}"
    "@db.fgaxjjpktwksdoizerwh.supabase.co:6543/postgres"
    "?sslmode=require"
)

# =========================
# 2. DB 연결
# =========================
@st.cache_resource
def get_connection():
    return psycopg2.connect(DATABASE_URL)

conn = get_connection()

# =========================
# 3. Streamlit UI
# =========================
st.set_page_config(
    page_title="제품 가격 히스토리 조회",
    layout="centered"
)

st.title("📊 제품 가격 히스토리 조회")
st.caption("제품명을 입력하면 과거 가격 변동 추이를 확인할 수 있습니다.")

product_name = st.text_input(
    "제품명을 입력하세요",
    placeholder="예: 버츄오 팝 캔디 핑크"
)

# =========================
# 4. 조회 로직
# =========================
if st.button("조회"):
    if not product_name.strip():
        st.warning("제품명을 입력해주세요.")
    else:
        query = """
            SELECT
                date,
                price
            FROM product_events
            WHERE product_name = %s
              AND price IS NOT NULL
            ORDER BY date;
        """

        df = pd.read_sql(query, conn, params=(product_name,))

        if df.empty:
            st.error("해당 제품의 가격 데이터가 없습니다.")
        else:
            st.subheader("📈 가격 변동 추이")
            st.line_chart(df.set_index("date")["price"])

            st.subheader("📋 가격 이력")
            st.dataframe(df, use_container_width=True)

# =========================
# 5. 푸터
# =========================
st.divider()
st.caption("ⓒ Underwatch · Price Intelligence PoC")


