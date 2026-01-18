import streamlit as st
import pandas as pd
import psycopg2

conn = psycopg2.connect(
    host="db.fgaxjjpktwksdoizerwh.supabase.co",
    database="postgres",
    user="postgres",
    password="KANU2026PRICE!!",
    port=5432
    sslmode="require"
)

st.title("📊 제품 가격 히스토리 조회")

product = st.text_input(
    "제품명을 입력하세요",
    "버츄오 팝 캔디 핑크"
)

if st.button("조회"):
    sql = f"""
        select date, price
        from product_events
        where product_name = '{product}'
        order by date
    """
    df = pd.read_sql(sql, conn)

    if df.empty:
        st.warning("해당 제품 데이터가 없습니다.")
    else:
        st.subheader("📈 가격 변동 추이")
        st.line_chart(df.set_index("date")["price"])

        st.subheader("📋 가격 이력")
        st.dataframe(df)

