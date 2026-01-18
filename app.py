import streamlit as st
import pandas as pd
from supabase import create_client

# =========================
# Supabase REST 연결 정보
# =========================
SUPABASE_URL = "https://fgaxjjpktwksdoizerwh.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZnYXhqanBrdHdrc2RvaXplcndoIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2ODcyNzM3MSwiZXhwIjoyMDg0MzAzMzcxfQ.bBSInJ9t08yA1Spw4HuOQnczUtVElzhO_QPSUBkMk1g"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# =========================
# UI
# =========================
st.title("📊 제품 가격 히스토리 조회")

product_name = st.text_input("제품명 입력")

if st.button("조회"):
    if not product_name:
        st.warning("제품명을 입력하세요.")
    else:
        response = (
            supabase
            .table("product_events")
            .select("date, price")
            .eq("product_name", product_name)
            .order("date")
            .execute()
        )

        if not response.data:
            st.error("데이터 없음")
        else:
            df = pd.DataFrame(response.data)
            df["date"] = pd.to_datetime(df["date"])

            st.line_chart(df.set_index("date")["price"])
            st.dataframe(df)
