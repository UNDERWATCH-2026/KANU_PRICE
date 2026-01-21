import streamlit as st
from supabase import create_client
import pandas as pd

# =========================
# Supabase 연결 정보
# =========================
SUPABASE_URL = "https://fgaxjjpktwksdoizerwh.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZnYXhqanBrdHdrc2RvaXplcndoIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2ODcyNzM3MSwiZXhwIjoyMDg0MzAzMzcxfQ.bBSInJ9t08yA1Spw4HuOQnczUtVElzhO_QPSUBkMk1g"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

st.title("📊 가격 이벤트 조회 (1단계)")

st.write("제품명을 입력하면 이벤트 타임라인을 보여줍니다.")

# =========================
# 사용자 입력
# =========================
product_name = st.text_input("제품명 입력")

if product_name:
    query = """
        select
          event_date,
          event_type,
          event_detail
        from product_event_timeline
        where product_name = %s
        order by event_date
    """

    result = supabase.rpc(
        "execute_sql",
        {
            "sql": query,
            "params": [product_name]
        }
    ).execute()

    if result.data:
        df = pd.DataFrame(result.data)
        st.dataframe(df)
    else:
        st.warning("해당 제품의 이벤트가 없습니다.")
