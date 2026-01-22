import streamlit as st
import pandas as pd
from supabase import create_client

# =========================
# 0️⃣ Supabase 설정
# =========================
SUPABASE_URL = "https://fgaxjjpktwksdoizerwh.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZnYXhqanBrdHdrc2RvaXplcndoIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2ODcyNzM3MSwiZXhwIjoyMDg0MzAzMzcxfQ.bBSInJ9t08yA1Spw4HuOQnczUtVElzhO_QPSUBkMk1g"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

st.set_page_config(page_title="캡슐 커피 이벤트 타임라인", layout="wide")
st.title("캡슐 커피 가격 · 상태 이벤트 타임라인")

# =========================
# 1️⃣ 입력 UI
# =========================
product_name = st.text_input("제품명 입력 (부분 검색 가능)")

# 이벤트 유형 필터 UI (기본: 전체, 숨김)
use_event_filter = st.checkbox("이벤트 유형 선택", value=False)

event_types = [
    "신제품", "품절", "복원",
    "정상가 인상", "정상가 인하",
    "할인 시작", "할인 종료"
]

selected_events = None
if use_event_filter:
    selected_events = st.multiselect(
        "보고 싶은 이벤트 유형 선택",
        event_types,
        default=event_types
    )

# =========================
# 2️⃣ 데이터 조회
# =========================
if product_name:
    query = supabase.table("product_all_events") \
    .select(
        "product_name, event_date, event_type, "
        "prev_normal_price, current_normal_price, "
        "prev_sale_price, current_sale_price"
    ) \
    .ilike("product_name", f"%{product_name}%") \
    .order("event_date")

    # 👉 필터를 사용할 때만 event_type 조건 추가
    if selected_events is not None:
        query = query.in_("event_type", selected_events)

    res = query.execute()


    if not res.data:
        st.warning("해당 제품의 이벤트가 없습니다.")
    else:
        df = pd.DataFrame(res.data)

        def format_price(v):
            if v is None:
                return "-"
            try:
                return f"{int(v):,}"
            except:
                return "-"

        df["가격변동"] = df.apply(
            lambda r: (
                f"{format_price(r['prev_normal_price'])} → {format_price(r['current_normal_price'])}"
                if r["event_type"] in ["정상가 인상", "정상가 인하"]
                else
                f"{format_price(r['prev_sale_price'])} → {format_price(r['current_sale_price'])}"
                if r["event_type"] in ["할인 시작", "할인 종료"]
                else "-"
            ),
            axis=1
        )

        # 스타일 함수는 데이터 있을 때 정의
        def highlight_event(row):
            color_map = {
                "신제품": "#E3F2FD",
                "할인 시작": "#E8F5E9",
                "할인 종료": "#FFFDE7",
                "정상가 인상": "#FBE9E7",
                "정상가 인하": "#E1F5FE",
                "품절": "#FCE4EC",
                "복원": "#F3E5F5"
            }
            return [f"background-color: {color_map.get(row.event_type, '')}"] * len(row)

        # ✅ 제품별 타임라인 출력 (여기가 반드시 이 위치)
        for product, g in df.groupby("product_name"):
            st.subheader(product)
            df_view = g[["event_date", "event_type", "가격변동"]]
            st.dataframe(
                df_view.style.apply(highlight_event, axis=1),
                use_container_width=True
            )

else:
    st.info("상단에 제품명을 입력하세요.")

