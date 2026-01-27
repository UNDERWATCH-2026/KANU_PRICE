import streamlit as st
import pandas as pd
from supabase import create_client

# =========================
# 0️⃣ Supabase 설정
# =========================
SUPABASE_URL = "https://fgaxjjpktwksdoizerwh.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZnYXhqanBrdHdrc2RvaXplcndoIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2ODcyNzM3MSwiZXhwIjoyMDg0MzAzMzcxfQ.bBSInJ9t08yA1Spw4HuOQnczUtVElzhO_QPSUBkMk1g"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

st.set_page_config(page_title="Capsule Price Intelligence", layout="wide")
st.title("📊 Capsule Price Intelligence")

st.markdown("""
<div style="color:#6B7280; font-size:14px; margin-bottom:15px;">
제품 단위 가격 · 할인 · 정상가 정책 · 품절 이벤트 분석
</div>
""", unsafe_allow_html=True)

st.divider()

# =========================
# 1️⃣ 입력 UI (상단 필터 영역)
# =========================
product_name = st.text_input("제품명 입력 (부분 검색 가능)")

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
# 2️⃣ 데이터 조회 + 화면 구성
# =========================
if product_name:

    left, right = st.columns([3, 2])

    query = supabase.table("product_all_events") \
        .select(
            "product_name, event_date, event_type, "
            "prev_normal_price, current_normal_price, "
            "prev_sale_price, current_sale_price"
        ) \
        .ilike("product_name", f"%{product_name}%") \
        .order("event_date")

    if selected_events is not None:
        query = query.in_("event_type", selected_events)

    res = query.execute()

    # =========================
    # 🔹 왼쪽: 타임라인 패널
    # =========================
    with left:
        st.subheader("🕒 가격 · 상태 이벤트 타임라인")

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

            for product, g in df.groupby("product_name"):
                st.markdown(f"### {product}")
                df_view = g[["event_date", "event_type", "가격변동"]]
                st.dataframe(
                    df_view.style.apply(highlight_event, axis=1),
                    use_container_width=True
                )

    # =========================
    # 🔹 오른쪽: 챗 분석 패널
    # =========================
    with right:
        st.subheader("💬 가격 분석 질문")

        st.markdown("""
        <div style="color:#6B7280; font-size:13px; line-height:1.6;">
        예시 질문<br>
        • 할인 시작 기간 알려줘<br>
        • 정상가 인상 언제 있었어<br>
        • 최근 할인 패턴 요약해줘<br>
        • 품절이 가장 길었던 구간은?
        </div>
        """, unsafe_allow_html=True)

        question = st.text_area(
            "질문 입력",
            height=90,
            placeholder="예: 바리스타 레시피 메이커 할인 기간 정리"
        )

        ask = st.button("분석 실행", use_container_width=True)

        # =========================
        # 3️⃣ 질문 처리 로직 (룰 기반)
        # =========================
        if ask and question.strip() != "" and not res.data:
            st.info("먼저 제품을 검색하세요.")

        elif ask and question.strip() != "" and res.data:

            st.divider()
            st.subheader("📊 분석 결과")

            # ① 할인 기간 질문
            if "할인" in question and "기간" in question:
                discounts = df[df["event_type"] == "할인 시작"]

                if discounts.empty:
                    st.info("할인 시작 이벤트가 없습니다.")
                else:
                    start = discounts["event_date"].min()
                    end = discounts["event_date"].max()

                    st.success(f"할인 시작 구간: {start} ~ {end}")
                    st.dataframe(discounts[["event_date", "event_type", "가격변동"]])

            # ② 정상가 변동 질문
            elif "정상가" in question and ("인상" in question or "변동" in question):
                changes = df[df["event_type"].isin(["정상가 인상", "정상가 인하"])]

                if changes.empty:
                    st.info("정상가 변동 이벤트가 없습니다.")
                else:
                    up = (changes["event_type"] == "정상가 인상").sum()
                    down = (changes["event_type"] == "정상가 인하").sum()

                    st.success(f"정상가 변동 {len(changes)}회 (인상 {up}회 / 인하 {down}회)")
                    st.dataframe(changes[["event_date", "event_type", "가격변동"]])

            # ③ 품절 질문
            elif "품절" in question:
                soldout = df[df["event_type"] == "품절"]

                if soldout.empty:
                    st.info("품절 이벤트가 없습니다.")
                else:
                    st.success(f"품절 발생 {len(soldout)}회")
                    st.dataframe(soldout[["event_date", "event_type"]])

            # ④ 최근 할인 패턴 요약
            elif "할인" in question and "패턴" in question:
                discounts = df[df["event_type"].isin(["할인 시작", "할인 종료"])]

                if discounts.empty:
                    st.info("할인 이벤트가 없습니다.")
                else:
                    cnt = (discounts["event_type"] == "할인 시작").sum()
                    first = discounts["event_date"].min()
                    last = discounts["event_date"].max()

                    st.success(
                        f"할인 시작 {cnt}회 발생 / 첫 할인 {first} / 최근 할인 {last}"
                    )
                    st.dataframe(discounts[["event_date", "event_type", "가격변동"]])

            else:
                st.warning("아직 이 질문 유형은 분석 규칙이 등록되지 않았습니다.")

else:
    st.info("상단에 제품명을 입력하세요.")
