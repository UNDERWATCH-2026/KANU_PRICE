import streamlit as st
import pandas as pd
from supabase import create_client

# =========================
# 0️⃣ Supabase 설정
# =========================
SUPABASE_URL = "https://fgaxjjpktwksdoizerwh.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZnYXhqanBrdHdrc2RvaXplcndoIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2ODcyNzM3MSwiZXhwIjoyMDg0MzAzMzcxfQ.bBSInJ9t08yA1Spw4HuOQnczUtVElzhO_QPSUBkMk1g"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# =========================
# 0️⃣ 기본 페이지 설정
# =========================
st.set_page_config(
    page_title="Capsule Price Intelligence",
    layout="wide"
)

st.markdown("""
<style>
.stApp { background-color: #F7F8FA; }

.card {
    background: #FFFFFF;
    border-radius: 12px;
    padding: 14px 16px;        /* 🔽 padding 줄임 */
    box-shadow: 0 2px 8px rgba(0,0,0,0.04);
    margin-bottom: 10px;
    min-height: 80px;          /* 🔽 카드 높이 고정 */
}

.section-title {
    font-size: 20px;
    font-weight: 700;
    margin: 10px 0 14px 0;
}

.kpi-label {
    font-size: 12px;
    color: #6B7280;
}


.kpi-number {
    font-size: 22px;           /* 🔽 숫자 크기 */
    font-weight: 700;
    margin-top: 2px;
}

.event-date {
    font-size: 12px;
    color: #9CA3AF;
}
</style>
""", unsafe_allow_html=True)

# =========================
# 타이틀
# =========================
st.title("📊 Capsule Price Intelligence")
st.markdown(
    "<div style='color:#6B7280; font-size:14px; margin-bottom:12px;'>"
    "제품 단위 가격 · 할인 · 정상가 정책 · 품절 이벤트 분석"
    "</div>",
    unsafe_allow_html=True
)

st.divider()

# =========================
# 1️⃣ 상단 입력 영역
# =========================
view_mode = st.radio(
    "보기 기준",
    ["이벤트 기준", "제품 기준"],
    horizontal=True
)

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

    if selected_events is not None:
        query = query.in_("event_type", selected_events)

    res = query.execute()

    if not res.data:
        st.warning("해당 조건의 데이터가 없습니다.")
        st.stop()

    df = pd.DataFrame(res.data)

    # =========================
    # 가격 변동 컬럼 생성
    # =========================
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
    def kpi(label, value):
    st.markdown(f"""
    <div class="card">
        <div class="kpi-label">{label}</div>
        <div class="kpi-number">{value}</div>
    </div>
    """, unsafe_allow_html=True)

    # =========================
    # KPI 요약
    # =========================
    def kpi(label, value):
        st.markdown(f"""
        <div class="card">
            <div class="kpi-label">{label}</div>
            <div class="kpi-number">{value}</div>
        </div>
        """, unsafe_allow_html=True)
    
    k1, k2, k3, k4 = st.columns(4)
    
    with k1:
        kpi("할인 시작", (df["event_type"] == "할인 시작").sum())
    
    with k2:
        kpi("할인 종료", (df["event_type"] == "할인 종료").sum())
    
    with k3:
        kpi("정상가 변동", df["event_type"].isin(["정상가 인상", "정상가 인하"]).sum())
    
    with k4:
        kpi("품절", (df["event_type"] == "품절").sum())


    st.divider()

    # =========================
    # 3️⃣ 메인 화면 (토글 분기)
    # =========================
    left, right = st.columns([3, 2])

    # -------------------------
    # 🅰 이벤트 기준
    # -------------------------
    if view_mode == "이벤트 기준":
        with left:
            st.subheader("🕒 가격 · 상태 이벤트 타임라인")

            for _, r in df.iterrows():
                st.markdown(f"""
                <div class="card">
                    <div class="event-date">
                        {r['event_date']} · {r['product_name']}
                    </div>
                    <strong>{r['event_type']}</strong><br>
                    <span style="color:#6B7280;">
                        {r['가격변동']}
                    </span>
                </div>
                """, unsafe_allow_html=True)

    # -------------------------
    # 🅱 제품 기준
    # -------------------------
    else:
        with left:
            st.subheader("📦 제품별 가격 · 상태 히스토리")

            for product, g in df.groupby("product_name"):
                last_event = g.iloc[-1]

                st.markdown(f"""
                <div class="card">
                    <h4>{product}</h4>
                    <div style="color:#6B7280; font-size:13px;">
                        최근 이벤트: <strong>{last_event['event_type']}</strong><br>
                        최근 날짜: {last_event['event_date']}
                    </div>
                </div>
                """, unsafe_allow_html=True)

                for _, r in g.iterrows():
                    st.markdown(f"""
                    <div class="card" style="margin-left:12px;">
                        <div class="event-date">{r['event_date']}</div>
                        <strong>{r['event_type']}</strong><br>
                        <span style="color:#6B7280;">
                            {r['가격변동']}
                        </span>
                    </div>
                    """, unsafe_allow_html=True)

    # =========================
    # 4️⃣ 질문 분석 패널 (공통)
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

        if ask and question.strip() != "":
            st.divider()
            st.subheader("📊 분석 결과")

            # 할인 기간
            if "할인" in question and "기간" in question:
                discounts = df[df["event_type"] == "할인 시작"]
                if discounts.empty:
                    st.info("할인 시작 이벤트가 없습니다.")
                else:
                    st.success(
                        f"할인 시작 구간: {discounts['event_date'].min()} ~ {discounts['event_date'].max()}"
                    )
                    st.dataframe(discounts[["event_date", "event_type", "가격변동"]])

            # 정상가 변동
            elif "정상가" in question:
                changes = df[df["event_type"].isin(["정상가 인상", "정상가 인하"])]
                if changes.empty:
                    st.info("정상가 변동 이벤트가 없습니다.")
                else:
                    st.success(f"정상가 변동 {len(changes)}회")
                    st.dataframe(changes[["event_date", "event_type", "가격변동"]])

            # 품절
            elif "품절" in question:
                soldout = df[df["event_type"] == "품절"]
                if soldout.empty:
                    st.info("품절 이벤트가 없습니다.")
                else:
                    st.success(f"품절 발생 {len(soldout)}회")
                    st.dataframe(soldout[["event_date", "event_type"]])

            # 할인 패턴
            elif "패턴" in question:
                discounts = df[df["event_type"].isin(["할인 시작", "할인 종료"])]
                if discounts.empty:
                    st.info("할인 이벤트가 없습니다.")
                else:
                    st.success(
                        f"할인 시작 {(discounts['event_type'] == '할인 시작').sum()}회 / "
                        f"첫 할인 {discounts['event_date'].min()} / "
                        f"최근 할인 {discounts['event_date'].max()}"
                    )
                    st.dataframe(discounts[["event_date", "event_type", "가격변동"]])

            else:
                st.warning("아직 이 질문 유형은 분석 규칙이 등록되지 않았습니다.")

else:
    st.info("상단에 제품명을 입력하세요.")

