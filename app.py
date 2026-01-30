import streamlit as st
import pandas as pd
from supabase import create_client

# =========================
# Supabase 설정
# =========================
SUPABASE_URL = "https://fgaxjjpktwksdoizerwh.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZnYXhqanBrdHdrc2RvaXplcndoIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2ODcyNzM3MSwiZXhwIjoyMDg0MzAzMzcxfQ.bBSInJ9t08yA1Spw4HuOQnczUtVElzhO_QPSUBkMk1g"
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# =========================
# 페이지 설정
# =========================
st.set_page_config(page_title="Capsule Price Intelligence", layout="wide")

st.markdown("""
<style>
.stApp { background-color: #F7F8FA; }
header { visibility: hidden; height: 0px; }
.block-container { padding-top: 1rem; }
.card {
    background:#FFF;
    border-radius:12px;
    padding:14px 16px;
    box-shadow:0 2px 8px rgba(0,0,0,.04);
    margin-bottom:10px;
    min-height:80px;
}
.kpi-label { font-size:12px; color:#6B7280; }
.kpi-number { font-size:22px; font-weight:700; }
.event-date { font-size:12px; color:#9CA3AF; }
</style>
""", unsafe_allow_html=True)

# =========================
# 공통 함수
# =========================
def format_price(v):
    if v is None:
        return "-"
    try:
        return f"{int(v):,}"
    except:
        return "-"

def kpi(label, value):
    st.markdown(f"""
    <div class="card">
        <div class="kpi-label">{label}</div>
        <div class="kpi-number">{value}</div>
    </div>
    """, unsafe_allow_html=True)

# =========================
# 타이틀
# =========================
st.title("Capsule Price Intelligence")
st.caption("제품 단위 가격 · 할인 · 정상가 · 품절 이벤트 분석")
st.divider()

# =========================
# 상단 입력
# =========================
view_mode = st.radio("보기 기준", ["이벤트 기준", "제품 기준"], horizontal=True)
product_name = st.text_input("제품명 입력 (부분 검색 가능)")

# =========================
# 데이터 조회
# =========================
if product_name:

    query = (
        supabase.table("product_price_events")
        .select(
            "product_name,event_date,price_event_type,"
            "prev_normal_price,current_normal_price,"
            "prev_sale_price,current_sale_price"
        )
        .ilike("product_name", f"%{product_name}%")
        .order("event_date")
    )


    res = query.execute()

    if not res.data:
        st.warning("데이터가 없습니다.")
        st.stop()

    df = pd.DataFrame(res.data)

    # 가격 변동 컬럼
    df["가격변동"] = df.apply(
        lambda r:
        f"{format_price(r['prev_normal_price'])} → {format_price(r['current_normal_price'])}"
        if r["price_event_type"] in ["NORMAL_UP","NORMAL_DOWN"]
        else
        f"{format_price(r['prev_sale_price'])} → {format_price(r['current_sale_price'])}"
        if r["price_event_type"] in ["DISCOUNT_START","DISCOUNT_END"]
        else "-",
        axis=1
    )


    # =========================
    # KPI 요약 (4칸)
    # =========================
    c1, c2, c3, c4 = st.columns(4)

    with c1:
        kpi("할인 시작", (df.event_type == "할인 시작").sum())
    with c2:
        kpi("할인 종료", (df.event_type == "할인 종료").sum())
    with c3:
        kpi("정상가 변동", df.event_type.isin(["정상가 인상","정상가 인하"]).sum())
    with c4:
        kpi("품절", (df.event_type == "품절").sum())

    st.divider()

    # =========================
    # 메인 영역
    # =========================
    left, right = st.columns([3,2])

    # 🔹 이벤트 기준
    if view_mode == "이벤트 기준":
        with left:
            st.subheader("🕒 이벤트 타임라인")
            for _, r in df.iterrows():
                st.markdown(f"""
                <div class="card">
                    <div class="event-date">{r['event_date']} · {r['product_name']}</div>
                    <strong>{r['event_type']}</strong><br>
                    <span style="color:#6B7280">{r['가격변동']}</span>
                </div>
                """, unsafe_allow_html=True)

    # 🔹 제품 기준
    else:
        with left:
            st.subheader("📦 제품 히스토리")
            for product, g in df.groupby("product_name"):
                last = g.iloc[-1]
                st.markdown(f"""
                <div class="card">
                    <h4>{product}</h4>
                    최근 이벤트: <strong>{last['event_type']}</strong><br>
                    날짜: {last['event_date']}
                </div>
                """, unsafe_allow_html=True)

                for _, r in g.iterrows():
                    st.markdown(f"""
                    <div class="card" style="margin-left:12px;">
                        <div class="event-date">{r['event_date']}</div>
                        <strong>{r['event_type']}</strong><br>
                        <span style="color:#6B7280">{r['가격변동']}</span>
                    </div>
                    """, unsafe_allow_html=True)

    # =========================
    # 질문 분석
    # =========================
    with right:
        st.subheader("💬 가격 분석 질문")
        q = st.text_area("질문 입력", height=90)
        if st.button("분석 실행", use_container_width=True) and q:
            st.success("질문 처리 로직 연결 위치")

else:
    st.info("상단에 제품명을 입력하세요.")




