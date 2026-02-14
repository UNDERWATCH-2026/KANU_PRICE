import streamlit as st
import pandas as pd
import altair as alt
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
    cols = [
        "product_url",
        "brand",
        "category1",
        "category2",
        "product_name",
        "current_unit_price",
        "is_discount",
        "first_seen_date",
        "last_seen_date",
        "event_count",
        "product_event_status",
        "is_new_product",
    ]
    res = supabase.table("product_price_summary_enriched").select(", ".join(cols)).execute()
    return pd.DataFrame(res.data)

@st.cache_data(ttl=300)
def load_events(product_url: str):
    res = (
        supabase.table("product_all_events")
        .select("date, unit_price, event_type")
        .eq("product_url", product_url)
        .order("date", desc=True)
        .execute()
    )
    return pd.DataFrame(res.data)

# =========================
# 2-1️⃣ 질문 로그 저장
# =========================
def save_question_log(question: str, q_type: str, used_llm: bool):
    try:
        supabase.table("question_logs").insert({
            "question_text": question,
            "question_type": q_type,
            "used_llm": used_llm
        }).execute()
    except Exception as e:
        print("로그 저장 실패:", e)


# =========================
# 3️⃣ 유틸
# =========================
def _norm_series(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str)

def options_from(df: pd.DataFrame, col: str):
    if col not in df.columns:
        return []
    vals = df[col].dropna().astype(str)
    vals = [v.strip() for v in vals.tolist() if v.strip()]
    return sorted(list(dict.fromkeys(vals)))

# =========================
# 4️⃣ 세션 상태
# =========================
if "selected_products" not in st.session_state:
    st.session_state.selected_products = set()
if "keyword_searches" not in st.session_state:
    st.session_state.keyword_searches = []
if "active_mode" not in st.session_state:
    st.session_state.active_mode = "키워드 검색"
if "show_results" not in st.session_state:
    st.session_state.show_results = False

# =========================
# 5️⃣ 메인 UI
# =========================
st.title("☕ Capsule Price Intelligence")

# -------------------------
# 조회 기준 선택
# -------------------------
st.subheader("🔎 조회 기준")
search_mode = st.radio(
    "검색 방식 선택",
    ["키워드 검색", "필터 선택 (브랜드/카테고리)"],
    horizontal=True
)

if search_mode != st.session_state.active_mode:
    st.session_state.active_mode = search_mode
    st.session_state.selected_products = set()
    st.session_state.keyword_searches = []
    st.session_state.show_results = False
    st.rerun()

st.divider()

# -------------------------
# 데이터 로딩
# -------------------------
df_all = load_product_summary()
if df_all.empty:
    st.warning("아직 집계된 제품 데이터가 없습니다.")
    st.stop()

# -------------------------
# 상단 버튼
# -------------------------
col_query, col_clear = st.columns([1, 1])
with col_query:
    if st.button("📊 조회하기", type="primary", use_container_width=True):
        st.session_state.show_results = True
with col_clear:
    if st.button("🗑️ 전체 삭제", use_container_width=True):
        st.session_state.selected_products = set()
        st.session_state.keyword_searches = []
        st.session_state.show_results = False
        st.rerun()

st.divider()

# =========================
# 6️⃣ 조회 조건
# =========================
st.subheader("🔍 조회 조건")
candidates_df = pd.DataFrame()

# --- A) 키워드 검색 ---
if search_mode == "키워드 검색":
    col_input, col_add, col_reset = st.columns([6, 2, 2])
    with col_input:
        keyword_input = st.text_input(
            "제품명 키워드 입력",
            placeholder="예: 다크, 디카페인",
            label_visibility="collapsed"
        )
    with col_add:
        if st.button("🔍 검색 추가", use_container_width=True):
            kw = keyword_input.strip()
            if kw and kw not in st.session_state.keyword_searches:
                st.session_state.keyword_searches.append(kw)
                st.rerun()
    with col_reset:
        if st.button("🧹 초기화", use_container_width=True):
            st.session_state.keyword_searches = []
            st.session_state.selected_products = set()
            st.session_state.show_results = False
            st.rerun()

    if st.session_state.keyword_searches:
        mask = pd.Series(False, index=df_all.index)
        for kw in st.session_state.keyword_searches:
            mask |= _norm_series(df_all["product_name"]).str.contains(kw, case=False)
        candidates_df = df_all[mask].copy()
    else:
        st.info("제품명 키워드를 추가하세요.")

# --- B) 필터 선택 ---
else:

    st.markdown("### 🔍 조회 조건")

    col1, col2, col3 = st.columns(3)

    with col1:
        brands = options_from(df_all, "brand")
        sel_brand = st.selectbox("브랜드", ["(전체)"] + brands)

    df1 = df_all if sel_brand == "(전체)" else df_all[df_all["brand"] == sel_brand]

    with col2:
        cat1s = options_from(df1, "category1")
        sel_cat1 = st.selectbox("카테고리1", ["(전체)"] + cat1s)

    df2 = df1 if sel_cat1 == "(전체)" else df1[df1["category1"] == sel_cat1]

    with col3:
        cat2s = options_from(df2, "category2")
        sel_cat2 = st.selectbox("카테고리2", ["(전체)"] + cat2s)

    candidates_df = df2 if sel_cat2 == "(전체)" else df2[df2["category2"] == sel_cat2]

# =========================
# 7️⃣ 제품 선택
# =========================
st.subheader("📦 비교할 제품 선택")

def toggle_product(pname):
    if pname in st.session_state.selected_products:
        st.session_state.selected_products.remove(pname)
    else:
        st.session_state.selected_products.add(pname)

product_list = sorted(candidates_df["product_name"].unique().tolist())
for pname in product_list:
    st.checkbox(
        pname,
        value=pname in st.session_state.selected_products,
        key=f"chk_{pname}",
        on_change=toggle_product,
        args=(pname,)
    )

selected_products = list(st.session_state.selected_products)
if not selected_products:
    st.info("제품을 선택하세요.")
    st.stop()

# =========================
# 8️⃣ 결과 표시
# =========================
if not st.session_state.show_results:
    st.info("제품을 선택한 뒤 ‘조회하기’를 클릭하세요.")
    st.stop()

st.divider()
st.subheader(f"📊 조회 결과 ({len(selected_products)}개 제품)")

# =========================
# 8-1️⃣ 개당 가격 타임라인 비교 차트
# =========================
timeline_rows = []

for pname in selected_products:
    row = df_all[df_all["product_name"] == pname].iloc[0]
    df_ev = load_events(row["product_url"])
    if df_ev.empty:
        continue

    tmp = df_ev.copy()
    tmp["product_name"] = pname
    tmp["event_date"] = pd.to_datetime(tmp["date"])
    tmp["unit_price"] = tmp["unit_price"].astype(float)

    timeline_rows.append(tmp[["product_name", "event_date", "unit_price"]])

if timeline_rows:
    df_timeline = pd.concat(timeline_rows, ignore_index=True)
    chart = (
        alt.Chart(df_timeline)
        .mark_line(point=True)
        .encode(
            x=alt.X("event_date:T", title="날짜"),
            y=alt.Y("unit_price:Q", title="개당 가격 (원)"),
            color=alt.Color("product_name:N", title="제품"),
            tooltip=[
                alt.Tooltip("product_name:N", title="제품"),
                alt.Tooltip("event_date:T", title="날짜"),
                alt.Tooltip("unit_price:Q", title="개당 가격", format=",.1f"),
            ],
        )
        .properties(height=420)
        .interactive()
    )
    st.altair_chart(chart, use_container_width=True)
else:
    st.info("비교 가능한 이벤트 데이터가 없습니다.")

st.divider()

# =========================
# 8-2️⃣ 제품별 카드
# =========================
for pname in selected_products:
    p = df_all[df_all["product_name"] == pname].iloc[0]
    st.markdown(f"### {p['product_name']}")

    c1, c2, c3, c4 = st.columns(4)

    with c1:
        st.metric("개당 가격", f"{float(p['current_unit_price']):,.1f}원")

    with c2:
        if p["is_discount"]:
            st.success("할인 중")
        else:
            st.info("정상가")

    with c3:
        if p["is_new_product"]:
            st.warning("🆕 신제품")
        else:
            st.caption(f"관측 시작일\n{p['first_seen_date']}")

    with c4:
        st.caption(f"마지막 관측일\n{p['last_seen_date']}")

    # 이벤트 상태
    if p["product_event_status"] == "NO_EVENT_STABLE":
        st.info("📊 가격 변동 없음")
    else:
        st.success(f"📈 가격 이벤트 {p['event_count']}건")

    # 이벤트 히스토리
    with st.expander("📅 이벤트 히스토리"):
        df_ev = load_events(p["product_url"])
        if not df_ev.empty:
            df_ev["date"] = pd.to_datetime(df_ev["date"]).dt.date
            st.dataframe(df_ev, use_container_width=True, hide_index=True)
        else:
            st.caption("이벤트 없음")

    st.divider()


# =========================
# 9️⃣ 자연어 질문 (Rule → LLM fallback)
# =========================
st.divider()
st.subheader("🤖 가격 인사이트 질문")

question = st.text_input(
    "자연어로 질문하세요",
    placeholder="예: 에스프레소 중 최저가 / 최근 3개월 변동폭 큰 제품",
)

from datetime import datetime, timedelta


# -------------------------
# 1️⃣ 의도 분류
# -------------------------
def classify_intent(q: str):
    q = q.lower()

    if "할인" in q:
        return "DISCOUNT"

    if "신제품" in q:
        return "NEW"

    if "가장 싼" in q or "최저가" in q:
        return "PRICE_MIN"

    if "비싼" in q or "최고가" in q:
        return "PRICE_MAX"

    if "오른" in q or "상승" in q:
        return "PRICE_UP"

    if "변동" in q or "많이 바뀐" in q:
        return "VOLATILITY"

    return "UNKNOWN"


# -------------------------
# 2️⃣ 기간 추출
# -------------------------
def extract_period(q: str):
    today = datetime.today()

    if "최근 7일" in q:
        return today - timedelta(days=7)

    if "최근 한달" in q or "최근 30일" in q:
        return today - timedelta(days=30)

    if "최근 3개월" in q:
        return today - timedelta(days=90)

    if "최근 1년" in q:
        return today - timedelta(days=365)

    return None


# -------------------------
# 3️⃣ Brew Type 추출
# -------------------------
def extract_brew_type(q: str, df_all: pd.DataFrame):
    q = q.lower()
    brew_list = df_all["brew_type_kr"].dropna().unique().tolist()

    for brew in brew_list:
        if brew and brew.lower() in q:
            return brew

    return None


# -------------------------
# 4️⃣ Rule 실행
# -------------------------
def execute_rule(intent, question, df_summary):

    df_work = df_summary.copy()

    # Brew Type 조건 반영
    brew_condition = extract_brew_type(question, df_summary)
    if brew_condition:
        df_work = df_work[df_work["brew_type_kr"] == brew_condition]

    start_date = extract_period(question)

    # 1️⃣ 현재 할인
    if intent == "DISCOUNT" and not start_date:
        df = df_work[df_work["is_discount"] == True]
        if df.empty:
            return None
        return "현재 할인 중 제품:\n- " + "\n- ".join(df["product_name"].tolist())

    # 2️⃣ 최저가
    if intent == "PRICE_MIN":
        df = df_work.sort_values("current_unit_price")
        if df.empty:
            return None
        top = df.iloc[0]
        return f"가장 저렴한 제품은 '{top['product_name']}'이며 {float(top['current_unit_price']):,.1f}원입니다."

    # 3️⃣ 변동성 (기간 포함)
    if intent == "VOLATILITY" and start_date:
        res = (
            supabase.table("product_all_events")
            .select("product_url, unit_price, date")
            .gte("date", start_date.strftime("%Y-%m-%d"))
            .execute()
        )

        if not res.data:
            return None

        df = pd.DataFrame(res.data)
        df["unit_price"] = df["unit_price"].astype(float)

        volatility = (
            df.groupby("product_url")["unit_price"]
            .agg(lambda x: x.max() - x.min())
            .sort_values(ascending=False)
        )

        if volatility.empty:
            return None

        top_url = volatility.index[0]
        top_value = volatility.iloc[0]

        row = df_summary[df_summary["product_url"] == top_url]
        if row.empty:
            return None

        return f"최근 기간 가격 변동 폭이 가장 큰 제품은 '{row.iloc[0]['product_name']}'이며 변동폭은 {top_value:,.1f}원입니다."

    return None


# -------------------------
# 5️⃣ LLM fallback
# -------------------------
def llm_fallback(question: str, df_summary: pd.DataFrame):
    context = df_summary[
        ["product_name", "current_unit_price", "is_discount", "is_new_product", "brew_type_kr"]
    ].to_dict(orient="records")

    prompt = f"""
    당신은 커피 캡슐 가격 분석 전문가입니다.
    아래 데이터 기반으로 질문에 답하세요.

    데이터:
    {context}

    질문:
    {question}
    """

    from openai import OpenAI
    client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
    )

    return response.choices[0].message.content


# -------------------------
# 6️⃣ 오케스트레이션
# -------------------------
if question:
    intent = classify_intent(question)
    answer = execute_rule(intent, question, df_all)

    if answer:
        save_question_log(question, intent, False)
        st.success(answer)
    else:
        with st.spinner("분석 중..."):
            answer = llm_fallback(question, df_all)
        save_question_log(question, intent, True)
        st.success(answer)




