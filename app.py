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
        "brew_type_kr",  # 🔥 추가
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

@st.cache_data(ttl=300)
def load_lifecycle_events(product_url: str):
    res = (
        supabase.table("product_lifecycle_events")
        .select("date, lifecycle_event")
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
# 🔧 제품 선택 토글 함수 (전역으로 이동)
# =========================
def toggle_product(pname):
    if "selected_products" not in st.session_state:
        st.session_state.selected_products = set()

    if pname in st.session_state.selected_products:
        st.session_state.selected_products.remove(pname)
    else:
        st.session_state.selected_products.add(pname)


# =========================
# 4️⃣ 세션 상태
# =========================
if "selected_products" not in st.session_state:
    st.session_state.selected_products = set()
if "keyword_results" not in st.session_state:
    st.session_state.keyword_results = {}
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

        # 선택 제품 초기화
        st.session_state.selected_products = set()

        # 🔥 검색 결과 완전 초기화
        st.session_state.keyword_results = {}

        # 조회 상태 초기화
        st.session_state.show_results = False

        # (선택) 키워드 입력값도 초기화하고 싶으면
        st.session_state.keyword_input = ""

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
            placeholder="예: 스노우, 쥬시",
            label_visibility="collapsed"
        )

    with col_add:
        if st.button("🔍 검색 추가", use_container_width=True):
            kw = keyword_input.strip()
            if kw:
                mask = _norm_series(df_all["product_name"]).str.contains(kw, case=False)
                result_df = df_all[mask].copy()

                if not result_df.empty:
                    st.session_state.keyword_results[kw] = result_df

                st.rerun()

    with col_reset:
        if st.button("🧹 전체 초기화", use_container_width=True):
            st.session_state.keyword_results = {}
            st.session_state.selected_products = set()
            st.session_state.show_results = False
            st.rerun()

    # -------------------------
    # 🔥 키워드별 결과 출력
    # -------------------------

    st.subheader("📦 비교할 제품 선택")

    if st.session_state.keyword_results:

        all_candidates = []

        # 최근 검색이 위
        for kw in reversed(list(st.session_state.keyword_results.keys())):

            st.markdown(f"#### 🔎 '{kw}' 검색 결과")

            col_title, col_delete = st.columns([8, 2])

            with col_delete:
                if st.button("검색 결과 삭제", key=f"del_{kw}"):

                    df_kw = st.session_state.keyword_results[kw]
                    remove_list = df_kw["product_name"].tolist()
                
                    # 선택된 제품 중 해당 키워드 결과에 해당하는 것만 제거
                    st.session_state.selected_products = {
                        p for p in st.session_state.selected_products
                        if p not in remove_list
                    }
                
                    del st.session_state.keyword_results[kw]
                    st.rerun()


            df_kw = st.session_state.keyword_results[kw]
            product_list = sorted(df_kw["product_name"].unique().tolist())

            for pname in product_list:
                st.checkbox(
                    pname,
                    value=pname in st.session_state.selected_products,
                    key=f"chk_{kw}_{pname}",
                    on_change=toggle_product,
                    args=(pname,),
                )

            all_candidates.append(df_kw)

        candidates_df = pd.concat(all_candidates).drop_duplicates()

    else:
        st.info("제품명 키워드를 추가하세요.")
        candidates_df = pd.DataFrame()

# --- B) 필터 선택 ---
else:



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

# 필터 결과에서 제품 선택
if search_mode == "필터 선택 (브랜드/카테고리)":

    st.subheader("📦 비교할 제품 선택")

    with st.expander("목록 펼치기 / 접기", expanded=False):
    
        product_list = sorted(candidates_df["product_name"].unique().tolist())
    
        for pname in product_list:
            st.checkbox(
                pname,
                value=pname in st.session_state.selected_products,
                key=f"chk_filter_{pname}",
                on_change=toggle_product,
                args=(pname,),
            )



# =========================
# 8️⃣ 결과 표시
# =========================


selected_products = list(st.session_state.selected_products)

if not selected_products:
    st.info("제품을 선택하세요.")
    st.stop()

if not st.session_state.show_results:
    st.info("제품을 선택한 뒤 ‘조회하기’를 클릭하세요.")
    st.stop()

st.divider()
st.subheader(f"📊 조회 결과 ({len(selected_products)}개 제품)")

# 🔥 반드시 여기에서 초기화
timeline_rows = []
lifecycle_rows = []

for pname in selected_products:
    row = df_all[df_all["product_name"] == pname].iloc[0]

    # 가격 이벤트
    df_price = load_events(row["product_url"])
    if not df_price.empty:
        tmp = df_price.copy()
        tmp["product_name"] = pname
        tmp["event_date"] = pd.to_datetime(tmp["date"])
        tmp["unit_price"] = tmp["unit_price"].astype(float)
        timeline_rows.append(tmp[["product_name", "event_date", "unit_price"]])

    # lifecycle 이벤트
    df_life = load_lifecycle_events(row["product_url"])
    if not df_life.empty:
        tmp2 = df_life.copy()
        tmp2["product_name"] = pname
        tmp2["event_date"] = pd.to_datetime(tmp2["date"])
        lifecycle_rows.append(tmp2[["product_name", "event_date", "lifecycle_event"]])

# =========================
# 8-1️⃣ 개당 가격 타임라인 비교 차트
# =========================

if timeline_rows:

    df_timeline = pd.concat(timeline_rows, ignore_index=True)

    base_line = (
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
    )

    layers = [base_line]

    # ---------------------------------
    # 🔥 lifecycle 이벤트 마커 + 텍스트
    # ---------------------------------
    if lifecycle_rows:

        df_life_all = pd.concat(lifecycle_rows, ignore_index=True)

        icon_config = {
            "NEW_PRODUCT": {"color": "green", "label": "NEW"},
            "OUT_OF_STOCK": {"color": "red", "label": "품절"},
            "RESTOCK": {"color": "orange", "label": "복원"},
        }

        for event_type, cfg in icon_config.items():

            df_filtered = df_life_all[
                df_life_all["lifecycle_event"] == event_type
            ]

            if df_filtered.empty:
                continue

            # 1️⃣ 마커
            point_layer = (
                alt.Chart(df_filtered)
                .mark_point(
                    size=200,
                    shape="triangle-up",
                    color=cfg["color"],
                )
                .encode(
                    x="event_date:T",
                    tooltip=[
                        alt.Tooltip("product_name:N", title="제품"),
                        alt.Tooltip("event_date:T", title="날짜"),
                        alt.Tooltip("lifecycle_event:N", title="이벤트"),
                    ],
                )
            )

            # 2️⃣ 텍스트 라벨
            text_layer = (
                alt.Chart(df_filtered)
                .mark_text(
                    dy=-15,
                    fontSize=11,
                    fontWeight="bold",
                    color=cfg["color"],
                )
                .encode(
                    x="event_date:T",
                    text=alt.value(cfg["label"]),
                )
            )

            layers.append(point_layer)
            layers.append(text_layer)

    chart = (
        alt.layer(*layers)
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
            st.success("현재(마지막 관측일 기준) 할인 중")
        else:
            st.info("정상가")

    with c3:
        df_life = load_lifecycle_events(p["product_url"])
        has_new = (
            not df_life.empty and
            (df_life["lifecycle_event"] == "NEW_PRODUCT").any()
        )
    
        if has_new:
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

        df_price = load_events(p["product_url"])
        df_life = load_lifecycle_events(p["product_url"])
    
        frames = []
    
        if not df_price.empty:
            frames.append(
                df_price[["date", "event_type"]]
            )
    
        if not df_life.empty:
            df_life = df_life[df_life["lifecycle_event"].notna()]
            df_life = df_life.rename(columns={"lifecycle_event": "event_type"})
            frames.append(
                df_life[["date", "event_type"]]
            )
    
        if frames:
            df_all_events = pd.concat(frames)
            df_all_events["date"] = pd.to_datetime(df_all_events["date"]).dt.date
            df_all_events = df_all_events.sort_values("date", ascending=False)
    
            icon_map = {
                "DISCOUNT": "💸 할인",
                "NORMAL": "💰 정상가",
                "NEW_PRODUCT": "🆕 신제품",
                "OUT_OF_STOCK": "❌ 품절",
                "RESTOCK": "🔄 복원",
            }
    
            df_all_events["event_type"] = (
                df_all_events["event_type"]
                .map(icon_map)
                .fillna(df_all_events["event_type"])
            )
    
            df_all_events = df_all_events.rename(columns={
                "date": "날짜",
                "event_type": "이벤트"
            })
    
            st.dataframe(df_all_events, use_container_width=True, hide_index=True)
    
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

    if any(word in q for word in ["오른", "상승", "올랐", "증가"]):
        return "PRICE_UP"

    if "변동" in q or "많이 바뀐" in q:
        return "VOLATILITY"

    if "품절" in q:
        return "OUT"

    if "복원" in q:
        return "RESTORE"

    if "정상가" in q and "변동" in q:
        return "NORMAL_CHANGE"


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

    # ---------------------------------
    # 1️⃣ Brew Type 조건 반영
    # ---------------------------------
    brew_condition = extract_brew_type(question, df_summary)
    if brew_condition:
        df_work = df_work[df_work["brew_type_kr"] == brew_condition]

    # ---------------------------------
    # 2️⃣ 기간 조건 추출
    # ---------------------------------
    start_date = extract_period(question)

    # ---------------------------------
    # 3️⃣ 현재 할인 제품
    # ---------------------------------
    if intent == "DISCOUNT" and not start_date:
        df = df_work[df_work["is_discount"] == True]
        if df.empty:
            return None

        return "현재 할인 중 제품:\n- " + "\n- ".join(df["product_name"].tolist())

    # ---------------------------------
    # 4️⃣ 최저가 + 기간 묶기
    # ---------------------------------
    if intent == "PRICE_MIN":
    
        # 1. 전체 중 최저가 계산
        min_price = df_work["current_unit_price"].min()
        df_min = df_work[df_work["current_unit_price"] == min_price]
    
        if df_min.empty:
            return None
    
        output_lines = []
    
        for _, row in df_min.iterrows():
    
            # 2. 해당 제품의 과거 가격 이벤트 불러오기
            res = (
                supabase.table("product_all_events")
                .select("date, unit_price")
                .eq("product_url", row["product_url"])
                .execute()
            )
    
            if not res.data:
                continue
    
            df_hist = pd.DataFrame(res.data)
            df_hist["date"] = pd.to_datetime(df_hist["date"])
            df_hist["unit_price"] = df_hist["unit_price"].astype(float)
    
            # 3. 최저가 기록한 날짜만 필터
            df_low = df_hist[df_hist["unit_price"] == min_price]
    
            if df_low.empty:
                continue
    
            start_date = df_low["date"].min().date()
            end_date = df_low["date"].max().date()
    
            output_lines.append(
                f"- {row['product_name']} / {min_price:,.1f}원\n"
                f"  최저가 기간: {start_date} ~ {end_date}"
            )
    
        if not output_lines:
            return None
    
        return "최저가 제품 목록:\n\n" + "\n\n".join(output_lines)


    # ---------------------------------
    # 5️⃣ 최고가 제품
    # ---------------------------------
    if intent == "PRICE_MAX":
        df = df_work.sort_values("current_unit_price", ascending=False)
        if df.empty:
            return None

        top = df.iloc[0]
        return f"가장 비싼 제품은 '{top['product_name']}'이며 {float(top['current_unit_price']):,.1f}원입니다."

    # ---------------------------------
    # 6️⃣ 최근 신제품
    # ---------------------------------
    if intent == "NEW":

        res = (
            supabase.table("product_lifecycle_events")
            .select("product_url")
            .eq("lifecycle_event", "NEW_PRODUCT")
            .execute()
        )

        if not res.data:
            return None

        urls = [r["product_url"] for r in res.data]
        df = df_work[df_work["product_url"].isin(urls)]

        if df.empty:
            return None

        return "최근 신제품:\n- " + "\n- ".join(df["product_name"].tolist())

    # ---------------------------------
    # 7️⃣ 최근 품절 제품
    # ---------------------------------
    if intent == "OUT":

        res = (
            supabase.table("product_lifecycle_events")
            .select("product_url")
            .eq("lifecycle_event", "OUT_OF_STOCK")
            .execute()
        )

        if not res.data:
            return None

        urls = [r["product_url"] for r in res.data]
        df = df_work[df_work["product_url"].isin(urls)]

        if df.empty:
            return None

        return "최근 품절 제품:\n- " + "\n- ".join(df["product_name"].tolist())

    # ---------------------------------
    # 8️⃣ 최근 복원 제품
    # ---------------------------------
    if intent == "RESTORE":

        res = (
            supabase.table("product_lifecycle_events")
            .select("product_url")
            .eq("lifecycle_event", "RESTOCK")
            .execute()
        )

        if not res.data:
            return None

        urls = [r["product_url"] for r in res.data]
        df = df_work[df_work["product_url"].isin(urls)]

        if df.empty:
            return None

        return "최근 복원된 제품:\n- " + "\n- ".join(df["product_name"].tolist())

    # ---------------------------------
    # 9️⃣ 가격 변동폭 (기간 포함)
    # ---------------------------------
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

        row = df_work[df_work["product_url"] == top_url]
        if row.empty:
            return None

        return (
            f"최근 기간 가격 변동 폭이 가장 큰 제품은 "
            f"'{row.iloc[0]['product_name']}'이며 "
            f"변동폭은 {top_value:,.1f}원입니다."
        )


    
    # ---------------------------------
    # 10️⃣ 정상가 변동 
    # ---------------------------------

    if intent == "NORMAL_CHANGE":

        start_date = extract_period(question)

        query = supabase.table("product_normal_price_events").select("*")

    if start_date:
        query = query.gte("date", start_date.strftime("%Y-%m-%d"))

    res = query.order("date", desc=True).execute()

    if not res.data:
        return "해당 기간 내 정상가 변동이 없습니다."

    df = pd.DataFrame(res.data)

    results = []

    for _, row in df.iterrows():

        product_row = df_summary[
            df_summary["product_url"] == row["product_url"]
        ]

        if product_row.empty:
            continue

        pname = product_row.iloc[0]["product_name"]

        results.append(
            f"- {pname} / {row['prev_price']:,.0f}원 → "
            f"{row['date']}에 {row['normal_price']:,.0f}원 "
            f"({row['price_diff']:+,.0f}원)"
        )

    return "기간 내 정상가 변동 제품 목록:\n" + "\n".join(results)
    
    # ---------------------------------
    # 10️⃣ Rule 미적용 → LLM fallback
    # ---------------------------------
    
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




















