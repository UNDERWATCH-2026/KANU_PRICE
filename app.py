import streamlit as st
import pandas as pd
import altair as alt
from supabase import create_client
from datetime import datetime, timedelta

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
        "brew_type_kr",
    ]
    res = supabase.table("product_price_summary_enriched").select(", ".join(cols)).execute()
    return pd.DataFrame(res.data)

@st.cache_data(ttl=300)
def load_events_bulk(product_urls: list[str]):
    if not product_urls:
        return pd.DataFrame()
    res = (
        supabase.table("product_all_events")
        .select("product_url, date, unit_price, event_type")
        .in_("product_url", product_urls)
        .execute()
    )
    df = pd.DataFrame(res.data)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    return df

@st.cache_data(ttl=300)
def load_lifecycle_events_bulk(product_urls: list[str]):
    if not product_urls:
        return pd.DataFrame()
    res = (
        supabase.table("product_lifecycle_events")
        .select("product_url, date, lifecycle_event")
        .in_("product_url", product_urls)
        .execute()
    )
    df = pd.DataFrame(res.data)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    return df

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
if "keyword_input" not in st.session_state:
    st.session_state.keyword_input = ""
if "question_input" not in st.session_state:
    st.session_state.question_input = ""

# =========================
# 5️⃣ 메인 UI
# =========================
st.title("☕ Capsule Price Intelligence")

st.subheader("🔎 조회 기준")
search_mode = st.radio(
    "검색 방식 선택",
    ["키워드 검색", "필터 선택 (브랜드/카테고리)"],
    horizontal=True
)

if search_mode != st.session_state.active_mode:
    st.session_state.active_mode = search_mode
    st.session_state.selected_products = set()
    st.session_state.keyword_results = {}
    st.session_state.show_results = False
    st.session_state.keyword_input = ""
    st.rerun()

st.divider()

df_all = load_product_summary()
if df_all.empty:
    st.warning("아직 집계된 제품 데이터가 없습니다.")
    st.stop()

# =========================
# 6️⃣ 조회 조건
# =========================
st.subheader("🔍 조회 조건")
candidates_df = pd.DataFrame()

# --- A) 키워드 검색 ---
if search_mode == "키워드 검색":

    # ✅ Enter로 검색어 추가되게 form 사용
    with st.form("kw_form", clear_on_submit=False):
        col_input, col_add, col_reset = st.columns([6, 2, 2])

        with col_input:
            st.session_state.keyword_input = st.text_input(
                "제품명 키워드 입력",
                value=st.session_state.keyword_input,
                placeholder="예: 스노우, 쥬시",
                label_visibility="collapsed"
            )

        with col_add:
            submitted_add = st.form_submit_button("🔍 검색어 추가", use_container_width=True)

        with col_reset:
            submitted_reset = st.form_submit_button("🧹 검색 초기화", use_container_width=True)

    if submitted_reset:
        st.session_state.keyword_results = {}
        st.session_state.selected_products = set()
        st.session_state.show_results = False
        st.session_state.keyword_input = ""
        st.rerun()

    if submitted_add:
        kw = st.session_state.keyword_input.strip()
        if kw:
            mask = _norm_series(df_all["product_name"]).str.contains(kw, case=False)
            result_df = df_all[mask].copy()
            if not result_df.empty:
                st.session_state.keyword_results[kw] = result_df
        st.rerun()

    st.subheader("📦 비교할 제품 선택")

    if st.session_state.keyword_results:
        all_candidates = []
        for kw in reversed(list(st.session_state.keyword_results.keys())):
            st.markdown(f"#### 🔎 '{kw}' 검색 결과")

            col_title, col_delete = st.columns([8, 2])
            with col_delete:
                if st.button("검색 결과 삭제", key=f"del_{kw}", use_container_width=True):
                    df_kw = st.session_state.keyword_results[kw]
                    remove_list = df_kw["product_name"].tolist()
                    st.session_state.selected_products = {
                        p for p in st.session_state.selected_products if p not in remove_list
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

st.divider()

# =========================
# 7️⃣ 조회 실행/초기화 (Enter로도 조회되게 form)
# =========================
with st.form("run_form", clear_on_submit=False):
    col_query, col_clear = st.columns([1, 1])

    with col_query:
        run_clicked = st.form_submit_button("📊 조회하기", use_container_width=True, type="primary")

    with col_clear:
        clear_clicked = st.form_submit_button("🗑️ 전체 삭제", use_container_width=True)

if clear_clicked:
    st.session_state.selected_products = set()
    st.session_state.keyword_results = {}
    st.session_state.show_results = False
    st.session_state.keyword_input = ""
    st.rerun()

if run_clicked:
    st.session_state.show_results = True

# =========================
# 8️⃣ 결과 표시
# =========================
selected_products = list(st.session_state.selected_products)

if not selected_products:
    st.info("제품을 선택하세요.")
    st.stop()

if not st.session_state.show_results:
    st.info("제품을 선택한 뒤 ‘조회하기’를 클릭(또는 Enter)하세요.")
    st.stop()

st.divider()
st.subheader(f"📊 조회 결과 ({len(selected_products)}개 제품)")

# 선택 제품의 product_url 확보
sel_rows = df_all[df_all["product_name"].isin(selected_products)].copy()
product_urls = sel_rows["product_url"].dropna().unique().tolist()

# ✅ Bulk 로딩 (N+1 제거)
df_events_all = load_events_bulk(product_urls)
df_life_all = load_lifecycle_events_bulk(product_urls)

# name 매핑
url_to_name = dict(zip(sel_rows["product_url"], sel_rows["product_name"]))

if not df_events_all.empty:
    df_events_all["product_name"] = df_events_all["product_url"].map(url_to_name)

if not df_life_all.empty:
    df_life_all["product_name"] = df_life_all["product_url"].map(url_to_name)

# =========================
# 8-1️⃣ 개당 가격 타임라인 비교 차트 (+ lifecycle 마커 y 보정)
# =========================
timeline_rows = []
if not df_events_all.empty:
    tmp = df_events_all.copy()
    tmp["event_date"] = tmp["date"]
    timeline_rows.append(tmp[["product_name", "event_date", "unit_price", "event_type"]])

if timeline_rows:
    df_timeline = pd.concat(timeline_rows, ignore_index=True)
    df_timeline = df_timeline.dropna(subset=["product_name", "event_date", "unit_price"])
    df_timeline = df_timeline.sort_values(["product_name", "event_date"])

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
                alt.Tooltip("event_type:N", title="이벤트"),
            ],
        )
    )

    layers = [base_line]

    # lifecycle 이벤트를 가격 y축에 얹기: 해당 날짜 기준 "직전 관측 가격"으로 y 산출
    if not df_life_all.empty and not df_timeline.empty:
        life = df_life_all[df_life_all["lifecycle_event"].isin(["NEW_PRODUCT", "OUT_OF_STOCK", "RESTOCK"])].copy()
        life = life.dropna(subset=["product_name", "date"]).sort_values(["product_name", "date"])

        # merge_asof로 직전 가격 매칭
        price_for_asof = df_timeline.rename(columns={"event_date": "date"})[["product_name", "date", "unit_price"]].sort_values(["product_name", "date"])
        life = pd.merge_asof(
            life,
            price_for_asof,
            by="product_name",
            on="date",
            direction="backward"
        )

        icon_config = {
            "NEW_PRODUCT": {"label": "NEW"},
            "OUT_OF_STOCK": {"label": "품절"},
            "RESTOCK": {"label": "복원"},
        }

        for et, cfg in icon_config.items():
            df_filtered = life[life["lifecycle_event"] == et].dropna(subset=["unit_price"])
            if df_filtered.empty:
                continue

            point_layer = (
                alt.Chart(df_filtered)
                .mark_point(size=180, shape="triangle-up")
                .encode(
                    x=alt.X("date:T"),
                    y=alt.Y("unit_price:Q"),
                    tooltip=[
                        alt.Tooltip("product_name:N", title="제품"),
                        alt.Tooltip("date:T", title="날짜"),
                        alt.Tooltip("lifecycle_event:N", title="이벤트"),
                        alt.Tooltip("unit_price:Q", title="당시 개당가", format=",.1f"),
                    ],
                )
            )

            text_layer = (
                alt.Chart(df_filtered)
                .mark_text(dy=-15, fontSize=11, fontWeight="bold")
                .encode(
                    x=alt.X("date:T"),
                    y=alt.Y("unit_price:Q"),
                    text=alt.value(cfg["label"]),
                )
            )

            layers.append(point_layer)
            layers.append(text_layer)

    chart = alt.layer(*layers).properties(height=420).interactive()
    st.altair_chart(chart, use_container_width=True)
else:
    st.info("비교 가능한 이벤트 데이터가 없습니다.")

st.divider()

# =========================
# 8-2️⃣ 제품별 카드 + 이벤트 히스토리(정제)
# =========================
for pname in selected_products:
    p = df_all[df_all["product_name"] == pname].iloc[0]
    st.markdown(f"### {p['product_name']}")

    c1, c2, c3, c4 = st.columns(4)

    with c1:
        st.metric("개당 가격", f"{float(p['current_unit_price']):,.1f}원")

    with c2:
        st.success("현재(마지막 관측일 기준) 할인 중") if p["is_discount"] else st.info("정상가")

    with c3:
        df_life_p = df_life_all[df_life_all["product_url"] == p["product_url"]] if not df_life_all.empty else pd.DataFrame()
        has_new = (not df_life_p.empty) and (df_life_p["lifecycle_event"] == "NEW_PRODUCT").any()
        st.warning("🆕 신제품") if has_new else st.caption(f"관측 시작일\n{p['first_seen_date']}")

    with c4:
        st.caption(f"마지막 관측일\n{p['last_seen_date']}")

    if p["product_event_status"] == "NO_EVENT_STABLE":
        st.info("📊 가격 변동 없음")
    else:
        st.success(f"📈 가격 이벤트 {p['event_count']}건")

    with st.expander("📅 이벤트 히스토리"):
        df_price = df_events_all[df_events_all["product_url"] == p["product_url"]].copy() if not df_events_all.empty else pd.DataFrame()
        df_life = df_life_p.copy() if not df_life_p.empty else pd.DataFrame()

        frames = []

        # 1) 가격 이벤트 정제: NORMAL 제거
        if not df_price.empty:
            df_price = df_price[df_price["event_type"] != "NORMAL"].copy()
            frames.append(df_price[["date", "unit_price", "event_type"]])

        # 2) lifecycle 이벤트
        if not df_life.empty:
            df_life = df_life[df_life["lifecycle_event"].notna()].copy()
            df_life = df_life.rename(columns={"lifecycle_event": "event_type"})
            df_life["unit_price"] = None
            frames.append(df_life[["date", "unit_price", "event_type"]])

        if not frames:
            st.caption("이벤트 없음")
            continue

        df_all_events = pd.concat(frames, ignore_index=True)

        # 같은 날짜 + 같은 이벤트 중복 제거
        df_all_events = df_all_events.drop_duplicates(subset=["date", "event_type"])

        # 3) 할인 구간 묶기 (DISCOUNT 연속일 기준)
        discount_periods = pd.DataFrame()
        if not df_price.empty:
            df_discount = df_price[df_price["event_type"] == "DISCOUNT"].sort_values("date").copy()
            if not df_discount.empty:
                df_discount["gap"] = df_discount["date"].diff().dt.days.fillna(1)
                df_discount["group"] = (df_discount["gap"] > 1).cumsum()
                discount_periods = (
                    df_discount.groupby("group")
                    .agg(
                        start_date=("date", "min"),
                        end_date=("date", "max"),
                        unit_price=("unit_price", "first")
                    )
                    .reset_index(drop=True)
                )

        # 4) 표시용 행 구성 (정렬용 sort_key 포함)
        display_rows = []

        for _, row_d in discount_periods.iterrows():
            display_rows.append({
                "sort_key": pd.to_datetime(row_d["end_date"]),  # ✅ 구간은 종료일 기준 정렬
                "날짜": f"{row_d['start_date'].date()} ~ {row_d['end_date'].date()}",
                "개당 가격": round(float(row_d["unit_price"]), 1) if pd.notna(row_d["unit_price"]) else None,
                "이벤트": "💸 할인 기간"
            })

        icon_map = {
            "NEW_PRODUCT": "🆕 신제품",
            "OUT_OF_STOCK": "❌ 품절",
            "RESTOCK": "🔄 복원",
        }

        df_lifecycle_only = df_all_events[df_all_events["event_type"].isin(icon_map.keys())].copy()
        for _, row_l in df_lifecycle_only.iterrows():
            display_rows.append({
                "sort_key": pd.to_datetime(row_l["date"]),
                "날짜": row_l["date"].date(),
                "개당 가격": None,
                "이벤트": icon_map.get(row_l["event_type"], row_l["event_type"])
            })

        if not display_rows:
            st.caption("실제 변화 이벤트 없음")
            continue

        df_display = pd.DataFrame(display_rows)
        df_display = df_display.sort_values("sort_key", ascending=False).drop(columns=["sort_key"])

        st.dataframe(
            df_display.style.format({"개당 가격": "{:.1f}"}),
            use_container_width=True,
            hide_index=True
        )

# =========================
# 9️⃣ 자연어 질문 (Rule → LLM fallback)
# =========================
st.divider()
st.subheader("🤖 가격 인사이트 질문")

# ✅ Enter로 질문 실행되게 form 사용
with st.form("qa_form", clear_on_submit=False):
    st.session_state.question_input = st.text_input(
        "자연어로 질문하세요",
        value=st.session_state.question_input,
        placeholder="예: 에스프레소 중 최저가 / 최근 3개월 변동폭 큰 제품",
    )
    ask_clicked = st.form_submit_button("질문하기", use_container_width=True)

def classify_intent(q: str):
    ql = q.lower()
    if "할인" in ql:
        return "DISCOUNT"
    if "신제품" in ql:
        return "NEW"
    if "가장 싼" in ql or "최저가" in ql:
        return "PRICE_MIN"
    if "비싼" in ql or "최고가" in ql:
        return "PRICE_MAX"
    if any(word in ql for word in ["오른", "상승", "올랐", "증가"]):
        return "PRICE_UP"
    if "변동" in ql or "많이 바뀐" in ql:
        return "VOLATILITY"
    if "품절" in ql:
        return "OUT"
    if "복원" in ql:
        return "RESTORE"
    if "정상가" in ql and "변동" in ql:
        return "NORMAL_CHANGE"
    return "UNKNOWN"

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

def extract_brew_type(q: str, df_all: pd.DataFrame):
    ql = q.lower()
    brew_list = df_all["brew_type_kr"].dropna().unique().tolist()
    for brew in brew_list:
        if brew and brew.lower() in ql:
            return brew
    return None

def execute_rule(intent, question, df_summary):
    df_work = df_summary.copy()

    brew_condition = extract_brew_type(question, df_summary)
    if brew_condition:
        df_work = df_work[df_work["brew_type_kr"] == brew_condition]

    start_date = extract_period(question)

    if intent == "DISCOUNT" and not start_date:
        df = df_work[df_work["is_discount"] == True]
        if df.empty:
            return None
        return "현재 할인 중 제품:\n- " + "\n- ".join(df["product_name"].tolist())

    if intent == "PRICE_MIN":
        df_valid = df_work[df_work["current_unit_price"] > 0]
        if df_valid.empty:
            return "현재 판매 중인 제품이 없습니다."

        min_price = df_valid["current_unit_price"].min()
        df_min = df_valid[df_valid["current_unit_price"] == min_price]

        # ✅ Bulk 이벤트에서 최저가 기간 계산(추가 쿼리 제거)
        lines = []
        for _, row in df_min.iterrows():
            hist = df_events_all[df_events_all["product_url"] == row["product_url"]].copy() if not df_events_all.empty else pd.DataFrame()
            if hist.empty:
                continue
            hist = hist.dropna(subset=["unit_price"])
            hist = hist[hist["unit_price"] > 0]
            low = hist[hist["unit_price"] == min_price]
            if low.empty:
                continue
            sd = low["date"].min().date()
            ed = low["date"].max().date()
            lines.append(f"- {row['product_name']} / {min_price:,.1f}원\n  최저가 기간: {sd} ~ {ed}")

        if not lines:
            return "최저가 계산 대상 제품이 없습니다."
        return "최저가 제품 목록:\n\n" + "\n\n".join(lines)

    if intent == "PRICE_MAX":
        df = df_work[df_work["current_unit_price"] > 0].sort_values("current_unit_price", ascending=False)
        if df.empty:
            return None
        top = df.iloc[0]
        return f"가장 비싼 제품은 '{top['product_name']}'이며 {float(top['current_unit_price']):,.1f}원입니다."

    if intent == "NEW":
        if df_life_all.empty:
            return None
        urls = df_life_all[df_life_all["lifecycle_event"] == "NEW_PRODUCT"]["product_url"].unique().tolist()
        df = df_work[df_work["product_url"].isin(urls)]
        if df.empty:
            return None
        return "최근 신제품:\n- " + "\n- ".join(df["product_name"].tolist())

    if intent == "OUT":
        if df_life_all.empty:
            return None
        urls = df_life_all[df_life_all["lifecycle_event"] == "OUT_OF_STOCK"]["product_url"].unique().tolist()
        df = df_work[df_work["product_url"].isin(urls)]
        if df.empty:
            return None
        return "최근 품절 제품:\n- " + "\n- ".join(df["product_name"].tolist())

    if intent == "RESTORE":
        if df_life_all.empty:
            return None
        urls = df_life_all[df_life_all["lifecycle_event"] == "RESTOCK"]["product_url"].unique().tolist()
        df = df_work[df_work["product_url"].isin(urls)]
        if df.empty:
            return None
        return "최근 복원된 제품:\n- " + "\n- ".join(df["product_name"].tolist())

    if intent == "VOLATILITY" and start_date:
        if df_events_all.empty:
            return None
        df = df_events_all[df_events_all["date"] >= start_date].copy()
        if df.empty:
            return None
        df = df.dropna(subset=["unit_price"])
        vol = df.groupby("product_url")["unit_price"].agg(lambda x: x.max() - x.min()).sort_values(ascending=False)
        if vol.empty:
            return None
        top_url = vol.index[0]
        top_val = vol.iloc[0]
        row = df_work[df_work["product_url"] == top_url]
        if row.empty:
            return None
        return f"최근 기간 가격 변동 폭이 가장 큰 제품은 '{row.iloc[0]['product_name']}'이며 변동폭은 {top_val:,.1f}원입니다."

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
        for _, r in df.iterrows():
            pr = df_summary[df_summary["product_url"] == r["product_url"]]
            if pr.empty:
                continue
            pname = pr.iloc[0]["product_name"]
            results.append(
                f"- {pname} / {float(r['prev_price']):,.0f}원 → {r['date']}에 {float(r['normal_price']):,.0f}원 ({float(r['price_diff']):+,.0f}원)"
            )
        return "기간 내 정상가 변동 제품 목록:\n" + "\n".join(results) if results else "해당 기간 내 정상가 변동이 없습니다."

    return None

def llm_fallback(question: str, df_summary: pd.DataFrame):
    context = df_summary[["product_name", "current_unit_price", "is_discount", "is_new_product", "brew_type_kr"]].to_dict(orient="records")
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

if ask_clicked and st.session_state.question_input.strip():
    q = st.session_state.question_input.strip()
    intent = classify_intent(q)
    answer = execute_rule(intent, q, df_all)

    if answer:
        save_question_log(q, intent, False)
        st.success(answer)
    else:
        with st.spinner("분석 중..."):
            answer = llm_fallback(q, df_all)
        save_question_log(q, intent, True)
        st.success(answer)
