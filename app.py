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
# 3️⃣ 유틸 (제품명 보정 포함)
# =========================

import re

def clean_product_name(s: str) -> str:
    """
    깨진 한글(�) 및 자주 발생하는 인코딩 오류 패턴 보정
    """
    if s is None:
        return ""

    s = str(s)

    # 제어문자 제거
    s = re.sub(r"[\x00-\x1f\x7f-\x9f]", "", s).strip()

    # 🔥 자주 깨지는 패턴 사전
    fix_map = {
        "본���직영": "본사직영",
        "본��직영": "본사직영",
        "본�직영": "본사직영",

        "바닐���향": "바닐라향",
        "바닐��향": "바닐라향",

        "네스프���": "네스프레소",
        "스타���스": "스타벅스",
    }

    for bad, good in fix_map.items():
        if bad in s:
            s = s.replace(bad, good)

    # 🔥 패턴 기반 보정
    s = re.sub(r"바닐.*?향", "바닐라향", s)
    s = re.sub(r"본.*?직영", "본사직영", s)

    # 연속된 깨진 문자 제거
    s = re.sub(r"�{1,}", "", s)

    # 공백 정리
    s = re.sub(r"\s{2,}", " ", s).strip()

    return s

def detect_encoding_issues(df: pd.DataFrame):
    if "product_name_raw" not in df.columns:
        return

    mask = df["product_name_raw"].str.contains("�", na=False)
    issues = df[mask][["product_url", "product_name_raw"]]

    if not issues.empty:
        import logging
        logging.warning(f"[ENCODING ISSUE] {len(issues)}건 감지됨")

        try:
            supabase.table("product_name_encoding_issues").insert(
                issues.to_dict(orient="records")
            ).execute()
        except Exception as e:
            logging.error(f"로그 저장 실패: {e}")




def _norm_series(s: pd.Series) -> pd.Series:
    """
    검색 시 None/NaN 안전 처리 + 문자열 변환
    """
    return s.fillna("").astype(str)


def options_from(df: pd.DataFrame, col: str):
    """
    필터 selectbox용 고유 값 추출
    """
    if col not in df.columns:
        return []

    vals = df[col].dropna().astype(str)
    vals = [v.strip() for v in vals.tolist() if v.strip()]
    return sorted(list(dict.fromkeys(vals)))


# =========================
# 🔧 제품 선택 토글 함수 (안정화)
# =========================
def toggle_product(pname):
    """
    제품 선택/해제 토글
    """

    if "selected_products" not in st.session_state:
        st.session_state.selected_products = set()

    # pname이 None이거나 빈값이면 방어
    if not pname:
        return

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
    st.session_state.keyword_input = ""  # 🔥 Enter용 상태값

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
    st.session_state.keyword_results = {}
    st.session_state.show_results = False
    st.session_state.keyword_input = ""
    st.rerun()

st.divider()


# -------------------------
# 데이터 로딩
# -------------------------
df_all = load_product_summary()

# 데이터 없으면 즉시 중단
if df_all is None or df_all.empty:
    st.warning("아직 집계된 제품 데이터가 없습니다.")
    st.stop()

# -------------------------
# 제품명 정제
# -------------------------
df_all["product_name_raw"] = df_all["product_name"]
df_all["product_name"] = df_all["product_name"].apply(clean_product_name)

# -------------------------
# 깨진 문자열 감지 (운영 로그 전용)
# -------------------------
try:
    encoding_issues = detect_encoding_issues(df_all)

    if isinstance(encoding_issues, pd.DataFrame) and not encoding_issues.empty:
        print(f"[ENCODING] 깨진 제품명 {len(encoding_issues)}건 감지")

        # Supabase 저장용 최소 컬럼만 추출
        log_records = encoding_issues[[
            "product_url",
            "product_name_raw"
        ]].to_dict(orient="records")

        supabase.table("product_name_encoding_issues") \
                .insert(log_records) \
                .execute()

except Exception as e:
    print(f"[ENCODING_LOG_ERROR] {e}")


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
        st.session_state.keyword_results = {}
        st.session_state.show_results = False
        st.session_state.keyword_input = ""
        st.rerun()

st.divider()


# =========================
# 6️⃣ 조회 조건
# =========================
st.subheader("🔍 조회 조건")

with st.form("search_form"):
    keyword_input = st.text_input(
        "제품명 키워드 입력",
        placeholder="예: 쥬시, 멜로지오",
        label_visibility="collapsed"
    )

    submitted = st.form_submit_button("검색", use_container_width=True)

if submitted and keyword_input.strip():

    keywords = [k.strip() for k in keyword_input.split(",") if k.strip()]

    mask = False
    for kw in keywords:
        mask |= _norm_series(df_all["product_name"]).str.contains(kw, case=False)

    result_df = df_all[mask]

    if not result_df.empty:
        st.session_state.selected_products = set(result_df["product_name"].tolist())
    else:
        st.warning("검색 결과 없음")

    st.rerun()



    # -------------------------
    # 키워드별 결과 출력
    # -------------------------
    st.subheader("📦 비교할 제품 선택")

    if st.session_state.keyword_results:
        all_candidates = []

        for kw in reversed(list(st.session_state.keyword_results.keys())):
            st.markdown(f"#### 🔎 '{kw}' 검색 결과")

            col_title, col_delete = st.columns([8, 2])
            with col_delete:
                if st.button("검색 결과 삭제", key=f"del_{kw}"):
                    df_kw = st.session_state.keyword_results[kw]
                    remove_list = df_kw["product_name"].tolist()

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
        
        # 🔥 lifecycle 데이터 불러오기
        df_life = load_lifecycle_events(row["product_url"])
        
        if not df_life.empty:
            df_life["date"] = pd.to_datetime(df_life["date"])
        
            # 품절/복원 구간 계산
            out_dates = df_life[df_life["lifecycle_event"] == "OUT_OF_STOCK"]["date"].tolist()
            restore_dates = df_life[df_life["lifecycle_event"] == "RESTOCK"]["date"].tolist()
        
            for out_date in out_dates:
                # 해당 품절 이후 첫 복원 날짜 찾기
                restore_after = [d for d in restore_dates if d > out_date]
                if restore_after:
                    restore_date = min(restore_after)
        
                    # 🔥 품절~복원 사이 가격 제거
                    mask = (tmp["event_date"] > out_date) & (tmp["event_date"] < restore_date)
                    tmp.loc[mask, "unit_price"] = None
        
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

    # 1️⃣ 정렬 (필수)
    df_timeline = df_timeline.sort_values(
        ["product_name", "event_date"]
    )

    # 2️⃣ 숫자 강제 변환
    df_timeline["unit_price"] = pd.to_numeric(
        df_timeline["unit_price"], errors="coerce"
    )

    # 3️⃣ segment 컬럼 생성 (끊김 완전 분리용)
    df_timeline["segment"] = (
        df_timeline["unit_price"].isna()
        .groupby(df_timeline["product_name"])
        .cumsum()
    )

    # 4️⃣ NaN 제거 (끊긴 구간은 차트에서 제외)
    df_chart = df_timeline.dropna(subset=["unit_price"])

    # =========================
    # 📈 가격 선 차트
    # =========================
    base_line = (
        alt.Chart(df_chart)
        .mark_line(point=True)
        .encode(
            x=alt.X("event_date:T", title="날짜"),
            y=alt.Y("unit_price:Q", title="개당 가격 (원)"),
            color=alt.Color("product_name:N", title="제품"),
            detail="segment:N",  # 🔥 이게 핵심 (선 완전 분리)
            tooltip=[
                alt.Tooltip("product_name:N", title="제품"),
                alt.Tooltip("event_date:T", title="날짜"),
                alt.Tooltip("unit_price:Q", title="개당 가격", format=",.1f"),
            ],
        )
    )

    layers = [base_line]

    # =========================
    # 🔔 Lifecycle 아이콘 추가
    # =========================
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

            # 아이콘 위치를 가격선에 맞추기 위해 join
            df_filtered = df_filtered.merge(
                df_timeline[["product_name", "event_date", "unit_price"]],
                on=["product_name", "event_date"],
                how="left"
            )
            
            # (선택) 디버깅용 — 필요할 때만
            # if st.checkbox("디버그: lifecycle merge 보기"):
            #     st.dataframe(df_filtered[["product_name","event_date","unit_price"]])
            
            # 🔥 중요: unit_price 없는 lifecycle 제거 (가격선에 정확히 붙이기 위함)
            df_filtered = df_filtered.dropna(subset=["unit_price"])

            

            point_layer = (
               alt.Chart(df_filtered)
                .mark_point(
                    size=150,
                    shape="triangle-up",
                    color=cfg["color"]
                )
                .encode(
                    x="event_date:T",
                    y="unit_price:Q",   # 🔥 반드시 추가
                    tooltip=[
                        alt.Tooltip("product_name:N", title="제품"),
                        alt.Tooltip("event_date:T", title="날짜"),
                        alt.Tooltip("lifecycle_event:N", title="이벤트"),
                    ],
                )
            )

            text_layer = (
                alt.Chart(df_filtered)
                .mark_text(
                    dy=12,   # 🔥 아래로 12px 이동
                    fontSize=11,
                    fontWeight="bold",
                    color=cfg["color"]
                )
                .encode(
                    x="event_date:T",
                    y="unit_price:Q",   # 🔥 반드시 동일하게
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
    
        # 🔥 현재 선택된 기간 가져오기
        date_from = df_timeline["event_date"].min().date()
        date_to = df_timeline["event_date"].max().date()
    
        res = supabase.rpc(
            "get_discount_periods_in_range",
            {
                "p_product_url": p["product_url"],
                "p_date_from": str(date_from),
                "p_date_to": str(date_to),
            }
        ).execute()
    
        discount_rows = res.data if res.data else []
    
        if discount_rows:
    
            for d in discount_rows:
                st.success(
                    f"💸 할인 {d['discount_start_date']} ~ {d['discount_end_date']}"
                )
    
        else:
            st.info("정상가")


    with c3:
        df_life = load_lifecycle_events(p["product_url"])
        has_new = (not df_life.empty) and (df_life["lifecycle_event"] == "NEW_PRODUCT").any()
        if has_new:
            st.warning("🆕 신제품")
        else:
            st.caption(f"관측 시작일\n{p['first_seen_date']}")

    with c4:
        st.caption(f"마지막 관측일\n{p['last_seen_date']}")

    if p["product_event_status"] == "NO_EVENT_STABLE":
        st.info("📊 가격 변동 없음")
    else:
        st.success(f"📈 가격 이벤트 {p['event_count']}건")

    with st.expander("📅 이벤트 히스토리"):
        df_price = load_events(p["product_url"])
        df_life = load_lifecycle_events(p["product_url"])

        frames = []

        # 1) 가격 이벤트 정제
        if not df_price.empty:
            df_price = df_price.copy()
            df_price["date"] = pd.to_datetime(df_price["date"])
            df_price = df_price[df_price["event_type"] != "NORMAL"]
            if not df_price.empty:
                frames.append(df_price[["date", "unit_price", "event_type"]])

        # 2) Lifecycle 이벤트
        if not df_life.empty:
            df_life = df_life[df_life["lifecycle_event"].notna()]
            df_life = df_life.rename(columns={"lifecycle_event": "event_type"})
            df_life["unit_price"] = None
            df_life["date"] = pd.to_datetime(df_life["date"])
            frames.append(df_life[["date", "unit_price", "event_type"]])

        if not frames:
            st.caption("이벤트 없음")
            continue

        df_all_events = pd.concat(frames, ignore_index=True)
        df_all_events = df_all_events.drop_duplicates(subset=["date", "event_type"])

        # 4) 할인 구간 묶기
        if not df_price.empty:
            df_discount = df_price[df_price["event_type"] == "DISCOUNT"]
            if not df_discount.empty:
                df_discount = df_discount.sort_values("date")
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
            else:
                discount_periods = pd.DataFrame()
        else:
            discount_periods = pd.DataFrame()

        display_rows = []

        for _, row_d in discount_periods.iterrows():
            display_rows.append({
                "날짜": f"{row_d['start_date'].date()} ~ {row_d['end_date'].date()}",
                "개당 가격": round(float(row_d["unit_price"]), 1) if pd.notna(row_d["unit_price"]) else None,
                "이벤트": "💸 할인 기간"
            })

        icon_map = {
            "NEW_PRODUCT": "🆕 신제품",
            "OUT_OF_STOCK": "❌ 품절",
            "RESTOCK": "🔄 복원",
        }

        df_lifecycle_only = df_all_events[df_all_events["event_type"].isin(icon_map.keys())]
        for _, row_l in df_lifecycle_only.iterrows():
            display_rows.append({
                "날짜": row_l["date"].date(),
                "개당 가격": None,
                "이벤트": icon_map.get(row_l["event_type"], row_l["event_type"])
            })

        if not display_rows:
            st.caption("실제 변화 이벤트 없음")
            continue

        df_display = pd.DataFrame(display_rows)
        df_display = df_display.sort_values("날짜", ascending=False)

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

question = st.text_input(
    "자연어로 질문하세요",
    placeholder="예: 에스프레소 중 최저가 / 최근 3개월 변동폭 큰 제품",
)

def classify_intent(q: str):
    q = q.lower()

    if "할인" in q or "행사" in q:
        return "DISCOUNT"
    if any(word in q for word in ["신제품", "새롭게", "새로", "신규", "출시", "새로운", "처음"]):
        return "NEW"
    if "가장 싼" in q or "최저가" in q:
        return "PRICE_MIN"
    if "비싼" in q or "최고가" in q:
        return "PRICE_MAX"
    if any(word in q for word in ["상승", "증가"]) and "않" not in q:
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

def extract_period(q: str):
    today = datetime.today()

    if any(word in q for word in ["최근 7일", "최근 일주일", "최근 1주일"]):
        return today - timedelta(days=7)
    if any(word in q for word in ["최근 한 달", "최근 30일", "최근 1개월"]):
        return today - timedelta(days=30)
    if "최근 3개월" in q:
        return today - timedelta(days=90)
    if "최근 1년" in q:
        return today - timedelta(days=365)

    return None

def extract_brew_type(q: str, df_all: pd.DataFrame):
    q = q.lower()
    brew_list = df_all["brew_type_kr"].dropna().unique().tolist()

    for brew in brew_list:
        if brew and brew.lower() in q:
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

        output_lines = []
        for _, row in df_min.iterrows():
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
            df_hist = df_hist[df_hist["unit_price"] > 0]

            df_low = df_hist[df_hist["unit_price"] == min_price]
            if df_low.empty:
                continue

            sd = df_low["date"].min().date()
            ed = df_low["date"].max().date()
            output_lines.append(
                f"- {row['product_name']} / {min_price:,.1f}원\n"
                f"  최저가 기간: {sd} ~ {ed}"
            )

        if not output_lines:
            return "최저가 계산 대상 제품이 없습니다."

        return "최저가 제품 목록:\n\n" + "\n\n".join(output_lines)

    if intent == "PRICE_MAX":
        df = df_work[df_work["current_unit_price"] > 0].sort_values("current_unit_price", ascending=False)
        if df.empty:
            return None
        top = df.iloc[0]
        return f"가장 비싼 제품은 '{top['product_name']}'이며 {float(top['current_unit_price']):,.1f}원입니다."

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
            product_row = df_summary[df_summary["product_url"] == row["product_url"]]
            if product_row.empty:
                continue

            pname = product_row.iloc[0]["product_name"]
            results.append(
                f"- {pname} / {float(row['prev_price']):,.0f}원 → "
                f"{row['date']}에 {float(row['normal_price']):,.0f}원 "
                f"({float(row['price_diff']):+,.0f}원)"
            )

        return "기간 내 정상가 변동 제품 목록:\n" + "\n".join(results) if results else "해당 기간 내 정상가 변동이 없습니다."

    return None

def llm_fallback(question: str, df_summary: pd.DataFrame):
    context = df_summary.head(50)[
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








