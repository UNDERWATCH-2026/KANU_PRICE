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
def load_events_bulk(product_urls):
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
def load_lifecycle_bulk(product_urls):
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
# 3️⃣ 유틸
# =========================
def _norm_series(s):
    return s.fillna("").astype(str)

def options_from(df, col):
    if col not in df.columns:
        return []
    vals = df[col].dropna().astype(str)
    vals = [v.strip() for v in vals if v.strip()]
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

if "keyword_input" not in st.session_state:
    st.session_state.keyword_input = ""


# =========================
# 5️⃣ 메인 UI
# =========================
st.title("☕ Capsule Price Intelligence")

df_all = load_product_summary()

if df_all.empty:
    st.warning("제품 데이터가 없습니다.")
    st.stop()

st.divider()
st.subheader("🔎 제품 검색")

# =========================
# 🔥 Enter 즉시 검색
# =========================
def add_keyword():
    kw = st.session_state.keyword_input.strip()
    if kw:
        mask = _norm_series(df_all["product_name"]).str.contains(kw, case=False)
        result_df = df_all[mask].copy()
        if not result_df.empty:
            st.session_state.keyword_results[kw] = result_df
    st.session_state.keyword_input = ""

st.text_input(
    "제품명 키워드 입력",
    key="keyword_input",
    placeholder="예: 스노우, 쥬시",
    label_visibility="collapsed",
    on_change=add_keyword
)

# =========================
# 검색 결과 표시
# =========================
st.subheader("📦 비교할 제품 선택")

if st.session_state.keyword_results:

    for kw in reversed(list(st.session_state.keyword_results.keys())):
        st.markdown(f"#### 🔎 '{kw}' 검색 결과")

        df_kw = st.session_state.keyword_results[kw]
        product_list = sorted(df_kw["product_name"].unique())

        for pname in product_list:
            st.checkbox(
                pname,
                value=pname in st.session_state.selected_products,
                key=f"chk_{kw}_{pname}",
                on_change=toggle_product,
                args=(pname,)
            )

else:
    st.info("검색어를 입력하세요.")

# =========================
# 🔥 결과 즉시 반영
# =========================
selected_products = list(st.session_state.selected_products)

if not selected_products:
    st.stop()

if len(selected_products) > 10:
    st.warning("제품이 많으면 속도가 느려질 수 있습니다.")

st.divider()
st.subheader(f"📊 비교 결과 ({len(selected_products)}개 제품)")

sel_rows = df_all[df_all["product_name"].isin(selected_products)]
product_urls = sel_rows["product_url"].tolist()

df_events = load_events_bulk(product_urls)
df_life = load_lifecycle_bulk(product_urls)

url_to_name = dict(zip(sel_rows["product_url"], sel_rows["product_name"]))

if not df_events.empty:
    df_events["product_name"] = df_events["product_url"].map(url_to_name)

if not df_life.empty:
    df_life["product_name"] = df_life["product_url"].map(url_to_name)

# =========================
# 📈 가격 타임라인 차트
# =========================
if not df_events.empty:

    df_chart = df_events.copy()
    df_chart = df_chart.dropna(subset=["unit_price"])

    chart = (
        alt.Chart(df_chart)
        .mark_line(point=True)
        .encode(
            x="date:T",
            y="unit_price:Q",
            color="product_name:N",
            tooltip=["product_name", "date", "unit_price", "event_type"],
        )
        .properties(height=420)
        .interactive()
    )

    st.altair_chart(chart, use_container_width=True)

else:
    st.info("이벤트 데이터 없음")

st.divider()

# =========================
# 🤖 자연어 질문
# =========================
st.subheader("🤖 가격 인사이트 질문")

question = st.text_input(
    "질문 입력",
    placeholder="예: 최저가 제품은?",
)

def classify_intent(q):
    ql = q.lower()

    INTENT_KEYWORDS = {
        "PRICE_MIN": ["최저가", "가장 싼"],
        "PRICE_MAX": ["최고가", "가장 비싼"],
        "DISCOUNT": ["할인", "세일", "특가"],
    }

    for intent, keywords in INTENT_KEYWORDS.items():
        if any(word in ql for word in keywords):
            return intent

    return "UNKNOWN"

if question:
    intent = classify_intent(question)

    if intent == "PRICE_MIN":
        df_valid = sel_rows[sel_rows["current_unit_price"] > 0]
        if not df_valid.empty:
            min_price = df_valid["current_unit_price"].min()
            df_min = df_valid[df_valid["current_unit_price"] == min_price]
            st.success(
                "최저가 제품:\n" +
                "\n".join([f"- {row['product_name']} ({min_price:,.1f}원)" for _, row in df_min.iterrows()])
            )
        else:
            st.info("판매 중 제품 없음")

    elif intent == "PRICE_MAX":
        df_valid = sel_rows[sel_rows["current_unit_price"] > 0]
        if not df_valid.empty:
            max_price = df_valid["current_unit_price"].max()
            df_max = df_valid[df_valid["current_unit_price"] == max_price]
            st.success(
                "최고가 제품:\n" +
                "\n".join([f"- {row['product_name']} ({max_price:,.1f}원)" for _, row in df_max.iterrows()])
            )

    else:
        st.info("해당 질문은 아직 Rule에 정의되지 않았습니다.")
