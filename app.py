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
def save_question_log(question: str, q_type: str, used_llm: bool, answer: str = None, filters: dict = None):
    """
    질문 로그를 Supabase에 저장
    
    Args:
        question: 사용자 질문
        q_type: 질문 타입 (DISCOUNT, NEW, PRICE_MIN 등)
        used_llm: LLM 사용 여부
        answer: 생성된 답변 (선택)
        filters: 적용된 필터 정보 (선택)
    """
    try:
        log_data = {
            "question_text": question,
            "question_type": q_type,
            "used_llm": used_llm,
            "created_at": datetime.now().isoformat()
        }
        
        # 답변 추가 (있는 경우)
        if answer:
            # 답변이 dict인 경우 텍스트 추출
            if isinstance(answer, dict):
                log_data["answer_text"] = answer.get("text", str(answer))
                log_data["answer_type"] = answer.get("type", "unknown")
            else:
                log_data["answer_text"] = str(answer)
        
        # 필터 정보 추가 (있는 경우)
        if filters:
            log_data["filters"] = filters
        
        supabase.table("question_logs").insert(log_data).execute()
    except Exception as e:
        print("로그 저장 실패:", e)


# =========================
# 2-2️⃣ 질문 처리 함수들
# =========================

def normalize_brand_name(brand_query: str) -> str:
    """
    브랜드명을 정규화
    예: '카누', '카누바리스타', '카누 바리스타' → '카누 바리스타'
    """
    brand_query = brand_query.lower().strip()
    
    # 브랜드명 매핑 (공백 제거 버전 → 정식 명칭)
    brand_mapping = {
        "카누": "카누 바리스타",
        "카누바리스타": "카누 바리스타",
        "카누 바리스타": "카누 바리스타",
        "네스프레소": "네스프레소",
        "스타벅스": "스타벅스",
        "일리": "일리",
        "돌체구스토": "돌체구스토",
    }
    
    # 공백 제거하여 매칭
    for key, value in brand_mapping.items():
        if key.replace(" ", "") == brand_query.replace(" ", ""):
            return value
    
    return brand_query

def extract_brand_from_question(q: str, df_all: pd.DataFrame) -> str:
    """질문에서 브랜드명 추출"""
    q_lower = q.lower()
    brands = df_all["brand"].dropna().unique().tolist()
    
    for brand in brands:
        if brand and brand.lower() in q_lower:
            return brand
    
    # 정규화된 브랜드명으로 재시도
    for brand in brands:
        normalized = normalize_brand_name(q_lower)
        if brand.lower() == normalized.lower():
            return brand
    
    return None

def extract_product_name_from_question(q: str) -> str:
    """질문에서 제품명 키워드 추출"""
    # 할인, 기간 등의 키워드 제거
    exclude_words = ["할인", "기간", "언제", "얼마", "가격", "제품", "브랜드", "카누", "바리스타"]
    
    words = q.split()
    product_keywords = []
    
    for word in words:
        if len(word) >= 2 and not any(ex in word for ex in exclude_words):
            product_keywords.append(word)
    
    return " ".join(product_keywords) if product_keywords else None

def classify_intent(q: str):
    q = q.lower()

    # 🔥 "할인 기간" 키워드 감지
    if "할인" in q and ("기간" in q or "언제" in q):
        return "DISCOUNT_PERIOD"
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

def execute_rule(intent, question, df_summary, date_from=None, date_to=None):
    df_work = df_summary.copy()

    brew_condition = extract_brew_type(question, df_summary)
    if brew_condition:
        df_work = df_work[df_work["brew_type_kr"] == brew_condition]

    # 🔥 브랜드 필터링
    brand_from_q = extract_brand_from_question(question, df_summary)
    if brand_from_q:
        df_work = df_work[df_work["brand"] == brand_from_q]
    
    # 🔥 제품명 필터링
    product_keywords = extract_product_name_from_question(question)
    if product_keywords:
        mask = False
        for keyword in product_keywords.split():
            if len(keyword) >= 2:
                mask |= df_work["product_name"].str.contains(keyword, case=False, na=False)
        if mask is not False:
            df_work = df_work[mask]

    start_date = extract_period(question)

    # 🔥 할인 기간 조회
    if intent == "DISCOUNT_PERIOD":
        results = []
        
        for _, row in df_work.iterrows():
            # 할인 기간 조회
            res = supabase.rpc(
                "get_discount_periods_in_range",
                {
                    "p_product_url": row["product_url"],
                    "p_date_from": (date_from if date_from else datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d"),
                    "p_date_to": (date_to if date_to else datetime.now()).strftime("%Y-%m-%d"),
                }
            ).execute()
            
            discount_periods = res.data if res.data else []
            
            if discount_periods:
                for period in discount_periods:
                    # 할인 기간의 가격 조회
                    price_res = (
                        supabase.table("product_all_events")
                        .select("unit_price")
                        .eq("product_url", row["product_url"])
                        .eq("event_type", "DISCOUNT")
                        .gte("date", period["discount_start_date"])
                        .lte("date", period["discount_end_date"])
                        .limit(1)
                        .execute()
                    )
                    
                    discount_price = price_res.data[0]["unit_price"] if price_res.data else row["current_unit_price"]
                    
                    results.append({
                        "text": f"• **{row['brand']}** - {row['product_name']}\n"
                                f"  📅 할인 기간: {period['discount_start_date']} ~ {period['discount_end_date']}\n"
                                f"  💰 할인가: {float(discount_price):,.1f}원",
                        "product_name": row['product_name']
                    })
        
        if not results:
            return "해당 제품의 할인 기간 정보가 없습니다."
        
        return {
            "type": "product_list",
            "text": "할인 기간 정보:\n\n" + "\n\n".join([r["text"] for r in results]),
            "products": [r["product_name"] for r in results]
        }

    if intent == "DISCOUNT" and not start_date:
        df = df_work[df_work["is_discount"] == True]
        if df.empty:
            return None
        
        # 상세 정보 포함한 결과 생성
        results = []
        for _, row in df.iterrows():
            # 카테고리 정보 구성
            categories = []
            if pd.notna(row.get("category1")) and row["category1"]:
                categories.append(row["category1"])
            if pd.notna(row.get("category2")) and row["category2"]:
                categories.append(row["category2"])
            
            category_str = f" [{' > '.join(categories)}]" if categories else ""
            
            product_name = row['product_name']
            
            results.append({
                "text": f"• **{row['brand']}** - {product_name}{category_str}\n  💰 현재가: {float(row['current_unit_price']):,.1f}원",
                "product_name": product_name
            })
        
        if not results:
            return None
        
        return {
            "type": "product_list",
            "text": "현재 할인 중 제품:\n\n" + "\n\n".join([r["text"] for r in results]),
            "products": [r["product_name"] for r in results]
        }

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
            .select("product_url, date")
            .eq("lifecycle_event", "NEW_PRODUCT")
        )
        
        # 🔥 조회 기간 필터링
        if date_from:
            res = res.gte("date", date_from.strftime("%Y-%m-%d"))
        if date_to:
            res = res.lte("date", date_to.strftime("%Y-%m-%d"))
        
        res = res.execute()
        
        if not res.data:
            return None
        
        # URL과 출시 날짜 매핑
        new_product_data = {r["product_url"]: r["date"] for r in res.data}
        urls = list(new_product_data.keys())
        
        df = df_work[df_work["product_url"].isin(urls)]
        if df.empty:
            return None
        
        # 상세 정보 포함한 결과 생성
        results = []
        for _, row in df.iterrows():
            launch_date = new_product_data.get(row["product_url"])
            
            # 카테고리 정보 구성
            categories = []
            if pd.notna(row.get("category1")) and row["category1"]:
                categories.append(row["category1"])
            if pd.notna(row.get("category2")) and row["category2"]:
                categories.append(row["category2"])
            
            category_str = f" [{' > '.join(categories)}]" if categories else ""
            
            # 🔥 체크박스 추가 가능하도록 제품명만 포함
            product_name = row['product_name']
            
            results.append({
                "text": f"• **{row['brand']}** - {product_name}{category_str}\n  🎉 출시일: {launch_date}",
                "product_name": product_name
            })
        
        if not results:
            return None
        
        # 체크박스와 텍스트 분리 반환
        return {
            "type": "product_list",
            "text": "최근 신제품:\n\n" + "\n\n".join([r["text"] for r in results]),
            "products": [r["product_name"] for r in results]
        }

    if intent == "OUT":
        res = (
            supabase.table("product_lifecycle_events")
            .select("product_url, date")
            .eq("lifecycle_event", "OUT_OF_STOCK")
        )
        
        # 🔥 조회 기간 필터링
        if date_from:
            res = res.gte("date", date_from.strftime("%Y-%m-%d"))
        if date_to:
            res = res.lte("date", date_to.strftime("%Y-%m-%d"))
        
        res = res.execute()
        
        if not res.data:
            return None
        
        # URL과 품절 날짜 매핑
        out_of_stock_data = {r["product_url"]: r["date"] for r in res.data}
        urls = list(out_of_stock_data.keys())
        
        df = df_work[df_work["product_url"].isin(urls)]
        if df.empty:
            return None
        
        # 상세 정보 포함한 결과 생성
        results = []
        for _, row in df.iterrows():
            out_date = out_of_stock_data.get(row["product_url"])
            
            # 카테고리 정보 구성
            categories = []
            if pd.notna(row.get("category1")) and row["category1"]:
                categories.append(row["category1"])
            if pd.notna(row.get("category2")) and row["category2"]:
                categories.append(row["category2"])
            
            category_str = f" [{' > '.join(categories)}]" if categories else ""
            
            product_name = row['product_name']
            
            results.append({
                "text": f"• **{row['brand']}** - {product_name}{category_str}\n  📅 품절일: {out_date}",
                "product_name": product_name
            })
        
        if not results:
            return None
        
        return {
            "type": "product_list",
            "text": "최근 품절 제품:\n\n" + "\n\n".join([r["text"] for r in results]),
            "products": [r["product_name"] for r in results]
        }

    if intent == "RESTORE":
        res = (
            supabase.table("product_lifecycle_events")
            .select("product_url, date")
            .eq("lifecycle_event", "RESTOCK")
        )
        
        # 🔥 조회 기간 필터링
        if date_from:
            res = res.gte("date", date_from.strftime("%Y-%m-%d"))
        if date_to:
            res = res.lte("date", date_to.strftime("%Y-%m-%d"))
        
        res = res.execute()
        
        if not res.data:
            return None
        
        # URL과 복원 날짜 매핑
        restock_data = {r["product_url"]: r["date"] for r in res.data}
        urls = list(restock_data.keys())
        
        df = df_work[df_work["product_url"].isin(urls)]
        if df.empty:
            return None
        
        # 상세 정보 포함한 결과 생성
        results = []
        for _, row in df.iterrows():
            restock_date = restock_data.get(row["product_url"])
            
            # 카테고리 정보 구성
            categories = []
            if pd.notna(row.get("category1")) and row["category1"]:
                categories.append(row["category1"])
            if pd.notna(row.get("category2")) and row["category2"]:
                categories.append(row["category2"])
            
            category_str = f" [{' > '.join(categories)}]" if categories else ""
            
            product_name = row['product_name']
            
            results.append({
                "text": f"• **{row['brand']}** - {product_name}{category_str}\n  🔄 복원일: {restock_date}",
                "product_name": product_name
            })
        
        if not results:
            return None
        
        return {
            "type": "product_list",
            "text": "최근 복원된 제품:\n\n" + "\n\n".join([r["text"] for r in results]),
            "products": [r["product_name"] for r in results]
        }

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
# 4️⃣ 세션 상태 초기화
# =========================
if "selected_products" not in st.session_state:
    st.session_state.selected_products = set()
if "keyword_results" not in st.session_state:
    st.session_state.keyword_results = {}
if "active_mode" not in st.session_state:
    st.session_state.active_mode = "키워드 검색"
if "show_results" not in st.session_state:
    st.session_state.show_results = False
if "search_keyword" not in st.session_state:
    st.session_state.search_keyword = ""
if "search_history" not in st.session_state:
    st.session_state.search_history = []  # 🔥 검색 이력 [{keyword: "쥬시", results: [...]}]

# =========================
# 5️⃣ 메인 UI
# =========================
st.title("☕ Capsule Price Intelligence")

# -------------------------
# 데이터 로딩 (탭 이전에 로드)
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
# 조회 기준 선택 및 조회 조건 통합
# -------------------------
col_main_left, col_main_right = st.columns([3, 1])

with col_main_left:
    st.subheader("🔎 조회 기준")

with col_main_right:
    st.subheader("📅 조회 기간")

# 🔥 메인 레이아웃: 탭(좌) + 조회조건(우)
col_tabs, col_controls = st.columns([3, 1])

with col_controls:
    # 🔥 시작일/종료일을 한 줄에 배치
    col_from, col_to = st.columns(2)
    with col_from:
        st.write("시작일")
        date_from = st.date_input(
            "시작일",
            value=datetime.now() - timedelta(days=90),
            key="date_from",
            label_visibility="collapsed"
        )
    with col_to:
        st.write("종료일")
        date_to = st.date_input(
            "종료일",
            value=datetime.now(),
            key="date_to",
            label_visibility="collapsed"
        )
    
    st.button("📊 조회하기", type="primary", use_container_width=True, key="btn_search_trigger", on_click=lambda: st.session_state.update({"show_results": True}))
    
    if st.button("🗑️ 전체 초기화", use_container_width=True, key="btn_reset_all"):
        # 🔥 모든 세션 상태 완전 초기화
        st.session_state.selected_products = set()
        st.session_state.keyword_results = {}
        st.session_state.show_results = False
        st.session_state.search_keyword = ""
        st.session_state.search_history = []
        
        # 🔥 질문 입력창 및 이력 초기화
        if "insight_question" in st.session_state:
            del st.session_state.insight_question
        if "insight_question_input" in st.session_state:
            del st.session_state.insight_question_input
        if "question_history" in st.session_state:
            del st.session_state.question_history
        
        # 🔥 기간 초기화
        if "date_from" in st.session_state:
            del st.session_state.date_from
        if "date_to" in st.session_state:
            del st.session_state.date_to
        
        # 🔥 필터 selectbox 상태 초기화
        if "filter_brand" in st.session_state:
            del st.session_state.filter_brand
        if "filter_cat1" in st.session_state:
            del st.session_state.filter_cat1
        if "filter_cat2" in st.session_state:
            del st.session_state.filter_cat2
        
        # 🔥 모든 체크박스 및 버튼 키 삭제 (검색 결과 카드 제거용)
        keys_to_delete = [key for key in st.session_state.keys() if key.startswith(("chk_kw_", "chk_filter_", "chk_nlp_", "delete_search_", "delete_q_"))]
        for key in keys_to_delete:
            del st.session_state[key]
        
        st.rerun()

with col_tabs:
    tab1, tab2, tab3 = st.tabs(["🔍 키워드 검색", "🎛️ 필터 선택", "🤖 자연어 질문"])

    # =========================
    # TAB 1: 키워드 검색
    # =========================
    with tab1:
        # 🔍 검색 입력 (Enter 가능)
        with st.form("search_form", clear_on_submit=True):
            keyword_input = st.text_input(
                "제품명 검색",
                placeholder="예: 쥬시, 멜로지오",
                key="keyword_input_field"
            )
            submitted = st.form_submit_button("검색")

        if submitted and keyword_input.strip():
            search_keyword = keyword_input.strip()
            st.session_state.search_keyword = search_keyword
            st.session_state.active_mode = "키워드 검색"
            
            # 🔥 검색 결과 계산
            keywords = [k.strip() for k in search_keyword.split(",") if k.strip()]
            mask = False
            for kw in keywords:
                mask |= _norm_series(df_all["product_name"]).str.contains(kw, case=False)
            candidates_df = df_all[mask].copy()
            
            # 🔥 키워드 검색 로그 저장
            try:
                supabase.table("search_logs").insert({
                    "search_type": "KEYWORD",
                    "search_term": search_keyword,
                    "result_count": len(candidates_df),
                    "created_at": datetime.now().isoformat()
                }).execute()
            except Exception as e:
                print("검색 로그 저장 실패:", e)
            
            # 🔥 검색 이력에 추가 (중복 검색어는 덮어쓰기)
            existing_idx = None
            for idx, history in enumerate(st.session_state.search_history):
                if history["keyword"] == search_keyword:
                    existing_idx = idx
                    break
            
            search_result = {
                "keyword": search_keyword,
                "results": sorted(candidates_df["product_name"].unique().tolist()) if not candidates_df.empty else []
            }
            
            if existing_idx is not None:
                st.session_state.search_history[existing_idx] = search_result
            else:
                st.session_state.search_history.append(search_result)
            
            st.rerun()

        # 📦 제품 선택 - 검색 이력별로 구획화
        st.markdown("### 📦 비교할 제품 선택")
        
        if not st.session_state.search_history:
            st.info("검색 결과가 없습니다.")
        else:
            # 🔥 검색어를 3개씩 가로로 배열
            num_cols = 3
            total_searches = len(st.session_state.search_history)
            
            for row_idx in range(0, total_searches, num_cols):
                cols = st.columns(num_cols)
                
                for col_idx in range(num_cols):
                    history_idx = row_idx + col_idx
                    
                    if history_idx >= total_searches:
                        break
                    
                    history = st.session_state.search_history[history_idx]
                    
                    with cols[col_idx]:
                        # 🔥 박스 스타일로 표시
                        with st.container(border=True):
                            # 검색어 제목과 삭제 버튼
                            col_title, col_delete = st.columns([4, 1])
                            
                            with col_title:
                                st.markdown(f"**🔍 {history['keyword']}**")
                            
                            with col_delete:
                                if st.button("🗑️", key=f"delete_search_{history_idx}", help="검색 결과 삭제"):
                                    # 해당 검색 결과의 제품들을 선택에서 제거
                                    for pname in history['results']:
                                        if pname in st.session_state.selected_products:
                                            st.session_state.selected_products.remove(pname)
                                    
                                    # 검색 이력에서 제거
                                    st.session_state.search_history.pop(history_idx)
                                    st.rerun()
                            
                            st.markdown("---")
                            
                            if not history['results']:
                                st.caption("📭 검색 결과 없음")
                            else:
                                # 제품 체크박스
                                for pname in history['results']:
                                    st.checkbox(
                                        pname,
                                        value=pname in st.session_state.selected_products,
                                        key=f"chk_kw_{history_idx}_{pname}",
                                        on_change=toggle_product,
                                        args=(pname,)
                                    )

    # =========================
    # TAB 2: 필터 선택
    # =========================
    with tab2:
        col1, col2, col3 = st.columns(3)

        with col1:
            brands = options_from(df_all, "brand")
            sel_brand = st.selectbox(
                "브랜드",
                ["(전체)"] + brands,
                key="filter_brand"
            )

        df1 = df_all if sel_brand == "(전체)" else df_all[df_all["brand"] == sel_brand]

        with col2:
            cat1s = options_from(df1, "category1")
            sel_cat1 = st.selectbox(
                "카테고리1",
                ["(전체)"] + cat1s,
                key="filter_cat1"
            )

        df2 = df1 if sel_cat1 == "(전체)" else df1[df1["category1"] == sel_cat1]

        with col3:
            cat2s = options_from(df2, "category2")
            sel_cat2 = st.selectbox(
                "카테고리2",
                ["(전체)"] + cat2s,
                key="filter_cat2"
            )

        candidates_df = df2 if sel_cat2 == "(전체)" else df2[df2["category2"] == sel_cat2]
        
        # 필터 변경 시 active_mode 업데이트 및 로그 저장
        if sel_brand != "(전체)" or sel_cat1 != "(전체)" or sel_cat2 != "(전체)":
            st.session_state.active_mode = "필터 선택"
            
            # 🔥 필터 선택 로그 저장 (이전 상태와 비교하여 변경 시만 저장)
            current_filter = f"{sel_brand}|{sel_cat1}|{sel_cat2}"
            if "last_filter" not in st.session_state or st.session_state.last_filter != current_filter:
                try:
                    supabase.table("search_logs").insert({
                        "search_type": "FILTER",
                        "search_term": current_filter,
                        "filter_data": {
                            "brand": sel_brand,
                            "category1": sel_cat1,
                            "category2": sel_cat2
                        },
                        "result_count": len(candidates_df),
                        "created_at": datetime.now().isoformat()
                    }).execute()
                    st.session_state.last_filter = current_filter
                except Exception as e:
                    print("필터 로그 저장 실패:", e)

        st.markdown("### 📦 비교할 제품 선택")

        with st.expander("목록 펼치기 / 접기", expanded=False):
            for pname in sorted(candidates_df["product_name"].unique()):
                st.checkbox(
                    pname,
                    value=pname in st.session_state.selected_products,
                    key=f"chk_filter_{pname}",
                    on_change=toggle_product,
                    args=(pname,)
                )

    # =========================
    # TAB 3: 자연어 질문
    # =========================
    with tab3:
        # 🔥 Form을 사용하여 제출 후 자동으로 입력창 비우기
        with st.form("question_form", clear_on_submit=True):
            question = st.text_area(
                "자연어로 질문하세요",
                placeholder="예:\n- 네스프레소 중 최저가는?\n- 최근 1개월 할인 제품\n- 에스프레소 품절 제품",
                height=100,
                key="insight_question_input"
            )
            ask_question = st.form_submit_button("🔍 질문하기", type="primary", use_container_width=True)
    
        # 🔥 질문 처리
        if ask_question and question:
            st.session_state.active_mode = "자연어 질문"
        
            # 🔥 새 질문 시 이전 질문 이력 모두 삭제
            st.session_state.question_history = []
        
            intent = classify_intent(question)
        
            # 🔥 기간 설정 (세션 상태에서 가져오기 또는 기본값 사용)
            date_from = st.session_state.get("date_from", datetime.now() - timedelta(days=90))
            date_to = st.session_state.get("date_to", datetime.now())
        
            # 날짜 객체로 변환 (필요시)
            if not isinstance(date_from, datetime):
                date_from = datetime.combine(date_from, datetime.min.time()) if hasattr(date_from, 'year') else datetime.now() - timedelta(days=90)
            if not isinstance(date_to, datetime):
                date_to = datetime.combine(date_to, datetime.min.time()) if hasattr(date_to, 'year') else datetime.now()
        
            # 🔥 현재 검색/필터 조건을 반영한 데이터셋 생성
            filtered_df = df_all.copy()
        
            # 🔥 조회 기간 적용 (브랜드/제품명 필터링은 execute_rule에서 처리)
            answer = execute_rule(intent, question, filtered_df, date_from, date_to)

            # 🔥 필터 정보 수집
            filter_info = {
                "date_from": date_from.strftime("%Y-%m-%d") if hasattr(date_from, 'strftime') else str(date_from),
                "date_to": date_to.strftime("%Y-%m-%d") if hasattr(date_to, 'strftime') else str(date_to),
                "total_products": len(filtered_df),
                "filtered": len(filtered_df) < len(df_all)
            }

            if answer:
                # 🔥 로그 저장 (답변 포함)
                save_question_log(question, intent, False, answer, filter_info)
            
                # 🔥 답변을 질문 이력에 저장
                st.session_state.question_history.append({
                    "question": question,
                    "answer": answer,
                    "intent": intent
                })
            
            else:
                with st.spinner("분석 중..."):
                    answer = llm_fallback(question, filtered_df)
                    answer = {"type": "text", "text": answer}  # 통일된 형식으로 변환
                
                # 🔥 로그 저장 (답변 포함)
                save_question_log(question, intent, True, answer, filter_info)
            
                # 🔥 답변을 질문 이력에 저장
                st.session_state.question_history.append({
                    "question": question,
                    "answer": answer,
                    "intent": intent
                })
        
            # 🔥 질문 처리 후 Form이 자동으로 입력창 비움
            st.rerun()
    
        # 🔥 질문 이력 표시
        if "question_history" in st.session_state and st.session_state.question_history:
            st.markdown("---")
        
            for idx, history in enumerate(reversed(st.session_state.question_history)):
                with st.container(border=True):
                    col_q, col_del = st.columns([10, 1])
                
                    with col_q:
                        st.markdown(f"**Q:** {history['question']}")
                
                    with col_del:
                        if st.button("🗑️", key=f"delete_q_{idx}", help="질문 삭제"):
                            st.session_state.question_history.pop(len(st.session_state.question_history) - 1 - idx)
                            st.rerun()
                
                    # 🔥 답변 표시
                    answer_data = history['answer']
                
                    if isinstance(answer_data, dict) and answer_data.get("type") == "product_list":
                        # 제품 목록이 있는 경우
                        st.markdown(f"**A:** {answer_data['text']}")
                    
                        # 체크박스 추가
                        if answer_data.get("products"):
                            st.markdown("##### 📦 비교할 제품으로 추가")
                            cols = st.columns(3)
                            for pidx, pname in enumerate(answer_data["products"]):
                                with cols[pidx % 3]:
                                    st.checkbox(
                                        pname,
                                        value=pname in st.session_state.selected_products,
                                        key=f"chk_nlp_{idx}_{pidx}_{pname}",
                                        on_change=toggle_product,
                                        args=(pname,)
                                    )
                    elif isinstance(answer_data, dict):
                        # 딕셔너리지만 product_list가 아닌 경우
                        st.markdown(f"**A:** {answer_data.get('text', str(answer_data))}")
                    else:
                        # 일반 텍스트 답변
                        st.markdown(f"**A:** {answer_data}")

st.divider()

# =========================
# 8️⃣ 결과 표시
# =========================
selected_products = list(st.session_state.selected_products)

# 🔥 자연어 질문 모드가 아닐 때만 제품 선택 확인
if st.session_state.get("active_mode") != "자연어 질문":
    if not selected_products:
        st.info("제품을 선택하세요.")
        st.stop()

    if not st.session_state.show_results:
        st.info("제품을 선택한 뒤 '조회하기'를 클릭하세요.")
        st.stop()
else:
    # 자연어 질문 모드에서는 제품 선택 없이도 진행
    if not selected_products:
        st.stop()  # 조용히 중단

st.divider()
st.subheader(f"📊 조회 결과 ({len(selected_products)}개 제품)")

# 🔥 기간 유효성 검사
if date_from > date_to:
    st.error("❌ 시작일이 종료일보다 늦습니다. 기간을 다시 설정해주세요.")
    st.stop()

st.info(f"📅 조회 기간: {date_from.strftime('%Y-%m-%d')} ~ {date_to.strftime('%Y-%m-%d')}")

timeline_rows = []
lifecycle_rows = []

# 🔥 선택된 기간 가져오기
filter_date_from = pd.to_datetime(date_from)
filter_date_to = pd.to_datetime(date_to)

for pname in selected_products:
    row = df_all[df_all["product_name"] == pname].iloc[0]

    # 가격 이벤트
    df_price = load_events(row["product_url"])
    if not df_price.empty:
        tmp = df_price.copy()
        # 🔥 브랜드 + 제품명으로 표시
        display_name = f"{row['brand']} - {pname}"
        tmp["product_name"] = display_name
        tmp["event_date"] = pd.to_datetime(tmp["date"])
        
        # 🔥 기간 필터 적용
        tmp = tmp[(tmp["event_date"] >= filter_date_from) & (tmp["event_date"] <= filter_date_to)]
        
        if tmp.empty:
            continue
            
        tmp["unit_price"] = tmp["unit_price"].astype(float)
        
        # 🔥 할인 여부 추가
        tmp["is_discount"] = tmp["event_type"] == "DISCOUNT"
        tmp["price_status"] = tmp["is_discount"].map({True: "💸 할인 중", False: "정상가"})
        
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
        
                    # 🔥 품절(포함) ~ 복원(제외) 사이 가격 제거
                    mask = (tmp["event_date"] >= out_date) & (tmp["event_date"] < restore_date)
                    tmp.loc[mask, "unit_price"] = None
                else:
                    # 복원 이벤트가 없으면 품절 이후 모든 데이터 제거
                    mask = tmp["event_date"] >= out_date
                    tmp.loc[mask, "unit_price"] = None
        
        timeline_rows.append(tmp[["product_name", "event_date", "unit_price", "price_status"]])
        

    # lifecycle 이벤트
    df_life = load_lifecycle_events(row["product_url"])
    if not df_life.empty:
        tmp2 = df_life.copy()
        # 🔥 브랜드 + 제품명으로 표시
        display_name = f"{row['brand']} - {pname}"
        tmp2["product_name"] = display_name
        tmp2["event_date"] = pd.to_datetime(tmp2["date"])
        
        # 🔥 기간 필터 적용
        tmp2 = tmp2[(tmp2["event_date"] >= filter_date_from) & (tmp2["event_date"] <= filter_date_to)]
        
        if not tmp2.empty:
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
    # 📊 차트와 범례를 분리된 레이아웃으로 표시
    # =========================
    col_chart, col_legend = st.columns([3, 1])
    
    with col_chart:
        # =========================
        # 📈 가격 선 차트 (범례 없음)
        # =========================
        base_line = (
            alt.Chart(df_chart)
            .mark_line(point=True)
            .encode(
                x=alt.X("event_date:T", title="날짜"),
                y=alt.Y("unit_price:Q", title="개당 가격 (원)"),
                color=alt.Color("product_name:N", title="제품", legend=None),  # 🔥 범례 제거
                detail="segment:N",  # 🔥 이게 핵심 (선 완전 분리)
                tooltip=[
                    alt.Tooltip("product_name:N", title="제품"),
                    alt.Tooltip("event_date:T", title="날짜", format="%Y-%m-%d"),
                    alt.Tooltip("unit_price:Q", title="개당 가격", format=",.1f"),
                    alt.Tooltip("price_status:N", title="상태"),  # 🔥 할인 여부 추가
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

                # 🔥 아이콘 위치를 가격선에 맞추기 위해 join
                df_filtered = df_filtered.merge(
                    df_timeline[["product_name", "event_date", "unit_price"]],
                    on=["product_name", "event_date"],
                    how="left"
                )
                
                # 🔥 품절/복원 아이콘은 실제 가격선 위에만 표시
                if event_type in ["OUT_OF_STOCK", "RESTOCK"]:
                    # 품절 시작점: 품절 직전 가격 사용
                    if event_type == "OUT_OF_STOCK":
                        for idx, row in df_filtered[df_filtered["unit_price"].isna()].iterrows():
                            product_prices = df_timeline[
                                (df_timeline["product_name"] == row["product_name"]) &
                                (df_timeline["event_date"] < row["event_date"]) &
                                (df_timeline["unit_price"].notna())
                            ]
                            if not product_prices.empty:
                                closest = product_prices.nsmallest(1, "event_date").iloc[-1]
                                df_filtered.at[idx, "unit_price"] = closest["unit_price"]
                    
                    # 복원 시점: 복원 당일 가격 사용 (이미 있으면 그대로, 없으면 직후 가격)
                    elif event_type == "RESTOCK":
                        # 복원 날짜는 가격선에 포함되므로 대부분 unit_price가 이미 있음
                        # 없는 경우에만 직후 가격 사용
                        for idx, row in df_filtered[df_filtered["unit_price"].isna()].iterrows():
                            product_prices = df_timeline[
                                (df_timeline["product_name"] == row["product_name"]) &
                                (df_timeline["event_date"] >= row["event_date"]) &
                                (df_timeline["unit_price"].notna())
                            ]
                            if not product_prices.empty:
                                closest = product_prices.nsmallest(1, "event_date").iloc[0]
                                df_filtered.at[idx, "unit_price"] = closest["unit_price"]
                
                else:
                    # NEW 이벤트: 가장 가까운 가격 사용
                    for idx, row in df_filtered[df_filtered["unit_price"].isna()].iterrows():
                        product_prices = df_timeline[
                            (df_timeline["product_name"] == row["product_name"]) &
                            (df_timeline["unit_price"].notna())
                        ]
                        
                        if not product_prices.empty:
                            # 이벤트 날짜와 가장 가까운 가격 찾기
                            product_prices["date_diff"] = abs(
                                (product_prices["event_date"] - row["event_date"]).dt.total_seconds()
                            )
                            closest = product_prices.nsmallest(1, "date_diff").iloc[0]
                            df_filtered.at[idx, "unit_price"] = closest["unit_price"]
                
                # unit_price 없는 lifecycle 제거 (매칭 실패한 경우)
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
                            alt.Tooltip("event_date:T", title="날짜", format="%Y-%m-%d"),
                            alt.Tooltip("unit_price:Q", title="개당 가격", format=",.1f"),  # 🔥 가격 추가
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
    
    with col_legend:
        st.markdown("#### 📋 제품 목록")
        
        # 🔥 제품별로 색상 구분하여 표시
        unique_products = sorted(df_chart["product_name"].unique())
        
        for product in unique_products:
            st.markdown(f"**•** {product}")

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
                "날짜_정렬용": row_d['start_date'],  # 🔥 정렬용 컬럼 추가
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
                "날짜": str(row_l["date"].date()),
                "날짜_정렬용": row_l["date"],  # 🔥 정렬용 컬럼 추가
                "개당 가격": None,
                "이벤트": icon_map.get(row_l["event_type"], row_l["event_type"])
            })

        if not display_rows:
            st.caption("실제 변화 이벤트 없음")
            continue

        df_display = pd.DataFrame(display_rows)
        
        # 🔥 정렬용 컬럼으로 정렬 후 제거
        df_display = df_display.sort_values("날짜_정렬용", ascending=False)
        df_display = df_display.drop(columns=["날짜_정렬용"])

        # 🔥 None 값 처리 - 포맷팅 전에 "-"로 변경
        df_display["개당 가격"] = df_display["개당 가격"].apply(
            lambda x: f"{x:.1f}" if pd.notna(x) else "-"
        )

        st.dataframe(
            df_display,
            use_container_width=True,
            hide_index=True
        )
