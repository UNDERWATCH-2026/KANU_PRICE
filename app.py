import streamlit as st
import pandas as pd
import altair as alt
from supabase import create_client
from datetime import datetime, timedelta
import hashlib   # ✅ 반드시 추가

# =========================
# 0️⃣ 기본 설정
# =========================
st.set_page_config(page_title="Capsule Price Intelligence", layout="wide")

st.markdown("""
<style>
button[data-baseweb="tab"] {
    font-size: 15px;
    padding: 10px 30px;   /* 좌우 간격 조절 */
}
</style>
""", unsafe_allow_html=True)

def mk_widget_key(prefix: str, product_url: str, scope: str) -> str:
    raw = f"{prefix}|{product_url}|{scope}"
    return prefix + "_" + hashlib.md5(raw.encode("utf-8")).hexdigest()[:16]


# =========================
# 1️⃣ Supabase 설정
# =========================
SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_ANON_KEY"]
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# =========================
# 2️⃣ 데이터 로딩
# =========================


def load_product_summary():
    cols = [
        "product_url",
        "brand",
        "category1",
        "category2",
        "product_name",
        "current_unit_price",
        "normal_unit_price",
        "is_discount",
        "first_seen_date",
        "last_seen_date",
        "event_count",
        "product_event_status",
        "is_new_product",
        "brew_type_kr",
    ]
    res = supabase.table("product_price_summary_enriched").select(", ".join(cols)).execute()
    df = pd.DataFrame(res.data)
    # 🔥 로딩 시점에 URL 정제
    df["product_url"] = df["product_url"].astype(str).str.strip("_").str.strip()
    return df

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
        "카누돌체구스토": "카누 돌체구스토",
        "카누 돌체구스토": "카누 돌체구스토",
        "네스프레소": "네스프레소",
        "스타벅스": "스타벅스",
        "일리": "일리",
        "돌체구스토": "돌체구스토",
        "네스카페": "네스카페",
    }
    
    # 공백 제거하여 매칭
    for key, value in brand_mapping.items():
        if key.replace(" ", "") == brand_query.replace(" ", ""):
            return value
    
    return brand_query

def extract_brand_from_question(q: str, df_all: pd.DataFrame) -> list:
    """질문에서 브랜드명 추출 (부분 매칭 지원, 여러 브랜드 반환 가능)"""
    q_lower = q.lower()
    brands = df_all["brand"].dropna().unique().tolist()
    matched_brands = []
    
    # 1단계: 완전 매칭
    for brand in brands:
        if brand and brand.lower() in q_lower:
            matched_brands.append(brand)
    
    if matched_brands:
        return matched_brands
    
    # 2단계: 정규화된 브랜드명으로 매칭
    for brand in brands:
        normalized = normalize_brand_name(q_lower)
        if brand.lower() == normalized.lower():
            matched_brands.append(brand)
    
    if matched_brands:
        return matched_brands
    
    # 3단계: 부분 매칭 (브랜드명의 일부가 질문에 포함)
    for brand in brands:
        brand_lower = brand.lower()
        # 브랜드명을 공백으로 분리하여 각 단어 검색
        brand_parts = brand_lower.split()
        for part in brand_parts:
            if len(part) >= 2 and part in q_lower:
                if brand not in matched_brands:
                    matched_brands.append(brand)
                break
    
    return matched_brands if matched_brands else None

def extract_product_name_from_question(q: str) -> list:
    """질문에서 제품명 키워드 추출 (여러 키워드 반환)"""
    # 제외할 키워드 (질문 관련 단어만 제외)
    exclude_words = [
        "할인", "기간", "언제", "얼마", "가격", "제품",
        "최저가", "최고가", "신제품", "품절", "복원", "중", "는", "은", "의",
        # 🔥 intent 관련 키워드 추가
        "신상", "출시", "새로", "신규", "새로운", "처음",
        "할인가", "정상가", "변동", "상승", "증가", "하락",
        "비싼", "싼", "저렴", "최근", "알려줘", "보여줘",
        "있어", "없어", "언제부터", "언제까지", "기간은", "얼마야",
    ]
    words = q.split()
    product_keywords = []
    
    for word in words:
        # 2글자 이상이고 제외 단어가 아닌 경우
        if len(word) >= 2 and not any(ex in word for ex in exclude_words):
            product_keywords.append(word)
    
    return product_keywords  # 리스트로 반환

def classify_intent(q: str):
    q = q.lower()

    # 🔥 복합 표현 먼저 - 신제품 + 품절
    if any(word in q for word in ["신제품", "새로", "신규", "출시", "신상"]) and "품절" in q:
        return "NEW_AND_OUT"

    # 🔥 복합 표현 먼저 - 품절 + 복원
    if "품절" in q and ("복원" in q or "재입고" in q):
        if any(word in q for word in ["후", "다시", "그후"]):
            return "RESTORE"
        return "OUT_AND_RESTORE"

    if "할인" in q and ("기간" in q or "언제" in q):
        return "DISCOUNT_PERIOD"
    if "할인" in q or "행사" in q:
        return "DISCOUNT"
    if any(word in q for word in ["신제품", "새롭게", "새로", "신규", "출시", "새로운", "처음", "신상"]):
        return "NEW"
    if any(word in q for word in ["가장 싼", "제일 싼", "제일 저렴한", "가장 저렴한", "최저가"]):
        return "PRICE_MIN"
    if any(word in q for word in ["가장 비싼", "제일 비싼", "최고가"]):
        return "PRICE_MAX"
    if any(word in q for word in ["상승", "증가"]) and "않" not in q:
        return "PRICE_UP"
    if "변동" in q or "많이 바뀐" in q:
        return "VOLATILITY"
    if any(word in q for word in ["복원", "재입고", "입고", "돌아온"]):
        return "RESTORE"
    if "품절" in q:
        return "OUT"
    if "정상가" in q and ("변동" in q or "상승" in q):
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

def extract_period_from_question(q: str):
    """
    질문에서 기간 추출
    반환: (date_from, date_to, period_label) 또는 None (기간 없음)
    """
    today = datetime.today()
    q_lower = q.lower()

    # 최근 N 표현
    if any(w in q_lower for w in ["최근 7일", "최근 일주일", "최근 1주일"]):
        return today - timedelta(days=7), today, "최근 7일 내"
    if any(w in q_lower for w in ["최근 한 달", "최근 30일", "최근 1개월"]):
        return today - timedelta(days=30), today, "최근 1개월 내"
    if "최근 3개월" in q_lower:
        return today - timedelta(days=90), today, "최근 3개월 내"
    if "최근 1년" in q_lower:
        return today - timedelta(days=365), today, "최근 1년 내"

    # 연도+월 표현 (예: 2025년 10월)
    import re
    month_match = re.search(r"(\d{4})년\s*(\d{1,2})월", q)
    if month_match:
        year = int(month_match.group(1))
        month = int(month_match.group(2))
        from_dt = datetime(year, month, 1)
        # 말일 계산
        if month == 12:
            to_dt = datetime(year + 1, 1, 1) - timedelta(days=1)
        else:
            to_dt = datetime(year, month + 1, 1) - timedelta(days=1)
        return from_dt, to_dt, f"{year}년 {month}월"

    # 연도만 (예: 2025년)
    year_match = re.search(r"(\d{4})년", q)
    if year_match:
        year = int(year_match.group(1))
        return datetime(year, 1, 1), datetime(year, 12, 31), f"{year}년"

    return None  # 기간 없음 → 조회기간 사용
    

def extract_brew_type(q: str, df_all: pd.DataFrame):
    q = q.lower()
    brew_list = df_all["brew_type_kr"].dropna().unique().tolist()

    for brew in brew_list:
        if brew and brew.lower() in q:
            return brew
    return None

def execute_rule(intent, question, df_summary, date_from=None, date_to=None):
    df_work = df_summary.copy()
    
    # 🔥 기간 레이블 생성
    question_period = extract_period_from_question(question)
    if question_period:
        _, _, period_label = question_period
    else:
        # 조회기간 설정값으로 레이블 생성
        if date_from and date_to:
            period_label = f"{date_from.strftime('%Y-%m-%d')} ~ {date_to.strftime('%Y-%m-%d')}"
        else:
            period_label = "조회 기간 내"

    # 🔥 키워드 추출 (brew_type은 별도 처리)
    brew_condition = extract_brew_type(question, df_summary)
    if brew_condition:
        df_work = df_work[df_work["brew_type_kr"] == brew_condition]

    # 🔥 질문에서 의미있는 키워드만 추출
    all_keywords = extract_product_name_from_question(question)
    
    # 🔥 각 키워드는 모든 필드 중 어디든 포함되어야 함 (AND of OR)
    # 예: "카누 디카페인" → "카누"가 어디든 있고 AND "디카페인"도 어디든 있어야 함
    if all_keywords:
        for keyword in all_keywords:
            if len(keyword) >= 2:
                # 각 키워드마다 모든 필드에서 OR 검색
                keyword_mask = False
                keyword_mask |= _norm_series(df_work["product_name"]).str.contains(keyword, case=False)
                keyword_mask |= _norm_series(df_work["brand"]).str.contains(keyword, case=False)
                keyword_mask |= _norm_series(df_work["category1"]).str.contains(keyword, case=False)
                keyword_mask |= _norm_series(df_work["category2"]).str.contains(keyword, case=False)
                
                # 해당 키워드가 어디든 포함된 제품만 남김 (AND 조건)
                if keyword_mask is not False and keyword_mask.any():
                    df_work = df_work[keyword_mask]
    
    # 🔥 키워드 필터링 후 결과가 없는 경우 메시지 반환
    if all_keywords and df_work.empty:
        keywords_str = ", ".join(all_keywords)
        return f"'{keywords_str}'에 해당하는 제품이 없습니다."

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
                    # 할인가 조회
                    discount_price_res = (
                        supabase.table("product_all_events")
                        .select("unit_price")
                        .eq("product_url", row["product_url"])
                        .eq("event_type", "DISCOUNT")
                        .gte("date", period["discount_start_date"])
                        .lte("date", period["discount_end_date"])
                        .limit(1)
                        .execute()
                    )
                    
                    discount_price = discount_price_res.data[0]["unit_price"] if discount_price_res.data else None
                    
                    # 정상가 조회 (할인 직전 가격)
                    normal_price_res = (
                        supabase.table("product_all_events")
                        .select("unit_price")
                        .eq("product_url", row["product_url"])
                        .eq("event_type", "NORMAL")
                        .lt("date", period["discount_start_date"])
                        .order("date", desc=True)
                        .limit(1)
                        .execute()
                    )
                    
                    normal_price = normal_price_res.data[0]["unit_price"] if normal_price_res.data else None
                    
                    # 가격 정보 구성
                    price_info = ""
                    if normal_price and discount_price:
                        discount_rate = ((normal_price - discount_price) / normal_price) * 100
                        price_info = (f"  💰 정상가: {float(normal_price):,.1f}원 → "
                                    f"할인가: {float(discount_price):,.1f}원 "
                                    f"({discount_rate:.0f}% 할인)")
                    elif discount_price:
                        price_info = f"  💰 할인가: {float(discount_price):,.1f}원"
                    
                    results.append({
                        "text": f"• {row['brand']} - {row['product_name']}\n"
                                f"  📅 할인 기간: {period['discount_start_date']} ~ {period['discount_end_date']}\n"
                                f"{price_info}",
                        "product_url": str(row["product_url"])
                    })
        
        if not results:
            return "해당 제품의 할인 기간 정보가 없습니다."
        
        return {
            "type": "product_list",
            "text": "할인 기간 정보:\n\n" + "\n\n".join([r["text"] for r in results]),
            "products": [
                str(r["product_url"]).strip().lower()
                for r in results
                if r.get("product_url")
            ]
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
                "text": f"• {row['brand']} - {product_name}{category_str}\n  💰 현재가: {float(row['current_unit_price']):,.1f}원",
                "product_url": str(row["product_url"])
            })
        
        if not results:
            return None
        
        return {
            "type": "product_list",
            "text": f"{period_label} 할인 중 제품 ({len(results)}개)",
            "products": [
                str(r["product_url"]).strip().lower()
                for r in results
                if r.get("product_url")
            ]
        }

    if intent == "PRICE_MIN":
        df_valid = df_work[df_work["current_unit_price"] > 0]
        if df_valid.empty:
            return "현재 판매 중인 제품이 없습니다."

        min_price = df_valid["current_unit_price"].min()
        df_min = df_valid[df_valid["current_unit_price"] == min_price]

        results = []
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
            
            # 카테고리 정보 구성
            categories = []
            if pd.notna(row.get("category1")) and row["category1"]:
                categories.append(row["category1"])
            if pd.notna(row.get("category2")) and row["category2"]:
                categories.append(row["category2"])
            
            category_str = f" [{' > '.join(categories)}]" if categories else ""
            
            results.append({
                "text": f"• {row['brand']} - {row['product_name']}{category_str}\n"
                        f"  💰 최저가: {min_price:,.1f}원 (기간: {sd} ~ {ed})",
                "product_url": str(row["product_url"])
            })

        if not results:
            return "최저가 계산 대상 제품이 없습니다."

        return {
            "type": "product_list",
            "text": f"{period_label} 최저가 제품 ({len(results)}개)",
            "products": [
                str(r["product_url"]).strip().lower()
                for r in results
                if r.get("product_url")
            ]
        }

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
                "text": f"• {row['brand']} - {product_name}{category_str}\n  🆕 출시일: {launch_date}",
                "product_url": str(row["product_url"])
            })
        
        if not results:
            return None
        
        # 체크박스와 텍스트 분리 반환
        return {
            "type": "product_list",
            "text": f"{period_label} 신제품 ({len(results)}개)",
            "products": [
                str(r["product_url"]).strip().lower()
                for r in results
                if r.get("product_url")
            ]
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
                "text": f"• {row['brand']} - {product_name}{category_str}\n  📅 품절일: {out_date}",
                "product_url": str(row["product_url"])
            })
        
        if not results:
            return None
        
        return {
            "type": "product_list",
            "text": f"{period_label} 품절 제품 ({len(results)}개)",
            "products": [
                str(r["product_url"]).strip().lower()
                for r in results
                if r.get("product_url")
            ]
        }

    if intent == "RESTORE":
        res = (
            supabase.table("product_lifecycle_events")
            .select("product_url, date")
            .eq("lifecycle_event", "RESTOCK")
        )
        
    if intent == "OUT_AND_RESTORE":
        results = []
    
        # 품절 조회
        res_out = (
            supabase.table("product_lifecycle_events")
            .select("product_url, date")
            .eq("lifecycle_event", "OUT_OF_STOCK")
        )
        if date_from:
            res_out = res_out.gte("date", date_from.strftime("%Y-%m-%d"))
        if date_to:
            res_out = res_out.lte("date", date_to.strftime("%Y-%m-%d"))
        res_out = res_out.execute()
    
        out_data = {r["product_url"]: r["date"] for r in (res_out.data or [])}
    
        # 복원 조회
        res_restore = (
            supabase.table("product_lifecycle_events")
            .select("product_url, date")
            .eq("lifecycle_event", "RESTOCK")
        )
        if date_from:
            res_restore = res_restore.gte("date", date_from.strftime("%Y-%m-%d"))
        if date_to:
            res_restore = res_restore.lte("date", date_to.strftime("%Y-%m-%d"))
        res_restore = res_restore.execute()
    
        restore_data = {r["product_url"]: r["date"] for r in (res_restore.data or [])}
    
        all_urls = set(list(out_data.keys()) + list(restore_data.keys()))
        df = df_work[df_work["product_url"].isin(all_urls)]
    
        if df.empty:
            return "해당 기간 품절 또는 복원 제품이 없습니다."
    
        # 품절 목록
        out_results = []
        for _, row in df[df["product_url"].isin(out_data)].iterrows():
            categories = []
            if pd.notna(row.get("category1")) and row["category1"]:
                categories.append(row["category1"])
            if pd.notna(row.get("category2")) and row["category2"]:
                categories.append(row["category2"])
            category_str = f" [{' > '.join(categories)}]" if categories else ""
    
            out_results.append({
                "text": f"• {row['brand']} - {row['product_name']}{category_str}\n  📅 품절일: {out_data[row['product_url']]}",
                "product_url": str(row["product_url"])
            })
    
        # 복원 목록
        restore_results = []
        for _, row in df[df["product_url"].isin(restore_data)].iterrows():
            categories = []
            if pd.notna(row.get("category1")) and row["category1"]:
                categories.append(row["category1"])
            if pd.notna(row.get("category2")) and row["category2"]:
                categories.append(row["category2"])
            category_str = f" [{' > '.join(categories)}]" if categories else ""
    
            restore_results.append({
                "text": f"• {row['brand']} - {row['product_name']}{category_str}\n  🔄 복원일: {restore_data[row['product_url']]}",
                "product_url": str(row["product_url"])
            })
    
        all_results = out_results + restore_results
    
        text = ""
        if out_results:
            text += "❌ 품절 제품:\n\n" + "\n\n".join([r["text"] for r in out_results])
        if restore_results:
            if text:
                text += "\n\n---\n\n"
            text += "🔄 복원 제품:\n\n" + "\n\n".join([r["text"] for r in restore_results])
    
        return {
            "type": "product_list",
            "text": text,
            "products": [
                str(r["product_url"]).strip().lower()
                for r in all_results
                if r.get("product_url")
            ]
        }
            
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
                "text": f"• {row['brand']} - {product_name}{category_str}\n  🔄 복원일: {restock_date}",
                "product_url": str(row["product_url"])
            })
        
        if not results:
            return None
        
        return {
            "type": "product_list",
            "text": f"{period_label} 복원 제품 ({len(results)}개)",
            "products": [
                str(r["product_url"]).strip().lower()
                for r in results
                if r.get("product_url")
            ]
        }

    if intent == "NEW_AND_OUT":
        # 신제품 조회
        res_new = (
            supabase.table("product_lifecycle_events")
            .select("product_url, date")
            .eq("lifecycle_event", "NEW_PRODUCT")
        )
        if date_from:
            res_new = res_new.gte("date", date_from.strftime("%Y-%m-%d"))
        if date_to:
            res_new = res_new.lte("date", date_to.strftime("%Y-%m-%d"))
        res_new = res_new.execute()
        new_data = {r["product_url"]: r["date"] for r in (res_new.data or [])}

        # 품절 조회
        res_out = (
            supabase.table("product_lifecycle_events")
            .select("product_url, date")
            .eq("lifecycle_event", "OUT_OF_STOCK")
        )
        if date_from:
            res_out = res_out.gte("date", date_from.strftime("%Y-%m-%d"))
        if date_to:
            res_out = res_out.lte("date", date_to.strftime("%Y-%m-%d"))
        res_out = res_out.execute()
        out_data = {r["product_url"]: r["date"] for r in (res_out.data or [])}

        all_urls = set(list(new_data.keys()) + list(out_data.keys()))
        df = df_work[df_work["product_url"].isin(all_urls)]

        if df.empty:
            return "해당 기간 신제품 또는 품절 제품이 없습니다."

        new_results = []
        for _, row in df[df["product_url"].isin(new_data)].iterrows():
            categories = []
            if pd.notna(row.get("category1")) and row["category1"]:
                categories.append(row["category1"])
            if pd.notna(row.get("category2")) and row["category2"]:
                categories.append(row["category2"])
            category_str = f" [{' > '.join(categories)}]" if categories else ""
            new_results.append({
                "text": f"• {row['brand']} - {row['product_name']}{category_str}\n  🆕 출시일: {new_data[row['product_url']]}",
                "product_url": str(row["product_url"])
            })

        out_results = []
        for _, row in df[df["product_url"].isin(out_data)].iterrows():
            categories = []
            if pd.notna(row.get("category1")) and row["category1"]:
                categories.append(row["category1"])
            if pd.notna(row.get("category2")) and row["category2"]:
                categories.append(row["category2"])
            category_str = f" [{' > '.join(categories)}]" if categories else ""
            out_results.append({
                "text": f"• {row['brand']} - {row['product_name']}{category_str}\n  📅 품절일: {out_data[row['product_url']]}",
                "product_url": str(row["product_url"])
            })

        all_results = new_results + out_results

        if new_results and out_results:
            text = f"🆕 신제품 ({len(new_results)}개) + ❌ 품절 ({len(out_results)}개)"
        elif new_results:
            text = f"🆕 신제품 ({len(new_results)}개)"
        elif out_results:
            text = f"❌ 품절 제품 ({len(out_results)}개)"
        else:
            text = ""

        return {
            "type": "product_list",
            "text": text,
            "products": [
                str(r["product_url"]).strip().lower()
                for r in all_results
                if r.get("product_url")
            ],
            "new_products": [str(r["product_url"]).strip().lower() for r in new_results],
            "out_products": [str(r["product_url"]).strip().lower() for r in out_results],
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

규칙:
- 답변은 간결하게 핵심만 말하세요.
- 데이터 컬럼명(is_new_product, is_discount 등 영문 필드명)은 절대 언급하지 마세요.
- 기술적인 내부 정보(컬럼명, 값, 코드)는 노출하지 마세요.
- 결과가 없으면 "해당 조건에 맞는 제품이 없습니다." 로만 답하세요.

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
    깨진 한글 및 자주 발생하는 인코딩 오류 패턴 보정
    """
    if s is None:
        return ""

    s = str(s)

    # 제어문자 제거
    s = re.sub(r"[\x00-\x1f\x7f-\x9f]", "", s).strip()

    # 🔥 자주 깨지는 패턴 사전
    fix_map = {
        "본   직영": "본사직영",
        "본  직영": "본사직영",
        "본 직영": "본사직영",
        "바닐   향": "바닐라향",
        "바닐  향": "바닐라향",
        "네스프   ": "네스프레소",
        "스타   스": "스타벅스",
    }

    for bad, good in fix_map.items():
        if bad in s:
            s = s.replace(bad, good)

    # 🔥 패턴 기반 보정
    s = re.sub(r"바닐.*?향", "바닐라향", s)
    s = re.sub(r"본.*?직영", "본사직영", s)

    # 🔥 여러 공백을 한 칸으로
    s = re.sub(r"\s{2,}", " ", s).strip()

    return s

def detect_encoding_issues(df: pd.DataFrame):
    if "product_name_raw" not in df.columns:
        return

    mask = df["product_name_raw"].str.contains(" ", na=False)
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

def format_product_label(row):
    brand = row.get("brand")
    product_name = row.get("product_name")
    category1 = row.get("category1")
    category2 = row.get("category2")

    brand = str(brand).strip() if pd.notna(brand) else ""
    product_name = str(product_name).strip() if pd.notna(product_name) else ""
    category1 = str(category1).strip() if pd.notna(category1) else ""
    category2 = str(category2).strip() if pd.notna(category2) else ""

    # 🔥 카테고리 생략 브랜드
    if brand in {"카누 바리스타", "네슬레", "일리카페"}:
        return f"{brand} - {product_name}"

    parts = [brand]

    if category1:
        parts.append(category1)

    if category2:
        parts.append(category2)

    parts.append(product_name)

    return " - ".join(parts)


import re

def render_card(bg, border, title, content):

    return f"""
    <div style="
        background:{bg};
        padding:18px;
        border-radius:12px;
        border-left:6px solid {border};
        min-height:120px;
        box-shadow:0 1px 3px rgba(0,0,0,0.06);
    ">
        <div style="font-weight:600;font-size:15px;margin-bottom:8px;">
            {title}
        </div>
        {content}
    </div>
    """
# =========================
# 🔧 제품 선택 토글 함수 (안정화)
# =========================
def toggle_product(product_url):
    if product_url in st.session_state.selected_products:
        st.session_state.selected_products.remove(product_url)
    else:
        st.session_state.selected_products.add(product_url)

# =========================
# 정상가 변동 
# =========================

def get_normal_price_change_dates(product_url, date_from, date_to):

    res = (
        supabase.table("product_normal_price_events")
        .select("date")
        .eq("product_url", product_url)
        .gte("date", date_from.strftime("%Y-%m-%d"))
        .lte("date", date_to.strftime("%Y-%m-%d"))
        .execute()
    )

    if not res.data:
        return []

    return [r["date"] for r in res.data]
    
# =========================
# 4️⃣ 체크박스 key 등록
# =========================

def register_product_checkbox_key(product_url: str, widget_key: str):
    if "product_checkbox_keys" not in st.session_state:
        st.session_state["product_checkbox_keys"] = {}
    st.session_state["product_checkbox_keys"].setdefault(product_url, set()).add(widget_key)

def remove_product_everywhere(product_url: str):
    clean_url = str(product_url).strip("_").strip()
    st.session_state.selected_products.discard(clean_url)
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

# 세션 상태 초기화 부분에 추가
if "selected_products" not in st.session_state:
    st.session_state.selected_products = set()

# 🔥 추가
if "_removed_products" not in st.session_state:
    st.session_state["_removed_products"] = set()

# =========================
# 5️⃣ 메인 UI
# =========================
st.title("☕ Coffee Capsule Price Intelligence")

# -------------------------
# 데이터 로딩 (탭 이전에 로드)
# -------------------------
df_all = load_product_summary()


df_all["product_url"] = (
    df_all["product_url"]
    .astype(str)
    .str.strip("_")
    .str.strip()
)


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
            st.session_state.question_history = []
        
        # 🔥 기간 초기화
        if "date_from" in st.session_state:
            del st.session_state.date_from
        if "date_to" in st.session_state:
            del st.session_state.date_to
        
        # 🔥 필터 selectbox 상태 완전 초기화 (삭제)
        st.session_state["filter_brand"] = "(전체)"
        st.session_state["filter_cat1"] = "(전체)"
        st.session_state["filter_cat2"] = "(전체)"
        if "last_filter" in st.session_state:
            del st.session_state.last_filter

        # 🔥 모든 체크박스, 버튼, form 입력 키 삭제
        keys_to_delete = [
            key for key in list(st.session_state.keys())
            if key.startswith(("tab", "chk_tab", "remove_product_", "delete_"))
        ]

        st.session_state.selected_products = set()
        st.session_state["_removed_products"] = set()  # 🔥 추가

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
                placeholder="예: 카누 디카페인 (공백=AND) / 쥬시, 멜로지오 (쉼표=OR)",
                key="keyword_input_field"
            )
            submitted = st.form_submit_button("검색")

        if submitted and keyword_input.strip():
            search_keyword = keyword_input.strip()
            st.session_state.search_keyword = search_keyword
            st.session_state.active_mode = "키워드 검색"
            
            # 🔥 검색 결과 계산
            # 쉼표로 구분: OR 검색 (예: "쥬시, 멜로지오" → 쥬시 OR 멜로지오)
            # 공백으로 구분: AND 검색 (예: "카누 디카페인" → 카누 AND 디카페인)
            
            if "," in search_keyword:
                # 쉼표 구분: OR 검색
                keywords = [k.strip() for k in search_keyword.split(",") if k.strip()]
                mask = False
                for kw in keywords:
                    # 모든 필드에서 검색
                    mask |= _norm_series(df_all["product_name"]).str.contains(kw, case=False)
                    mask |= _norm_series(df_all["brand"]).str.contains(kw, case=False)
                    mask |= _norm_series(df_all["category1"]).str.contains(kw, case=False)
                    mask |= _norm_series(df_all["category2"]).str.contains(kw, case=False)
                    mask |= _norm_series(df_all["brew_type_kr"]).str.contains(kw, case=False)
                candidates_df = df_all[mask].copy()
            else:
                # 공백 구분: AND 검색
                keywords = search_keyword.split()
                candidates_df = df_all.copy()
                
                for kw in keywords:
                    if len(kw) >= 2:
                
                        keyword_mask = (
                            _norm_series(candidates_df["product_name"]).str.contains(kw, case=False) |
                            _norm_series(candidates_df["brand"]).str.contains(kw, case=False) |
                            _norm_series(candidates_df["category1"]).str.contains(kw, case=False) |
                            _norm_series(candidates_df["category2"]).str.contains(kw, case=False) |
                            _norm_series(candidates_df["brew_type_kr"]).str.contains(kw, case=False)
                        )
                
                        # 🔥 무조건 필터 적용
                        candidates_df = candidates_df[keyword_mask]
                
                        # 🔥 매칭이 하나도 없으면 바로 종료
                        if candidates_df.empty:
                            break
            
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
            
            sorted_df = (
                candidates_df
                .fillna("")
                .drop_duplicates(subset=["product_url"])
                .sort_values(
                    by=["brand", "category1", "category2", "product_name"]
                )
            )
            
            search_result = {
                "keyword": search_keyword,
                "results": sorted_df["product_url"].tolist()
            }
            
            if existing_idx is not None:
                st.session_state.search_history[existing_idx] = search_result
            else:
                st.session_state.search_history.append(search_result)
            


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
                                    for product_url in history["results"]:
                                        st.session_state.selected_products.discard(product_url)
                                        # 🔥 체크박스 위젯 상태도 False로 초기화
                                        if "product_checkbox_keys" in st.session_state:
                                            keys = st.session_state["product_checkbox_keys"].get(product_url, set())
                                            for k in list(keys):
                                                if k in st.session_state:
                                                    del st.session_state[k]  # 🔥 False 대신 삭제
                                    st.session_state.search_history.pop(history_idx)
                                    st.rerun()
                                                                                       
                            if not history['results']:
                                st.caption("📭 검색 결과 없음")
                            
                            else:
                                # 🔥 product_url 기준 정렬
                                sorted_df = (
                                    df_all[df_all["product_url"].isin(history["results"])]
                                    .fillna("")
                                    .drop_duplicates(subset=["product_url"])
                                    .sort_values(
                                        by=["brand", "category1", "category2", "product_name"]
                                    )
                                )
                            

                                with st.expander(f"목록 펼치기 / 접기 ({len(sorted_df)}개)", expanded=False):

                                    for _, row in sorted_df.iterrows():
                                
                                        product_url = row["product_url"]
                                        label = format_product_label(row)
                                
                                        scope = f"hist_{history_idx}"
                                
                                        # 🔥 여기부터 교체
                                        is_selected = product_url in st.session_state.selected_products
                                        k = mk_widget_key("chk_tab1", product_url, scope) + ("_1" if is_selected else "_0")
                                        register_product_checkbox_key(product_url, k)
                                
                                        col_chk, col_lbl = st.columns([0.02, 0.98], vertical_alignment="center")
                                
                                        with col_chk:
                                            checked = st.checkbox(
                                                "",
                                                key=k,
                                                value=is_selected
                                            )
                                
                                        with col_lbl:
                                            st.markdown(
                                                f"""
                                                <div style="
                                                    white-space:normal;
                                                    word-break:keep-all;
                                                    overflow-wrap:break-word;
                                                    line-height:1.35;
                                                    padding:5px 0 6px 0;
                                                ">
                                                    {label}
                                                </div>
                                                """,
                                                unsafe_allow_html=True
                                            )
                                
                                        if checked:
                                            st.session_state.selected_products.add(product_url)
                                        else:
                                            st.session_state.selected_products.discard(product_url)
                                
                                    # 🔽 for 밖
                                    st.markdown("<div style='height:10px;'></div>", unsafe_allow_html=True)
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
                index=0,  # 🔥 기본값 명시
                key="filter_brand"
            )

        df1 = df_all if sel_brand == "(전체)" else df_all[df_all["brand"] == sel_brand]

        with col2:
            cat1s = options_from(df1, "category1")
            sel_cat1 = st.selectbox(
                "카테고리1",
                ["(전체)"] + cat1s,
                index=0,  # 🔥 기본값 명시
                key="filter_cat1"
            )

        df2 = df1 if sel_cat1 == "(전체)" else df1[df1["category1"] == sel_cat1]

        with col3:
            cat2s = options_from(df2, "category2")
            sel_cat2 = st.selectbox(
                "카테고리2",
                ["(전체)"] + cat2s,
                index=0,  # 🔥 기본값 명시
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

        # 🔥 정렬 + 중복 제거 먼저
        unique_df = (
            candidates_df
            .fillna("")
            .drop_duplicates(subset=["product_url"])
            .sort_values(
                by=["brand", "category1", "category2", "product_name"]
            )
        )
        
        with st.expander(f"목록 펼치기 / 접기 ({len(unique_df)}개)", expanded=False):
        
            for _, row in unique_df.iterrows():
        
                product_url = row["product_url"]
                label = format_product_label(row)
        
                scope = f"{sel_brand}|{sel_cat1}|{sel_cat2}"
        
                # 🔥 여기부터 교체
                is_selected = product_url in st.session_state.selected_products
                k = mk_widget_key("chk_tab2", product_url, scope) + ("_1" if is_selected else "_0")
                register_product_checkbox_key(product_url, k)
        
                col_chk, col_lbl = st.columns([0.02, 0.98], vertical_alignment="center")
        
                with col_chk:
                    checked = st.checkbox(
                        "",
                        key=k,
                        value=is_selected
                    )
        
                with col_lbl:
                    st.markdown(
                        f"""
                        <div style="
                            white-space:normal;
                            word-break:keep-all;
                            overflow-wrap:break-word;
                            line-height:1.35;
                            padding:5px 0 6px 0;
                        ">
                            {label}
                        </div>
                        """,
                        unsafe_allow_html=True
                    )
        
                if checked:
                    st.session_state.selected_products.add(product_url)
                else:
                    st.session_state.selected_products.discard(product_url)
        
            # 🔽 for 밖
            st.markdown("<div style='height:10px;'></div>", unsafe_allow_html=True)
    # =========================
    # TAB 3: 자연어 질문
    # =========================
    with tab3:
        # 🔥 Form을 사용하여 제출 후 자동으로 입력창 비우기
        with st.form("question_form", clear_on_submit=True):
            question = st.text_input(
                "자연어로 질문하세요",
                placeholder="예: 카누 바리스타 쥬시 할인 기간 / 네스프레소 최저가 제품",
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
        
            # 교체
            # 🔥 질문에서 기간 추출 시도
            question_period = extract_period_from_question(question)
            
            if question_period:
                # 질문에 기간이 있으면 질문 기간 우선
                q_date_from, q_date_to, _ = question_period
                answer = execute_rule(intent, question, filtered_df, q_date_from, q_date_to)
            else:
                # 질문에 기간 없으면 조회기간 설정값 사용
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
                            st.session_state.question_history.pop(
                                len(st.session_state.question_history) - 1 - idx
                            )
                            st.rerun()
        
                    answer_data = history["answer"]
                    
                    with st.expander("💬 답변 펼치기 / 접기", expanded=True):
                    
                        # =========================
                        # ✅ 제품 리스트 답변 처리
                        # =========================
                        if isinstance(answer_data, dict) and answer_data.get("type") == "product_list":
                    
                            # 🔥 헤더 텍스트만 출력 (제품 목록은 체크박스로 대체)
                            header_text = answer_data['text'].split('\n')[0]
                            st.markdown(f"**A:** {header_text}")
                    
                            if answer_data.get("products"):
                    
                                st.markdown(
                                    "<div style='font-size:13px; color:#6b7280; margin:6px 0 8px 0;'>"
                                    "* 비교할 제품을 선택해 주세요"
                                    "</div>",
                                    unsafe_allow_html=True
                                )
    
                                # 🔥 체크박스 렌더링 헬퍼
                                def render_product_checkboxes(product_urls, scope_prefix):
                                    sorted_df = (
                                        df_all[df_all["product_url"].isin(product_urls)]
                                        .fillna("")
                                        .drop_duplicates(subset=["product_url"])
                                        .sort_values(by=["brand", "category1", "category2", "product_name"])
                                    )
                                    if sorted_df.empty:
                                        st.caption("⚠️ 매칭되는 제품이 없습니다.")
                                        return
                                    for _, row in sorted_df.iterrows():
                                        product_url = row["product_url"]
                                        label = format_product_label(row)
                                        scope = f"tab3_{idx}_{scope_prefix}"
                                        is_selected = product_url in st.session_state.selected_products
                                        k = mk_widget_key("chk_tab3", product_url, scope) + ("_1" if is_selected else "_0")
                                        register_product_checkbox_key(product_url, k)
                                        col_chk, col_lbl = st.columns([0.02, 0.98], vertical_alignment="center")
                                        with col_chk:
                                            checked = st.checkbox("", key=k, value=is_selected)
                                        with col_lbl:
                                            st.markdown(
                                                f"<div style='white-space:normal; word-break:keep-all; overflow-wrap:break-word; line-height:1.35; padding:5px 0 6px 0;'>{label}</div>",
                                                unsafe_allow_html=True
                                            )
                                        if checked:
                                            st.session_state.selected_products.add(product_url)
                                        else:
                                            st.session_state.selected_products.discard(product_url)
                                    st.markdown("<div style='height:10px;'></div>", unsafe_allow_html=True)
    
                                # 🔥 NEW_AND_OUT: 신제품/품절 구분 표시
                                new_products = answer_data.get("new_products", [])
                                out_products = answer_data.get("out_products", [])
    
                                if new_products or out_products:
                                    if new_products:
                                        st.markdown("**🆕 신제품**")
                                        render_product_checkboxes(new_products, "new")
                                    if out_products:
                                        st.markdown("**❌ 품절 제품**")
                                        render_product_checkboxes(out_products, "out")
                                else:
                                    # 기존 방식 (구분 없는 경우)
                                    render_product_checkboxes(answer_data["products"], "all")
                    
                            else:
                                st.caption("표시할 제품이 없습니다.")

                    
                        # =========================
                        # 일반 텍스트 답변
                        # =========================
                        elif isinstance(answer_data, dict):
                            st.markdown(f"**A:** {answer_data.get('text', str(answer_data))}")
                    
                        else:
                            st.markdown(f"**A:** {answer_data}")

# st.divider()

# =========================
# 8️⃣ 결과 표시
# =========================
selected_products = list(st.session_state.selected_products)

if selected_products:   # 🔥 조건 반전

    st.divider()

    # 🔥 제목과 다운로드 버튼
    col_title, col_download = st.columns([4, 1])
    with col_title:
        st.subheader(f"📊 조회 결과 ({len(selected_products)}개 제품)")
    with col_download:
        download_placeholder = st.empty()

    # 🔥 이하 차트 / 결과 코드 전부 이 안으로 넣기
    
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
    
    for product_url in selected_products:
        product_row = df_all[df_all["product_url"] == product_url]
    
        if product_row.empty:
            st.session_state.selected_products.discard(product_url)
            continue
    
        row = product_row.iloc[0]
        pname = row["product_name"]
    
        # 👉 여기서는 아무것도 출력하지 않음
    
        
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

            # 🔥 0원 → None (품절 구간 선 끊기)
            tmp.loc[tmp["unit_price"] == 0, "unit_price"] = None

            # 🔥 할인 여부 추가
            tmp["is_discount"] = tmp["event_type"] == "DISCOUNT"
            tmp["price_status"] = tmp["is_discount"].map({True: "💸 할인 중", False: "정상가"})

            # 🔥 None(품절)인 경우 덮어쓰기
            tmp.loc[tmp["unit_price"].isna(), "price_status"] = "품절"
            tmp.loc[tmp["unit_price"].isna(), "price_detail"] = "품절"

            # 🔥 정상가와 할인율 정보 추가 (툴팁용)
            tmp["normal_price"] = None
            tmp["discount_rate"] = None
            tmp["price_detail"] = ""
            
            # 할인 중인 행에 대해 정상가 찾기
            for idx, price_row in tmp[tmp["is_discount"]].iterrows():
            
                # 🔥 개당 정상가 조회 (unit 기준)
                normal_price_res = (
                    supabase.table("raw_daily_prices_unit")
                    .select("unit_normal_price")
                    .eq("product_url", row["product_url"])
                    .eq("date", price_row["event_date"].strftime("%Y-%m-%d"))
                    .limit(1)
                    .execute()
                )
            
                normal_price = (
                    float(normal_price_res.data[0]["unit_normal_price"])
                    if normal_price_res.data
                    else None
                )
            
                discount_price = (
                    float(price_row["unit_price"])
                    if pd.notna(price_row["unit_price"])
                    else None
                )
            
                if normal_price and discount_price:
                    discount_rate = ((normal_price - discount_price) / normal_price) * 100
            
                    tmp.at[idx, "normal_price"] = normal_price
                    tmp.at[idx, "discount_rate"] = discount_rate
                    tmp.at[idx, "price_detail"] = (
                        f"정상가: {normal_price:,.1f}원 → "
                        f"할인가: {discount_price:,.1f}원 "
                        f"({discount_rate:.0f}% 할인)"
                    )
            
                elif discount_price:
                    tmp.at[idx, "price_detail"] = f"할인가: {discount_price:,.1f}원"
            
                else:
                    tmp.at[idx, "price_detail"] = "-"
            # 정상가인 경우
            for idx, price_row in tmp[~tmp["is_discount"]].iterrows():
                tmp.at[idx, "price_detail"] = f"정상가: {price_row['unit_price']:,.1f}원"
                       
            # 🔥 lifecycle 데이터 불러오기 - 기간 필터 없이 전체 조회
            df_life = load_lifecycle_events(row["product_url"])  # 🔥 이 줄 추가
            
            if not df_life.empty:
                df_life["date"] = pd.to_datetime(df_life["date"], errors="coerce")
                df_life = df_life.dropna(subset=["date"])
                
                # 🔥 0원 가격 날짜를 OUT_OF_STOCK으로 추가
                zero_dates = tmp[tmp["unit_price"].isna()]["event_date"].tolist()
                for zdate in zero_dates:
                    existing = df_life[
                        (df_life["lifecycle_event"] == "OUT_OF_STOCK") &
                        (df_life["date"] == zdate)
                    ]
                    if existing.empty:
                        df_life = pd.concat([df_life, pd.DataFrame([{
                            "date": zdate,
                            "lifecycle_event": "OUT_OF_STOCK"
                        }])], ignore_index=True)

                out_dates = df_life[df_life["lifecycle_event"] == "OUT_OF_STOCK"]["date"].tolist()
                restore_dates = df_life[df_life["lifecycle_event"] == "RESTOCK"]["date"].tolist()

                for out_date in out_dates:
                    restore_after = [d for d in restore_dates if d > out_date]
                    if restore_after:
                        restore_date = min(restore_after)
                        mask = (tmp["event_date"] >= out_date) & (tmp["event_date"] < restore_date)
                        tmp.loc[mask, "unit_price"] = None
                    else:
                        mask = tmp["event_date"] >= out_date
                        tmp.loc[mask, "unit_price"] = None
                        
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
            
            tmp["product_url"] = row["product_url"]
    
            timeline_rows.append(
                tmp[["product_url", "product_name", "event_date", "unit_price", "price_status", "price_detail"]]
            )

            # ✅ 이 블록을 추가
            if not df_life.empty:
                lc_tmp = df_life.copy()
                display_name = f"{row['brand']} - {pname}"
                lc_tmp["product_name"] = display_name
                lc_tmp["event_date"] = pd.to_datetime(lc_tmp["date"])
                lc_tmp = lc_tmp[
                    (lc_tmp["event_date"] >= filter_date_from) &
                    (lc_tmp["event_date"] <= filter_date_to)
                ]
                if not lc_tmp.empty:
                    lifecycle_rows.append(
                        lc_tmp[["product_name", "event_date", "lifecycle_event"]]
                    )

            # 🔥 0원 = 품절 → lifecycle에 없어도 OUT_OF_STOCK 이벤트 강제 추가
            zero_price_dates = tmp[tmp["unit_price"].isna() & (tmp["price_detail"] == "품절")]["event_date"].tolist()

            if zero_price_dates and not df_life.empty:
                existing_out_dates = df_life[df_life["lifecycle_event"] == "OUT_OF_STOCK"]["date"].tolist()
                for zdate in zero_price_dates:
                    if zdate not in existing_out_dates:
                        new_row = pd.DataFrame([{
                            "date": zdate,
                            "lifecycle_event": "OUT_OF_STOCK"
                        }])
                        df_life = pd.concat([df_life, new_row], ignore_index=True)
    
        # lifecycle 이벤트
        df_life_all = load_lifecycle_events(row["product_url"])
        df_life = load_lifecycle_events(row["product_url"])
        if not df_life_all.empty:
            df_life_all["date"] = pd.to_datetime(df_life_all["date"], errors="coerce")
            df_life_all = df_life_all.dropna(subset=["date"])
            # 🔥 0원 가격 날짜를 OUT_OF_STOCK으로 추가
            zero_dates = tmp[tmp["unit_price"].isna()]["event_date"].tolist()
            for zdate in zero_dates:
                existing = df_life_all[
                    (df_life_all["lifecycle_event"] == "OUT_OF_STOCK") &
                    (df_life_all["date"] == zdate)
                ]
                if existing.empty:
                    df_life_all = pd.concat([df_life_all, pd.DataFrame([{
                        "date": zdate,
                        "lifecycle_event": "OUT_OF_STOCK"
                    }])], ignore_index=True)
            out_dates = df_life_all[df_life_all["lifecycle_event"] == "OUT_OF_STOCK"]["date"].tolist()
            restore_dates = df_life_all[df_life_all["lifecycle_event"] == "RESTOCK"]["date"].tolist()
            for out_date in out_dates:
                restore_after = [d for d in restore_dates if d > out_date]
                if restore_after:
                    restore_date = min(restore_after)
                    mask = (tmp["event_date"] >= out_date) & (tmp["event_date"] < restore_date)
                    tmp.loc[mask, "unit_price"] = None
                else:
                    mask = tmp["event_date"] >= out_date
                    tmp.loc[mask, "unit_price"] = None
        else:
            # 🔥 df_life_all 없어도 0원 날짜로 선 끊기
            df_life_all = pd.DataFrame(columns=["date", "lifecycle_event"])
            zero_dates = tmp[tmp["unit_price"].isna()]["event_date"].tolist()
            for zdate in zero_dates:
                df_life_all = pd.concat([df_life_all, pd.DataFrame([{
                    "date": zdate,
                    "lifecycle_event": "OUT_OF_STOCK"
                }])], ignore_index=True)
            out_dates = df_life_all[df_life_all["lifecycle_event"] == "OUT_OF_STOCK"]["date"].tolist()
            for out_date in out_dates:
                mask = tmp["event_date"] >= out_date
                tmp.loc[mask, "unit_price"] = None

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
        df_chart = df_timeline.dropna(subset=["unit_price"]).copy()


        # 같은 날짜+가격에서 겹친 점 순번
        df_chart["dup_rank"] = (
            df_chart.groupby(["event_date", "unit_price"])
            .cumcount()
        )

        # 점만 살짝 이동시키기 (0.06일 ≈ 1.44시간)
        df_chart["event_date_jitter"] = (
            df_chart["event_date"] +
            pd.to_timedelta(df_chart["dup_rank"] * 0.06, unit="D")
        )
       
        # =========================
        # 📊 차트와 범례를 분리된 레이아웃으로 표시
        # =========================

        # =========================
        # ✅ 공통 색상 매핑 유틸 (필요하면 파일 상단 유틸에 1회만 선언해도 됨)
        # =========================
        def get_or_create_color_map(keys: list[str]) -> dict[str, str]:
            palette = [
                "#4c78a8","#f58518","#e45756","#72b7b2","#54a24b",
                "#eeca3b","#b279a2","#ff9da6","#9d755d","#bab0ac",
                "#1f77b4","#ff7f0e","#2ca02c","#d62728","#9467bd",
                "#8c564b","#e377c2","#7f7f7f","#bcbd22","#17becf",
            ]
            if "color_map" not in st.session_state:
                st.session_state["color_map"] = {}
            cmap = st.session_state["color_map"]
            for k in sorted(set([str(x) for x in keys if str(x).strip()])):
                if k not in cmap:
                    cmap[k] = palette[len(cmap) % len(palette)]
            st.session_state["color_map"] = cmap
            return cmap
        
        def color_dot(hex_color: str) -> str:
            return (
                f"<span style='"
                f"color:{hex_color};"
                f"font-size:22px;"        # 🔥 점 크기 확대 (16 → 20)
                f"margin-right:12px;"     # 🔥 제품명과 간격 증가
                f"display:inline-block;"
                f"vertical-align:middle;"
                f"'>●</span>"
            )
                
        
        # =========================
        # 📊 차트와 제품목록(색점) 분리 레이아웃
        # =========================
        col_chart, col_legend = st.columns([3, 1])
        
        # ✅ 이 시점 df_chart에는 product_name = "브랜드 - 제품명" display_name이 들어있음
        color_map = get_or_create_color_map(df_chart["product_name"].unique().tolist())
        color_domain = list(color_map.keys())
        color_range = [color_map[k] for k in color_domain]
        
        with col_chart:
            # 🔥 겹침 표시 토글
            show_overlap = st.toggle("겹친 제품 수 표시", value=False, key="toggle_overlap")

            # =========================
            # 📈 가격 선 차트 (범례 없음 + 색 고정)
            # =========================
            # 선 레이어 (원래 날짜 사용)
            base_line = (
                alt.Chart(df_chart)
                .mark_line()
                .encode(
                    x=alt.X("event_date:T", title="날짜", axis=alt.Axis(format="%m/%d")),
                    y=alt.Y("unit_price:Q", title="개당 가격 (원)"),
                    color=alt.Color(
                        "product_name:N",
                        scale=alt.Scale(domain=color_domain, range=color_range),
                        legend=None
                    ),
                    detail="segment:N",
                )
            )
        
            # 🔥 겹친 점 처리
            df_points = (
                df_chart.groupby(["event_date", "unit_price"])
                .agg(
                    product_names=("product_name", lambda x: "\n".join(sorted(set(x)))),
                    price_detail=("price_detail", lambda x: " / ".join(dict.fromkeys(x))),
                    price_status=("price_status", "first"),
                    count=("product_name", "count"),
                    product_name=("product_name", "first"),
                )
                .reset_index()
            )

            df_overlap = df_points[df_points["count"] > 1].copy()
            df_single = df_points[df_points["count"] == 1].copy()

            # 단일 제품 점
            point_single = (
                alt.Chart(df_single)
                .mark_point(size=60, filled=True)
                .encode(
                    x=alt.X("event_date:T"),
                    y=alt.Y("unit_price:Q"),
                    color=alt.Color(
                        "product_name:N",
                        scale=alt.Scale(domain=color_domain, range=color_range),
                        legend=None
                    ),
                    tooltip=[
                        alt.Tooltip("product_names:N", title="제품"),
                        alt.Tooltip("event_date:T", title="날짜", format="%Y-%m-%d"),
                        alt.Tooltip("price_detail:N", title="가격 정보"),
                        alt.Tooltip("price_status:N", title="상태"),
                    ],
                )
            )

            layers = [base_line, point_single]

            # 🔥 겹친 점 - 토글 ON일 때만 추가
            if not df_overlap.empty:
                point_overlap = (
                    alt.Chart(df_overlap)
                    .mark_point(size=120, filled=True, color="red")
                    .encode(
                        x=alt.X("event_date:T"),
                        y=alt.Y("unit_price:Q"),
                        tooltip=[
                            alt.Tooltip("product_names:N", title="제품 (겹침)"),
                            alt.Tooltip("event_date:T", title="날짜", format="%Y-%m-%d"),
                            alt.Tooltip("price_detail:N", title="가격 정보"),
                            alt.Tooltip("count:Q", title="겹친 제품 수"),
                        ],
                    )
                )
                layers.append(point_overlap)

                if show_overlap:
                    text_overlap = (
                        alt.Chart(df_overlap)
                        .mark_text(dy=-10, fontSize=11, fontWeight="bold", color="red")
                        .encode(
                            x=alt.X("event_date:T"),
                            y=alt.Y("unit_price:Q"),
                            text=alt.Text("count:Q"),
                        )
                    )
                    layers.append(text_overlap)
        
            # =========================
            # 🔔 Lifecycle 아이콘 추가
            # =========================
            if lifecycle_rows:
                df_life_all = pd.concat(lifecycle_rows, ignore_index=True)
                
                # 🔥 디버그
                st.write("OUT_OF_STOCK 제거 후:", df_life_all[df_life_all["lifecycle_event"]=="OUT_OF_STOCK"])
                    
                icon_config = {
                    "NEW_PRODUCT": {"color": "green", "label": "NEW"},
                    "OUT_OF_STOCK": {"color": "red", "label": "품절"},
                    "RESTOCK": {"color": "orange", "label": "복원"},
                }
        
                for event_type, cfg in icon_config.items():
                    df_filtered = df_life_all[df_life_all["lifecycle_event"] == event_type].copy()
                    if df_filtered.empty:
                        continue


                    # 🔥 OUT_OF_STOCK: 제품별 구간 첫 날짜만 남기기
                    if event_type == "OUT_OF_STOCK":
                        restock_df = df_life_all[df_life_all["lifecycle_event"] == "RESTOCK"].copy()
                        df_filtered = df_filtered.sort_values(["product_name", "event_date"])
                        kept_rows = []
                
                        for pname, grp in df_filtered.groupby("product_name"):
                            restock_dates = restock_df[restock_df["product_name"] == pname]["event_date"].sort_values().tolist()
                            last_restock = pd.Timestamp.min
                
                            for _, r in grp.iterrows():
                                out_date = r["event_date"]
                                prior_restocks = [d for d in restock_dates if d <= out_date]
                                current_boundary = max(prior_restocks) if prior_restocks else pd.Timestamp.min
                
                                if current_boundary != last_restock:
                                    kept_rows.append(r)
                                    last_restock = current_boundary
                
                        if not kept_rows:
                            continue
                        df_filtered = pd.DataFrame(kept_rows)[["product_name", "event_date", "lifecycle_event"]]

                        # 🔥 디버그 - 여기서 확인
                        st.write("중복 제거 후 df_filtered:", df_filtered)

                    # 가격선 위치 맞추기 위해 join
                    df_filtered = df_filtered.merge(
                        df_timeline[["product_name", "event_date", "unit_price", "price_detail"]],
                        on=["product_name", "event_date"],
                        how="left"
                    )
                
                    # 품절/복원 아이콘은 가격선 위에만 표시되도록 보정
                    if event_type in ["OUT_OF_STOCK", "RESTOCK"]:
                        if event_type == "OUT_OF_STOCK":
                            for idx2, r2 in df_filtered[df_filtered["unit_price"].isna()].iterrows():
                                product_prices = df_timeline[
                                    (df_timeline["product_name"] == r2["product_name"]) &
                                    (df_timeline["event_date"] < r2["event_date"]) &
                                    (df_timeline["unit_price"].notna())
                                ]
                                if not product_prices.empty:
                                    closest = product_prices.nsmallest(1, "event_date").iloc[-1]
                                    df_filtered.at[idx2, "unit_price"] = closest["unit_price"]
                            df_filtered["price_detail"] = "-"
                            df_filtered["price_status"] = "품절"
                
                        elif event_type == "RESTOCK":
                            for idx2, r2 in df_filtered[df_filtered["unit_price"].isna()].iterrows():
                                product_prices = df_timeline[
                                    (df_timeline["product_name"] == r2["product_name"]) &
                                    (df_timeline["event_date"] >= r2["event_date"]) &
                                    (df_timeline["unit_price"].notna())
                                ]
                                if not product_prices.empty:
                                    closest = product_prices.nsmallest(1, "event_date").iloc[0]
                                    df_filtered.at[idx2, "unit_price"] = closest["unit_price"]
                                    df_filtered.at[idx2, "price_detail"] = closest["price_detail"]
                    else:
                        # NEW: 가장 가까운 가격 사용
                        for idx2, r2 in df_filtered[df_filtered["unit_price"].isna()].iterrows():
                            product_prices = df_timeline[
                                (df_timeline["product_name"] == r2["product_name"]) &
                                (df_timeline["unit_price"].notna())
                            ]
                            if not product_prices.empty:
                                pp = product_prices.copy()
                                pp["date_diff"] = (pp["event_date"] - r2["event_date"]).abs().dt.total_seconds()
                                closest = pp.nsmallest(1, "date_diff").iloc[0]
                                df_filtered.at[idx2, "unit_price"] = closest["unit_price"]
                                df_filtered.at[idx2, "price_detail"] = closest["price_detail"]
                
                    df_filtered = df_filtered.dropna(subset=["unit_price"])
                    if df_filtered.empty:
                        continue
                        
                    event_label_map = {
                        "NEW_PRODUCT": "신제품",
                        "OUT_OF_STOCK": "품절",
                        "RESTOCK": "복원",
                    }
                    df_filtered["event_label"] = df_filtered["lifecycle_event"].map(event_label_map).fillna(df_filtered["lifecycle_event"])
        
                    point_layer = (
                        alt.Chart(df_filtered)
                        .mark_point(size=150, shape="triangle-up", color=cfg["color"])
                        .encode(
                            x="event_date:T",
                            y="unit_price:Q",
                            tooltip=[
                                alt.Tooltip("product_name:N", title="제품"),
                                alt.Tooltip("event_date:T", title="날짜", format="%Y-%m-%d"),
                                alt.Tooltip("price_detail:N", title="가격 정보"),
                                alt.Tooltip("event_label:N", title="이벤트"),
                            ],
                        )
                    )
        
                    text_layer = (
                        alt.Chart(df_filtered)
                        .mark_text(dy=12, fontSize=11, fontWeight="bold", color=cfg["color"])
                        .encode(
                            x="event_date:T",
                            y="unit_price:Q",
                            text=alt.value(cfg["label"]),
                        )
                    )
        
                    layers.append(point_layer)
                    layers.append(text_layer)
        
            chart = alt.layer(*layers).properties(height=420).interactive()
            st.altair_chart(chart, use_container_width=True)
        
        with col_legend:
            st.markdown("#### 📋 제품 목록")
        
            # ✅ 차트에 실제로 그려진 제품(product_url)만 목록에 표시
            unique_urls = sorted(df_chart["product_url"].unique())

            for product_url in unique_urls:
                product_row = df_all[df_all["product_url"] == product_url]
                if product_row.empty:
                    continue
        
                row = product_row.iloc[0]
                label = format_product_label(row)
        
                # ✅ 색상키는 반드시 차트의 product_name과 동일한 display_name이어야 함
                display_name = f"{row['brand']} - {row['product_name']}"
                hex_color = color_map.get(display_name, "#999999")
        
                col_btn, col_name = st.columns([1, 10])
        
                with col_btn:
                    # 교체 - URL 정제 후 전달
                    if st.button("×", key=f"remove_product_{product_url}", help="차트에서 제거"):
                        clean_url = str(product_url).strip().replace(r"^_+|_+$", "")
                        # 🔥 정규식 대신 strip 방식 사용
                        clean_url = product_url.strip("_")
                        remove_product_everywhere(clean_url)
                        st.rerun()

                with col_name:
                    html = (
                        f"<div style='display:flex; align-items:center; gap:12px;'>"
                        f"<span style='color:{hex_color}; font-size:22px; line-height:1; flex:0 0 auto;'>●</span>"
                        f"<div style='white-space:normal; word-break:keep-all; overflow-wrap:break-word; line-height:1.4;'>"
                        f"<b>{label}</b>"
                        f"</div>"
                        f"</div>"
                    )
                
                    st.markdown(html, unsafe_allow_html=True)
                    
        # 🔥 엑셀 다운로드 버튼 추가
        with download_placeholder:
            # 엑셀 파일 생성
            from io import BytesIO
            import openpyxl
            from openpyxl.styles import Font, Alignment, PatternFill
            
            # 🔥 데이터 준비 - 브랜드, 카테고리 정보 추가
            excel_data = df_chart[[
                "product_url",
                "product_name",
                "event_date",
                "unit_price",
                "price_status"
            ]].copy()
                        
            # 브랜드, 카테고리 정보 추출 (product_name에서 브랜드 분리)
            excel_data["brand"] = excel_data["product_name"].str.split(" - ").str[0]
            excel_data["product_name_only"] = excel_data["product_name"].str.split(" - ").str[1]
            
            # 원본 데이터프레임에서 카테고리 정보 가져오기
            excel_data["category1"] = ""
            excel_data["category2"] = ""
            
            for idx, row in excel_data.iterrows():
                pname_only = row["product_name_only"]
                original_row = df_all[df_all["product_name"] == pname_only]
                if not original_row.empty:
                    excel_data.at[idx, "category1"] = original_row.iloc[0].get("category1", "")
                    excel_data.at[idx, "category2"] = original_row.iloc[0].get("category2", "")
            
            # 🔥 이벤트 정보 (할인 중 / 정상가)
            excel_data["event"] = excel_data["price_status"].map({
                "💸 할인 중": "할인",
                "정상가": "정상가"
            })

            # 🔥 정상가/할인가 분리 (unit 기준)
            
            excel_data["normal_price"] = None
            excel_data["discount_price"] = None
            excel_data["discount_rate"] = None
            
            for idx, row in excel_data.iterrows():
            
                # 🔥 날짜 문자열 변환 (DB 비교용)
                event_date_str = pd.to_datetime(row["event_date"]).strftime("%Y-%m-%d")
            
                if row["price_status"] == "💸 할인 중":
            
                    # 현재 unit 할인가
                    discount_price = float(row["unit_price"])
                    excel_data.at[idx, "discount_price"] = round(discount_price, 1)
            
                    # ✅ unit 정상가 조회
                    normal_price_res = (
                        supabase.table("raw_daily_prices_unit")
                        .select("unit_normal_price")
                        .eq("product_url", row["product_url"])
                        .eq("date", event_date_str)
                        .limit(1)
                        .execute()
                    )
            
                    if normal_price_res.data:
                        normal_price = float(normal_price_res.data[0]["unit_normal_price"])
                        excel_data.at[idx, "normal_price"] = round(normal_price, 1)
            
                        # 할인율 계산
                        discount_rate = ((normal_price - discount_price) / normal_price) * 100
                        excel_data.at[idx, "discount_rate"] = f"{round(discount_rate, 1)}%"
            
                else:
                    # 정상가 상태일 때는 unit_price 자체가 정상가
                    excel_data.at[idx, "normal_price"] = round(float(row["unit_price"]), 1)
            
            
            # 날짜 형식 변환
            excel_data["event_date"] = pd.to_datetime(excel_data["event_date"]).dt.strftime("%Y-%m-%d")
            
            
            # 최종 컬럼 선택
            excel_data = excel_data[[
                "brand", "category1", "category2", "product_name_only",
                "event_date", "event", "normal_price", "discount_price", "discount_rate"
            ]]
            
            excel_data.columns = [
                "브랜드", "카테고리1", "카테고리2", "제품명",
                "날짜", "이벤트", "정상가", "할인가", "할인율"
            ]
                
            output = BytesIO()
    
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                excel_data.to_excel(writer, sheet_name='가격 데이터', index=False)
    
                workbook = writer.book
                worksheet = writer.sheets['가격 데이터']
    
                # 헤더 스타일
                header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
                header_font = Font(bold=True, color="FFFFFF")
    
                for cell in worksheet[1]:
                    cell.fill = header_fill
                    cell.font = header_font
                    cell.alignment = Alignment(horizontal="center")
    
                # 열 너비 조정
                worksheet.column_dimensions['A'].width = 20
                worksheet.column_dimensions['B'].width = 15
                worksheet.column_dimensions['C'].width = 15
                worksheet.column_dimensions['D'].width = 50
                worksheet.column_dimensions['E'].width = 12
                worksheet.column_dimensions['F'].width = 12
                worksheet.column_dimensions['G'].width = 15
                worksheet.column_dimensions['H'].width = 15
                worksheet.column_dimensions['I'].width = 12
    
                # 🔥 정상가/할인가 소수점 1자리
                for row in worksheet.iter_rows(min_row=2, min_col=7, max_col=8):
                    for cell in row:
                        if cell.value is not None:
                            cell.number_format = '#,##0.0'
    
                # 🔥 할인율 텍스트 형식 (% 문자열 그대로 표시)
                for row in worksheet.iter_rows(min_row=2, min_col=9, max_col=9):
                    for cell in row:
                        cell.number_format = '@'
    
            output.seek(0)
    
            st.download_button(
                label="📥 엑셀 다운로드",
                data=output.getvalue(),
                file_name=f"가격비교_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
    
    else:
        st.info("비교 가능한 이벤트 데이터가 없습니다.")
    
    
    
    # =========================
    # 8-2️⃣ 제품별 카드
    # =========================
    
    for product_url in selected_products:
    
        product_row = df_all[df_all["product_url"] == product_url]
    
        if product_row.empty:
            st.session_state.selected_products.discard(product_url)
            continue
    
        p = product_row.iloc[0]
    
        label = format_product_label(p)
        st.markdown(f"### {label}")
    
        # 🔥 lifecycle 데이터 1회만 로딩
        df_life = load_lifecycle_events(p["product_url"])
        if not df_life.empty:
            df_life["date"] = pd.to_datetime(df_life["date"], errors="coerce")
            df_life = df_life.dropna(subset=["date"])
            df_life = df_life[
                (df_life["date"] >= pd.Timestamp(filter_date_from)) &
                (df_life["date"] <= pd.Timestamp(filter_date_to))
            ]

        
        # 🔥 정상가 변동 조회 (카드용)
        normal_change_res = (
            supabase.table("product_normal_price_events")
            .select("date, prev_price, normal_price, price_diff")
            .eq("product_url", p["product_url"])
            .gte("date", filter_date_from.strftime("%Y-%m-%d"))
            .lte("date", filter_date_to.strftime("%Y-%m-%d"))
            .order("date", desc=True)
            .limit(1)
            .execute()
        )
    
        normal_change_rows = normal_change_res.data if normal_change_res.data else []
    
        
        c1, c2, c3, c4 = st.columns(4)
    
    
        # =========================
        # C1 가격
        # =========================
        with c1:
            normal_value = p.get("normal_unit_price")
        
            if normal_value is not None and pd.notna(normal_value):
        
                if float(normal_value) == 0:
                    st.metric("개당 정상가", "품절", delta="재고 없음")
                else:
                    st.metric(
                        "개당 정상가",
                        f"{float(normal_value):,.1f}원"
                    )
        
            else:
                st.metric("개당 정상가", "-")
        
        cards = []
        # 💸 할인
        discount_res = supabase.rpc(
            "get_discount_periods_in_range",
            {
                "p_product_url": p["product_url"],
                "p_date_from": filter_date_from.strftime("%Y-%m-%d"),
                "p_date_to": filter_date_to.strftime("%Y-%m-%d"),
            }
        ).execute()
        
        discount_rows = discount_res.data if discount_res.data else []
        
        if discount_rows:
            latest_discount = discount_rows[0]
            cards.append(render_card(
                "#e9f3ec",
                "#2f7d32",
                "💸 할인 진행",
                f"시작: {latest_discount['discount_start_date']}<br>"
                f"종료: {latest_discount['discount_end_date']}"
            ))
                        
        # 🆕 신제품
        if not df_life.empty:
            new_events = df_life[df_life["lifecycle_event"] == "NEW_PRODUCT"]
            if not new_events.empty:
                latest_new = new_events.sort_values("date", ascending=False).iloc[0]
                cards.append(render_card(
                    bg="#f6f1e6",
                    border="#c88a00",
                    title="🆕 신제품",
                    content=f"발견일: {latest_new['date'].date()}"
                ))
        
     
        # 🔄 복원 (lifecycle)
        if not df_life.empty:
            restore_events = df_life[df_life["lifecycle_event"] == "RESTOCK"]
            if not restore_events.empty:
                restore_dates_str = "<br>".join([
                    f"날짜: {r['date'].date()}"
                    for _, r in restore_events.sort_values("date", ascending=False).iterrows()
                ])
                cards.append(render_card(
                    bg="#fff8e1",
                    border="#f59e0b",
                    title="🔄 복원",
                    content=restore_dates_str
                ))

        # 🔥 raw_daily_prices에서 품절/복원 보정
        raw_res = (
            supabase.table("raw_daily_prices")
            .select("date, normal_price")
            .eq("product_url", p["product_url"])
            .gte("date", filter_date_from.strftime("%Y-%m-%d"))
            .lte("date", filter_date_to.strftime("%Y-%m-%d"))
            .order("date", desc=False)
            .execute()
        )
        if raw_res.data:
            raw_df = pd.DataFrame(raw_res.data)
            raw_df["normal_price"] = raw_df["normal_price"].astype(float)
            raw_df["date"] = pd.to_datetime(raw_df["date"])

            # 0원 날짜 = 품절 (연속 구간의 첫 날짜만)
            out_rows = raw_df[raw_df["normal_price"] == 0].copy()
            out_rows["prev_normal"] = raw_df["normal_price"].shift(1)
            out_start_rows = out_rows[out_rows["prev_normal"] != 0]  # 이전이 0이 아닌 것 = 품절 시작일
            lifecycle_out_dates = []
            if not df_life.empty:
                lifecycle_out_dates = [
                    str(d.date()) for d in df_life[df_life["lifecycle_event"] == "OUT_OF_STOCK"]["date"].tolist()
                ]
            missing_out = out_start_rows[~out_start_rows["date"].dt.strftime("%Y-%m-%d").isin(lifecycle_out_dates)]

            all_out_dates = lifecycle_out_dates + [
                str(r["date"].date()) for _, r in missing_out.iterrows()
            ]
            all_out_dates = sorted(list(set(all_out_dates)), reverse=True)
            
            if all_out_dates and not any("품절" in c for c in cards):
                out_dates_str = "<br>".join([f"날짜: {d}" for d in all_out_dates])
                cards.append(render_card(
                    bg="#e8f0f8",
                    border="#2c5aa0",
                    title="❌ 품절",
                    content=out_dates_str
                ))

            # 0→양수 전환 날짜 = 복원
            raw_df["prev_price"] = raw_df["normal_price"].shift(1)
            restore_rows = raw_df[(raw_df["prev_price"] == 0) & (raw_df["normal_price"] > 0)]
            if not restore_rows.empty and not any("복원" in c for c in cards):
                restore_dates_str = "<br>".join([f"날짜: {r['date'].date()}" for _, r in restore_rows.iterrows()])
                cards.append(render_card(
                    bg="#fff8e1",
                    border="#f59e0b",
                    title="🔄 복원",
                    content=restore_dates_str
                ))
        
        # 📈 정상가 변동 / 품절 / 복원
        if normal_change_rows:
            latest_change = normal_change_rows[0]
        
            prev_price = float(latest_change["prev_price"])
            current_price = float(latest_change["normal_price"])

            # 🔥 0원 = 품절 (lifecycle에 없는 경우)
            if current_price == 0:
                # 이미 lifecycle에서 품절 카드가 추가된 경우 중복 방지
                already_has_out = any("품절" in c for c in cards)
                if not already_has_out:
                    cards.append(render_card(
                        bg="#e8f0f8",
                        border="#2c5aa0",
                        title="❌ 품절",
                        content=f"날짜: {latest_change['date']}<br>정상가 {prev_price:,.0f}원 → 품절"
                    ))

            # 🔥 이전 0원 → 현재 1원 이상 = 복원 (lifecycle에 없는 경우)
            elif prev_price == 0 and current_price > 0:
                already_has_restore = any("복원" in c for c in cards)
                if not already_has_restore:
                    cards.append(render_card(
                        bg="#fff8e1",
                        border="#f59e0b",
                        title="🔄 복원",
                        content=f"날짜: {latest_change['date']}<br>품절 → 정상가 {current_price:,.0f}원"
                    ))

            else:
                diff = current_price - prev_price
                diff_rate = (diff / prev_price) * 100 if prev_price != 0 else 0
            
                if diff > 0:
                    bg = "#fdecea"
                    border = "#b91c1c"
                    icon = "📈 정상가 상승"
                else:
                    bg = "#eaf2ff"
                    border = "#1d4ed8"
                    icon = "📉 정상가 하락"
            
                cards.append(render_card(
                    bg=bg,
                    border=border,
                    title=icon,
                    content=(
                        f"날짜: {latest_change['date']}<br>"
                        f"{prev_price:,.0f}원 → {current_price:,.0f}원 "
                        f"({diff_rate:+.1f}%)"
                    )
                ))
        
        # 📊 특이 이벤트 없음
        if not cards:
            cards.append(render_card(
                "#f3f4f6",
                "#9aa0a6",
                "📊 특이 이벤트 없음",
                ""
            ))

        # =========================
        # 카드 배치 (3개씩 2줄)
        # =========================
        for row_start in range(0, len(cards), 3):
            row_cards = cards[row_start:row_start + 3]
            _, col1, col2, col3 = st.columns(4)
            for i, col in enumerate([col1, col2, col3]):
                if i < len(row_cards):
                    with col:
                        st.markdown(row_cards[i], unsafe_allow_html=True)
    
        st.markdown("<br><br>", unsafe_allow_html=True)
        # =========================
    
        with st.expander("📅 이벤트 히스토리"):
        
            display_rows = []


            # =========================
            # 2️⃣ 할인 시작 / 종료 이벤트 (가격 포함)
            # =========================
            discount_res = supabase.rpc(
                "get_discount_periods_in_range",
                {
                    "p_product_url": p["product_url"],
                    "p_date_from": filter_date_from.strftime("%Y-%m-%d"),
                    "p_date_to": filter_date_to.strftime("%Y-%m-%d"),
                }
            ).execute()
            
            discount_rows = discount_res.data if discount_res.data else []
            
            # 🔥 품절 날짜 미리 가져오기
            df_life = load_lifecycle_events(p["product_url"])
            out_dates = []
            if not df_life.empty:
                out_dates = pd.to_datetime(
                    df_life[df_life["lifecycle_event"] == "OUT_OF_STOCK"]["date"]
                ).tolist()
            
            for row in discount_rows:
            
                discount_start = pd.to_datetime(row["discount_start_date"])
                discount_end = pd.to_datetime(row["discount_end_date"])
            
                # -------------------------
                # 💸 할인 시작
                # -------------------------
                discount_price_res = (
                    supabase.table("product_all_events")
                    .select("unit_price")
                    .eq("product_url", p["product_url"])
                    .eq("event_type", "DISCOUNT")
                    .eq("date", row["discount_start_date"])
                    .limit(1)
                    .execute()
                )
            
                discount_price = (
                    float(discount_price_res.data[0]["unit_price"])
                    if discount_price_res.data else None
                )
            
                display_rows.append({
                    "날짜": row["discount_start_date"],
                    "이벤트": "💸 할인 시작",
                    "가격 정보": f"할인가 {discount_price:,.1f}원" if discount_price else ""
                })
            
                # -------------------------
                # 🔥 할인 종료 조건 체크
                # -------------------------
                show_discount_end = True
            
                for out_date in out_dates:
                    if discount_end + pd.Timedelta(days=1) == out_date:
                        show_discount_end = False
                        break
            
                if show_discount_end:
            
                    discount_end_price_res = (
                        supabase.table("product_all_events")
                        .select("unit_price")
                        .eq("product_url", p["product_url"])
                        .eq("event_type", "DISCOUNT")
                        .eq("date", row["discount_end_date"])
                        .limit(1)
                        .execute()
                    )
            
                    discount_end_price = (
                        float(discount_end_price_res.data[0]["unit_price"])
                        if discount_end_price_res.data else None
                    )
            
                    display_rows.append({
                        "날짜": row["discount_end_date"],
                        "이벤트": "💸 할인 종료",
                        "가격 정보": f"종료가 {discount_end_price:,.1f}원" if discount_end_price else ""
                    })
  
            # =========================
            # 3️⃣ Lifecycle 이벤트 (히스토리용 보정)
            # =========================
            # 🔥 lifecycle 전체 (기간 필터 없음)
            df_life_all = load_lifecycle_events(p["product_url"])
            if not df_life_all.empty:
                df_life_all["date"] = pd.to_datetime(df_life_all["date"], errors="coerce")
                df_life_all = df_life_all.dropna(subset=["date"])

                # 🔥 df_life = 기간 필터 적용본
                df_life = df_life_all.copy()
                df_life = df_life[
                    (df_life["date"] >= pd.Timestamp(filter_date_from)) &
                    (df_life["date"] <= pd.Timestamp(filter_date_to))
                ]
            else:
                df_life = pd.DataFrame(columns=["date", "lifecycle_event"])

            lifecycle_map = {
                "NEW_PRODUCT": "🆕 신제품",
                "OUT_OF_STOCK": "❌ 품절",
                "RESTOCK": "🔄 복원",
            }

            for _, row in df_life.iterrows():

                event_date = row["date"]
                event_type = row["lifecycle_event"]
                price_info = ""
                if event_type == "OUT_OF_STOCK":
                    price_res = (
                        supabase.table("product_all_events")
                        .select("unit_price, event_type")
                        .eq("product_url", p["product_url"])
                        .lt("date", event_date.strftime("%Y-%m-%d"))
                        .order("date", desc=True)
                        .limit(1)
                        .execute()
                    )
                    if price_res.data:
                        prev = float(price_res.data[0]["unit_price"])
                        etype = price_res.data[0]["event_type"]
                        price_label = "할인가" if etype == "DISCOUNT" else "정상가"
                        price_info = f"{price_label} {prev:,.1f}원 → 품절"
                elif event_type == "RESTOCK":
                    price_res = (
                        supabase.table("product_all_events")
                        .select("unit_price, event_type")
                        .eq("product_url", p["product_url"])
                        .gte("date", event_date.strftime("%Y-%m-%d"))
                        .order("date", desc=False)
                        .limit(1)
                        .execute()
                    )
                    if price_res.data:
                        after = float(price_res.data[0]["unit_price"])
                        etype = price_res.data[0]["event_type"]
                        price_label = "할인가" if etype == "DISCOUNT" else "정상가"
                        price_info = f"품절 → {price_label} {after:,.1f}원"

                display_rows.append({
                    "날짜": event_date.strftime("%Y-%m-%d"),
                    "이벤트": lifecycle_map.get(event_type, ""),
                    "가격 정보": price_info
                })
            
            # =========================
            # 1️⃣ 가격 변동 이벤트
            # =========================
            df_life = load_lifecycle_events(p["product_url"])
    
            if not df_life.empty:
                df_life["date"] = pd.to_datetime(df_life["date"], errors="coerce")
                df_life = df_life.dropna(subset=["date"])
                df_life = df_life[
                    (df_life["date"] >= pd.Timestamp(filter_date_from)) &
                    (df_life["date"] <= pd.Timestamp(filter_date_to))
                ]
            df_changes_res = (
                supabase.table("product_price_change_events")
                .select("*")
                .eq("product_url", p["product_url"])
                .order("date", desc=True)
                .execute()
            )
        
            df_changes = pd.DataFrame(df_changes_res.data)
        
            if not df_changes.empty:
        
                icon_map = {
                    "DISCOUNT_DOWN": "💸 할인가 하락",
                    "DISCOUNT_UP": "💸 할인가 상승",
                    "NORMAL_UP": "📈 정상가 상승",
                    "NORMAL_DOWN": "📉 정상가 하락",
                }

                # 🔥 복원 + 할인 종료 날짜 수집
                restore_dates_in_display = []
                if not df_life_all.empty:
                    restore_dates_in_display = [
                        str(d.date())
                        for d in df_life_all[df_life_all["lifecycle_event"] == "RESTOCK"]["date"].tolist()
                    ]
                discount_end_dates = [
                    r["날짜"] for r in display_rows if r["이벤트"] == "💸 할인 종료"
                ]
                restore_dates_in_display += discount_end_dates

                # 🔥 할인 종료 다음 날도 추가
                discount_end_dates_plus1 = [
                    str((pd.Timestamp(d) + pd.Timedelta(days=1)).date())
                    for d in discount_end_dates
                ]
                restore_dates_in_display += discount_end_dates_plus1

                for _, row in df_changes.iterrows():
        
                    prev_price = float(row["prev_price"]) if row["prev_price"] else 0
                    current_price = float(row["unit_price"]) if row["unit_price"] else 0

                    # 🔥 정상가 0원 = 품절
                    if current_price == 0 and row["price_change_type"] in ("NORMAL_DOWN", "NORMAL_UP"):
                        display_rows.append({
                            "날짜": row["date"],
                            "이벤트": "❌ 품절",
                            "가격 정보": f"정상가 {prev_price:,.1f}원 → 품절"
                        })
                        continue

                    # 🔥 이전 0원 → 현재 1원 이상 = 복원
                    if prev_price == 0 and current_price > 0 and row["price_change_type"] in ("NORMAL_DOWN", "NORMAL_UP"):
                        display_rows.append({
                            "날짜": row["date"],
                            "이벤트": "🔄 복원",
                            "가격 정보": f"품절 → 정상가 {current_price:,.1f}원"
                        })
                        continue

                    # 🔥 복원/할인종료 날짜와 같은 날 NORMAL_UP 스킵
                    if row["price_change_type"] == "NORMAL_UP" and str(row["date"]) in restore_dates_in_display:
                        continue

                    if prev_price > 0:
                        diff = current_price - prev_price
                        diff_rate = (diff / prev_price) * 100
                        rate_text = f"({diff_rate:+.1f}%)"
                    else:
                        rate_text = ""
        
                    display_rows.append({
                        "날짜": row["date"],
                        "이벤트": icon_map.get(row["price_change_type"], ""),
                        "가격 정보": (
                            f"{prev_price:,.1f}원 → "
                            f"{current_price:,.1f}원 "
                            f"{rate_text}"
                        )
                    })

            
            # =========================
            # 정상가 변동 이벤트 추가
            # =========================
            normal_res = (
                supabase.table("product_normal_price_events")
                .select("*")
                .eq("product_url", p["product_url"])
                #.gte("date", filter_date_from.strftime("%Y-%m-%d"))
                #.lte("date", filter_date_to.strftime("%Y-%m-%d"))
                .execute()
            )
            
            normal_rows = normal_res.data if normal_res.data else []

            for row in normal_rows:
                     
                prev_price = float(row["prev_price"])
                current_price = float(row["normal_price"])

                # 🔥 정상가 0원 = 품절
                if current_price == 0:
                    display_rows.append({
                        "날짜": row["date"],
                        "이벤트": "❌ 품절",
                        "가격 정보": f"정상가 {prev_price:,.1f}원 → 품절"
                    })
                    continue

                # 🔥 이전 정상가 0원 → 현재 1원 이상 = 복원
                if prev_price == 0 and current_price > 0:
                    display_rows.append({
                        "날짜": row["date"],
                        "이벤트": "🔄 복원",
                        "가격 정보": f"품절 → 정상가 {current_price:,.1f}원"
                    })
                    continue
            
                diff = current_price - prev_price
                diff_rate = (diff / prev_price) * 100 if prev_price != 0 else 0
            
                event_label = "📈 정상가 상승" if diff > 0 else "📉 정상가 하락"
            
                display_rows.append({
                    "날짜": row["date"],
                    "이벤트": event_label,
                    "가격 정보": (
                        f"{prev_price:,.1f}원 → "
                        f"{current_price:,.1f}원 "
                        f"({diff_rate:+.1f}%)"
                    )
                })

            # =========================
            # 4️⃣ 정렬 + 색상 강조
            # =========================
            if display_rows:
        
                df_display = pd.DataFrame(display_rows)
        
                df_display["날짜_정렬용"] = pd.to_datetime(
                    df_display["날짜"], errors="coerce"
                )
        
                df_display = df_display.sort_values("날짜_정렬용", ascending=False)
                df_display = df_display.drop(columns=["날짜_정렬용"])
        
                st.dataframe(
                    df_display,
                    use_container_width=True,
                    hide_index=True
                )
        
            else:
                st.caption("이벤트 없음")









