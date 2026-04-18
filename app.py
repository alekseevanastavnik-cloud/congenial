import re
import time
import requests
import pandas as pd
import streamlit as st
from datetime import datetime
from langdetect import detect, DetectorFactory
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DetectorFactory.seed = 0

st.set_page_config(page_title="App Store Reviews Scraper", layout="wide")
st.title("App Store Reviews Scraper")
st.write("Сбор отзывов App Store в CSV с фильтром по русскому языку, году и удалением дублей.")

DEFAULT_COUNTRIES = [
    "us", "de", "fr", "it", "es", "gb", "ca", "au",
    "ua", "kz", "pl", "nl", "se", "no", "fi", "at", "ch",
    "cz", "sk", "tr", "ee", "lv", "lt", "il", "md", "am",
    "az", "ge", "kg", "uz"
]

def create_session():
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*"
    })
    return session

def build_candidate_urls(country: str, app_id: str, page: int):
    return [
        f"https://itunes.apple.com/{country}/rss/customerreviews/page={page}/id={app_id}/sortBy=mostRecent/json",
        f"https://itunes.apple.com/{country}/rss/customerreviews/page={page}/id={app_id}/sortby=mostrecent/json",
        f"https://itunes.apple.com/{country}/rss/customerreviews/id={app_id}/sortBy=mostRecent/page={page}/json",
        f"https://itunes.apple.com/{country}/rss/customerreviews/id={app_id}/sortby=mostrecent/page={page}/json",
        f"https://itunes.apple.com/{country}/rss/customerreviews/id={app_id}/sortBy=mostRecent/json",
        f"https://itunes.apple.com/{country}/rss/customerreviews/id={app_id}/sortby=mostrecent/json",
    ]

def fetch_json(session, country: str, app_id: str, page: int, timeout: int = 20):
    for url in build_candidate_urls(country, app_id, page):
        try:
            r = session.get(url, timeout=timeout, allow_redirects=True)

            if r.status_code != 200:
                continue

            try:
                data = r.json()
            except Exception:
                continue

            if isinstance(data, dict) and "feed" in 
                return data, url, r.status_code

        except Exception:
            continue

    return None, None, None

def safe_get(d, *keys, default=None):
    cur = d
    for key in keys:
        if isinstance(cur, dict) and key in cur:
            cur = cur[key]
        else:
            return default
    return cur

def normalize_text(text):
    if text is None:
        return ""
    text = str(text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{2,}", "\n", text)
    return text.strip()

def looks_russian(text: str) -> bool:
    text = normalize_text(text)
    if not text:
        return False

    if not re.search(r"[А-Яа-яЁё]", text):
        return False

    compact = re.sub(r"[^А-Яа-яЁёA-Za-z ]+", " ", text).strip()
    if len(compact) < 12:
        return True

    try:
        return detect(text) == "ru"
    except Exception:
        return False

def parse_review_entries(data, country, page, source_url):
    feed = data.get("feed", {})
    entries = feed.get("entry", [])

    if isinstance(entries, dict):
        entries = [entries]

    rows = []

    for entry in entries:
        rating = safe_get(entry, "im:rating", "label")
        author = safe_get(entry, "author", "name", "label")
        updated = safe_get(entry, "updated", "label")
        version = safe_get(entry, "im:version", "label")
        content = safe_get(entry, "content", "label")
        title = safe_get(entry, "title", "label")
        review_id = safe_get(entry, "id", "label")

        if rating is None or author is None or updated is None:
            continue

        rows.append({
            "review_id": "" if review_id is None else str(review_id).strip(),
            "review_date": updated,
            "user_name": "" if author is None else str(author).strip(),
            "rating": "" if rating is None else str(rating).strip(),
            "app_version": "" if version is None else str(version).strip(),
            "review_title": "" if title is None else normalize_text(title),
            "review_text": "" if content is None else normalize_text(content),
            "storefront": country,
            "page": page,
            "source_url": source_url
        })

    return rows

@st.cache_data(show_spinner=False)
def collect_reviews(app_id: str, target_year: int, countries: tuple, max_pages: int):
    session = create_session()

    start_date = pd.Timestamp(f"{target_year}-01-01 00:00:00", tz="UTC")
    end_date = pd.Timestamp(f"{target_year}-12-31 23:59:59", tz="UTC")

    all_rows = []
    request_logs = []

    for country in countries:
        for page in range(1, max_pages + 1):
            data, used_url, status = fetch_json(session, country, app_id, page)

            request_logs.append({
                "country": country,
                "page": page,
                "status_code": status,
                "success": data is not None,
                "used_url": used_url
            })

            if not 
                break

            rows = parse_review_entries(data, country, page, used_url)

            if not rows:
                break

            all_rows.extend(rows)
            time.sleep(0.2)

    raw_df = pd.DataFrame(all_rows)
    log_df = pd.DataFrame(request_logs)

    if raw_df.empty:
        return raw_df, raw_df, log_df

    df = raw_df.copy()
    df["review_date"] = pd.to_datetime(df["review_date"], errors="coerce", utc=True)
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce").astype("Int64")
    df["user_name"] = df["user_name"].fillna("").astype(str).str.strip()
    df["app_version"] = df["app_version"].fillna("").astype(str).str.strip()
    df["review_text"] = df["review_text"].fillna("").astype(str).map(normalize_text)
    df["review_title"] = df["review_title"].fillna("").astype(str).map(normalize_text)

    df = df[df["review_text"].str.len() > 0].copy()
    df = df[(df["review_date"] >= start_date) & (df["review_date"] <= end_date)].copy()

    df["is_russian"] = df["review_text"].map(looks_russian)
    df = df[df["is_russian"]].copy()

    df = df[["review_date", "user_name", "rating", "app_version", "review_text"]].copy()
    df = df.drop_duplicates(keep="first").copy()
    df = df.sort_values("review_date", ascending=False).reset_index(drop=True)
    df["review_date"] = df["review_date"].dt.strftime("%Y-%m-%d %H:%M:%S%z")

    return raw_df, df, log_df

@st.cache_data(show_spinner=False)
def convert_df_to_csv(df: pd.DataFrame):
    return df.to_csv(index=False, sep=";", encoding="utf-8-sig").encode("utf-8-sig")

with st.sidebar:
    st.header("Параметры")
    app_id = st.text_input("App ID", value="570060128")
    current_year = datetime.now().year
    target_year = st.number_input("Год отзывов", min_value=2008, max_value=current_year, value=current_year - 1, step=1)
    max_pages = st.slider("Макс. страниц на storefront", min_value=1, max_value=10, value=10)
    countries = st.multiselect("Storefront страны", DEFAULT_COUNTRIES, default=["us", "de", "ua", "kz"])
    run_btn = st.button("Собрать отзывы", type="primary")

if run_btn:
    if not app_id.strip():
        st.error("Укажи app id.")
    elif not countries:
        st.error("Выбери хотя бы один storefront.")
    else:
        with st.spinner("Собираю отзывы..."):
            raw_df, clean_df, log_df = collect_reviews(
                app_id=app_id.strip(),
                target_year=int(target_year),
                countries=tuple(countries),
                max_pages=int(max_pages)
            )

        if raw_df.empty:
            st.warning("Не удалось получить данные. Попробуй другие storefront или меньшее число страниц.")
        else:
            c1, c2, c3 = st.columns(3)
            c1.metric("Сырые строки", len(raw_df))
            c2.metric("Итоговые отзывы", len(clean_df))
            c3.metric("Storefront", len(countries))

            st.subheader("Очищенные данные")
            st.dataframe(clean_df, use_container_width=True, height=500)

            csv_clean = convert_df_to_csv(clean_df)
            st.download_button(
                label="Скачать очищенный CSV",
                data=csv_clean,
                file_name=f"duolingo_reviews_ru_{target_year}.csv",
                mime="text/csv"
            )

            with st.expander("Сырые данные"):
                st.dataframe(raw_df, use_container_width=True, height=300)

            csv_raw = convert_df_to_csv(raw_df)
            st.download_button(
                label="Скачать raw CSV",
                data=csv_raw,
                file_name=f"duolingo_reviews_raw_{target_year}.csv",
                mime="text/csv"
            )

            with st.expander("Лог запросов"):
                st.dataframe(log_df, use_container_width=True, height=300)
