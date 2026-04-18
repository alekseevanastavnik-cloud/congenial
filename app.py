import re
import time
from datetime import datetime

import pandas as pd
import requests
import streamlit as st
from langdetect import DetectorFactory, detect

st.set_page_config(page_title="Duolingo Reviews Scraper", layout="wide")

DetectorFactory.seed = 0

DEFAULT_APP_ID = "570060128"
DEFAULT_COUNTRIES = ["us", "de", "ua", "kz", "gb", "fr", "it", "es", "ca", "au"]


def normalize_text(text):
    if text is None:
        return ""
    text = str(text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{2,}", "\n", text)
    return text.strip()


def is_russian(text):
    text = normalize_text(text)
    if not text:
        return False

    if not re.search(r"[А-Яа-яЁё]", text):
        return False

    short_text = re.sub(r"[^А-Яа-яЁёA-Za-z ]+", " ", text).strip()
    if len(short_text) < 12:
        return True

    try:
        return detect(text) == "ru"
   except LangDetectException:
    return False


def build_review_url(country, app_id, page):
    return (
        f"https://itunes.apple.com/{country}/rss/customerreviews/"
        f"page={page}/id={app_id}/sortBy=mostRecent/json"
    )


def fetch_page(country, app_id, page):
    url = build_review_url(country, app_id, page)
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*",
    }

    try:
        response = requests.get(url, headers=headers, timeout=20)
        if response.status_code != 200:
            return None, url

        data = response.json()
        if isinstance(data, dict) and "feed" in data:
            return data, url
    except Exception:
        return None, url

    return None, url


def safe_get(d, *keys):
    cur = d
    for key in keys:
        if isinstance(cur, dict) and key in cur:
            cur = cur[key]
        else:
            return None
    return cur


def parse_entries(data, country, page, source_url):
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
        review_id = safe_get(entry, "id", "label")

        if rating is None or author is None or updated is None:
            continue

        rows.append(
            {
                "review_id": "" if review_id is None else str(review_id).strip(),
                "review_date": updated,
                "user_name": str(author).strip(),
                "rating": str(rating).strip(),
                "app_version": "" if version is None else str(version).strip(),
                "review_text": "" if content is None else normalize_text(content),
                "storefront": country,
                "page": page,
                "source_url": source_url,
            }
        )

    return rows


@st.cache_data(show_spinner=False)
def collect_reviews(app_id, target_year, countries, max_pages):
    start_date = pd.Timestamp(f"{target_year}-01-01 00:00:00", tz="UTC")
    end_date = pd.Timestamp(f"{target_year}-12-31 23:59:59", tz="UTC")

    all_rows = []

    for country in countries:
        for page in range(1, max_pages + 1):
            data, source_url = fetch_page(country, app_id, page)

            if not data:
                break

            rows = parse_entries(data, country, page, source_url)

            if not rows:
                break

            all_rows.extend(rows)
            time.sleep(0.2)

    raw_df = pd.DataFrame(all_rows)

    if raw_df.empty:
        return raw_df, raw_df

    df = raw_df.copy()
    df["review_date"] = pd.to_datetime(df["review_date"], errors="coerce", utc=True)
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    df["review_text"] = df["review_text"].fillna("").astype(str).map(normalize_text)
    df["user_name"] = df["user_name"].fillna("").astype(str).str.strip()
    df["app_version"] = df["app_version"].fillna("").astype(str).str.strip()

    df = df[df["review_text"].str.len() > 0].copy()
    df = df[(df["review_date"] >= start_date) & (df["review_date"] <= end_date)].copy()
    df = df[df["review_text"].map(is_russian)].copy()

    df = df[
        ["review_date", "user_name", "rating", "app_version", "review_text"]
    ].drop_duplicates()

    df = df.sort_values("review_date", ascending=False).reset_index(drop=True)
    df["review_date"] = df["review_date"].dt.strftime("%Y-%m-%d %H:%M:%S%z")

    return raw_df, df


@st.cache_data(show_spinner=False)
def convert_df_to_csv(df):
    return df.to_csv(index=False, sep=";", encoding="utf-8-sig").encode("utf-8-sig")


st.title("Duolingo App Store Reviews Scraper")
st.write("Сбор русских отзывов Duolingo из App Store в CSV.")

with st.sidebar:
    st.header("Параметры")
    app_id = st.text_input("App ID", value=DEFAULT_APP_ID)
    current_year = datetime.now().year
    target_year = st.number_input(
        "Год отзывов",
        min_value=2008,
        max_value=current_year,
        value=current_year - 1,
        step=1,
    )
    max_pages = st.slider("Максимум страниц на storefront", min_value=1, max_value=10, value=10)
    countries = st.multiselect(
        "Storefront страны",
        DEFAULT_COUNTRIES,
        default=["us", "de", "ua", "kz"],
    )
    run_btn = st.button("Собрать отзывы", type="primary")

if run_btn:
    if not app_id.strip():
        st.error("Укажи App ID.")
    elif not countries:
        st.error("Выбери хотя бы одну страну storefront.")
    else:
        with st.spinner("Собираю отзывы..."):
            raw_df, clean_df = collect_reviews(
                app_id.strip(),
                int(target_year),
                tuple(countries),
                int(max_pages),
            )

        if raw_df.empty:
            st.warning("Не удалось получить отзывы. Попробуй другие storefront или меньше страниц.")
        else:
            c1, c2 = st.columns(2)
            c1.metric("Сырые строки", len(raw_df))
            c2.metric("Итоговые русские отзывы", len(clean_df))

            st.subheader("Очищенные отзывы")
            st.dataframe(clean_df, use_container_width=True, height=500)

            st.download_button(
                label="Скачать очищенный CSV",
                data=convert_df_to_csv(clean_df),
                file_name=f"duolingo_reviews_ru_{target_year}.csv",
                mime="text/csv",
            )

            with st.expander("Raw данные"):
                st.dataframe(raw_df, use_container_width=True, height=300)

            st.download_button(
                label="Скачать raw CSV",
                data=convert_df_to_csv(raw_df),
                file_name=f"duolingo_reviews_raw_{target_year}.csv",
                mime="text/csv",
            )

   
        


       
              

  
  
