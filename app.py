import pandas as pd
import requests
import streamlit as st

st.set_page_config(page_title="Duolingo Reviews", layout="wide")

APP_ID = "570060128"
COUNTRIES = ["us", "de", "ua", "kz"]
MAX_PAGES = 10


def fetch_reviews(app_id, countries, max_pages):
    rows = []

    for country in countries:
        for page in range(1, max_pages + 1):
            url = f"https://itunes.apple.com/{country}/rss/customerreviews/page={page}/id={app_id}/sortBy=mostRecent/json"

            try:
                response = requests.get(url, timeout=20)
                if response.status_code != 200:
                    break

                data = response.json()
                feed = data.get("feed", {})
                entries = feed.get("entry", [])

                if isinstance(entries, dict):
                    entries = [entries]

                if not entries:
                    break

                for entry in entries:
                    if "im:rating" not in entry:
                        continue

                    rows.append(
                        {
                            "review_date": entry.get("updated", {}).get("label", ""),
                            "user_name": entry.get("author", {}).get("name", {}).get("label", ""),
                            "rating": entry.get("im:rating", {}).get("label", ""),
                            "app_version": entry.get("im:version", {}).get("label", ""),
                            "review_text": entry.get("content", {}).get("label", ""),
                            "storefront": country,
                            "page": page,
                        }
                    )
            except Exception:
                break

    return pd.DataFrame(rows)


@st.cache_data
def convert_df(df):
    return df.to_csv(index=False, sep=";", encoding="utf-8-sig").encode("utf-8-sig")


st.title("Duolingo App Store Reviews")
st.write("Простая выгрузка отзывов Duolingo из App Store")

if st.button("Собрать отзывы"):
    df = fetch_reviews(APP_ID, COUNTRIES, MAX_PAGES)

    if df.empty:
        st.warning("Отзывы не найдены")
    else:
        st.success(f"Найдено строк: {len(df)}")
        st.dataframe(df, use_container_width=True)

        csv = convert_df(df)
        st.download_button(
            label="Скачать CSV",
            data=csv,
            file_name="duolingo_reviews.csv",
            mime="text/csv",
        )
