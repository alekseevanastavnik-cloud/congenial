import pandas as pd
import requests
import streamlit as st

st.set_page_config(page_title="Duolingo Reviews", layout="wide")
st.title("Duolingo App Store Reviews")
st.write("Тестовый сбор отзывов Duolingo из App Store")

APP_ID = "570060128"
COUNTRY = "us"
URL = f"https://itunes.apple.com/{COUNTRY}/rss/customerreviews/page=1/id={APP_ID}/sortby=mostrecent/json"

response = requests.get(URL, timeout=20)
data = response.json()

rows = []
feed = data.get("feed", {})
entries = feed.get("entry", [])

if isinstance(entries, dict):
    entries = [entries]

for entry in entries:
    rating = entry.get("im:rating", {}).get("label", "")
    author = entry.get("author", {}).get("name", {}).get("label", "")
    updated = entry.get("updated", {}).get("label", "")
    version = entry.get("im:version", {}).get("label", "")
    content = entry.get("content", {}).get("label", "")

    if rating and author and updated:
        rows.append(
            {
                "review_date": updated,
                "user_name": author,
                "rating": rating,
                "app_version": version,
                "review_text": content,
            }
        )

df = pd.DataFrame(rows)

st.write(f"Найдено строк: {len(df)}")
st.dataframe(df, use_container_width=True)

csv_data = df.to_csv(index=False).encode("utf-8-sig")
st.download_button(
    "Скачать CSV",
    data=csv_data,
    file_name="duolingo_reviews_test.csv",
    mime="text/csv",
)
  
    
           
