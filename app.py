import re
import time
from datetime import datetime

import pandas as pd
import requests
import streamlit as st
from langdetect import DetectorFactory, detect
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
        allowed_methods=["GET"],
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json,text/plain,*/*",
        }
    )
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

        rows.append(
            {
                "review_id": "" if review_id is None else str(review_id).strip(),
                "review_date": updated,
                "user_name": "" if author is None else str(author).strip(),
                "rating": "" if rating is None else str(rating).strip(),
                "app_version": "" if version is None else str(version).stri

