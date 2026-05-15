"""
fetch_gscpi.py
NY連銀 グローバルサプライチェーン圧力指数（GSCPI）を取得し、
docs/data_gscpi.json に出力する。

データソース：
  https://www.newyorkfed.org/medialibrary/research/interactives/gscpi/downloads/gscpi_data.xlsx
  シート：GSCPI Monthly Data
  列：A=日付、B=値（6行目からデータ）
"""

import json
import os
import io
from datetime import datetime, timezone

import requests
import pandas as pd

URL = "https://www.newyorkfed.org/medialibrary/research/interactives/gscpi/downloads/gscpi_data.xlsx"
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "docs", "data_gscpi.json")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*",
}


def read_excel_bytes(content: bytes) -> pd.DataFrame:
    """xlsx → openpyxl、失敗したら xlrd（.xls）でリトライ"""
    buf = io.BytesIO(content)
    try:
        return pd.read_excel(
            buf,
            sheet_name="GSCPI Monthly Data",
            header=None,
            skiprows=5,
            usecols=[0, 1],
            engine="openpyxl",
        )
    except Exception:
        buf.seek(0)
        return pd.read_excel(
            buf,
            sheet_name="GSCPI Monthly Data",
            header=None,
            skiprows=5,
            usecols=[0, 1],
            engine="xlrd",
        )


def main():
    print(f"[{datetime.now()}] GSCPI データ取得開始")

    resp = requests.get(URL, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    print(f"  HTTP {resp.status_code}, {len(resp.content):,} bytes, "
          f"Content-Type: {resp.headers.get('content-type','?')}")

    df = read_excel_bytes(resp.content)
    df.columns = ["date", "value"]
    df = df.dropna(subset=["date", "value"])
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df = df.sort_values("date")

    series = [
        [row["date"].strftime("%Y-%m-%d"), round(float(row["value"]), 6)]
        for _, row in df.iterrows()
    ]

    payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "data": {"GSCPI": series},
    }

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))

    print(f"[{datetime.now()}] 書き込み完了 → {OUTPUT_PATH}")
    print(f"  GSCPI: {len(series)} 件 (最新: {series[-1] if series else 'なし'})")


if __name__ == "__main__":
    main()