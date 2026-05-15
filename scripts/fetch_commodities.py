"""
fetch_commodities.py
コモディティ価格を yfinance で取得し、docs/data_commodities.json に出力する。
"""

import json
import os
from datetime import datetime, timezone

import yfinance as yf
import pandas as pd

SYMBOLS = {
    "GSCI":         "^SPGSCI",
    "原油 (WTI)":   "CL=F",
    "天然ガス":     "NG=F",
    "金":           "GC=F",
    "プラチナ":     "PL=F",
    "銀":           "SI=F",
    "銅":           "HG=F",
    "トウモロコシ": "ZC=F",
    "小麦":         "ZW=F",
    "大豆":         "ZS=F",
}

START_DATE = "2000-01-01"
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "docs", "data_commodities.json")


def main():
    print(f"[{datetime.now()}] データ取得開始")

    raw = yf.download(
        list(SYMBOLS.values()),
        start=START_DATE,
        interval="1d",
        auto_adjust=True,
        progress=False,
    )

    # 単一銘柄のときも MultiIndex になるよう統一
    if isinstance(raw.columns, pd.MultiIndex):
        close_df = raw["Close"].rename(columns={v: k for k, v in SYMBOLS.items()})
    else:
        ticker = list(SYMBOLS.values())[0]
        name   = list(SYMBOLS.keys())[0]
        close_df = raw[["Close"]].rename(columns={"Close": name})

    close_df = close_df.dropna(how="all")
    close_df.index = pd.to_datetime(close_df.index)

    result = {}
    for col in close_df.columns:
        series = close_df[col].dropna()
        result[col] = [
            [row.strftime("%Y-%m-%d"), round(float(val), 4)]
            for row, val in zip(series.index, series.values)
        ]

    payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "data": result,
    }

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))

    print(f"[{datetime.now()}] 書き込み完了 → {OUTPUT_PATH}")
    for k, v in result.items():
        print(f"  {k}: {len(v)} 件 (最新: {v[-1] if v else 'なし'})")


if __name__ == "__main__":
    main()