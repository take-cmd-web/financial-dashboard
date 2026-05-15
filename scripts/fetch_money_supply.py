"""
fetch_money_supply.py
TradingView から各国マネーサプライ（M2）を月次で取得し
docs/data_money_supply.json に差分追記する。

・既存データの最終日付以降のみ取得（データ制限対策）
・全銘柄ダウンロード失敗でも他銘柄に影響しない
・出力形式: { "meta": {...}, "series": { ticker: [[date, value], ...] } }
"""

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from tvDatafeed import TvDatafeed, Interval

# ── 設定 ────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent
REPO_ROOT   = SCRIPT_DIR.parent
TICKER_CSV  = SCRIPT_DIR / "tickers_money_supply.csv"
OUTPUT_JSON = REPO_ROOT / "docs" / "data_money_supply.json"

TV_USERNAME = os.environ.get("TV_USERNAME", "")
TV_PASSWORD = os.environ.get("TV_PASSWORD", "")

# TradingView の M2 指標は ECONOMICS セクションに存在
EXCHANGE    = "ECONOMICS"
INTERVAL    = Interval.in_monthly          # 月次
N_BARS      = 300                          # 1回あたりの最大取得バー数（約25年分）
START_DATE  = "2008-01-01"                 # 取得開始日
SLEEP_SEC   = 2                            # 銘柄間のウェイト（レート制限対策）
# ────────────────────────────────────────────────────────


def load_existing(path: Path) -> dict:
    """既存 JSON を読み込む。存在しなければ空構造を返す。"""
    if path.exists():
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return {"meta": {}, "series": {}}


def last_date(records: list) -> str | None:
    """[[date, value], ...] の最終日付文字列を返す。"""
    if not records:
        return None
    return records[-1][0]  # ソート済み前提


def fetch_series(tv: TvDatafeed, ticker: str, n_bars: int) -> pd.DataFrame | None:
    """TvDatafeed でデータ取得。失敗時は None を返す。"""
    try:
        df = tv.get_hist(
            symbol=ticker,
            exchange=EXCHANGE,
            interval=INTERVAL,
            n_bars=n_bars,
        )
        return df
    except Exception as e:
        print(f"  [WARN] {ticker}: 取得失敗 — {e}", file=sys.stderr)
        return None


def df_to_records(df: pd.DataFrame, since: str | None) -> list:
    """DataFrame を [[date_str, value], ...] に変換し、since 以降のみ返す。"""
    if df is None or df.empty:
        return []

    # close 列を使用（M2 系指標は close に値が入る）
    col = "close" if "close" in df.columns else df.columns[-1]
    df = df[[col]].copy()
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()

    # 月初に正規化（YYYY-MM-01）
    df.index = df.index.to_period("M").to_timestamp()
    df = df[~df.index.duplicated(keep="last")]

    # START_DATE フィルタ
    df = df[df.index >= pd.Timestamp(START_DATE)]

    # 差分フィルタ：既存最終日付より後のみ
    if since:
        df = df[df.index > pd.Timestamp(since)]

    records = [
        [row.strftime("%Y-%m-%d"), round(float(val), 4)]
        for row, val in zip(df.index, df[col])
        if pd.notna(val)
    ]
    return records


def main():
    # ── TvDatafeed 認証 ──
    if TV_USERNAME and TV_PASSWORD:
        tv = TvDatafeed(TV_USERNAME, TV_PASSWORD)
        print("TvDatafeed: 認証ログイン")
    else:
        tv = TvDatafeed()
        print("TvDatafeed: 匿名ログイン（データ制限あり）")

    # ── ティッカー CSV 読み込み ──
    tickers_df = pd.read_csv(TICKER_CSV)
    tickers = tickers_df["ticker"].tolist()
    labels  = dict(zip(tickers_df["ticker"], tickers_df["label"]))

    # ── 既存データ読み込み ──
    existing = load_existing(OUTPUT_JSON)
    series   = existing.get("series", {})

    updated = 0
    skipped = 0

    for ticker in tickers:
        label   = labels.get(ticker, ticker)
        current = series.get(ticker, [])
        since   = last_date(current)

        if since:
            print(f"  {ticker} ({label}): {since} 以降を取得中…")
        else:
            print(f"  {ticker} ({label}): 全件取得中（{START_DATE}〜）…")

        df      = fetch_series(tv, ticker, N_BARS)
        new_rec = df_to_records(df, since)

        if new_rec:
            series[ticker] = current + new_rec
            print(f"    → {len(new_rec)} 件追加（累計 {len(series[ticker])} 件）")
            updated += 1
        else:
            print(f"    → 新規データなし（スキップ）")
            skipped += 1
            # 既存データがなければ空配列で初期化
            if ticker not in series:
                series[ticker] = []

        time.sleep(SLEEP_SEC)

    # ── メタ情報更新 ──
    now_jst = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    existing["meta"] = {
        "generated_at": now_jst,
        "source": "TradingView (ECONOMICS)",
        "interval": "monthly",
        "start_date": START_DATE,
        "tickers": {
            t: {"label": labels.get(t, t), "currency": row["currency"]}
            for _, row in tickers_df.iterrows()
            for t in [row["ticker"]]
        },
    }
    existing["series"] = series

    # ── JSON 書き出し ──
    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(existing, f, ensure_ascii=False, separators=(",", ":"))

    print(f"\n完了: 更新 {updated} 件 / スキップ {skipped} 件")
    print(f"出力: {OUTPUT_JSON}")


if __name__ == "__main__":
    main()
