"""
fetch_money_supply.py
TradingView から各国マネーサプライ（M2）を月次で取得し
docs/data_money_supply.json に差分追記する。

・既存データの最終日付以降のみ取得（データ制限対策）
・過去6ヶ月分は常に再取得して上書き
・yfinance で月次 FX レートを取得して meta.fx_rates に保存
・出力形式: { "meta": {...}, "series": { ticker: [[date, value], ...] } }
"""

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf
from tvDatafeed import TvDatafeed, Interval

# ── 設定 ────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent
REPO_ROOT   = SCRIPT_DIR.parent
TICKER_CSV  = SCRIPT_DIR / "tickers_money_supply.csv"
OUTPUT_JSON = REPO_ROOT / "docs" / "data_money_supply.json"

TV_USERNAME = os.environ.get("TV_USERNAME", "")
TV_PASSWORD = os.environ.get("TV_PASSWORD", "")

EXCHANGE   = "ECONOMICS"
INTERVAL   = Interval.in_monthly
N_BARS     = 300
START_DATE = "2008-01-01"
SLEEP_SEC  = 2

# 各通貨の yfinance FX ティッカー（対USD）
FX_TICKERS = {
    "CNY": "CNYUSD=X",
    "EUR": "EURUSD=X",
    "JPY": "JPYUSD=X",
    "GBP": "GBPUSD=X",
    "CAD": "CADUSD=X",
    "AUD": "AUDUSD=X",
    "KRW": "KRWUSD=X",
    "RUB": "RUBUSD=X",
    "BRL": "BRLUSD=X",
}

# フォールバック固定レート（yfinance 失敗時）
FX_FALLBACK = {
    "USD": 1.0,
    "CNY": 0.1385,
    "EUR": 1.105,
    "JPY": 0.00667,
    "GBP": 1.27,
    "CAD": 0.738,
    "AUD": 0.645,
    "KRW": 0.000735,
    "RUB": 0.01095,
    "BRL": 0.172,
}
# ────────────────────────────────────────────────────────


def load_existing(path: Path) -> dict:
    if path.exists():
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return {"meta": {}, "series": {}}


def last_date(records: list) -> str | None:
    if not records:
        return None
    return records[-1][0]


def fetch_fx_rates(start: str) -> dict:
    """yfinance で各通貨の月次 FX レート（対USD）を取得。
    戻り値: { "YYYY-MM": { currency: rate, ... }, ... }
    """
    print("\n── FX レート取得中 ──")
    monthly_rates = {}  # { "YYYY-MM": { currency: rate } }

    for currency, fx_ticker in FX_TICKERS.items():
        try:
            df = yf.download(
                fx_ticker,
                start=start,
                interval="1mo",
                auto_adjust=True,
                progress=False,
            )
            if df.empty:
                raise ValueError("empty")

            # Close 列を取得
            if isinstance(df.columns, pd.MultiIndex):
                close = df["Close"].iloc[:, 0]
            else:
                close = df["Close"]

            close.index = pd.to_datetime(close.index).to_period("M").to_timestamp()

            for dt, rate in zip(close.index, close.values):
                if pd.isna(rate):
                    continue
                key = dt.strftime("%Y-%m")
                if key not in monthly_rates:
                    monthly_rates[key] = {"USD": 1.0}
                monthly_rates[key][currency] = round(float(rate), 8)

            print(f"  {currency} ({fx_ticker}): {len(close)} ヶ月分取得")
        except Exception as e:
            print(f"  [WARN] {currency}: FX取得失敗 — {e}", file=sys.stderr)

    # 最新月の単一レート辞書も作成（HTMLフォールバック用）
    latest_month = max(monthly_rates.keys()) if monthly_rates else None
    latest_rates = {"USD": 1.0}
    if latest_month:
        latest_rates.update(monthly_rates[latest_month])
    # フォールバックで埋める
    for cur, rate in FX_FALLBACK.items():
        if cur not in latest_rates:
            latest_rates[cur] = rate

    print(f"  最新レート月: {latest_month}")
    return monthly_rates, latest_rates


def fetch_series(tv: TvDatafeed, ticker: str, n_bars: int) -> pd.DataFrame | None:
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
    if df is None or df.empty:
        return []

    col = "close" if "close" in df.columns else df.columns[-1]
    df = df[[col]].copy()
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    df.index = df.index.to_period("M").to_timestamp()
    df = df[~df.index.duplicated(keep="last")]
    df = df[df.index >= pd.Timestamp(START_DATE)]

    if since:
        six_months_ago = pd.Timestamp(since) - pd.DateOffset(months=6)
        df = df[df.index > six_months_ago]

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
    currencies = dict(zip(tickers_df["ticker"], tickers_df["currency"]))

    # ── 既存データ読み込み ──
    existing = load_existing(OUTPUT_JSON)
    series   = existing.get("series", {})

    # ── FX レート取得 ──
    monthly_fx, latest_fx = fetch_fx_rates(START_DATE)

    updated = 0
    skipped = 0

    print("\n── M2 データ取得中 ──")
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
            new_dates = {r[0] for r in new_rec}
            kept   = [r for r in current if r[0] not in new_dates]
            merged = sorted(kept + new_rec, key=lambda r: r[0])
            series[ticker] = merged
            print(f"    → {len(new_rec)} 件更新（累計 {len(series[ticker])} 件）")
            updated += 1
        else:
            print(f"    → 新規データなし（スキップ）")
            skipped += 1
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
        "fx_rates": latest_fx,           # 最新月の単一レート（HTML用フォールバック）
        "fx_rates_monthly": monthly_fx,  # 月別レート（将来的な精緻化用）
        "tickers": {
            t: {"label": labels.get(t, t), "currency": currencies.get(t, "USD")}
            for t in tickers
        },
    }
    existing["series"] = series

    # ── JSON 書き出し ──
    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(existing, f, ensure_ascii=False, separators=(",", ":"))

    print(f"\n完了: 更新 {updated} 件 / スキップ {skipped} 件")
    print(f"出力: {OUTPUT_JSON}")
    print(f"最新FXレート: {latest_fx}")


if __name__ == "__main__":
    main()