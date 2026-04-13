import yfinance as yf
import pandas as pd
import json
import os
import requests
from datetime import datetime

# =====================
# 設定
# =====================
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
GITHUB_PAGES_URL  = os.environ.get("GITHUB_PAGES_URL", "")

TICKERS = [
    "7203.T","6758.T","8306.T","9432.T","6902.T","8316.T","7974.T",
    "4063.T","6861.T","8058.T","9433.T","4502.T","8031.T","6954.T",
    "2914.T","8001.T","6501.T","6367.T","4661.T","7267.T"
]

# =====================
# データ取得（完全一括）
# =====================
def fetch_all_data():
    print("=== データ一括取得中 ===")

    price_data = yf.download(
        tickers=TICKERS,
        period="5y",
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
        threads=False
    )

    dividend_data = {}
    for ticker in TICKERS:
        try:
            dividend_data[ticker] = yf.Ticker(ticker).dividends
        except:
            dividend_data[ticker] = pd.Series()

    return price_data, dividend_data


# =====================
# スクリーニングロジック
# =====================
def analyze_ticker(ticker, price_data, dividends):
    try:
        if ticker not in price_data:
            return None

        df = price_data[ticker].dropna()

        if df.empty:
            return None

        current_price = df["Close"].iloc[-1]

        if dividends.empty:
            return None

        dividends.index = dividends.index.tz_localize(None)

        # 年次配当
        annual_divs = dividends.resample("Y").sum()
        annual_divs.index = annual_divs.index.year

        yearly_yields = []

        for year, annual_div in annual_divs.items():
            if annual_div == 0:
                continue

            year_prices = df[df.index.year == year]["Close"]
            if year_prices.empty:
                continue

            avg_price = year_prices.mean()

            if avg_price > 0:
                yield_pct = (annual_div / avg_price) * 100
                yearly_yields.append(yield_pct)

        if not yearly_yields:
            return None

        max_yield_5y = max(yearly_yields)
        avg_yield_5y = sum(yearly_yields) / len(yearly_yields)

        # 直近配当（実績ベース）
        forward_dividend = dividends[-4:].sum()
        if forward_dividend == 0:
            return None

        buy_price_best   = round(forward_dividend / (max_yield_5y / 100), 1)
        buy_price_better = round(forward_dividend / (avg_yield_5y / 100), 1)
        current_yield    = round((forward_dividend / current_price) * 100, 2)

        return {
            "ticker": ticker,
            "current_price": float(current_price),
            "forward_annual_dividend": float(forward_dividend),
            "current_yield_pct": current_yield,
            "max_yield_5y_pct": round(max_yield_5y, 2),
            "avg_yield_5y_pct": round(avg_yield_5y, 2),
            "buy_price_best": buy_price_best,
            "buy_price_better": buy_price_better,
            "is_best": current_price <= buy_price_best,
            "is_better": current_price <= buy_price_better,
            "upside_to_best_pct": round((buy_price_best - current_price) / current_price * 100, 1),
            "upside_to_better_pct": round((buy_price_better - current_price) / current_price * 100, 1),
        }

    except Exception as e:
        print(f"{ticker}: エラー {e}")
        return None


# =====================
# メイン処理
# =====================
def run_screening():
    price_data, dividend_data = fetch_all_data()

    results = []
    best_signals = []
    better_signals = []

    for ticker in TICKERS:
        print(f"{ticker} 分析中...")
        data = analyze_ticker(ticker, price_data, dividend_data[ticker])

        if not data:
            continue

        results.append(data)

        if data["is_best"]:
            best_signals.append(data)
        elif data["is_better"]:
            better_signals.append(data)

    output = {
        "last_updated": datetime.now().isoformat(),
        "total_screened": len(results),
        "best_count": len(best_signals),
        "better_count": len(better_signals),
        "best_signals": sorted(best_signals, key=lambda x: x["upside_to_best_pct"], reverse=True),
        "better_signals": sorted(better_signals, key=lambda x: x["upside_to_better_pct"], reverse=True),
        "all_results": sorted(results, key=lambda x: x["current_yield_pct"], reverse=True),
    }

    os.makedirs("results", exist_ok=True)

    with open("results/latest.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print("=== 完了 ===")
    return output


# =====================
# Slack通知
# =====================
def send_slack_notification(data):
    if not SLACK_WEBHOOK_URL:
        return

    best = data["best_signals"]
    better = data["better_signals"]

    if not best and not better:
        return

    now_str = datetime.now().strftime("%Y/%m/%d %H:%M")

    text = f"📊 スクリーニング結果 {now_str}\n"
    text += f"Best: {len(best)} / Better: {len(better)}\n\n"

    for s in best[:5]:
        text += f"🟢 {s['ticker']} ¥{s['current_price']:,} → ¥{s['buy_price_best']:,}\n"

    for s in better[:5]:
        text += f"🔵 {s['ticker']} ¥{s['current_price']:,} → ¥{s['buy_price_better']:,}\n"

    if GITHUB_PAGES_URL:
        text += f"\n詳細: {GITHUB_PAGES_URL}"

    requests.post(SLACK_WEBHOOK_URL, json={"text": text})


# =====================
# 実行
# =====================
if __name__ == "__main__":
    data = run_screening()
    send_slack_notification(data)
