import yfinance as yf
import pandas as pd
import json
import os
import requests
import time
import random
from datetime import datetime
from requests.adapters import HTTPAdapter

# =====================
# 設定
# =====================
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")

# セッションを使い回してレート制限を回避
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36'
})

def get_prime_tickers():
    return [
        "7203.T",  # トヨタ
        "6758.T",  # ソニー
        "8306.T",  # 三菱UFJ
        "9432.T",  # NTT
        "6902.T",  # デンソー
        "8316.T",  # 三井住友
        "7974.T",  # 任天堂
        "4063.T",  # 信越化学
        "6861.T",  # キーエンス
        "8058.T",  # 三菱商事
        "9433.T",  # KDDI
        "4502.T",  # 武田薬品
        "8031.T",  # 三井物産
        "6954.T",  # ファナック
        "2914.T",  # JT
        "8001.T",  # 伊藤忠
        "6501.T",  # 日立
        "6367.T",  # ダイキン
        "4661.T",  # OLC
        "7267.T",  # ホンダ
    ]


def fetch_stock_data(ticker_symbol):
    max_retries = 3

    for attempt in range(max_retries):
        try:
            ticker = yf.Ticker(ticker_symbol, session=session)
            info = ticker.info

            # 現在株価
            current_price = info.get("currentPrice") or info.get("regularMarketPrice")
            if not current_price:
                print(f"  {ticker_symbol}: 株価データなし、スキップ")
                return None

            # 年間配当見込額
            forward_annual_dividend = info.get("dividendRate")
            if not forward_annual_dividend or forward_annual_dividend == 0:
                print(f"  {ticker_symbol}: 無配当、スキップ")
                return None

            # 過去5年の株価・配当履歴
            dividends = ticker.dividends
            history   = ticker.history(period="5y")

            if history.empty or dividends.empty:
                print(f"  {ticker_symbol}: 履歴データなし、スキップ")
                return None

            # タイムゾーン統一
            dividends.index = dividends.index.tz_localize(None) if dividends.index.tzinfo else dividends.index
            history.index   = history.index.tz_localize(None)   if history.index.tzinfo   else history.index

            # 年次配当合計を算出して利回りを計算
            annual_divs = dividends.resample("Y").sum()
            annual_divs.index = annual_divs.index.year

            yearly_yields = []
            for year, annual_div in annual_divs.items():
                if annual_div == 0:
                    continue
                year_prices = history[history.index.year == year]["Close"]
                if year_prices.empty:
                    continue
                avg_price_year = year_prices.mean()
                if avg_price_year > 0:
                    yield_pct = (annual_div / avg_price_year) * 100
                    yearly_yields.append(yield_pct)

            if not yearly_yields:
                print(f"  {ticker_symbol}: 利回り計算不可、スキップ")
                return None

            max_yield_5y = max(yearly_yields)
            avg_yield_5y = sum(yearly_yields) / len(yearly_yields)

            if max_yield_5y == 0 or avg_yield_5y == 0:
                return None

            # 買い水準株価の算出
            buy_price_best   = round(forward_annual_dividend / (max_yield_5y / 100), 1)
            buy_price_better = round(forward_annual_dividend / (avg_yield_5y / 100), 1)
            current_yield    = round((forward_annual_dividend / current_price) * 100, 2)

            return {
                "ticker":                  ticker_symbol,
                "name":                    info.get("longName", ticker_symbol),
                "current_price":           current_price,
                "forward_annual_dividend": forward_annual_dividend,
                "current_yield_pct":       current_yield,
                "max_yield_5y_pct":        round(max_yield_5y, 2),
                "avg_yield_5y_pct":        round(avg_yield_5y, 2),
                "buy_price_best":          buy_price_best,
                "buy_price_better":        buy_price_better,
                "is_best":                 current_price <= buy_price_best,
                "is_better":               current_price <= buy_price_better,
                "upside_to_best_pct":      round((buy_price_best   - current_price) / current_price * 100, 1),
                "upside_to_better_pct":    round((buy_price_better - current_price) / current_price * 100, 1),
            }

        except Exception as e:
            err_str = str(e)
            if "Too Many Requests" in err_str or "Rate limited" in err_str or "429" in err_str:
                wait = 15 + random.uniform(5, 10) * (attempt + 1)
                print(f"  {ticker_symbol}: レート制限。{wait:.0f}秒待機してリトライ ({attempt+1}/{max_retries})")
                time.sleep(wait)
            else:
                print(f"  {ticker_symbol}: エラー → {e}")
                return None

    print(f"  {ticker_symbol}: {max_retries}回リトライ失敗、スキップ")
    return None


def run_screening():
    tickers = get_prime_tickers()
    results        = []
    best_signals   = []
    better_signals = []

    print(f"=== スクリーニング開始: {len(tickers)}銘柄 ===")

    for i, ticker in enumerate(tickers):
        print(f"[{i+1}/{len(tickers)}] {ticker} 取得中...")
        data = fetch_stock_data(ticker)

        if data:
            results.append(data)
            if data["is_best"]:
                best_signals.append(data)
                print(f"  → 🟢 Best検出！ 現在値¥{data['current_price']:,} / 買い水準¥{data['buy_price_best']:,}")
            elif data["is_better"]:
                better_signals.append(data)
                print(f"  → 🔵 Better検出！ 現在値¥{data['current_price']:,} / 買い水準¥{data['buy_price_better']:,}")
            else:
                print(f"  → 対象外 (利回り{data['current_yield_pct']}%)")

        # 銘柄間の待機（3〜6秒のランダム）
        wait = random.uniform(3, 6)
        print(f"  {wait:.1f}秒待機...")
        time.sleep(wait)

    # 結果をJSONに保存
    output = {
        "last_updated":   datetime.now().isoformat(),
        "total_screened": len(results),
        "best_count":     len(best_signals),
        "better_count":   len(better_signals),
        "best_signals":   sorted(best_signals,   key=lambda x: x["upside_to_best_pct"],   reverse=True),
        "better_signals": sorted(better_signals, key=lambda x: x["upside_to_better_pct"], reverse=True),
        "all_results":    sorted(results,        key=lambda x: x["current_yield_pct"],     reverse=True),
    }

    os.makedirs("results", exist_ok=True)
    with open("results/latest.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n=== 完了: Best {len(best_signals)}銘柄 / Better {len(better_signals)}銘柄 / 取得成功 {len(results)}銘柄 ===")
    return output


def send_slack_notification(data):
    if not SLACK_WEBHOOK_URL:
        print("SLACK_WEBHOOK_URL未設定、Slack通知スキップ")
        return

    best   = data["best_signals"]
    better = data["better_signals"]

    if not best and not better:
        print("シグナルなし、Slack通知スキップ")
        return

    now_str = datetime.now().strftime("%Y/%m/%d %H:%M")
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"📊 株価スクリーニング結果 {now_str}"}
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*スクリーニング対象:* {data['total_screened']}銘柄\n"
                    f"*🟢 Best（最大利回り水準）:* {len(best)}銘柄\n"
                    f"*🔵 Better（平均利回り水準）:* {len(better)}銘柄"
                )
            }
        }
    ]

    if best:
        lines = []
        for s in best[:5]:
            lines.append(
                f"• *{s['name']}* ({s['ticker']})\n"
                f"  現在値: ¥{s['current_price']:,} | 買い水準: ¥{s['buy_price_best']:,} "
                f"| 利回り: {s['current_yield_pct']}%"
            )
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "🟢 *Best 買い水準銘柄*\n" + "\n".join(lines)}
        })

    if better:
        lines = []
        for s in better[:5]:
            lines.append(
                f"• *{s['name']}* ({s['ticker']})\n"
                f"  現在値: ¥{s['current_price']:,} | 買い水準: ¥{s['buy_price_better']:,} "
                f"| 利回り: {s['current_yield_pct']}%"
            )
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "🔵 *Better 買い水準銘柄*\n" + "\n".join(lines)}
        })

    pages_url = os.environ.get("GITHUB_PAGES_URL", "")
    if pages_url:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"<{pages_url}|📋 詳細一覧を見る>"}
        })

    response = requests.post(SLACK_WEBHOOK_URL, json={"blocks": blocks})
    print(f"Slack通知送信: {response.status_code}")


if __name__ == "__main__":
    data = run_screening()
    send_slack_notification(data)
