import yfinance as yf
import pandas as pd
import json
import os
import requests
from datetime import datetime, date
import time

# =====================
# 設定
# =====================
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")

# プライム市場の主要銘柄リスト（例示。実際はより広範なリストを使用）
# JPX公式からCSVを取得する方式に後で拡張可能
def get_prime_tickers():
    """
    JPXのプライム市場銘柄リストを取得。
    本番ではhttps://www.jpx.co.jp/markets/statistics-equities/misc/01.htmlから取得推奨。
    ここでは主要どころを例示。
    """
    # 代表的なプライム市場銘柄（証券コード + ".T" でyfinance対応）
    sample_tickers = [
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
    return sample_tickers

def fetch_stock_data(ticker_symbol):
    """
    yfinanceで株価・配当データを取得
    """
    try:
        ticker = yf.Ticker(ticker_symbol)
        info = ticker.info

        # 現在株価
        current_price = info.get("currentPrice") or info.get("regularMarketPrice")
        if not current_price:
            return None

        # 年間配当見込額（forward dividend）
        forward_annual_dividend = info.get("dividendRate")  # 年間配当（予想）
        if not forward_annual_dividend or forward_annual_dividend == 0:
            return None  # 無配当はスキップ

        # 過去5年の配当履歴を取得
        dividends = ticker.dividends
        history = ticker.history(period="5y")

        if history.empty or dividends.empty:
            return None

        # 過去5年間の各日の配当利回りを算出するため、
        # 年次配当合計と株価履歴を使って最大・平均利回りを計算
        # 配当を年次集計
        dividends.index = dividends.index.tz_localize(None) if dividends.index.tzinfo else dividends.index
        history.index = history.index.tz_localize(None) if history.index.tzinfo else history.index

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
            return None

        max_yield_5y = max(yearly_yields)    # 過去5年最大利回り
        avg_yield_5y = sum(yearly_yields) / len(yearly_yields)  # 過去5年平均利回り

        if max_yield_5y == 0 or avg_yield_5y == 0:
            return None

        # 買い水準株価の算出
        buy_price_best   = round(forward_annual_dividend / (max_yield_5y / 100), 1)
        buy_price_better = round(forward_annual_dividend / (avg_yield_5y / 100), 1)

        current_yield = round((forward_annual_dividend / current_price) * 100, 2)

        return {
            "ticker": ticker_symbol,
            "name": info.get("longName", ticker_symbol),
            "current_price": current_price,
            "forward_annual_dividend": forward_annual_dividend,
            "current_yield_pct": current_yield,
            "max_yield_5y_pct": round(max_yield_5y, 2),
            "avg_yield_5y_pct": round(avg_yield_5y, 2),
            "buy_price_best": buy_price_best,
            "buy_price_better": buy_price_better,
            "is_best":   current_price <= buy_price_best,
            "is_better": current_price <= buy_price_better,
            "upside_to_best_pct":   round((buy_price_best   - current_price) / current_price * 100, 1),
            "upside_to_better_pct": round((buy_price_better - current_price) / current_price * 100, 1),
        }

    except Exception as e:
        print(f"Error fetching {ticker_symbol}: {e}")
        return None


def run_screening():
    tickers = get_prime_tickers()
    results = []
    best_signals   = []
    better_signals = []

    print(f"Screening {len(tickers)} tickers...")

    for i, ticker in enumerate(tickers):
        print(f"  [{i+1}/{len(tickers)}] {ticker}")
        data = fetch_stock_data(ticker)
        if data:
            results.append(data)
            if data["is_best"]:
                best_signals.append(data)
            elif data["is_better"]:
                better_signals.append(data)
        time.sleep(0.5)  # レート制限対策

    # 結果をJSONに保存
    output = {
        "last_updated": datetime.now().isoformat(),
        "total_screened": len(results),
        "best_count": len(best_signals),
        "better_count": len(better_signals),
        "best_signals": sorted(best_signals,   key=lambda x: x["upside_to_best_pct"],   reverse=True),
        "better_signals": sorted(better_signals, key=lambda x: x["upside_to_better_pct"], reverse=True),
        "all_results": sorted(results, key=lambda x: x["current_yield_pct"], reverse=True),
    }

    os.makedirs("results", exist_ok=True)
    with open("results/latest.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"Done. Best: {len(best_signals)}, Better: {len(better_signals)}")
    return output


def send_slack_notification(data):
    """
    Slack Incoming Webhookで通知を送信
    """
    if not SLACK_WEBHOOK_URL:
        print("SLACK_WEBHOOK_URL not set, skipping notification.")
        return

    best   = data["best_signals"]
    better = data["better_signals"]

    if not best and not better:
        # シグナルなしの場合は通知しない（静音モード）
        print("No signals found, skipping Slack notification.")
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
                "text": f"*スクリーニング対象:* {data['total_screened']}銘柄\n"
                        f"*🟢 Best（最大利回り水準）:* {len(best)}銘柄\n"
                        f"*🔵 Better（平均利回り水準）:* {len(better)}銘柄"
            }
        }
    ]

    if best:
        best_lines = []
        for s in best[:5]:  # 上位5件
            best_lines.append(
                f"• *{s['name']}* ({s['ticker']})\n"
                f"  現在値: ¥{s['current_price']:,} | 買い水準: ¥{s['buy_price_best']:,} "
                f"| 利回り: {s['current_yield_pct']}% | 上値余地: {abs(s['upside_to_best_pct'])}%↓"
            )
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "🟢 *Best 買い水準銘柄*\n" + "\n".join(best_lines)}
        })

    if better:
        better_lines = []
        for s in better[:5]:  # 上位5件
            better_lines.append(
                f"• *{s['name']}* ({s['ticker']})\n"
                f"  現在値: ¥{s['current_price']:,} | 買い水準: ¥{s['buy_price_better']:,} "
                f"| 利回り: {s['current_yield_pct']}% | 上値余地: {abs(s['upside_to_better_pct'])}%↓"
            )
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "🔵 *Better 買い水準銘柄*\n" + "\n".join(better_lines)}
        })

    # GitHub Pages URLがあれば追加
    pages_url = os.environ.get("GITHUB_PAGES_URL", "")
    if pages_url:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"<{pages_url}|📋 詳細一覧を見る>"}
        })

    payload = {"blocks": blocks}
    response = requests.post(SLACK_WEBHOOK_URL, json=payload)
    print(f"Slack notification sent: {response.status_code}")


if __name__ == "__main__":
    data = run_screening()
    send_slack_notification(data)
