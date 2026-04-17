import yfinance as yf
import pandas as pd
import json
import os
import requests
import time
import random
from datetime import datetime

# =====================
# 設定
# =====================
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")

# yfinance用セッション（User-Agentを偽装してレート制限を回避）
session = requests.Session()
session.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
})


def get_prime_tickers():
    """プライム市場の主要銘柄リスト"""
    return [
        "7203.T",  # トヨタ自動車
        "6758.T",  # ソニーグループ
        "8306.T",  # 三菱UFJフィナンシャル・グループ
        "9432.T",  # 日本電信電話(NTT)
        "6902.T",  # デンソー
        "8316.T",  # 三井住友フィナンシャルグループ
        "7974.T",  # 任天堂
        "4063.T",  # 信越化学工業
        "6861.T",  # キーエンス
        "8058.T",  # 三菱商事
        "9433.T",  # KDDI
        "4502.T",  # 武田薬品工業
        "8031.T",  # 三井物産
        "6954.T",  # ファナック
        "2914.T",  # 日本たばこ産業(JT)
        "8001.T",  # 伊藤忠商事
        "6501.T",  # 日立製作所
        "6367.T",  # ダイキン工業
        "4661.T",  # オリエンタルランド
        "7267.T",  # 本田技研工業
    ]


def safe_sleep(min_sec, max_sec):
    """ランダム待機（レート制限回避）"""
    wait = random.uniform(min_sec, max_sec)
    print(f"  待機: {wait:.1f}秒")
    time.sleep(wait)


def fetch_with_retry(ticker_symbol, max_retries=3):
    """
    yfinanceでデータ取得。レート制限時は指数バックオフでリトライ。
    初回リクエスト前に長めの待機を入れることで制限を回避する。
    """
    for attempt in range(max_retries):
        try:
            # リクエスト前に必ず待機（初回も含む）
            if attempt == 0:
                safe_sleep(5, 10)  # 初回: 5〜10秒
            else:
                wait = 20 + random.uniform(10, 20) * attempt
                print(f"  リトライ待機: {wait:.0f}秒 (試行 {attempt+1}/{max_retries})")
                time.sleep(wait)

            ticker = yf.Ticker(ticker_symbol, session=session)

            # info取得（ここで429が出やすい）
            info = ticker.info

            # 株価チェック
            current_price = info.get("currentPrice") or info.get("regularMarketPrice")
            if not current_price:
                print(f"  {ticker_symbol}: 株価データなし → スキップ")
                return None

            # 配当チェック
            forward_div = info.get("dividendRate")
            if not forward_div or forward_div == 0:
                print(f"  {ticker_symbol}: 無配当 → スキップ")
                return None

            # 配当履歴・株価履歴取得
            safe_sleep(2, 4)  # historyリクエスト前にも待機
            dividends = ticker.dividends
            history   = ticker.history(period="5y")

            if history.empty or dividends.empty:
                print(f"  {ticker_symbol}: 履歴データなし → スキップ")
                return None

            # タイムゾーン統一
            if dividends.index.tzinfo:
                dividends.index = dividends.index.tz_localize(None)
            if history.index.tzinfo:
                history.index = history.index.tz_localize(None)

            # 年次配当合計で利回りを算出
            annual_divs = dividends.resample("YE").sum()
            annual_divs.index = annual_divs.index.year

            yearly_yields = []
            for year, annual_div in annual_divs.items():
                if annual_div == 0:
                    continue
                year_prices = history[history.index.year == year]["Close"]
                if year_prices.empty:
                    continue
                avg_price = year_prices.mean()
                if avg_price > 0:
                    yearly_yields.append((annual_div / avg_price) * 100)

            if not yearly_yields:
                print(f"  {ticker_symbol}: 利回り計算不可 → スキップ")
                return None

            max_yield = max(yearly_yields)
            avg_yield = sum(yearly_yields) / len(yearly_yields)

            if max_yield == 0 or avg_yield == 0:
                return None

            buy_best   = round(forward_div / (max_yield / 100), 1)
            buy_better = round(forward_div / (avg_yield / 100), 1)
            cur_yield  = round((forward_div / current_price) * 100, 2)

            return {
                "ticker":                  ticker_symbol,
                "name":                    info.get("longName", ticker_symbol),
                "current_price":           float(current_price),
                "forward_annual_dividend": float(forward_div),
                "current_yield_pct":       float(cur_yield),
                "max_yield_5y_pct":        float(round(max_yield, 2)),
                "avg_yield_5y_pct":        float(round(avg_yield, 2)),
                "buy_price_best":          float(buy_best),
                "buy_price_better":        float(buy_better),
                "is_best":                 bool(current_price <= buy_best),
                "is_better":               bool(current_price <= buy_better),
                "upside_to_best_pct":      float(round((buy_best   - current_price) / current_price * 100, 1)),
                "upside_to_better_pct":    float(round((buy_better - current_price) / current_price * 100, 1)),
            }


        except Exception as e:
            err = str(e)
            if any(kw in err for kw in ["429", "Too Many Requests", "Rate limited"]):
                if attempt < max_retries - 1:
                    wait = 30 + random.uniform(10, 20) * (attempt + 1)
                    print(f"  {ticker_symbol}: レート制限 → {wait:.0f}秒後にリトライ ({attempt+1}/{max_retries})")
                    time.sleep(wait)
                else:
                    print(f"  {ticker_symbol}: リトライ上限到達 → スキップ")
                    return None
            else:
                print(f"  {ticker_symbol}: 予期しないエラー → {err[:80]}")
                return None

    return None


def run_screening():
    tickers        = get_prime_tickers()
    results        = []
    best_signals   = []
    better_signals = []

    print(f"=== スクリーニング開始: {len(tickers)}銘柄 ===\n")

    for i, ticker in enumerate(tickers):
        print(f"[{i+1}/{len(tickers)}] {ticker} 処理中...")
        data = fetch_with_retry(ticker)

        if data:
            results.append(data)
            if data["is_best"]:
                best_signals.append(data)
                print(f"  🟢 Best! 現在値¥{data['current_price']:,} / 買い水準¥{data['buy_price_best']:,} / 利回り{data['current_yield_pct']}%")
            elif data["is_better"]:
                better_signals.append(data)
                print(f"  🔵 Better! 現在値¥{data['current_price']:,} / 買い水準¥{data['buy_price_better']:,} / 利回り{data['current_yield_pct']}%")
            else:
                print(f"  ✓ 取得OK (利回り{data['current_yield_pct']}% / 買い水準まで未達)")
        else:
            print(f"  ✗ データ取得失敗")

        print()

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

    print(f"=== 完了: Best {len(best_signals)}銘柄 / Better {len(better_signals)}銘柄 / 取得成功 {len(results)}銘柄 ===")
    return output


def send_slack_notification(data):
    if not SLACK_WEBHOOK_URL:
        print("SLACK_WEBHOOK_URL未設定 → 通知スキップ")
        return

    best   = data["best_signals"]
    better = data["better_signals"]

    if not best and not better:
        print("シグナルなし → Slack通知スキップ")
        return

    now_str = datetime.now().strftime("%Y/%m/%d %H:%M")
    blocks  = [
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
        lines = [
            f"• *{s['name']}* ({s['ticker']})\n"
            f"  現在値: ¥{s['current_price']:,} | 買い水準: ¥{s['buy_price_best']:,} | 利回り: {s['current_yield_pct']}%"
            for s in best[:5]
        ]
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "🟢 *Best 買い水準銘柄*\n" + "\n".join(lines)}
        })

    if better:
        lines = [
            f"• *{s['name']}* ({s['ticker']})\n"
            f"  現在値: ¥{s['current_price']:,} | 買い水準: ¥{s['buy_price_better']:,} | 利回り: {s['current_yield_pct']}%"
            for s in better[:5]
        ]
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

    resp = requests.post(SLACK_WEBHOOK_URL, json={"blocks": blocks})
    print(f"Slack通知: ステータス {resp.status_code}")


if __name__ == "__main__":
    result = run_screening()
    send_slack_notification(result)
