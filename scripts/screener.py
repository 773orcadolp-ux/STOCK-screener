import pandas as pd
import requests
import json
import os
import io
import time
from datetime import datetime, timedelta
import pytz

JST = pytz.timezone('Asia/Tokyo')


def get_jquants_token():
    """リフレッシュトークンからIDトークンを取得"""
    refresh_token = os.environ["JQUANTS_REFRESH_TOKEN"]

    resp = requests.post(
        "https://api.jquants.com/v1/token/auth_refresh",
        params={"refreshtoken": refresh_token},
        timeout=30
    )
    resp.raise_for_status()
    id_token = resp.json()["idToken"]
    print("J-Quants認証成功")
    return id_token


def get_prime_market_stocks(token: str):
    """プライム市場の銘柄一覧をJ-Quantsから取得"""
    resp = requests.get(
        "https://api.jquants.com/v1/listed/info",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30
    )
    resp.raise_for_status()
    df = pd.DataFrame(resp.json()["info"])

    # プライム市場のみ絞り込み（MarketCode=111）
    prime = df[df["MarketCode"] == "0111"].copy()
    codes_names = list(zip(
        prime["Code"].astype(str).str[:4].tolist(),
        prime["CompanyName"].tolist()
    ))
    print(f"プライム市場銘柄数: {len(codes_names)}")
    return codes_names


def fetch_price_history(code: str, token: str) -> pd.DataFrame | None:
    """J-Quantsから株価履歴を取得（過去2年）"""
    end_d   = datetime.now().strftime("%Y%m%d")
    start_d = (datetime.now() - timedelta(days=2 * 366)).strftime("%Y%m%d")

    resp = requests.get(
        "https://api.jquants.com/v1/prices/daily_quotes",
        headers={"Authorization": f"Bearer {token}"},
        params={"code": code + "0", "from": start_d, "to": end_d},
        timeout=30
    )
    if resp.status_code != 200:
        return None

    data = resp.json().get("daily_quotes", [])
    if not data:
        return None

    df = pd.DataFrame(data)
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values("Date").set_index("Date")
    df = df.rename(columns={"Close": "Close"})
    return df if len(df) >= 20 else None


def fetch_dividends(code: str, token: str) -> dict:
    """J-Quantsから配当データを取得 → {year: 年間配当額}"""
    end_d   = datetime.now().strftime("%Y%m%d")
    start_d = (datetime.now() - timedelta(days=2 * 366)).strftime("%Y%m%d")

    resp = requests.get(
        "https://api.jquants.com/v1/fins/dividend",
        headers={"Authorization": f"Bearer {token}"},
        params={"code": code + "0", "from": start_d, "to": end_d},
        timeout=30
    )
    if resp.status_code != 200:
        return {}

    data = resp.json().get("dividend", [])
    if not data:
        return {}

    # 年ごとに配当合計
    result = {}
    for d in data:
        try:
            year = int(str(d.get("ReferenceDate", ""))[:4])
            val  = float(d.get("DividendPayableDate", 0) or 0)
            if val <= 0:
                val = float(d.get("AnnualDividendPerShare", 0) or 0)
            if year > 0 and val > 0:
                result[year] = result.get(year, 0) + val
        except Exception:
            continue
    return result


def analyze_stock(code: str, name: str, token: str) -> dict | None:
    hist = fetch_price_history(code, token)
    if hist is None or "Close" not in hist.columns:
        return None

    current_price = float(hist["Close"].iloc[-1])
    if current_price <= 0:
        return None

    div_data = fetch_dividends(code, token)
    if not div_data:
        return None

    # 2年間の年次利回り計算
    now_year = datetime.now(JST).year
    annual_yields = []
    for year in range(now_year - 2, now_year):
        yr_div = div_data.get(year, 0)
        if yr_div <= 0:
            continue
        yr_prices = hist[hist.index.year == year]["Close"]
        if yr_prices.empty:
            continue
        avg_price = float(yr_prices.mean())
        if avg_price > 0:
            annual_yields.append(yr_div / avg_price)

    if not annual_yields:
        return None

    max_yield = max(annual_yields)
    avg_yield = sum(annual_yields) / len(annual_yields)

    # 直近の年間配当
    recent_div = div_data.get(now_year - 1) or div_data.get(now_year)
    if not recent_div or recent_div <= 0:
        return None

    best_price   = recent_div / max_yield
    better_price = recent_div / avg_yield

    return {
        "code":              code,
        "name":              name,
        "current_price":     round(current_price, 1),
        "annual_div":        round(recent_div, 1),
        "current_yield_pct": round(recent_div / current_price * 100, 2),
        "max_yield_2y_pct":  round(max_yield * 100, 2),
        "avg_yield_2y_pct":  round(avg_yield * 100, 2),
        "best_price":        round(best_price, 1),
        "better_price":      round(better_price, 1),
        "vs_best_pct":       round((current_price / best_price - 1) * 100, 1),
        "vs_better_pct":     round((current_price / better_price - 1) * 100, 1),
    }


def run_screening(codes_names: list, token: str):
    best_stocks   = []
    better_stocks = []
    total = len(codes_names)

    for i, (code, name) in enumerate(codes_names):
        data = analyze_stock(code, name, token)
        if data:
            cp = data["current_price"]
            if cp <= data["best_price"]:
                best_stocks.append({**data, "level": "Best"})
            elif cp <= data["better_price"]:
                better_stocks.append({**data, "level": "Better"})

        if (i + 1) % 50 == 0:
            print(f"  進捗: {i+1}/{total} | Best={len(best_stocks)} Better={len(better_stocks)}")
        time.sleep(0.3)

    return best_stocks, better_stocks


def send_slack(best: list, better: list, webhook_url: str):
    if not best and not better:
        print("該当銘柄なし → Slack通知スキップ")
        return

    now_str = datetime.now(JST).strftime("%Y/%m/%d %H:%M")

    def format_stocks(stocks, label, price_key, diff_key):
        if not stocks:
            return ""
        lines = [f"*{label} ({len(stocks)}件)*"]
        for s in stocks[:15]:
            gap = s[diff_key]
            lines.append(
                f"• *{s['code']} {s['name']}*  "
                f"株価 {s['current_price']}円  "
                f"水準 {s[price_key]}円  "
                f"({gap:+.1f}%)  "
                f"利回り {s['current_yield_pct']}%"
            )
        if len(stocks) > 15:
            lines.append(f"  …他{len(stocks)-15}件（サイト参照）")
        return "\n".join(lines)

    text = "\n\n".join(filter(None, [
        f":bar_chart: *配当スクリーニング結果* ({now_str} JST)",
        format_stocks(best,   "🏆 Best買い水準",   "best_price",   "vs_best_pct"),
        format_stocks(better, "✅ Better買い水準", "better_price", "vs_better_pct"),
    ]))

    resp = requests.post(webhook_url, json={"text": text}, timeout=10)
    print(f"Slack送信 → HTTP {resp.status_code}")


def save_results(best: list, better: list):
    os.makedirs("docs", exist_ok=True)
    payload = {
        "updated_at":    datetime.now(JST).isoformat(),
        "best_stocks":   sorted(best,   key=lambda x: x["vs_best_pct"]),
        "better_stocks": sorted(better, key=lambda x: x["vs_better_pct"]),
    }
    with open("docs/results.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"保存完了 → Best={len(best)} Better={len(better)}")


def main():
    print("=" * 50)
    print("配当利回りスクリーナー 起動")
    print("=" * 50)

    token = get_jquants_token()
    codes_names = get_prime_market_stocks(token)
    best, better = run_screening(codes_names, token)
    save_results(best, better)

    webhook = os.environ.get("SLACK_WEBHOOK_URL", "")
    if webhook:
        send_slack(best, better, webhook)
    else:
        print("SLACK_WEBHOOK_URL未設定 → 通知スキップ")


if __name__ == "__main__":
    main()
