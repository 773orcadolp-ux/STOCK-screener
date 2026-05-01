import pandas as pd
import requests
import json
import os
import time
from datetime import datetime, timedelta
import pytz

JST = pytz.timezone('Asia/Tokyo')


def get_headers():
    return {"x-api-key": os.environ["JQUANTS_API_KEY"]}


def fetch_with_pagination(url, headers, params, key="data"):
    """ページング対応の汎用GET"""
    all_items = []
    while True:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code == 429:
            print(f"  429エラー → 60秒待機")
            time.sleep(60)
            continue
        if resp.status_code != 200:
            print(f"  HTTPエラー: {resp.status_code} {resp.text[:200]}")
            return []
        data = resp.json()
        all_items.extend(data.get(key, []))
        pkey = data.get("pagination_key")
        if not pkey:
            break
        params["pagination_key"] = pkey
        time.sleep(0.5)
    return all_items


def get_prime_market_stocks(headers):
    """プライム市場の銘柄一覧"""
    items = fetch_with_pagination(
        "https://api.jquants.com/v2/equities/master",
        headers, {}, key="data"
    )
    df = pd.DataFrame(items)
    print(f"取得銘柄数: {len(df)}")
    prime = df[df["Mkt"] == "0111"].copy()
    prime["Code4"] = prime["Code"].astype(str).str[:4]
    print(f"プライム銘柄数: {len(prime)}")
    return prime[["Code", "Code4", "CoName"]].to_dict("records")


def fetch_prices_by_date(date_str: str, headers):
    """指定日付の全銘柄株価を一括取得"""
    items = fetch_with_pagination(
        "https://api.jquants.com/v2/equities/bars/daily",
        headers, {"date": date_str}, key="data"
    )
    if not items:
        return None
    df = pd.DataFrame(items)
    if "AdjC" not in df.columns:
        return None
    df = df[["Code", "AdjC"]].copy()
    df["AdjC"] = pd.to_numeric(df["AdjC"], errors="coerce")
    df = df.dropna(subset=["AdjC"])
    df = df[df["AdjC"] > 0]
    df["Code4"] = df["Code"].astype(str).str[:4]
    return df.set_index("Code4")["AdjC"]


def get_price_samples(headers, num_months: int = 24):
    """過去N月の月末株価をサンプリング取得"""
    print(f"\n=== 過去{num_months}ヶ月分の株価サンプリング ===")
    samples = {}  # {YYYY-MM: Series of {code4: price}}
    today = datetime.now()
    
    # 月末日リスト作成
    target_dates = []
    for i in range(num_months):
        # i月前の月末を狙う
        d = today - timedelta(days=30 * i)
        # その月の最終営業日を取得（土日除く）
        last_day = d.replace(day=1) + timedelta(days=32)
        last_day = last_day.replace(day=1) - timedelta(days=1)
        while last_day.weekday() >= 5:
            last_day -= timedelta(days=1)
        target_dates.append(last_day.strftime("%Y-%m-%d"))
    
    for i, date_str in enumerate(target_dates):
        prices = fetch_prices_by_date(date_str, headers)
        if prices is not None and len(prices) > 0:
            samples[date_str] = prices
            print(f"  [{i+1}/{num_months}] {date_str}: {len(prices)}銘柄")
        time.sleep(1.0)
    
    return samples


def get_current_prices(headers):
    """直近営業日の株価取得"""
    for i in range(7):
        d = datetime.now() - timedelta(days=i)
        if d.weekday() >= 5:
            continue
        date_str = d.strftime("%Y-%m-%d")
        prices = fetch_prices_by_date(date_str, headers)
        if prices is not None and len(prices) > 100:
            print(f"現在株価日付: {date_str} ({len(prices)}銘柄)")
            return prices
        time.sleep(1.0)
    return None


def get_dividend_data(code5: str, headers):
    """個別銘柄の配当データ取得"""
    items = fetch_with_pagination(
        "https://api.jquants.com/v2/fins/dividend",
        headers, {"code": code5}, key="data"
    )
    if not items:
        return None
    
    # 直近の予想配当・実績配当を年単位で集約
    annual = {}  # {year: total_dividend}
    for d in items:
        date_str = d.get("AnnouncementDate") or d.get("ReferenceDate") or ""
        try:
            year = int(str(date_str)[:4])
        except:
            continue
        
        # 配当値を取得（複数候補をチェック）
        val = None
        for key in ["DividendPerShare", "AnnualDividendPerShare", "ForecastDividendPerShare"]:
            v = d.get(key)
            if v is None or v == "" or v == "-":
                continue
            try:
                val = float(v)
                if val > 0:
                    break
            except:
                continue
        
        if val is not None and val > 0:
            annual[year] = annual.get(year, 0) + val
    
    return annual if annual else None


def main():
    print("=" * 50)
    print("配当利回りスクリーナー V2 起動")
    print("=" * 50)
    
    headers = get_headers()
    
    # 1. 銘柄リスト取得
    stocks = get_prime_market_stocks(headers)
    if not stocks:
        print("銘柄取得失敗")
        return
    
    # 2. 過去2年の月末株価サンプリング（24回コール）
    price_samples = get_price_samples(headers, num_months=24)
    if not price_samples:
        print("株価サンプリング失敗")
        return
    
    # 3. 現在株価取得
    current_prices = get_current_prices(headers)
    if current_prices is None:
        print("現在株価取得失敗")
        return
    
    # 4. 各銘柄の年次平均株価を計算
    print("\n=== 年次平均株価計算 ===")
    year_avg = {}  # {code4: {year: avg_price}}
    for date_str, prices in price_samples.items():
        year = int(date_str[:4])
        for code4, price in prices.items():
            if code4 not in year_avg:
                year_avg[code4] = {}
            if year not in year_avg[code4]:
                year_avg[code4][year] = []
            year_avg[code4][year].append(float(price))
    
    # 5. 各銘柄をスクリーニング
    print("\n=== 配当データ取得 & スクリーニング ===")
    best_stocks = []
    better_stocks = []
    now_year = datetime.now(JST).year
    
    for i, stock in enumerate(stocks):
        code5 = stock["Code"]
        code4 = stock["Code4"]
        name = stock["CoName"]
        
        # 現在株価
        if code4 not in current_prices.index:
            continue
        cp = float(current_prices[code4])
        
        # 年次平均株価
        if code4 not in year_avg:
            continue
        yr_data = year_avg[code4]
        
        # 配当データ
        div_data = get_dividend_data(code5, headers)
        if not div_data:
            time.sleep(0.3)
            continue
        
        # 年次利回り計算
        annual_yields = []
        for year in [now_year - 2, now_year - 1]:
            yr_div = div_data.get(year, 0)
            yr_prices = yr_data.get(year, [])
            if yr_div > 0 and yr_prices:
                avg_p = sum(yr_prices) / len(yr_prices)
                if avg_p > 0:
                    annual_yields.append(yr_div / avg_p)
        
        if not annual_yields:
            time.sleep(0.3)
            continue
        
        max_y = max(annual_yields)
        avg_y = sum(annual_yields) / len(annual_yields)
        
        # 直近配当
        recent_div = div_data.get(now_year - 1) or div_data.get(now_year)
        if not recent_div or recent_div <= 0:
            time.sleep(0.3)
            continue
        
        best_p = recent_div / max_y
        better_p = recent_div / avg_y
        
        result = {
            "code": code4,
            "name": name,
            "current_price": round(cp, 1),
            "annual_div": round(recent_div, 1),
            "current_yield_pct": round(recent_div / cp * 100, 2),
            "max_yield_pct": round(max_y * 100, 2),
            "avg_yield_pct": round(avg_y * 100, 2),
            "best_price": round(best_p, 1),
            "better_price": round(better_p, 1),
            "vs_best_pct": round((cp / best_p - 1) * 100, 1),
            "vs_better_pct": round((cp / better_p - 1) * 100, 1),
        }
        
        if cp <= best_p:
            best_stocks.append({**result, "level": "Best"})
        elif cp <= better_p:
            better_stocks.append({**result, "level": "Better"})
        
        if (i + 1) % 100 == 0:
            print(f"  進捗: {i+1}/{len(stocks)} | Best={len(best_stocks)} Better={len(better_stocks)}")
        time.sleep(0.5)
    
    print(f"\n完了: Best={len(best_stocks)} Better={len(better_stocks)}")
    
    # 6. 結果保存
    os.makedirs("docs", exist_ok=True)
    payload = {
        "updated_at": datetime.now(JST).isoformat(),
        "best_stocks": sorted(best_stocks, key=lambda x: x["vs_best_pct"]),
        "better_stocks": sorted(better_stocks, key=lambda x: x["vs_better_pct"]),
    }
    with open("docs/results.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print("結果保存完了")
    
    # 7. Slack通知
    webhook = os.environ.get("SLACK_WEBHOOK_URL", "")
    if webhook and (best_stocks or better_stocks):
        now_str = datetime.now(JST).strftime("%Y/%m/%d %H:%M")
        lines = [f":bar_chart: *配当スクリーニング結果* ({now_str} JST)"]
        if best_stocks:
            lines.append(f"\n*🏆 Best ({len(best_stocks)}件)*")
            for s in best_stocks[:10]:
                lines.append(f"• {s['code']} {s['name']} 現在{s['current_price']}円 / 水準{s['best_price']}円 利回り{s['current_yield_pct']}%")
        if better_stocks:
            lines.append(f"\n*✅ Better ({len(better_stocks)}件)*")
            for s in better_stocks[:10]:
                lines.append(f"• {s['code']} {s['name']} 現在{s['current_price']}円 / 水準{s['better_price']}円 利回り{s['current_yield_pct']}%")
        requests.post(webhook, json={"text": "\n".join(lines)}, timeout=10)
        print("Slack通知送信")


if __name__ == "__main__":
    main()
