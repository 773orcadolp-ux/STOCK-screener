import pandas as pd
import requests
import json
import os
import time
from datetime import datetime, timedelta
import pytz

JST = pytz.timezone('Asia/Tokyo')

# ─── テストモード設定 ───
TEST_MODE = False  # True=トヨタのみ / False=日経225全銘柄
TEST_CODE5 = "72030"
TEST_NAME = "トヨタ自動車"

# ─── 日経225銘柄リスト（4桁コード） ───
NIKKEI_225_CODES = [
    "1332","1333","1605","1721","1801","1802","1803","1808","1812","1925",
    "1928","1963","2002","2269","2282","2413","2432","2501","2502","2503",
    "2531","2768","2801","2802","2871","2914","3086","3092","3099","3101",
    "3103","3105","3289","3382","3401","3402","3405","3407","3436","3543",
    "3659","3861","3863","3865","4004","4005","4021","4042","4043","4061",
    "4063","4151","4183","4188","4204","4208","4324","4452","4502","4503",
    "4506","4507","4519","4523","4543","4568","4578","4631","4661","4689",
    "4704","4751","4755","4901","4902","4911","5019","5020","5101","5108",
    "5201","5202","5214","5232","5233","5301","5332","5333","5401","5406",
    "5411","5541","5631","5703","5706","5707","5711","5713","5714","5801",
    "5802","5803","5901","5938","5947","6098","6103","6113","6178","6273",
    "6301","6302","6305","6326","6361","6367","6471","6472","6473","6479",
    "6501","6502","6503","6504","6506","6594","6645","6701","6702","6724",
    "6752","6753","6758","6762","6770","6841","6857","6861","6902","6920",
    "6952","6954","6971","6976","6981","6988","7003","7004","7011","7012",
    "7013","7186","7201","7202","7203","7205","7211","7261","7267","7269",
    "7270","7272","7282","7309","7459","7532","7731","7733","7735","7741",
    "7751","7752","7762","7832","7911","7912","7951","7974","8001","8002",
    "8015","8031","8035","8053","8058","8233","8252","8253","8267","8270",
    "8306","8308","8309","8316","8331","8354","8355","8411","8473","8591",
    "8601","8604","8628","8630","8697","8725","8750","8766","8795","8801",
    "8802","8804","8830","9001","9005","9007","9008","9009","9020","9021",
    "9022","9062","9064","9101","9104","9107","9201","9202","9301","9412",
    "9432","9433","9434","9437","9501","9502","9503","9531","9532","9602",
    "9613","9735","9766","9831","9843","9983","9984"
]
# ─────────────────────


def get_headers():
    return {"x-api-key": os.environ["JQUANTS_API_KEY"]}


def fetch_with_pagination(url, headers, params, key="data"):
    """ページング対応＋429リトライの汎用GET"""
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


def get_target_stocks(headers):
    """対象銘柄取得（テスト時はトヨタのみ、本番時は日経225）"""
    if TEST_MODE:
        return [{"Code": TEST_CODE5, "Code4": TEST_CODE5[:4], "CoName": TEST_NAME}]
    
    items = fetch_with_pagination(
        "https://api.jquants.com/v2/equities/master",
        headers, {}, key="data"
    )
    df = pd.DataFrame(items)
    df["Code4"] = df["Code"].astype(str).str[:4]
    df = df[df["Code4"].isin(NIKKEI_225_CODES)]
    print(f"日経225マッチ銘柄数: {len(df)}/{len(NIKKEI_225_CODES)}")
    return df[["Code", "Code4", "CoName"]].to_dict("records")


def fetch_prices_by_date(date_str: str, headers):
    """指定日付の全銘柄株価を一括取得（重複Code4除去あり）"""
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
    df = df.drop_duplicates(subset=["Code4"], keep="first")
    return df.set_index("Code4")["AdjC"]


def get_price_samples(headers, num_months: int = 24):
    """過去N月の月末株価をサンプリング取得"""
    print(f"\n=== 過去{num_months}ヶ月分の株価サンプリング ===")
    samples = {}
    today = datetime.now()
    base = today - timedelta(days=85)
    
    target_dates = []
    for i in range(num_months):
        d = base - timedelta(days=30 * i)
        last_day = d.replace(day=1) + timedelta(days=32)
        last_day = last_day.replace(day=1) - timedelta(days=1)
        while last_day.weekday() >= 5:
            last_day -= timedelta(days=1)
        if last_day > base:
            last_day = base
            while last_day.weekday() >= 5:
                last_day -= timedelta(days=1)
        target_dates.append(last_day.strftime("%Y-%m-%d"))
    
    for i, date_str in enumerate(target_dates):
        prices = fetch_prices_by_date(date_str, headers)
        if prices is not None and len(prices) > 0:
            samples[date_str] = prices
            print(f"  [{i+1}/{num_months}] {date_str}: {len(prices)}銘柄")
        time.sleep(13)
    
    return samples


def get_current_prices(headers):
    """無料プランでの『最新株価』=12週間前あたり"""
    base = datetime.now() - timedelta(days=85)
    for i in range(14):
        d = base - timedelta(days=i)
        if d.weekday() >= 5:
            continue
        date_str = d.strftime("%Y-%m-%d")
        prices = fetch_prices_by_date(date_str, headers)
        if prices is not None and len(prices) > 100:
            print(f"現在株価日付: {date_str} ({len(prices)}銘柄)")
            return prices
        time.sleep(13)
    return None


def fetch_financial_summary(code5: str, headers):
    """財務情報（配当データ含む）取得"""
    items = fetch_with_pagination(
        "https://api.jquants.com/v2/fins/summary",
        headers, {"code": code5}, key="data"
    )
    return items


def extract_dividend_history(fin_items: list, current_year: int) -> dict:
    """財務情報から年次配当履歴を抽出"""
    annual = {}
    forecast = None
    
    for item in fin_items:
        disc_date = item.get("DiscDate", "")
        try:
            disc_year = int(disc_date[:4])
        except:
            continue
        
        doc_type = item.get("DocType", "")
        
        if "FY" in doc_type and "Forecast" not in doc_type:
            div_ann = item.get("DivAnn")
            if div_ann not in (None, "", "-"):
                try:
                    val = float(div_ann)
                    if val > 0:
                        cur_fy_en = item.get("CurFYEn", "")
                        try:
                            fy_year = int(cur_fy_en[:4])
                            annual[fy_year] = val
                        except:
                            annual[disc_year] = val
                except:
                    pass
        
        nx_div = item.get("NxFDivAnn")
        if nx_div not in (None, "", "-"):
            try:
                val = float(nx_div)
                if val > 0:
                    forecast = val
            except:
                pass
        
        f_div = item.get("FDivAnn")
        if f_div not in (None, "", "-"):
            try:
                val = float(f_div)
                if val > 0 and forecast is None:
                    forecast = val
            except:
                pass
    
    return {"annual": annual, "forecast": forecast}


def send_slack(webhook, text):
    """Slack通知（エラーログ付き）"""
    try:
        resp = requests.post(webhook, json={"text": text}, timeout=10)
        print(f"Slack通知 status: {resp.status_code}, len={len(text)}")
        if resp.status_code != 200:
            print(f"Slack エラー詳細: {resp.text[:300]}")
        return resp.status_code == 200
    except Exception as e:
        print(f"Slack送信失敗: {e}")
        return False


def main():
    print("=" * 50)
    print(f"配当利回りスクリーナー V2 起動 (TEST={TEST_MODE})")
    print("=" * 50)
    
    headers = get_headers()
    
    # 1. 銘柄リスト
    stocks = get_target_stocks(headers)
    if not stocks:
        print("銘柄取得失敗")
        return
    print(f"対象銘柄数: {len(stocks)}")
    
    # 2. 過去2年の月末株価サンプリング
    price_samples = get_price_samples(headers, num_months=24)
    if not price_samples:
        print("株価サンプリング失敗")
        return
    
    # 3. 現在株価
    current_prices = get_current_prices(headers)
    if current_prices is None:
        print("現在株価取得失敗")
        return
    
    # 4. 年次平均株価計算
    print("\n=== 年次平均株価計算 ===")
    year_avg = {}
    for date_str, prices in price_samples.items():
        year = int(date_str[:4])
        for code4, price in prices.items():
            year_avg.setdefault(code4, {}).setdefault(year, []).append(float(price))
    
    # 5. スクリーニング
    print("\n=== 配当データ取得 & スクリーニング ===")
    best_stocks = []
    better_stocks = []
    now_year = datetime.now(JST).year
    
    for i, stock in enumerate(stocks):
        code5 = stock["Code"]
        code4 = stock["Code4"]
        name = stock["CoName"]
        
        if code4 not in current_prices.index:
            if TEST_MODE:
                print(f"  [{name}] 現在株価データなし → スキップ")
            continue
        
        cp_val = current_prices[code4]
        if hasattr(cp_val, 'iloc'):
            cp_val = cp_val.iloc[0]
        cp = float(cp_val)
        
        if code4 not in year_avg:
            if TEST_MODE:
                print(f"  [{name}] 年次株価データなし → スキップ")
            continue
        yr_data = year_avg[code4]
        
        fin_items = fetch_financial_summary(code5, headers)
        if not fin_items:
            if TEST_MODE:
                print(f"  [{name}] 財務情報取得失敗")
            time.sleep(13)
            continue
        
        if TEST_MODE:
            print(f"\n  [{name}] 財務情報レコード数: {len(fin_items)}")
        
        div_info = extract_dividend_history(fin_items, now_year)
        annual_div = div_info["annual"]
        forecast_div = div_info["forecast"]
        
        if TEST_MODE:
            print(f"  年次配当履歴: {annual_div}")
            print(f"  予想配当: {forecast_div}")
            print(f"  現在株価: {cp}円")
            print(f"  年次平均株価: {[(y, round(sum(p)/len(p), 1)) for y, p in yr_data.items()]}")
        
        annual_yields = []
        for year, div_val in annual_div.items():
            yr_prices = yr_data.get(year, [])
            if div_val > 0 and yr_prices:
                avg_p = sum(yr_prices) / len(yr_prices)
                if avg_p > 0:
                    yld = div_val / avg_p
                    annual_yields.append(yld)
                    if TEST_MODE:
                        print(f"  {year}年: 配当{div_val}円 / 平均株価{avg_p:.0f}円 = 利回り{yld*100:.2f}%")
        
        if not annual_yields:
            if TEST_MODE:
                print(f"  [{name}] 利回り計算不可 → スキップ")
            time.sleep(13)
            continue
        
        max_y = max(annual_yields)
        avg_y = sum(annual_yields) / len(annual_yields)
        
        recent_div = forecast_div
        if not recent_div and annual_div:
            recent_div = annual_div[max(annual_div.keys())]
        
        if not recent_div or recent_div <= 0:
            if TEST_MODE:
                print(f"  [{name}] 配当予想なし → スキップ")
            time.sleep(13)
            continue
        
        best_p = recent_div / max_y
        better_p = recent_div / avg_y
        current_yield = recent_div / cp
        
        result = {
            "code": code4,
            "name": name,
            "current_price": round(cp, 1),
            "annual_div": round(recent_div, 1),
            "current_yield_pct": round(current_yield * 100, 2),
            "max_yield_pct": round(max_y * 100, 2),
            "avg_yield_pct": round(avg_y * 100, 2),
            "best_price": round(best_p, 1),
            "better_price": round(better_p, 1),
            "vs_best_pct": round((cp / best_p - 1) * 100, 1),
            "vs_better_pct": round((cp / better_p - 1) * 100, 1),
        }
        
        if TEST_MODE:
            print(f"\n  ━━━ {name} スクリーニング結果 ━━━")
            print(f"  現在株価: {result['current_price']}円")
            print(f"  予想配当: {result['annual_div']}円")
            print(f"  現在利回り: {result['current_yield_pct']}%")
            print(f"  最大利回り(2年): {result['max_yield_pct']}%")
            print(f"  平均利回り(2年): {result['avg_yield_pct']}%")
            print(f"  Best水準: {result['best_price']}円 (現在比{result['vs_best_pct']:+.1f}%)")
            print(f"  Better水準: {result['better_price']}円 (現在比{result['vs_better_pct']:+.1f}%)")
            if current_yield >= max_y:
                print(f"  → 除外: 現在利回り({current_yield*100:.2f}%) ≥ 過去最大利回り({max_y*100:.2f}%)")
            print(f"  ━━━━━━━━━━━━━━━━━━━━━━")
        
        # 【修正】現在利回りが過去最大利回りを超えていない銘柄のみ対象
        if current_yield < max_y:
            if cp <= best_p:
                best_stocks.append({**result, "level": "Best"})
            elif cp <= better_p:
                better_stocks.append({**result, "level": "Better"})
        
        if not TEST_MODE and (i + 1) % 25 == 0:
            print(f"  進捗: {i+1}/{len(stocks)} | Best={len(best_stocks)} Better={len(better_stocks)}")
        time.sleep(13)
    
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
    
    # 7. Slack通知（エラー検知＋メッセージ分割対応）
    webhook = os.environ.get("SLACK_WEBHOOK_URL", "")
    if not webhook:
        print("Slack Webhook未設定")
        return
    
    now_str = datetime.now(JST).strftime("%Y/%m/%d %H:%M")
    
    if TEST_MODE:
        text = (
            f":test_tube: *テスト実行完了* ({now_str} JST)\n"
            f"対象: {TEST_NAME}\n"
            f"Best該当: {len(best_stocks)}件 / Better該当: {len(better_stocks)}件"
        )
        send_slack(webhook, text)
        return
    
    # 本番モード: 該当なしでも通知
    if not best_stocks and not better_stocks:
        text = f":mag: *配当スクリーニング完了* ({now_str} JST)\n本日は該当銘柄なし"
        send_slack(webhook, text)
        return
    
    # ヘッダー
    header = (
        f":bar_chart: *配当スクリーニング結果* ({now_str} JST)\n"
        f"Best: {len(best_stocks)}件 / Better: {len(better_stocks)}件"
    )
    send_slack(webhook, header)
    time.sleep(1)
    
    # Best銘柄（10件ずつ分割）
    if best_stocks:
        for chunk_start in range(0, len(best_stocks), 10):
            chunk = best_stocks[chunk_start:chunk_start + 10]
            lines = [f"*🏆 Best ({chunk_start+1}〜{chunk_start+len(chunk)}件目)*"]
            for s in chunk:
                lines.append(
                    f"• {s['code']} {s['name']} "
                    f"現在{s['current_price']}円 / 水準{s['best_price']}円 "
                    f"利回り{s['current_yield_pct']}%"
                )
            send_slack(webhook, "\n".join(lines))
            time.sleep(1)
    
    # Better銘柄（10件ずつ分割）
    if better_stocks:
        for chunk_start in range(0, len(better_stocks), 10):
            chunk = better_stocks[chunk_start:chunk_start + 10]
            lines = [f"*✅ Better ({chunk_start+1}〜{chunk_start+len(chunk)}件目)*"]
            for s in chunk:
                lines.append(
                    f"• {s['code']} {s['name']} "
                    f"現在{s['current_price']}円 / 水準{s['better_price']}円 "
                    f"利回り{s['current_yield_pct']}%"
                )
            send_slack(webhook, "\n".join(lines))
            time.sleep(1)
    
    print("Slack通知完了")


if __name__ == "__main__":
    main()
