import pandas as pd
import requests
import json
import os
import io
import re
import time
from datetime import datetime, timedelta
import pytz

JST = pytz.timezone('Asia/Tokyo')


def get_prime_market_stocks():
    url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    df = pd.read_excel(io.BytesIO(resp.content))
    market_col = '市場・商品区分'
    code_col   = 'コード'
    name_col   = '銘柄名'
    prime_df = df[df[market_col].astype(str).str.contains("プライム", na=False)]
    result = prime_df[[code_col, name_col]].copy()
    result[code_col] = result[code_col].astype(str).str.zfill(4)
    codes_names = list(zip(result[code_col].tolist(), result[name_col].tolist()))
    print(f"プライム市場銘柄数: {len(codes_names)}")
    return codes_names


def fetch_price_history(code: str):
    """stooq.comから5年分の日次株価を取得"""
    end_d   = datetime.now().strftime("%Y%m%d")
    start_d = (datetime.now() - timedelta(days=5 * 366)).strftime("%Y%m%d")
    url = f"https://stooq.com/q/d/l/?s={code}.jp&d1={start_d}&d2={end_d}&i=d"
    try:
        resp = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        df = pd.read_csv(io.StringIO(resp.text), parse_dates=["Date"])
        df = df.dropna(subset=["Close"]).sort_values("Date").set_index("Date")
        return df if len(df) >= 50 else None
    except Exception:
        return None


def fetch_annual_dividends(code: str) -> dict:
    """irbank.netから年間配当実績 {year: yen} を取得"""
    url = f"https://irbank.net/{code}/div"
    try:
        resp = requests.get(
            url, timeout=15,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        )
        tables = pd.read_html(io.StringIO(resp.text))
        result = {}
        for t in tables:
            cols_str = " ".join(str(c) for c in t.columns)
            if "配当" not in cols_str and "Dividend" not in cols_str:
                continue
            for _, row in t.iterrows():
                try:
                    period = str(row.iloc[0])
                    m = re.search(r'(\d{4})', period)
                    if not m:
                        continue
                    year = int(m.group(1))
                    if not (2010 <= year <= datetime.now().year):
                        continue
                    div_val = None
                    for val in row.values[1:]:
                        try:
                            v = float(str(val).replace(',', '').replace('円', '').strip())
                            if 0 < v < 100000:
                                div_val = v
                        except Exception:
                            continue
                    if div_val is not None:
                        result[year] = div_val
                except Exception:
                    continue
            if result:
                return result
        return {}
    except Exception:
        return {}


def analyze_stock(code: str, name: str):
    # 株価履歴取得
    hist = fetch_price_history(code)
    if hist is None:
        return None

    current_price = float(hist["Close"].iloc[-1])
    if current_price <= 0:
        return None

    # 配当履歴取得
    div_data = fetch_annual_dividends(code)
    if not div_data:
        return None

    # 5年間の年次利回り計算
    now_year = datetime.now(JST).year
    annual_yields = []
    for year in range(now_year - 5, now_year):
        yr_div = div_data.get(year)
        if not yr_div or yr_div <= 0:
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

    # 直近1年の配当（今期予想 or 直近実績）
    recent_div = div_data.get(now_year - 1) or div_data.get(now_year)
    if not recent_div or recent_div <= 0:
        return None

    # 買い水準計算
    best_price   = recent_div / max_yield
    better_price = recent_div / avg_yield

    return {
        "code":              code,
        "name":              name,
        "current_price":     round(current_price, 1),
        "annual_div":        round(recent_div, 1),
        "current_yield_pct": round(recent_div / current_price * 100, 2),
        "max_yield_5y_pct":  round(max_yield * 100, 2),
        "avg_yield_5y_pct":  round(avg_yield * 100, 2),
        "best_price":        round(best_price, 1),
        "better_price":      round(better_price, 1),
        "vs_best_pct":       round((current_price / best_price - 1) * 100, 1),
        "vs_better_pct":     round((current_price / better_price - 1) * 100, 1),
    }


def run_screening(codes_names: list):
    best_stocks   = []
    better_stocks = []
    total = len(codes_names)

    for i, (code, name) in enumerate(codes_names):
        data = analyze_stock(code, name)
        if data:
            cp = data["current_price"]
            if cp <= data["best_price"]:
                best_stocks.append({**data, "level": "Best"})
            elif cp <= data["better_price"]:
                better_stocks.append({**data, "level": "Better"})

        if (i + 1) % 50 == 0:
            print(f"  進捗: {i+1}/{total} | Best={len(best_stocks)} Better={len(better_stocks)}")
        time.sleep(0.5)

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

    # ─── デバッグ用テスト ───
    print("=== テスト: トヨタ(7203) ===")
    hist = fetch_price_history("7203")
    print(f"株価データ: {type(hist)} / {len(hist) if hist is not None else 'None'}")
    if hist is not None:
        print(f"最新株価: {hist['Close'].iloc[-1]}")
    div = fetch_annual_dividends("7203")
    print(f"配当データ: {div}")
    print("=== テスト終了 ===")
    # ───────────────────────

    try:
        codes_names = get_prime_market_stocks()
    except Exception as e:
        print(f"銘柄リスト取得失敗: {e} → フォールバック使用")
        codes_names = [
            ("8306","三菱UFJ"),("8316","三井住友FG"),("9432","NTT"),
            ("7203","トヨタ"),("6758","ソニー"),("8058","三菱商事"),
        ]

    best, better = run_screening(codes_names)
    save_results(best, better)

    webhook = os.environ.get("SLACK_WEBHOOK_URL", "")
    if webhook:
        send_slack(best, better, webhook)
    else:
        print("SLACK_WEBHOOK_URL未設定 → 通知スキップ")


if __name__ == "__main__":
    main()
