"""
配当利回りスクリーナー
- プライム市場銘柄を対象に5年間の最大・平均配当利回りを算定
- Best水準: 現在株価 < 年間配当見込 ÷ 5年最大利回り
- Better水準: 現在株価 < 年間配当見込 ÷ 5年平均利回り
"""

import yfinance as yf
import pandas as pd
import requests
import json
import os
import time
import io
from datetime import datetime, timedelta
import pytz

JST = pytz.timezone('Asia/Tokyo')


# ─────────────────────────────────────────────
# 1. プライム市場銘柄リスト取得 (JPX公式Excel)
# ─────────────────────────────────────────────
def get_prime_market_stocks():
    url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()

    df = pd.read_excel(io.BytesIO(resp.content))

    # デバッグ用：列名を出力
    print(f"列名一覧: {df.columns.tolist()}")
    print(df.head(3).to_string())

    # 列名で市場列・コード列を検索（列順に依存しない）
    market_col = None
    code_col   = None
    for col in df.columns:
        col_str = str(col)
        if "市場" in col_str or "区分" in col_str:
            market_col = col
        if "コード" in col_str or "code" in col_str.lower():
            code_col = col

    # 列名で見つからない場合は位置で指定（JPX標準フォーマット）
    if market_col is None:
        market_col = df.columns[3]
    if code_col is None:
        code_col = df.columns[1]

    print(f"市場列: {market_col} / コード列: {code_col}")

    prime_df = df[df[market_col].astype(str).str.contains("プライム", na=False)]
    codes = prime_df[code_col].astype(str).str.zfill(4).tolist()
    print(f"プライム市場銘柄数: {len(codes)}")
    return codes


# ─────────────────────────────────────────────
# 2. 個別銘柄データ取得・利回り計算
# ─────────────────────────────────────────────
def analyze_stock(code: str) -> dict | None:
    ticker_str = f"{code}.T"
    tk = yf.Ticker(ticker_str)
    now = datetime.now(JST)

    try:
        hist = tk.history(
            start=(now - timedelta(days=5 * 366)).strftime("%Y-%m-%d"),
            end=now.strftime("%Y-%m-%d"),
            auto_adjust=True,
        )
        dividends = tk.dividends
    except Exception as e:
        print(f"  [{code}] データ取得失敗: {e}")
        return None

    if hist.empty or dividends.empty:
        return None

    # タイムゾーン統一
    if hist.index.tzinfo is not None:
        hist.index = hist.index.tz_convert(JST)
    if dividends.index.tzinfo is not None:
        dividends.index = dividends.index.tz_convert(JST)

    # ── 5年間の年次利回り計算 ──
    annual_yields = []
    for year in range(now.year - 5, now.year):
        yr_div = dividends[dividends.index.year == year].sum()
        if yr_div <= 0:
            continue
        yr_prices = hist[hist.index.year == year]["Close"]
        if yr_prices.empty:
            continue
        avg_price = yr_prices.mean()
        if avg_price > 0:
            annual_yields.append(float(yr_div) / float(avg_price))

    if not annual_yields:
        return None

    max_yield = max(annual_yields)
    avg_yield = sum(annual_yields) / len(annual_yields)

    # ── 現在の年間配当見込（直近1年の実績を使用）──
    # ※ EDINETのAPIキー取得後はここを予想配当に差し替え可能
    one_yr_ago = now - timedelta(days=365)
    recent_div = float(dividends[dividends.index >= one_yr_ago].sum())
    if recent_div <= 0:
        # 直近2年分÷2でフォールバック
        two_yr_ago = now - timedelta(days=730)
        recent_div = float(dividends[dividends.index >= two_yr_ago].sum()) / 2
    if recent_div <= 0:
        return None

    current_price = float(hist["Close"].iloc[-1])
    if current_price <= 0:
        return None

    # ── 買い水準計算 ──
    best_price   = recent_div / max_yield    # 5年最大利回りベース
    better_price = recent_div / avg_yield    # 5年平均利回りベース

    # 銘柄名取得（失敗してもコードで代替）
    try:
        name = tk.info.get("longName") or tk.info.get("shortName") or code
    except Exception:
        name = code

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


# ─────────────────────────────────────────────
# 3. スクリーニング実行
# ─────────────────────────────────────────────
def run_screening(codes: list, limit: int | None = None):
    if limit:
        codes = codes[:limit]

    best_stocks   = []
    better_stocks = []

    for i, code in enumerate(codes):
        data = analyze_stock(code)
        if data:
            cp = data["current_price"]
            if cp <= data["best_price"]:
                best_stocks.append({**data, "level": "Best"})
            elif cp <= data["better_price"]:
                better_stocks.append({**data, "level": "Better"})

        if (i + 1) % 50 == 0:
            print(f"  進捗: {i+1}/{len(codes)} "
                  f"| Best={len(best_stocks)} Better={len(better_stocks)}")
        time.sleep(0.4)   # yfinanceのレート制限対策

    return best_stocks, better_stocks


# ─────────────────────────────────────────────
# 4. Slack通知
# ─────────────────────────────────────────────
def send_slack(best: list, better: list, webhook_url: str):
    if not best and not better:
        print("該当銘柄なし → Slack通知スキップ")
        return

    now_str = datetime.now(JST).strftime("%Y/%m/%d %H:%M")

    def format_stocks(stocks, label, yield_key, price_key):
        if not stocks:
            return ""
        lines = [f"*{label} ({len(stocks)}件)*"]
        for s in stocks[:15]:
            gap = s["vs_best_pct"] if "Best" in label else s["vs_better_pct"]
            lines.append(
                f"• *{s['code']} {s['name']}*  "
                f"株価 {s['current_price']}円  "
                f"水準 {s[price_key]}円  "
                f"({gap:+.1f}%)  "
                f"現在利回り {s['current_yield_pct']}%  "
                f"5年{yield_key} {s[yield_key+'_pct']}%"
            )
        if len(stocks) > 15:
            lines.append(f"  …他{len(stocks)-15}件（サイト参照）")
        return "\n".join(lines)

    text = "\n\n".join(filter(None, [
        f":bar_chart: *配当スクリーニング結果* ({now_str} JST)",
        format_stocks(best,   "🏆 Best買い水準",   "max_yield_5y",  "best_price"),
        format_stocks(better, "✅ Better買い水準", "avg_yield_5y", "better_price"),
    ]))

    payload = {"text": text}
    resp = requests.post(webhook_url, json=payload, timeout=10)
    print(f"Slack送信 → HTTP {resp.status_code}")


# ─────────────────────────────────────────────
# 5. 結果保存 (docs/results.json → GitHub Pages)
# ─────────────────────────────────────────────
def save_results(best: list, better: list):
    os.makedirs("docs", exist_ok=True)
    payload = {
        "updated_at": datetime.now(JST).isoformat(),
        "best_stocks":   sorted(best,   key=lambda x: x["vs_best_pct"]),
        "better_stocks": sorted(better, key=lambda x: x["vs_better_pct"]),
    }
    with open("docs/results.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"保存完了 → Best={len(best)} Better={len(better)}")


# ─────────────────────────────────────────────
# メイン
# ─────────────────────────────────────────────
def main():
    print("=" * 50)
    print("配当利回りスクリーナー 起動")
    print("=" * 50)

    try:
        codes = get_prime_market_stocks()
    except Exception as e:
        print(f"銘柄リスト取得失敗: {e} → フォールバックリストを使用")
        # フォールバック: 代表的な高配当銘柄
        codes = [
            "8306","8316","8411","9432","9433","9984","7203","6758",
            "4502","4503","8058","8031","8001","5020","5019","1605",
        ]

    # ★ 全銘柄対象: limit=None
    # ★ テスト用:   limit=100 など
    best, better = run_screening(codes, limit=None)

    save_results(best, better)

    webhook = os.environ.get("SLACK_WEBHOOK_URL", "")
    if webhook:
        send_slack(best, better, webhook)
    else:
        print("SLACK_WEBHOOK_URL未設定 → 通知スキップ")


if __name__ == "__main__":
    main()
