import yfinance as yf
import pandas as pd
import requests
import json
import os
import time
from datetime import datetime, timedelta
import pytz

JST = pytz.timezone("Asia/Tokyo")

# Test mode
TEST_MODE = True
TEST_CODES = ["7203"]   # トヨタだけ
TEST_NAMES = ["Toyota"]

# Filter and history
MIN_YIELD = 0.030
YEARS_BACK = 3

# Nikkei 225 codes (4-digit)
NIKKEI_225 = [
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


def analyze_stock(code, name):
    try:
        ticker = yf.Ticker(code + ".T")
        info = ticker.info
        
        current_price = info.get("currentPrice", None)
        if not current_price:
            return None, "no_price"
        cp = float(current_price)
        
        forecast_div = info.get("dividendRate", None)
        if not forecast_div:
            forecast_div = info.get("trailingAnnualDividendRate", None)
        if not forecast_div or forecast_div <= 0:
            return None, "no_div"
        forecast_div = float(forecast_div)
        
        current_yield = forecast_div / cp
        
        if current_yield < MIN_YIELD:
            return None, "low_yield"
        
        divs = ticker.dividends
        if len(divs) == 0:
            return None, "no_div_hist"
        
        annual_div = divs.groupby(divs.index.year).sum()
        
        end_d = datetime.now()
        start_d = end_d - timedelta(days=YEARS_BACK * 366)
        hist = ticker.history(start=start_d.strftime("%Y-%m-%d"),
                               end=end_d.strftime("%Y-%m-%d"),
                               auto_adjust=False)
        if len(hist) == 0:
            return None, "no_price_hist"
        
        hist_year = hist.copy()
        hist_year["Year"] = hist_year.index.year
        year_avg_price = hist_year.groupby("Year")["Close"].mean()
        
        target_years = sorted([y for y in annual_div.index if y in year_avg_price.index])
        target_years = target_years[-YEARS_BACK:]
        
        annual_yields = []
        for y in target_years:
            div_val = float(annual_div[y])
            avg_p = float(year_avg_price[y])
            if div_val > 0 and avg_p > 0:
                annual_yields.append(div_val / avg_p)
        
        if len(annual_yields) == 0:
            return None, "no_yields"
        
        max_y = max(annual_yields)
        avg_y = sum(annual_yields) / len(annual_yields)
        
        best_p = forecast_div / max_y
        better_p = forecast_div / avg_y
        
        level = None
        if current_yield > max_y:
            level = "Premium"
        elif cp <= best_p:
            level = "Best"
        elif cp <= better_p:
            level = "Better"
        
        result = {
            "code": code,
            "name": name,
            "current_price": round(cp, 1),
            "annual_div": round(forecast_div, 1),
            "current_yield_pct": round(current_yield * 100, 2),
            "max_yield_pct": round(max_y * 100, 2),
            "avg_yield_pct": round(avg_y * 100, 2),
            "best_price": round(best_p, 1),
            "better_price": round(better_p, 1),
            "vs_best_pct": round((cp / best_p - 1) * 100, 1),
            "vs_better_pct": round((cp / better_p - 1) * 100, 1),
            "level": level,
        }
        return result, "ok"
    except Exception as e:
        import traceback
        print("ERR " + code + ":")
        print(traceback.format_exc())
        return None, "error"


def send_slack(webhook, text):
    try:
        resp = requests.post(webhook, json={"text": text}, timeout=10)
        print("Slack status: " + str(resp.status_code) + ", len=" + str(len(text)))
        if resp.status_code != 200:
            print("Slack error: " + resp.text[:300])
        return resp.status_code == 200
    except Exception as e:
        print("Slack failed: " + str(e))
        return False


def main():
    print("=" * 50)
    print("Stock Screener (Yahoo) - TEST=" + str(TEST_MODE))
    print("=" * 50)
    
    webhook_env = os.environ.get("SLACK_WEBHOOK_URL", "")
    print("DEBUG webhook length: " + str(len(webhook_env)))
    
    if TEST_MODE:
        codes = TEST_CODES
        names = TEST_NAMES
    else:
        codes = NIKKEI_225
        names = NIKKEI_225  # use code as name (Yahoo info has names anyway)
    
    print("Target: " + str(len(codes)) + " stocks")
    print("Filter: min yield " + str(MIN_YIELD*100) + "%, " + str(YEARS_BACK) + "yr history")
    print("")
    
    premium_stocks = []
    best_stocks = []
    better_stocks = []
    stats = {"ok": 0, "no_price": 0, "no_div": 0, "low_yield": 0,
             "no_div_hist": 0, "no_price_hist": 0, "no_yields": 0, "error": 0}
    
    for i, (code, name) in enumerate(zip(codes, names)):
        # Get Yahoo company name on the fly if name == code
        result, status = analyze_stock(code, name)
        
        # Try to get the real company name from Yahoo
        if result:
            try:
                ticker = yf.Ticker(code + ".T")
                short_name = ticker.info.get("shortName", code)
                result["name"] = short_name
            except:
                pass
        
        if status == "ok":
            stats["ok"] += 1
            if result and result["level"]:
                if result["level"] == "Premium":
                    premium_stocks.append(result)
                elif result["level"] == "Best":
                    best_stocks.append(result)
                elif result["level"] == "Better":
                    better_stocks.append(result)
        elif status.startswith("error"):
            stats["error"] += 1
        else:
            stats[status] = stats.get(status, 0) + 1
        
        if (i + 1) % 25 == 0:
            print("[" + str(i+1) + "/" + str(len(codes)) + "] Premium=" + str(len(premium_stocks)) +
                  " Best=" + str(len(best_stocks)) + " Better=" + str(len(better_stocks)))
        
        time.sleep(5)
    
    print("")
    print("=" * 50)
    print("Stats: " + str(stats))
    print("Hits: Premium=" + str(len(premium_stocks)) + " Best=" + str(len(best_stocks)) + " Better=" + str(len(better_stocks)))
    
    os.makedirs("docs", exist_ok=True)
    payload = {
        "updated_at": datetime.now(JST).isoformat(),
        "premium_stocks": sorted(premium_stocks, key=lambda x: -x["current_yield_pct"]),
        "best_stocks": sorted(best_stocks, key=lambda x: x["vs_best_pct"]),
        "better_stocks": sorted(better_stocks, key=lambda x: x["vs_better_pct"]),
    }
    with open("docs/results.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print("Saved")
    
    webhook = webhook_env
    if not webhook:
        print("No webhook")
        return
    
    now_str = datetime.now(JST).strftime("%Y/%m/%d %H:%M")
    
    if TEST_MODE:
        text = ":test_tube: Yahoo test done (" + now_str + " JST)\n"
        text += "Target: " + str(len(codes)) + " stocks\n"
        text += "Premium: " + str(len(premium_stocks)) + " / Best: " + str(len(best_stocks)) + " / Better: " + str(len(better_stocks))
        send_slack(webhook, text)
        return
    
    if not premium_stocks and not best_stocks and not better_stocks:
        text = ":mag: Stock Screener done (" + now_str + " JST)\nNo hits today"
        send_slack(webhook, text)
        return
    
    header = ":bar_chart: *Stock Screener Result* (" + now_str + " JST)\n"
    header += "Premium: " + str(len(premium_stocks)) + " / Best: " + str(len(best_stocks)) + " / Better: " + str(len(better_stocks))
    send_slack(webhook, header)
    time.sleep(1)
    
    if premium_stocks:
        for chunk_start in range(0, len(premium_stocks), 10):
            chunk = premium_stocks[chunk_start:chunk_start + 10]
            lines = ["*:gem: Premium (" + str(chunk_start+1) + "-" + str(chunk_start+len(chunk)) + ")*"]
            for s in chunk:
                lines.append("- " + s["code"] + " " + s["name"] +
                             " price=" + str(s["current_price"]) +
                             " yield=" + str(s["current_yield_pct"]) + "% (max " + str(s["max_yield_pct"]) + "%)")
            send_slack(webhook, "\n".join(lines))
            time.sleep(1)
    
    if best_stocks:
        for chunk_start in range(0, len(best_stocks), 10):
            chunk = best_stocks[chunk_start:chunk_start + 10]
            lines = ["*:trophy: Best (" + str(chunk_start+1) + "-" + str(chunk_start+len(chunk)) + ")*"]
            for s in chunk:
                lines.append("- " + s["code"] + " " + s["name"] +
                             " price=" + str(s["current_price"]) + " level=" + str(s["best_price"]) +
                             " yield=" + str(s["current_yield_pct"]) + "%")
            send_slack(webhook, "\n".join(lines))
            time.sleep(1)
    
    if better_stocks:
        for chunk_start in range(0, len(better_stocks), 10):
            chunk = better_stocks[chunk_start:chunk_start + 10]
            lines = ["*:white_check_mark: Better (" + str(chunk_start+1) + "-" + str(chunk_start+len(chunk)) + ")*"]
            for s in chunk:
                lines.append("- " + s["code"] + " " + s["name"] +
                             " price=" + str(s["current_price"]) + " level=" + str(s["better_price"]) +
                             " yield=" + str(s["current_yield_pct"]) + "%")
            send_slack(webhook, "\n".join(lines))
            time.sleep(1)
    
    print("Slack done")


if __name__ == "__main__":
    main()
