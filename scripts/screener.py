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
    """J-QuantsのAPIトークンを取得"""
    email    = os.environ["JQUANTS_EMAIL"]
    password = os.environ["JQUANTS_PASSWORD"]

    # リフレッシュトークン取得
    resp = requests.post(
        "https://api.jquants.com/v1/token/auth_user",
        json={"mailaddress": email, "password": password},
        timeout=30
    )
    resp.raise_for_status()
    refresh_token = resp.json()["refreshToken"]

    # IDトークン取得
    resp2 = requests.post(
        "https://api.jquants.com/v1/token/auth_refresh",
        params={"refreshtoken": refresh_token},
        timeout=30
    )
    resp2.raise_for_status()
    id_token = resp2.json()["idToken"]
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
    df = pd.DataFrame(resp.​​​​​​​​​​​​​​​​
