#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Week3疑似売買 開始時点の価格取得スクリプト。
5銘柄(MTG/シイエヌエス/ノースサンド/GO/SQUEEZE)の直近終値だけをJ-Quantsから取得し、
week3_entry_prices_YYYYMMDD.csv に出力する。

実行方法:
    python3 jquants_week3_price.py
APIキーの入力を求められるので、J-Quantsダッシュボードの「設定」→「APIキー」で
確認したキーを貼り付けてください(画面には表示されません)。
"""

import csv
import datetime
import getpass
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

API_BASE = "https://api.jquants.com/v2/equities/bars/daily"

# Week2を通過した5銘柄
TARGETS = [
    ("78060", "MTG"),
    ("40760", "シイエヌエス"),
    ("446A0", "ノースサンド"),
    ("581A0", "GO"),
    ("558A0", "SQUEEZE"),
]

MIN_REQUEST_INTERVAL_SEC = 1.5  # 5件だけなので十分余裕を持たせる
MAX_RETRIES_ON_429 = 3
DEFAULT_RETRY_WAIT_SEC = 65


def fetch_daily_bars(api_key: str, code: str, date_from: str, date_to: str):
    params = {"code": code, "from": date_from, "to": date_to}
    qs = "&".join(f"{k}={urllib.parse.quote(str(v))}" for k, v in params.items())
    url = f"{API_BASE}?{qs}"

    for attempt in range(MAX_RETRIES_ON_429 + 1):
        req = urllib.request.Request(url, headers={"x-api-key": api_key})
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                return data, None
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="ignore")
            if e.code == 429 and attempt < MAX_RETRIES_ON_429:
                retry_after = e.headers.get("Retry-After")
                wait_sec = float(retry_after) if retry_after else DEFAULT_RETRY_WAIT_SEC
                print(f"  [{code}] レート制限(429)。{wait_sec:.0f}秒待って再試行します…")
                time.sleep(wait_sec)
                continue
            return None, (e.code, body)
        except urllib.error.URLError as e:
            return None, (None, str(e))
    return None, ("retry_exhausted", "")


def main():
    print("=" * 70)
    print("Week3疑似売買: 5銘柄分の最新終値を取得します")
    print("=" * 70)
    api_key = getpass.getpass("J-Quants APIキーを入力してください(非表示): ").strip()
    if not api_key:
        print("APIキーが入力されませんでした。終了します。")
        sys.exit(1)

    today = datetime.date.today()
    date_from = (today - datetime.timedelta(days=15)).isoformat()
    date_to = today.isoformat()

    results = []
    for i, (code, name) in enumerate(TARGETS):
        if i > 0:
            time.sleep(MIN_REQUEST_INTERVAL_SEC)
        print(f"[{i + 1}/{len(TARGETS)}] {name}({code}) を取得中…")
        data, err = fetch_daily_bars(api_key, code, date_from, date_to)
        if err is not None:
            print(f"  → 取得失敗: {err}")
            results.append((code, name, None, None, f"エラー: {err}"))
            continue

        bars = data.get("daily_bars") or data.get("data") or []
        if not bars and isinstance(data, list):
            bars = data
        bars = [
            b for b in bars
            if b.get("AdjC") is not None and b.get("Date") is not None
        ]
        if not bars:
            print("  → データが見つかりませんでした")
            results.append((code, name, None, None, "データなし"))
            continue

        latest = max(bars, key=lambda b: b["Date"])
        latest_date = latest["Date"]
        latest_close = latest["AdjC"]
        print(f"  → {latest_date} 終値: {latest_close}円")
        results.append((code, name, latest_date, latest_close, "OK"))

    out_path = f"week3_entry_prices_{today.strftime('%Y%m%d')}.csv"
    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["code", "name", "date", "close", "status"])
        for row in results:
            writer.writerow(row)

    print("=" * 70)
    print(f"完了しました。結果を {out_path} に出力しました。")
    print("=" * 70)
    for code, name, date, close, status in results:
        print(f"  {name}({code}): {date} {close} [{status}]")


if __name__ == "__main__":
    main()
