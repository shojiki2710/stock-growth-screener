#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
3倍株バックテスト検証(v2): 深い履歴データでの再検証・バグ修正版
=====================================================================

背景:
  find_3x_stocks.py の compute_3x_events() には、
  「実行日からANALYSIS_WINDOW_DAYS(400日)より前の日付を機械的にスキャン
  対象外にする」ロジックがあり、これが原因で「3倍達成日」の多くが
  本来より大幅に遅い日付として記録されていた(スキャン窓の開始点を
  たまたま拾っていただけで、初回達成日ではなかった)。

  本スクリプトは、前回検出済みの63銘柄だけを対象に、株価データを
  2022年まで遡って取得し直し、足切りのない正しいロジックで
  「本当の初回3倍達成日」を再計算する。

使い方:
  1. cd ~/Documents/kabu_screening
  2. python3 find_3x_stocks_v2.py triple_events_20260904.csv
  3. J-QuantsのAPIキーを入力(画面には表示されません)
  4. 完了 or 429で止まった場合は、同じコマンドをもう一度実行してください
     (raw_cache_3x_v2/ にキャッシュ済みの銘柄は再取得されません)。

出力: triple_events_true_YYYYMMDD.csv
  (code, name, low_date, low_price, high_date, high_price, multiple,
   old_high_date, old_multiple, days_earlier)
"""

import csv
import getpass
import json
import os
import sys
import time
import urllib.request
import urllib.error
import urllib.parse
from datetime import date, datetime, timedelta

API_BASE = "https://api.jquants.com/v2/equities/bars/daily"
CACHE_DIR = "raw_cache_3x_v2"
# ここまで遡ってデータ取得する(IPOがこれより後の銘柄は、取得可能な範囲だけ返る)
FETCH_FROM_DATE = "20220101"
TIMEOUT_SEC = 30

MAX_REQUESTS_PER_MINUTE = 45
MIN_REQUEST_INTERVAL_SEC = 60.0 / MAX_REQUESTS_PER_MINUTE
MAX_RETRIES_ON_429 = 5
DEFAULT_RETRY_WAIT_SEC = 65


class RateLimiter:
    def __init__(self, min_interval_sec: float):
        self.min_interval_sec = min_interval_sec
        self.last_request_time = 0.0

    def wait(self):
        elapsed = time.monotonic() - self.last_request_time
        if elapsed < self.min_interval_sec:
            time.sleep(self.min_interval_sec - elapsed)
        self.last_request_time = time.monotonic()


limiter = RateLimiter(MIN_REQUEST_INTERVAL_SEC)


def _do_request(api_key: str, url: str):
    req = urllib.request.Request(url, headers={"x-api-key": api_key})
    limiter.wait()
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_SEC) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw), None
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="ignore")
        return None, (e.code, e.headers.get("Retry-After"), body)
    except Exception as e:
        return None, (None, None, str(e))


def fetch_daily_bars(api_key: str, code: str, date_from: str, date_to: str, log):
    rows = []
    pagination_key = None
    while True:
        params = {"code": code, "from": date_from, "to": date_to}
        if pagination_key:
            params["pagination_key"] = pagination_key
        qs = "&".join(f"{k}={urllib.parse.quote(str(v))}" for k, v in params.items())
        url = f"{API_BASE}?{qs}"

        data = None
        for attempt in range(1, MAX_RETRIES_ON_429 + 1):
            data, err = _do_request(api_key, url)
            if data is not None:
                break
            status, retry_after, body = err
            if status == 429:
                wait_sec = float(retry_after) if retry_after else DEFAULT_RETRY_WAIT_SEC
                msg = f"code={code}: 429 Too Many Requests。{wait_sec:.0f}秒待機して再試行({attempt}/{MAX_RETRIES_ON_429})"
                print(f"\n    [レート制限] {msg}", file=sys.stderr)
                log(msg)
                time.sleep(wait_sec)
                continue
            elif status in (500, 502, 503, 504):
                msg = f"code={code}: HTTP {status} 一時エラー。10秒待機して再試行({attempt}/{MAX_RETRIES_ON_429})"
                print(f"\n    [一時エラー] {msg}", file=sys.stderr)
                log(msg)
                time.sleep(10)
                continue
            else:
                msg = f"code={code}: HTTP {status} {body[:300] if body else ''}"
                print(f"\n    [HTTPエラー] {msg}", file=sys.stderr)
                log(msg)
                return rows
        else:
            msg = f"code={code}: 429が{MAX_RETRIES_ON_429}回続いたため断念"
            print(f"\n    [断念] {msg}", file=sys.stderr)
            log(msg)
            return rows

        chunk = None
        for key in ("daily_bars", "data", "bars", "quotes", "results"):
            if isinstance(data, dict) and key in data:
                chunk = data[key]
                break
        if chunk is None:
            if isinstance(data, list):
                chunk = data
            else:
                keys = list(data.keys()) if isinstance(data, dict) else type(data)
                msg = f"code={code}: 想定外のレスポンス形式です。キー一覧={keys}"
                print(f"\n    [警告] {msg}", file=sys.stderr)
                log(msg)
                return rows

        rows.extend(chunk)
        pagination_key = data.get("pagination_key") if isinstance(data, dict) else None
        if not pagination_key:
            break
    return rows


def get_field(row: dict, *keys):
    for k in keys:
        if k in row and row[k] is not None:
            return row[k]
    return None


def compute_true_3x_event(rows):
    """バグ修正版: analysis_startによる足切りをせず、直前365日分のデータが
    揃った最初の時点からスキャンして、「初めて3倍を達成した日」を返す。"""
    normalized = []
    for r in rows:
        d_raw = get_field(r, "Date", "date")
        h_raw = get_field(r, "H", "High", "high", "AdjH")
        c_raw = get_field(r, "C", "Close", "close", "AdjC")
        if d_raw is None or h_raw is None or c_raw is None:
            continue
        try:
            d = datetime.strptime(str(d_raw)[:10], "%Y-%m-%d").date()
            h = float(h_raw)
            c = float(c_raw)
        except (ValueError, TypeError):
            continue
        normalized.append((d, h, c))
    normalized.sort(key=lambda x: x[0])
    if not normalized:
        return None

    data_start = normalized[0][0]
    scan_start = data_start + timedelta(days=365)

    for (d, h, c) in normalized:
        if d < scan_start:
            continue
        window_start = d - timedelta(days=365)
        prior_closes = [(pd, pc) for (pd, ph, pc) in normalized if window_start <= pd < d]
        if not prior_closes:
            continue
        low_date, low_price = min(prior_closes, key=lambda x: x[1])
        if low_price <= 0:
            continue
        multiple = h / low_price
        if multiple >= 3.0:
            return {
                "low_date": low_date.isoformat(),
                "low_price": low_price,
                "high_date": d.isoformat(),
                "high_price": h,
                "multiple": round(multiple, 2),
                "data_start": data_start.isoformat(),
            }
    return None


def cache_path(code: str) -> str:
    return os.path.join(CACHE_DIR, f"{code}.json")


def load_cache(code: str):
    path = cache_path(code)
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return None


def save_cache(code: str, bars):
    os.makedirs(CACHE_DIR, exist_ok=True)
    with open(cache_path(code), "w", encoding="utf-8") as f:
        json.dump(bars, f, ensure_ascii=False)


def main():
    print("=" * 70)
    print("3倍株バックテスト検証(v2): 深い履歴データでの再取得・再計算")
    print("=" * 70)

    if len(sys.argv) < 2:
        print("使い方: python3 find_3x_stocks_v2.py triple_events_YYYYMMDD.csv")
        sys.exit(1)

    old_events_file = sys.argv[1]
    with open(old_events_file, encoding="utf-8-sig") as f:
        candidates = list(csv.DictReader(f))

    api_key = getpass.getpass("J-Quants APIキーを入力してください(入力内容は表示されません): ").strip()
    if not api_key:
        print("APIキーが空のため終了します。")
        sys.exit(1)

    today = date.today()
    date_to = today.strftime("%Y%m%d")
    date_from = FETCH_FROM_DATE

    print(f"対象銘柄数: {len(candidates)}件(前回検出済みの3倍株のみ)")
    print(f"取得期間: {date_from} 〜 {date_to}(2022年まで遡って再取得)")
    print(f"レート制限: {MAX_REQUESTS_PER_MINUTE}リクエスト/分\n")

    error_log_path = "errors_3x_v2.log"
    error_log_f = open(error_log_path, "a", encoding="utf-8")

    def log(msg):
        error_log_f.write(f"{datetime.now().isoformat()} {msg}\n")
        error_log_f.flush()

    newly_fetched = 0
    already_cached = 0
    failed_codes = []

    for idx, row in enumerate(candidates, 1):
        code = row["code"].strip()
        name = row.get("name", "").strip()

        cached = load_cache(code)
        if cached is not None:
            already_cached += 1
            print(f"[{idx}/{len(candidates)}] {code} {name} ... キャッシュ済み(スキップ)")
            continue

        print(f"[{idx}/{len(candidates)}] 取得中: {code} {name} ...", end=" ", flush=True)
        bars = fetch_daily_bars(api_key, code, date_from, date_to, log)
        if not bars:
            print("データなし")
            failed_codes.append(code)
            continue

        save_cache(code, bars)
        newly_fetched += 1
        print(f"取得完了(データ点数={len(bars)}件)、キャッシュに保存")

    error_log_f.close()

    results = []
    for row in candidates:
        code = row["code"].strip()
        name = row.get("name", "").strip()
        old_high_date = row.get("high_date", "")
        old_multiple = row.get("multiple", "")

        bars = load_cache(code)
        if not bars:
            continue
        event = compute_true_3x_event(bars)
        if not event:
            print(f"[注意] {code} {name}: 深い履歴でも3倍達成イベントを検出できませんでした")
            continue

        days_earlier = ""
        try:
            old_d = datetime.strptime(old_high_date, "%Y-%m-%d").date()
            new_d = datetime.strptime(event["high_date"], "%Y-%m-%d").date()
            days_earlier = (old_d - new_d).days
        except (ValueError, TypeError):
            pass

        results.append({
            "code": code,
            "name": name,
            "low_date": event["low_date"],
            "low_price": event["low_price"],
            "high_date": event["high_date"],
            "high_price": event["high_price"],
            "multiple": event["multiple"],
            "old_high_date": old_high_date,
            "old_multiple": old_multiple,
            "days_earlier": days_earlier,
        })

    output_file = f"triple_events_true_{today:%Y%m%d}.csv"
    with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "code", "name", "low_date", "low_price", "high_date", "high_price",
            "multiple", "old_high_date", "old_multiple", "days_earlier"
        ])
        writer.writeheader()
        for r in sorted(results, key=lambda x: -(x["days_earlier"] if isinstance(x["days_earlier"], int) else 0)):
            writer.writerow(r)

    still_missing = [row["code"].strip() for row in candidates if load_cache(row["code"].strip()) is None]

    print("\n" + "=" * 70)
    print(f"今回新規取得: {newly_fetched}件 / キャッシュ済みでスキップ: {already_cached}件 / 今回失敗: {len(failed_codes)}件")
    print(f"再計算完了: {len(results)}件 → {output_file} に出力しました。")
    if results:
        avg_earlier = sum(r["days_earlier"] for r in results if isinstance(r["days_earlier"], int)) / max(1, len(results))
        print(f"旧high_dateとの平均ズレ: 約{avg_earlier:.0f}日")
    if still_missing:
        print(f"\n[注意] 以下の{len(still_missing)}銘柄はまだデータ未取得です。"
              f"同じコマンドをもう一度実行すると続きから取得します:")
        print(f"  {', '.join(still_missing)}")
        print(f"\n詳細なエラー内容は {error_log_path} を確認してください。")
    else:
        print("\n全銘柄のデータ取得・再計算が完了しました。")
    print("=" * 70)


if __name__ == "__main__":
    main()
