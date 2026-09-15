#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
過去1年で株価が3倍になった銘柄の検出バッチ(東証グロース市場、検証用)
=====================================================================

目的:
  growth_universe.csv に列挙した東証グロース市場の全銘柄(EDINET DBで市場時価総額が
  取得できた399銘柄)について、J-Quants APIから実際の日次四本値を取得し、
  過去1年程度の期間内で「その日の高値が、直近365日以内の安値(終値ベース)の
  3倍以上になった日」を機械的に検出する。

  「銘柄選定プロセスの正当性検証」のために使う: 過去に実際に3倍になった銘柄を
  ファネル(フェーズ1〜3)にかけて、選定できていたかを後から確認する。

使い方:
  1. cd ~/Documents/kabu_screening
  2. python3 find_3x_stocks.py
  3. J-QuantsのAPIキーを入力(画面には表示されません)
  4. 完了 or 429で止まった場合は、同じコマンドをもう一度実行してください
     (raw_cache_3x/ にキャッシュ済みの銘柄は再取得されません)。

出力: triple_events_YYYYMMDD.csv (code, name, low_date, low_price, high_date, high_price, multiple)
  = 銘柄ごとに「最初に3倍を達成した日」1件のみを出力します(複数回3倍を繰り返す銘柄は最初の1回のみ)。

依存ライブラリ: 標準ライブラリのみ。Python 3.8以上を想定。
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
CANDIDATES_FILE = "growth_universe.csv"
CACHE_DIR = "raw_cache_3x"

# 解析対象期間: 直近この日数分について「3倍を達成した日」を洗い出す
ANALYSIS_WINDOW_DAYS = 400
# 3倍判定のための追加バッファ(解析開始日より前の365日分の履歴も取得しておく必要がある)
ROLLING_WINDOW_DAYS = 370
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


def compute_3x_events(rows, analysis_start: date):
    """日次バーのリストから「初めて3倍(高値/直近365日の終値最安値)を達成した日」を1件だけ抽出する"""
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

    for (d, h, c) in normalized:
        if d < analysis_start:
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
    print("J-Quants 日次株価取得 & 3倍株イベント検出バッチ(検証用)")
    print("=" * 70)

    api_key = getpass.getpass("J-Quants APIキーを入力してください(入力内容は表示されません): ").strip()
    if not api_key:
        print("APIキーが空のため終了します。")
        sys.exit(1)

    try:
        with open(CANDIDATES_FILE, encoding="utf-8-sig") as f:
            candidates = list(csv.DictReader(f))
    except FileNotFoundError:
        print(f"[エラー] {CANDIDATES_FILE} が見つかりません。"
              f"find_3x_stocks.py と同じフォルダに置いてください。")
        sys.exit(1)

    print(f"対象銘柄数: {len(candidates)}件")
    print(f"レート制限: {MAX_REQUESTS_PER_MINUTE}リクエスト/分に抑えて実行します"
          f"({len(candidates)}件なら約{len(candidates)/MAX_REQUESTS_PER_MINUTE:.0f}分)\n")

    today = date.today()
    date_to = today.strftime("%Y%m%d")
    date_from = (today - timedelta(days=ANALYSIS_WINDOW_DAYS + ROLLING_WINDOW_DAYS)).strftime("%Y%m%d")
    analysis_start = today - timedelta(days=ANALYSIS_WINDOW_DAYS)

    print(f"取得期間: {date_from} 〜 {date_to}")
    print(f"3倍達成の判定対象期間: {analysis_start.isoformat()} 〜 {today.isoformat()}\n")

    error_log_path = "errors_3x.log"
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

    all_events = []
    total_with_data = 0
    for row in candidates:
        code = row["code"].strip()
        name = row.get("name", "").strip()
        bars = load_cache(code)
        if not bars:
            continue
        total_with_data += 1
        event = compute_3x_events(bars, analysis_start)
        if event:
            all_events.append({"code": code, "name": name, **event})

    output_file = f"triple_events_{today:%Y%m%d}.csv"
    with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["code", "name", "low_date", "low_price", "high_date", "high_price", "multiple"])
        writer.writeheader()
        for e in sorted(all_events, key=lambda x: -x["multiple"]):
            writer.writerow(e)

    still_missing = [row["code"].strip() for row in candidates if load_cache(row["code"].strip()) is None]

    print("\n" + "=" * 70)
    print(f"今回新規取得: {newly_fetched}件 / キャッシュ済みでスキップ: {already_cached}件 / 今回失敗: {len(failed_codes)}件")
    print(f"累計データ保有銘柄数: {total_with_data}/{len(candidates)}件")
    print(f"3倍達成イベント合計: {len(all_events)}件 → {output_file} に出力しました。")
    if still_missing:
        print(f"\n[注意] 以下の{len(still_missing)}銘柄はまだデータ未取得です。"
              f"同じコマンドをもう一度実行すると続きから取得します:")
        print(f"  {', '.join(still_missing)}")
        print(f"\n詳細なエラー内容は {error_log_path} を確認してください。")
    else:
        print("\n全銘柄のデータ取得が完了しました。")
    print("=" * 70)


if __name__ == "__main__":
    main()
