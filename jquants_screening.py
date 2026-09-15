#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
J-Quants Light 日次株価取得 & 「実際の」52週新高値イベント検出バッチスクリプト (v2)
=====================================================================

目的:
  candidates.csv に列挙した銘柄(EDINET DBの一次スクリーニング通過済み: 東証グロース,
  増収10%以上・増益20%以上・ROE10%以上)について、J-Quants APIから実際の日次四本値
  (High/Low/Open/Close)を取得し、各営業日ごとに「その日のHighが、直近365日以内の
  Highの最大値を実際に上回ったか(=52週新高値を更新したか)」を機械的に判定します。

  近似(現在値が高値からX%以内、など)は一切使わず、実際の日次終値・高値の履歴データ
  のみから判定します。

v2での変更点(v1で発生した問題への対応):
  - J-Quants Lightプランは「1分あたり60リクエストまで」の制限があります。v1は
    リクエスト間隔が短すぎ(0.3秒間隔)、この制限に触れた後、制限が解除されないまま
    残り全銘柄が失敗し続けていました。v2ではリクエスト間隔を安全マージン込みで
    自動調整し、429(Too Many Requests)を検知したら自動的に待って再試行します。
  - 取得できた銘柄の生データを raw_cache/ フォルダにキャッシュします。再実行時は
    キャッシュ済みの銘柄をスキップし、「まだ取れていない銘柄」だけを取得しにいく
    ため、APIクォータと時間を無駄にしません。
  - 最終的な new_highs_*.csv は、今回の実行結果だけでなく raw_cache/ に保存済みの
    全銘柄分を集計して作成します(=複数回に分けて実行しても最後にまとめて正しい
    結果が得られます)。

重要:
  このスクリプトはJ-Quants APIへの実際のネットワーク接続が必要です。Claude(Cowork)側の
  クラウド環境・Macブリッジ経由のシェルはプロキシでAPIドメインへの接続がブロックされて
  いるため実行できません。必ずお使いのMacの「本物のTerminal.app」で実行してください。

使い方:
  1. このフォルダにTerminalでcdする: cd ~/Documents/kabu_screening
  2. 実行: python3 jquants_screening.py
  3. J-QuantsのAPIキーを入力(画面には表示されません)
  4. 完了 or 429で止まった場合は、そのまま同じコマンドをもう一度実行してください。
     キャッシュ済みの銘柄は再取得されず、残りだけを続きから取得します。

出力: new_highs_YYYYMMDD.csv (code, name, date, high, prior_52w_high)

依存ライブラリ: 標準ライブラリのみ(追加インストール不要)。Python 3.8以上を想定。
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
CANDIDATES_FILE = "candidates.csv"
CACHE_DIR = "raw_cache"

# 解析対象期間: 直近この日数分について「新高値を更新した日」を洗い出す
ANALYSIS_WINDOW_DAYS = 400
# 52週判定のための追加バッファ(解析開始日より前の365日分の履歴も取得しておく必要がある)
ROLLING_WINDOW_DAYS = 370
TIMEOUT_SEC = 30

# --- レート制限まわりの設定 ---
# J-Quants Lightプランは60リクエスト/分。安全マージンを見て45リクエスト/分に抑える。
MAX_REQUESTS_PER_MINUTE = 45
MIN_REQUEST_INTERVAL_SEC = 60.0 / MAX_REQUESTS_PER_MINUTE  # ≒1.33秒間隔
MAX_RETRIES_ON_429 = 5
DEFAULT_RETRY_WAIT_SEC = 65  # Retry-Afterヘッダがない場合の待機時間(1分の窓を確実に跨ぐ)


class RateLimiter:
    """直近リクエスト時刻を記録し、間隔が短すぎる場合は待機する簡易レートリミッタ"""

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
    """1回分のHTTPリクエストを実行し、成功時はdictを返す。429/5xxはNoneを返し呼び出し側で再試行させる。"""
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
    """指定コードの日次四本値を全ページ分取得して返す(list of dict)。失敗時は空リスト。"""
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
                msg = (f"code={code}: 429 Too Many Requests。{wait_sec:.0f}秒待機して再試行"
                       f"({attempt}/{MAX_RETRIES_ON_429})")
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
    """レスポンスのフィールド名の揺れを吸収するヘルパー"""
    for k in keys:
        if k in row and row[k] is not None:
            return row[k]
    return None


def compute_new_highs(rows, analysis_start: date):
    """日次バーのリストから「実際に52週新高値を更新した日」を抽出する"""
    normalized = []
    for r in rows:
        d_raw = get_field(r, "Date", "date")
        h_raw = get_field(r, "H", "High", "high", "AdjH")
        if d_raw is None or h_raw is None:
            continue
        try:
            d = datetime.strptime(str(d_raw)[:10], "%Y-%m-%d").date()
            h = float(h_raw)
        except (ValueError, TypeError):
            continue
        normalized.append((d, h))

    normalized.sort(key=lambda x: x[0])

    events = []
    for i, (d, h) in enumerate(normalized):
        if d < analysis_start:
            continue
        window_start = d - timedelta(days=365)
        prior_highs = [ph for (pd, ph) in normalized if window_start <= pd < d]
        if not prior_highs:
            continue
        prior_52w_high = max(prior_highs)
        if h > prior_52w_high:
            events.append({
                "date": d.isoformat(),
                "high": h,
                "prior_52w_high": prior_52w_high,
            })
    return events, len(normalized)


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
    print("J-Quants 日次株価取得 & 52週新高値イベント検出バッチ (v2: レート制限対応)")
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
              f"jquants_screening.py と同じフォルダに置いてください。")
        sys.exit(1)

    print(f"対象銘柄数: {len(candidates)}件")
    print(f"レート制限: {MAX_REQUESTS_PER_MINUTE}リクエスト/分に抑えて実行します(J-Quants Lightの上限は60/分)\n")

    today = date.today()
    date_to = today.strftime("%Y%m%d")
    date_from = (today - timedelta(days=ANALYSIS_WINDOW_DAYS + ROLLING_WINDOW_DAYS)).strftime("%Y%m%d")
    analysis_start = today - timedelta(days=ANALYSIS_WINDOW_DAYS)

    print(f"取得期間: {date_from} 〜 {date_to}")
    print(f"新高値判定の対象期間: {analysis_start.isoformat()} 〜 {today.isoformat()}\n")

    error_log_path = "errors.log"
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

    # --- 最終結果はキャッシュ全体(過去の実行分含む)から集計する ---
    # candidates.csv に「候補ルール」列(フェーズ1(現行) / フェーズ1'のみ(自動判定) など)が
    # あれば、そのまま出力に引き継ぐ(無ければ空欄。従来形式のcandidates.csvとの後方互換)。
    has_rule_column = candidates and "候補ルール" in candidates[0]

    all_events = []
    total_with_data = 0
    for row in candidates:
        code = row["code"].strip()
        name = row.get("name", "").strip()
        rule = row.get("候補ルール", "").strip() if has_rule_column else ""
        bars = load_cache(code)
        if not bars:
            continue
        total_with_data += 1
        events, _n_points = compute_new_highs(bars, analysis_start)
        for e in events:
            all_events.append({"code": code, "name": name, "候補ルール": rule, **e})

    output_file = f"new_highs_{today:%Y%m%d}.csv"
    fieldnames = ["code", "name", "候補ルール", "date", "high", "prior_52w_high"]
    with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for e in sorted(all_events, key=lambda x: (x["code"], x["date"])):
            writer.writerow(e)

    still_missing = [row["code"].strip() for row in candidates if load_cache(row["code"].strip()) is None]

    print("\n" + "=" * 70)
    print(f"今回新規取得: {newly_fetched}件 / キャッシュ済みでスキップ: {already_cached}件 / 今回失敗: {len(failed_codes)}件")
    print(f"累計データ保有銘柄数: {total_with_data}/{len(candidates)}件")
    print(f"新高値イベント合計: {len(all_events)}件 → {output_file} に出力しました。")
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
