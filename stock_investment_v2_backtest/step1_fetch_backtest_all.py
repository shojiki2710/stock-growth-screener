#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ステップ1: 全399銘柄バックテスト(全銘柄・全新高値イベント・仮想約定価格基準)
=====================================================================

前提条件(確定済み・Notion設計書「全銘柄バックテスト設計書」参照):
  - 対象: growth_universe.csv の全銘柄(過去に3倍達成したかで事前絞り込みしない)
  - 仮想約定価格 = 52週新高値更新日の終値
  - 損切り閾値 = 約定価格から -8%(日中安値ベースで判定)
  - 損切りルールは保有期間中ずっと有効
  - 「3倍到達」判定基準 = 約定価格の3倍(安値基準ではない)
  - 3倍到達の期限 = 約定日から1年以内(365日)
  - データ取得期間 = 2022年1月〜現在

精度向上策2(ギャップ・流動性)への対応として、追加で以下を出力する:
  - stop_is_gap: 損切り当日の始値時点で、既に-8%を超えて下に開いていたか
  - avg_trading_value_20d_yen: エントリー前20営業日(エントリー日を除く)の
    平均売買代金(終値×出来高、円)。薄商い銘柄の判定用。

前回(63銘柄)分は raw_cache_3x_v2/ に既にキャッシュがあるため、
新規APIコールを避けるためそこから自動でコピーして再利用します。
(raw_cache_3x_v2 フォルダが同じ場所にない場合はスキップされ、
 全銘柄を新規に取得します)

使い方:
  1. cd "/Users/masanao/Claude/stock investment"
  2. python3 step1_fetch_backtest_all.py growth_universe.csv
  3. J-QuantsのAPIキーを入力(画面には表示されません)
  4. 429で止まった場合は、同じコマンドをもう一度実行してください
     (raw_cache_step1_all/ にキャッシュ済みの銘柄は再取得されません)

出力: entry_to_3x_step1_results_YYYYMMDD.csv (全銘柄・全イベントの判定結果)
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
CACHE_DIR = "raw_cache_step1_all"
OLD_CACHE_DIR = "raw_cache_3x_v2"  # 既存63銘柄分の再利用元
FETCH_FROM_DATE = "20220101"
TIMEOUT_SEC = 30

STOP_LOSS_PCT = -8.0
TARGET_MULTIPLE = 3.0
DEADLINE_DAYS = 365
LIQUIDITY_WINDOW = 20

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


def _do_request(api_key, url):
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


def fetch_daily_bars(api_key, code, date_from, date_to, log):
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


def get_field(row, *keys):
    for k in keys:
        if k in row and row[k] is not None:
            return row[k]
    return None


def cache_path(code, cache_dir=CACHE_DIR):
    return os.path.join(cache_dir, f"{code}.json")


def load_cache(code, cache_dir=CACHE_DIR):
    path = cache_path(code, cache_dir)
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return None


def save_cache(code, bars, cache_dir=CACHE_DIR):
    os.makedirs(cache_dir, exist_ok=True)
    with open(cache_path(code, cache_dir), "w", encoding="utf-8") as f:
        json.dump(bars, f, ensure_ascii=False)


def normalize(rows):
    out = []
    for r in rows:
        d_raw = get_field(r, "Date", "date")
        o_raw = get_field(r, "O", "Open", "open")
        h_raw = get_field(r, "H", "High", "high")
        l_raw = get_field(r, "L", "Low", "low")
        c_raw = get_field(r, "C", "Close", "close")
        vo_raw = get_field(r, "Vo", "Volume", "volume")
        if d_raw is None or h_raw is None or l_raw is None or c_raw is None:
            continue
        try:
            d = datetime.strptime(str(d_raw)[:10], "%Y-%m-%d").date()
            o = float(o_raw) if o_raw is not None else None
            h, lo, c = float(h_raw), float(l_raw), float(c_raw)
            vo = float(vo_raw) if vo_raw is not None else None
        except (ValueError, TypeError):
            continue
        out.append((d, o, h, lo, c, vo))
    out.sort(key=lambda x: x[0])
    return out


def find_new_high_events(norm):
    """52週新高値更新イベントを全て検出する"""
    if not norm:
        return []
    data_start = norm[0][0]
    scan_start = data_start + timedelta(days=365)
    events = []
    for (d, o, h, lo, c, vo) in norm:
        if d < scan_start:
            continue
        window_start = d - timedelta(days=365)
        prior_highs = [ph for (pd, po, ph, pl, pc, pv) in norm if window_start <= pd < d]
        if not prior_highs:
            continue
        if h > max(prior_highs):
            events.append((d, c))  # 約定価格 = その日の終値
    return events


def avg_trading_value_20d(norm, entry_idx):
    """エントリー前20営業日(エントリー日を除く)の平均売買代金(終値×出来高)"""
    start = max(0, entry_idx - LIQUIDITY_WINDOW)
    window = norm[start:entry_idx]
    values = [c * vo for (d, o, h, lo, c, vo) in window if vo is not None]
    if not values:
        return None
    return sum(values) / len(values)


def simulate(norm, entry_idx, entry_price):
    deadline = norm[entry_idx][0] + timedelta(days=DEADLINE_DAYS)
    stop_price = entry_price * (1 + STOP_LOSS_PCT / 100)
    target_price = entry_price * TARGET_MULTIPLE

    for (d, o, h, lo, c, vo) in norm[entry_idx + 1:]:
        if d > deadline:
            return "TIMEOUT", None, None, None
        hit_stop = lo <= stop_price
        hit_target = h >= target_price
        if hit_stop:
            is_gap = (o is not None and o <= stop_price)
            pct = round((lo / entry_price - 1) * 100, 2)
            return "STOPPED", d.isoformat(), pct, is_gap
        if hit_target:
            pct = round((h / entry_price - 1) * 100, 2)
            return "SUCCESS", d.isoformat(), pct, None
    return "TIMEOUT", None, None, None


def main():
    print("=" * 70)
    print("ステップ1: 全銘柄バックテスト(仮想約定価格基準・全新高値イベント)")
    print("=" * 70)

    universe_file = sys.argv[1] if len(sys.argv) > 1 else "growth_universe.csv"
    if not os.path.exists(universe_file):
        print(f"エラー: {universe_file} が見つかりません。")
        sys.exit(1)

    with open(universe_file, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        candidates = []
        for row in reader:
            code = get_field(row, "code", "Code", "銘柄コード", "secCode")
            name = get_field(row, "name", "Name", "銘柄名", "companyName") or ""
            if code:
                candidates.append({"code": str(code).strip(), "name": str(name).strip()})

    if not candidates:
        print("エラー: 銘柄コードを読み取れませんでした。CSVの列名を確認してください。")
        sys.exit(1)

    api_key = getpass.getpass("J-Quants APIキーを入力してください(入力内容は表示されません): ").strip()
    if not api_key:
        print("APIキーが空のため終了します。")
        sys.exit(1)

    today = date.today()
    date_to = today.strftime("%Y%m%d")
    date_from = FETCH_FROM_DATE

    print(f"対象銘柄数: {len(candidates)}件(グロース市場全銘柄)")
    print(f"取得期間: {date_from} 〜 {date_to}")
    print(f"レート制限: {MAX_REQUESTS_PER_MINUTE}リクエスト/分\n")

    error_log_path = "errors_step1.log"
    error_log_f = open(error_log_path, "a", encoding="utf-8")

    def log(msg):
        error_log_f.write(f"{datetime.now().isoformat()} {msg}\n")
        error_log_f.flush()

    newly_fetched = 0
    reused_from_old_cache = 0
    already_cached = 0
    failed_codes = []

    for idx, cand in enumerate(candidates, 1):
        code = cand["code"]
        name = cand["name"]

        if load_cache(code) is not None:
            already_cached += 1
            print(f"[{idx}/{len(candidates)}] {code} {name} ... キャッシュ済み(スキップ)")
            continue

        old_bars = load_cache(code, OLD_CACHE_DIR)
        if old_bars is not None:
            save_cache(code, old_bars)
            reused_from_old_cache += 1
            print(f"[{idx}/{len(candidates)}] {code} {name} ... 既存キャッシュ(63銘柄分)を再利用")
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

    print("\n" + "=" * 70)
    print(f"新規取得: {newly_fetched}件 / 既存キャッシュ再利用: {reused_from_old_cache}件 / "
          f"取得済みスキップ: {already_cached}件 / 失敗: {len(failed_codes)}件")
    if failed_codes:
        print(f"[注意] 以下は取得できませんでした: {', '.join(failed_codes)}")

    # ---- バックテスト実行 ----
    print("\nバックテストを実行しています...")
    all_results = []
    cache_files = [c["code"] for c in candidates if load_cache(c["code"]) is not None]

    for code in cache_files:
        bars = load_cache(code)
        norm = normalize(bars)
        events = find_new_high_events(norm)
        date_to_idx = {d: i for i, (d, o, h, lo, c, vo) in enumerate(norm)}

        for (entry_date, entry_price) in events:
            entry_idx = date_to_idx[entry_date]
            outcome, event_date, pct, is_gap = simulate(norm, entry_idx, entry_price)
            hold_days = (datetime.strptime(event_date, "%Y-%m-%d").date() - entry_date).days if event_date else ""
            liq = avg_trading_value_20d(norm, entry_idx)
            all_results.append({
                "code": code, "entry_date": entry_date.isoformat(), "entry_price": entry_price,
                "outcome": outcome, "outcome_date": event_date or "",
                "outcome_pct": pct if pct is not None else "",
                "hold_days": hold_days,
                "stop_is_gap": is_gap if is_gap is not None else "",
                "avg_trading_value_20d_yen": round(liq) if liq is not None else "",
            })

    output_file = f"entry_to_3x_step1_results_{today:%Y%m%d}.csv"
    with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
        fieldnames = ["code", "entry_date", "entry_price", "outcome", "outcome_date",
                      "outcome_pct", "hold_days", "stop_is_gap", "avg_trading_value_20d_yen"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in all_results:
            writer.writerow(r)

    total = len(all_results)
    n_success = sum(1 for r in all_results if r["outcome"] == "SUCCESS")
    n_stopped = sum(1 for r in all_results if r["outcome"] == "STOPPED")
    n_timeout = sum(1 for r in all_results if r["outcome"] == "TIMEOUT")
    n_gap_stop = sum(1 for r in all_results if r["outcome"] == "STOPPED" and r["stop_is_gap"] is True)

    print(f"\n対象銘柄数: {len(cache_files)}")
    print(f"全新高値更新イベント数: {total}")
    if total:
        print(f"  SUCCESS(1年以内に約定価格の3倍到達): {n_success} ({n_success/total*100:.1f}%)")
        print(f"  STOPPED(-8%で損切り): {n_stopped} ({n_stopped/total*100:.1f}%)")
        if n_stopped:
            print(f"    うちギャップ損切り(寄付で既に-8%超過): {n_gap_stop} ({n_gap_stop/n_stopped*100:.1f}%)")
        print(f"  TIMEOUT(1年経過・未達): {n_timeout} ({n_timeout/total*100:.1f}%)")
    print(f"出力: {output_file}")
    print("=" * 70)


if __name__ == "__main__":
    main()
