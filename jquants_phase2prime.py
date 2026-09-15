#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
フェーズ2'(改訂条件案)判定バッチ (J-Quants API v2使用)
=====================================================================

目的:
  jquants_screening.py が出力した new_highs_*.csv (52週新高値を実際に更新した
  銘柄イベントの一覧: code, name, 候補ルール, date, high, prior_52w_high) を入力とし、
  各イベントについて「新高値更新日+5営業日後」時点の株価データを取得して、
  Notion「🔧 改良版ファネル並行運用計画」で方向性合意済みのフェーズ2'条件で判定します。

フェーズ2'の判定条件(2026-09-14合意):
  - 乖離率(新高値更新日+5営業日後の終値が、新高値更新日の高値からどれだけ乖離しているか)
    が +10%以上 → 最も強いシグナル(バックテストでの推定成功率 5.56% vs ベースライン0〜0.4%)。
    これが GO/NO-GO の主判定。
  - 出来高倍率(新高値更新日の出来高 ÷ 直前20営業日平均出来高)が3倍以上 → 補助的な
    (単独では弱い)シグナル。参考情報として記録するが、GO/NO-GO の主判定には使わない。
  - 移動平均線整合性(MA25>MA75かつ株価が両方より上)は、バックテストで判別力が
    確認できなかったため、フェーズ2'では不採用(判定に使わない)。

  計算ロジック自体は phase2_backtest/fetch_phase2_data.py の compute_metrics() と
  同じ考え方(移動平均線の判定部分のみ除外)。

【実行方法】
  1. このフォルダにTerminalでcdする: cd ~/Documents/kabu_screening/jquants_batch
     (このスクリプトは jquants_screening.py と同じフォルダに置いてください)
  2. 実行: python3 jquants_phase2prime.py [対象のnew_highs_*.csvファイル名(省略時は最新を自動選択)]
  3. J-QuantsのAPIキーを入力(画面には表示されません)
  4. 完了すると phase2prime_results_YYYYMMDD.csv に出力されます。

  このスクリプトも実際のJ-Quants APIへのネットワーク接続が必要なため、Claude(Cowork)側の
  クラウド環境・Macブリッジ経由のシェルでは実行できません。必ずお使いのMacの
  「本物のTerminal.app」で実行してください。

【+5営業日後のデータがまだ存在しない場合】
  新高値更新日から日が浅く、まだ+5営業日分の株価データが存在しないイベントは
  phase2_signal = "判定待ち" として出力されます(エラーではありません)。これらは
  出力CSVに保存されないため、日数が経ってから同じコマンドを再実行すれば、
  自動的に判定可能になった分だけ追加で判定されます(判定済みの行は再取得しません)。

依存ライブラリ: 標準ライブラリのみ(追加インストール不要)。Python 3.8以上を想定。
"""

import csv
import getpass
import glob
import json
import os
import sys
import time
import urllib.request
import urllib.error
import urllib.parse
from datetime import date, datetime, timedelta

API_BASE = "https://api.jquants.com/v2/equities/bars/daily"
OUTPUT_PREFIX = "phase2prime_results_"

FWD_TRADING_DAYS = 5       # 「+5営業日後」の営業日数
PRIOR_VOLUME_WINDOW = 20   # 出来高比較の直前営業日数
DEVIATION_GO_THRESHOLD = 10.0   # 乖離率のGO判定閾値(%)
VOLUME_NOTE_THRESHOLD = 3.0     # 出来高倍率の参考シグナル閾値(倍)

# 前後に余裕を持って取得する暦日数
DAYS_BEFORE = 60   # 直前20営業日の出来高平均計算に必要な余裕
DAYS_AFTER = 20    # entry+5営業日を確実に含めるための余裕
TIMEOUT_SEC = 30

# --- レート制限まわり(jquants_screening.pyと同じ方針) ---
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


def normalize_bars(rows):
    """レスポンスのフィールド名の揺れを吸収し、日付昇順に整えた {Date, High, Close, Volume} のリストを返す"""
    out = []
    for r in rows:
        d_raw = get_field(r, "Date", "date")
        if d_raw is None:
            continue
        try:
            d = str(d_raw)[:10]
        except Exception:
            continue
        high = get_field(r, "AdjH", "H", "High", "high")
        close = get_field(r, "AdjC", "C", "Close", "close")
        volume = get_field(r, "AdjVo", "Vo", "Volume", "volume")
        out.append({"Date": d, "High": high, "Close": close, "Volume": volume})
    out.sort(key=lambda q: q["Date"])
    return out


def compute_phase2prime(bars, entry_date_str, entry_high_hint=None):
    """1銘柄分の日次バーから、フェーズ2'の指標を計算する。
    戻り値: dict (volume_ratio, deviation_pct, phase2_signal, volume_note, note)"""
    dates = [b["Date"] for b in bars]
    if entry_date_str not in dates:
        return {"error": "entry_date_not_in_series"}

    idx = dates.index(entry_date_str)
    entry_row = bars[idx]
    entry_high = entry_row.get("High") or entry_high_hint
    entry_volume = entry_row.get("Volume")

    # 出来高倍率(参考シグナル)
    prior_rows = bars[max(0, idx - PRIOR_VOLUME_WINDOW):idx]
    prior_volumes = [r.get("Volume") for r in prior_rows if r.get("Volume") is not None]
    if entry_volume is None or len(prior_volumes) < PRIOR_VOLUME_WINDOW * 0.7:
        volume_ratio = None
    else:
        avg_prior_volume = sum(prior_volumes) / len(prior_volumes)
        volume_ratio = (entry_volume / avg_prior_volume) if avg_prior_volume else None

    # +5営業日後がまだ存在しない = 判定待ち
    fwd_idx = idx + FWD_TRADING_DAYS
    if fwd_idx >= len(bars) or entry_high is None:
        return {
            "volume_ratio": round(volume_ratio, 3) if volume_ratio is not None else None,
            "deviation_pct": None,
            "phase2_signal": "判定待ち(+5営業日未到達)",
            "volume_note": "",
            "error": None,
        }

    fwd_close = bars[fwd_idx].get("Close")
    if fwd_close is None:
        deviation_pct = None
    else:
        deviation_pct = (fwd_close - entry_high) / entry_high * 100.0

    if deviation_pct is None:
        phase2_signal = "判定不能(株価データ欠損)"
    elif deviation_pct >= DEVIATION_GO_THRESHOLD:
        phase2_signal = "GO(乖離率条件クリア)"
    else:
        phase2_signal = "NO-GO"

    volume_note = "出来高3倍以上(参考シグナルあり)" if (volume_ratio is not None and volume_ratio >= VOLUME_NOTE_THRESHOLD) else ""

    return {
        "volume_ratio": round(volume_ratio, 3) if volume_ratio is not None else None,
        "deviation_pct": round(deviation_pct, 2) if deviation_pct is not None else None,
        "phase2_signal": phase2_signal,
        "volume_note": volume_note,
        "error": None,
    }


def find_latest_new_highs_csv():
    candidates = sorted(glob.glob("new_highs_*.csv"))
    return candidates[-1] if candidates else None


def main():
    print("=" * 70)
    print("フェーズ2'(乖離率+10%@+5営業日)判定バッチ")
    print("=" * 70)

    input_csv = sys.argv[1] if len(sys.argv) > 1 else find_latest_new_highs_csv()
    if not input_csv or not os.path.exists(input_csv):
        print("[エラー] 入力ファイル(new_highs_*.csv)が見つかりません。"
              "jquants_screening.py を先に実行するか、ファイル名を引数で指定してください。")
        sys.exit(1)

    with open(input_csv, encoding="utf-8-sig") as f:
        events = list(csv.DictReader(f))
    print(f"入力ファイル: {input_csv} ({len(events)}イベント)\n")

    api_key = getpass.getpass("J-Quants APIキーを入力してください(入力内容は表示されません): ").strip()
    if not api_key:
        print("APIキーが空のため終了します。")
        sys.exit(1)

    today = date.today()
    output_file = f"{OUTPUT_PREFIX}{today:%Y%m%d}.csv"

    # 既存の出力を読み込み、GO/NO-GO判定済みの行は再取得しない(判定待ちの行は毎回再試行する)
    done_keys = set()
    existing_rows = []
    if os.path.exists(output_file):
        with open(output_file, newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                if row.get("phase2_signal") not in ("判定待ち(+5営業日未到達)", ""):
                    done_keys.add((row["code"], row["entry_date"]))
                existing_rows.append(row)
        print(f"既存の出力を検出: {len(existing_rows)}件(うち判定確定済み{len(done_keys)}件は再利用)。")

    error_log_path = "errors_phase2prime.log"
    error_log_f = open(error_log_path, "a", encoding="utf-8")

    def log(msg):
        error_log_f.write(f"{datetime.now().isoformat()} {msg}\n")
        error_log_f.flush()

    out_rows = [r for r in existing_rows if (r["code"], r["entry_date"]) in done_keys]

    # 銘柄コード単位でイベントをまとめる(1コード=1回のAPI取得で、その銘柄の全イベントを判定する)
    by_code = {}
    meta_by_key = {}
    for ev in events:
        code = ev["code"].strip()
        entry_date = ev["date"].strip()
        if (code, entry_date) in done_keys:
            continue
        by_code.setdefault(code, []).append(entry_date)
        meta_by_key[(code, entry_date)] = {
            "name": ev.get("name", "").strip(),
            "候補ルール": ev.get("候補ルール", "").strip(),
            "entry_high": float(ev["high"]) if ev.get("high") else None,
        }

    codes_sorted = sorted(by_code.keys())
    print(f"取得対象: {len(codes_sorted)}銘柄(未判定の{sum(len(v) for v in by_code.values())}イベント分)\n")

    fieldnames = ["code", "name", "候補ルール", "entry_date", "entry_high",
                  "volume_ratio", "deviation_pct", "phase2_signal", "volume_note"]

    for ci, code in enumerate(codes_sorted, 1):
        entry_dates = by_code[code]
        name = meta_by_key[(code, entry_dates[0])]["name"]
        min_dt = min(datetime.strptime(d, "%Y-%m-%d").date() for d in entry_dates)
        max_dt = max(datetime.strptime(d, "%Y-%m-%d").date() for d in entry_dates)
        date_from = (min_dt - timedelta(days=DAYS_BEFORE)).strftime("%Y%m%d")
        date_to = (max_dt + timedelta(days=DAYS_AFTER)).strftime("%Y%m%d")

        print(f"[{ci}/{len(codes_sorted)}] {code} {name} ({len(entry_dates)}イベント) 取得中...", end=" ", flush=True)
        raw_bars = fetch_daily_bars(api_key, code, date_from, date_to, log)

        if not raw_bars:
            print("データ取得失敗")
            for d in entry_dates:
                m = meta_by_key[(code, d)]
                out_rows.append({
                    "code": code, "name": m["name"], "候補ルール": m["候補ルール"], "entry_date": d,
                    "entry_high": m["entry_high"], "volume_ratio": "", "deviation_pct": "",
                    "phase2_signal": "取得失敗", "volume_note": "",
                })
        else:
            bars = normalize_bars(raw_bars)
            signals = []
            for d in entry_dates:
                m = meta_by_key[(code, d)]
                metrics = compute_phase2prime(bars, d, m["entry_high"])
                if metrics.get("error"):
                    phase2_signal = "判定不能"
                    volume_ratio = deviation_pct = ""
                    volume_note = ""
                else:
                    phase2_signal = metrics["phase2_signal"]
                    volume_ratio = metrics["volume_ratio"] if metrics["volume_ratio"] is not None else ""
                    deviation_pct = metrics["deviation_pct"] if metrics["deviation_pct"] is not None else ""
                    volume_note = metrics["volume_note"]
                out_rows.append({
                    "code": code, "name": m["name"], "候補ルール": m["候補ルール"], "entry_date": d,
                    "entry_high": m["entry_high"], "volume_ratio": volume_ratio,
                    "deviation_pct": deviation_pct, "phase2_signal": phase2_signal,
                    "volume_note": volume_note,
                })
                signals.append(phase2_signal)
            print(", ".join(signals))

        # 途中経過を都度保存(中断・レート制限対策)
        with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for r in out_rows:
                w.writerow({k: r.get(k, "") for k in fieldnames})

    error_log_f.close()

    go_count = sum(1 for r in out_rows if r.get("phase2_signal") == "GO(乖離率条件クリア)")
    pending_count = sum(1 for r in out_rows if r.get("phase2_signal") == "判定待ち(+5営業日未到達)")

    print("\n" + "=" * 70)
    print(f"完了。{output_file} に {len(out_rows)}件出力しました。")
    print(f"  GO(乖離率+10%以上): {go_count}件")
    print(f"  判定待ち(+5営業日未到達、後日再実行で判定可能): {pending_count}件")
    print("=" * 70)


if __name__ == "__main__":
    main()
