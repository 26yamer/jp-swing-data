#!/usr/bin/env python3
"""
後場スイング用 データ取得スクリプト（GitHub Actions 上で実行する前提）

なぜこの形か:
  Claude が動くクラウドコンテナは外向き通信がパッケージレジストリと GitHub に
  制限されており、Yahoo Finance 等へ直接アクセスできない。
  一方 raw.githubusercontent.com は読める。
  そこで「GitHub Actions が取りに行って、リポジトリにコミットする」ことで
  Claude 側は raw.githubusercontent.com から正確な数値を読めるようにする。

出力:
  data/latest.json        … 最新スナップショット（前場終値・マクロ）
  data/ohlcv/<code>.csv   … 日足OHLCV（累積・追記マージ）
  data/intraday/<code>.csv… 5分足（当日のみ）
  data/meta.json          … 取得時刻・成否・データソース・健全性

方針:
  一部の銘柄が取れなくてもスクリプトは異常終了しない。取れた分は必ずコミットし、
  何が取れなかったかを meta.json に残す。失敗を握りつぶすのではなく、
  「部分的な成功を捨てない」ための設計。
"""
import os, sys, time, json, datetime as dt
import pandas as pd

JST = dt.timezone(dt.timedelta(hours=9))
NOW = dt.datetime.now(JST)

# ── 監視ユニバース ────────────────────────────────────────────────
HOLDINGS = ["1306.T","1343.T","1540.T","1615.T","2559.T","2563.T","2621.T","4755.T","9432.T"]
# スイング候補の拡張ユニバース（値動きがあり流動性の高い日本株/ETF）
WATCH    = ["1321.T","1357.T","1459.T","1568.T","1699.T","2644.T",
            "8306.T","8316.T","8411.T","8604.T","7203.T","6758.T","6501.T",
            "9984.T","6857.T","8035.T","4063.T","6098.T","9433.T","9434.T"]
MACRO    = ["^N225","^GSPC","^IXIC","^VIX","^TNX","^TYX",
            "JPY=X","CL=F","GC=F","SI=F","^SOX"]
ALL = HOLDINGS + WATCH + MACRO

OUT = "data"
os.makedirs(f"{OUT}/ohlcv", exist_ok=True)
os.makedirs(f"{OUT}/intraday", exist_ok=True)
meta = {"fetched_at_jst": NOW.isoformat(), "sources": {}, "errors": []}

def slug(code):
    return code.replace("^", "_").replace("=", "_")

def norm_dates(s):
    """タイムゾーンの有無に関わらず、日付だけの naive な列に揃える。
       pandas 2系では tz-naive に tz_localize(None) を呼ぶと TypeError になるため、
       tz を持っているときだけ外す。utc=True は使わない（日付が1日ずれるため）。"""
    d = pd.to_datetime(s)
    if getattr(d.dt, "tz", None) is not None:
        d = d.dt.tz_localize(None)
    return d.dt.normalize()

def retry(fn, *a, **k):
    for i in range(3):
        try:
            return fn(*a, **k)
        except Exception as e:
            if i == 2:
                meta["errors"].append(f"{getattr(fn,'__name__','fn')}{a}: {type(e).__name__}: {e}")
                return None
            time.sleep(2 * (i + 1))

# ── 1) 日足: yfinance（主）→ stooq（副） ─────────────────────────
def fetch_daily_all(tickers):
    import yfinance as yf
    return yf.download(tickers, period="2y", interval="1d",
                       auto_adjust=False, group_by="ticker",
                       threads=True, progress=False)

def fetch_daily_stooq(code):
    """yfinance が落ちた時のフォールバック。1306.T -> 1306.jp"""
    import io, requests
    s = code.replace(".T", ".jp").lower()
    r = requests.get(f"https://stooq.com/q/d/l/?s={s}&i=d", timeout=30)
    if r.status_code != 200 or len(r.text) < 100:
        raise RuntimeError(f"stooq HTTP {r.status_code}")
    return pd.read_csv(io.StringIO(r.text), parse_dates=["Date"])

def merge_csv(path, new):
    """既存CSVに追記マージ（重複日は新しい方で上書き）。履歴を失わないため。"""
    new = new.dropna(subset=["Close"])
    if new.empty:
        return 0
    if os.path.exists(path):
        try:
            old = pd.read_csv(path, parse_dates=["Date"])
            new = pd.concat([old, new]).drop_duplicates(subset=["Date"], keep="last")
        except Exception as e:
            meta["errors"].append(f"merge {path}: {e}")
    new = new.sort_values("Date")
    new.to_csv(path, index=False)
    return len(new)

print(f"[{NOW:%Y-%m-%d %H:%M JST}] 日足取得開始 ({len(ALL)}銘柄)")
raw = retry(fetch_daily_all, ALL)
ok = 0
for code in ALL:
    d = None
    if raw is not None:
        try:
            d = raw[code].reset_index()
            d.columns = [str(c) for c in d.columns]
            datecol = "Date" if "Date" in d.columns else d.columns[0]
            d = d.rename(columns={datecol: "Date"})
            d = d[["Date", "Open", "High", "Low", "Close", "Volume"]]
            if d["Close"].dropna().empty:
                d = None
            else:
                meta["sources"][code] = "yfinance"
        except Exception:
            d = None
    if d is None and code.endswith(".T"):
        d = retry(fetch_daily_stooq, code)
        if d is not None:
            meta["sources"][code] = "stooq"
    if d is None or d.empty:
        meta["errors"].append(f"no-daily: {code}")
        continue
    try:
        d["Date"] = norm_dates(d["Date"])
        merge_csv(f"{OUT}/ohlcv/{slug(code)}.csv", d)
        ok += 1
    except Exception as e:
        meta["errors"].append(f"write {code}: {type(e).__name__}: {e}")
print(f"日足OK: {ok}/{len(ALL)}")

# ── 2) 当日5分足（前場の値動きを見るため） ───────────────────────
try:
    import yfinance as yf
    intra = yf.download(HOLDINGS + ["^N225"], period="1d", interval="5m",
                        group_by="ticker", threads=True, progress=False, prepost=False)
    for code in HOLDINGS + ["^N225"]:
        try:
            d = intra[code].reset_index().dropna(subset=["Close"])
            if not d.empty:
                d.to_csv(f"{OUT}/intraday/{slug(code)}.csv", index=False)
        except Exception:
            pass
except Exception as e:
    meta["errors"].append(f"intraday: {type(e).__name__}: {e}")

# ── 3) スナップショット ──────────────────────────────────────────
snap = {}
for code in ALL:
    p = f"{OUT}/ohlcv/{slug(code)}.csv"
    if not os.path.exists(p):
        continue
    try:
        d = pd.read_csv(p, parse_dates=["Date"]).sort_values("Date")
        if len(d) < 2:
            continue
        c, pv = float(d["Close"].iloc[-1]), float(d["Close"].iloc[-2])
        vol = d["Volume"].iloc[-1] if "Volume" in d.columns else None
        snap[code] = dict(date=str(d["Date"].iloc[-1].date()), close=c, prev=pv,
                          chg_pct=round((c / pv - 1) * 100, 3),
                          volume=(float(vol) if pd.notna(vol) else None),
                          bars=len(d))
    except Exception as e:
        meta["errors"].append(f"snapshot {code}: {e}")

# 前場のリアルタイム値（5分足の最終足）を上乗せ
for code in HOLDINGS:
    p = f"{OUT}/intraday/{slug(code)}.csv"
    if os.path.exists(p) and code in snap:
        try:
            d = pd.read_csv(p)
            if not d.empty:
                snap[code]["intraday_last"] = float(d["Close"].iloc[-1])
                snap[code]["intraday_time"] = str(d.iloc[-1, 0])
                snap[code]["intraday_high"] = float(d["High"].max())
                snap[code]["intraday_low"]  = float(d["Low"].min())
        except Exception:
            pass

json.dump({"generated_at_jst": NOW.isoformat(), "snapshot": snap},
          open(f"{OUT}/latest.json", "w"), ensure_ascii=False, indent=1)

meta["n_ok"] = ok
meta["n_total"] = len(ALL)
meta["holdings_ok"] = sum(1 for c in HOLDINGS if c in snap)
meta["health"] = ("ok" if meta["holdings_ok"] == len(HOLDINGS)
                  else "partial" if meta["holdings_ok"] >= 6 else "bad")
json.dump(meta, open(f"{OUT}/meta.json", "w"), ensure_ascii=False, indent=1)

print(f"保有銘柄: {meta['holdings_ok']}/{len(HOLDINGS)}  健全性: {meta['health']}")
print(f"エラー件数: {len(meta['errors'])}")
for e in meta["errors"][:10]:
    print("  -", e)
if meta["health"] == "bad":
    print("::warning::保有銘柄の取得が不足しています。data/meta.json を確認してください")
# 部分的にでも取れていればコミットさせるため、常に正常終了する
sys.exit(0)
