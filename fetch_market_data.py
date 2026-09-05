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
  data/meta.json          … 取得時刻・成否・データソース
"""
import os, json, sys, time, datetime as dt
import pandas as pd

JST = dt.timezone(dt.timedelta(hours=9))
NOW = dt.datetime.now(JST)

# ── 監視ユニバース ────────────────────────────────────────────────
HOLDINGS = ["1306.T","1343.T","1540.T","1615.T","2559.T","2563.T","2621.T","4755.T","9432.T"]
# スイング候補の拡張ユニバース（値動きがあり流動性の高い日本株/ETF）
WATCH    = ["1321.T","1357.T","1459.T","1568.T","1699.T","2644.T",
            "8306.T","8316.T","8411.T","8604.T","7203.T","6758.T","6501.T",
            "9984.T","6857.T","8035.T","4063.T","6098.T","9433.T","9434.T"]
MACRO    = ["^N225","^TOPX","^GSPC","^IXIC","^VIX","^TNX","^TYX",
            "JPY=X","CL=F","GC=F","SI=F","^SOX"]
ALL = HOLDINGS + WATCH + MACRO

OUT="data"; os.makedirs(f"{OUT}/ohlcv",exist_ok=True); os.makedirs(f"{OUT}/intraday",exist_ok=True)
meta={"fetched_at_jst":NOW.isoformat(),"sources":{},"errors":[]}

def safe(fn,*a,**k):
    for i in range(3):
        try: return fn(*a,**k)
        except Exception as e:
            if i==2: meta["errors"].append(f"{fn.__name__}:{a}:{type(e).__name__}:{e}"); return None
            time.sleep(2*(i+1))

# ── 1) 日足: yfinance（主）→ stooq（副） ─────────────────────────
def fetch_daily_yf(tickers):
    import yfinance as yf
    df = yf.download(tickers, period="2y", interval="1d",
                     auto_adjust=False, group_by="ticker",
                     threads=True, progress=False)
    return df

def fetch_daily_stooq(code):
    """yfinance が落ちた時のフォールバック。1306.T -> 1306.jp"""
    import io, requests
    s = code.replace(".T",".jp").lower()
    r = requests.get(f"https://stooq.com/q/d/l/?s={s}&i=d", timeout=30)
    if r.status_code!=200 or len(r.text)<100: raise RuntimeError(f"stooq {r.status_code}")
    return pd.read_csv(io.StringIO(r.text), parse_dates=["Date"])

def merge_csv(path, new):
    """既存CSVに追記マージ（重複日は新しい方で上書き）。履歴を失わないため。"""
    new = new.dropna(subset=["Close"])
    if os.path.exists(path):
        old = pd.read_csv(path, parse_dates=["Date"])
        new = pd.concat([old,new]).drop_duplicates(subset=["Date"], keep="last")
    new = new.sort_values("Date")
    new.to_csv(path, index=False)
    return len(new)

print(f"[{NOW:%Y-%m-%d %H:%M %Z}] 日足取得開始 ({len(ALL)}銘柄)")
raw = safe(fetch_daily_yf, ALL)
ok=0
for code in ALL:
    d=None
    if raw is not None:
        try:
            d = raw[code].reset_index()[["Date","Open","High","Low","Close","Volume"]]
        except Exception: d=None
    if d is None or d["Close"].dropna().empty:
        if code.endswith(".T"):
            d = safe(fetch_daily_stooq, code)
            if d is not None: meta["sources"][code]="stooq"
    else:
        meta["sources"][code]="yfinance"
    if d is None or d.empty: meta["errors"].append(f"no-daily:{code}"); continue
    d["Date"]=pd.to_datetime(d["Date"]).dt.tz_localize(None).dt.normalize()
    n=merge_csv(f"{OUT}/ohlcv/{code.replace('^','_').replace('=','_')}.csv", d)
    ok+=1
print(f"日足OK: {ok}/{len(ALL)}")

# ── 2) 当日5分足（前場の値動きを見るため） ───────────────────────
try:
    import yfinance as yf
    intra = yf.download(HOLDINGS+["^N225","^TOPX"], period="1d", interval="5m",
                        group_by="ticker", threads=True, progress=False, prepost=False)
    for code in HOLDINGS+["^N225","^TOPX"]:
        try:
            d=intra[code].reset_index().dropna(subset=["Close"])
            if not d.empty:
                d.to_csv(f"{OUT}/intraday/{code.replace('^','_')}.csv", index=False)
        except Exception: pass
except Exception as e:
    meta["errors"].append(f"intraday:{e}")

# ── 3) スナップショット（前場終値・マクロ） ───────────────────────
snap={}
for code in ALL:
    p=f"{OUT}/ohlcv/{code.replace('^','_').replace('=','_')}.csv"
    if not os.path.exists(p): continue
    d=pd.read_csv(p, parse_dates=["Date"]).sort_values("Date")
    if len(d)<2: continue
    c=d["Close"].iloc[-1]; pv=d["Close"].iloc[-2]
    snap[code]=dict(date=str(d["Date"].iloc[-1].date()), close=float(c),
                    prev=float(pv), chg_pct=round((c/pv-1)*100,3),
                    volume=float(d["Volume"].iloc[-1]) if "Volume" in d else None,
                    bars=len(d))
# 前場のリアルタイム値（5分足の最終足）で上書き
for code in HOLDINGS:
    p=f"{OUT}/intraday/{code}.csv"
    if os.path.exists(p):
        d=pd.read_csv(p)
        if not d.empty and code in snap:
            snap[code]["intraday_last"]=float(d["Close"].iloc[-1])
            snap[code]["intraday_time"]=str(d.iloc[-1,0])
            snap[code]["intraday_high"]=float(d["High"].max())
            snap[code]["intraday_low"]=float(d["Low"].min())

json.dump({"generated_at_jst":NOW.isoformat(),"snapshot":snap},
          open(f"{OUT}/latest.json","w"), ensure_ascii=False, indent=1)
meta["n_ok"]=ok; meta["n_total"]=len(ALL)
json.dump(meta, open(f"{OUT}/meta.json","w"), ensure_ascii=False, indent=1)
print(f"完了 errors={len(meta['errors'])}")
if ok < len(ALL)*0.6:
    print("::error::取得成功率が低すぎます"); sys.exit(1)
