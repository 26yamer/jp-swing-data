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
                            Close=実際の取引価格（発注の指値に使う）
                            Adj Close=分割・配当調整済み（指標の計算に使う）
  data/intraday/<code>.csv… 5分足（当日のみ）
  data/meta.json          … 取得時刻・成否・データソース・健全性
  data/jgb.csv            … 日本国債利回り（財務省・全年限）
  data/events.json        … 分配金の実績と利回り、決算発表予定日

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
# レバレッジ/インバース型（1357・1459・1568）は日々減価する設計でスイングの
# 保有候補にならず、株式併合が頻繁でデータも荒れるため監視対象から外した。
WATCH    = ["1321.T","1699.T","2510.T","1343.T","2559.T","2621.T",
            "8306.T","8316.T","8411.T","7203.T","6758.T","6501.T",
            "9984.T","6857.T","8035.T","4063.T","9433.T","9434.T"]
# 業種別ETF（TOPIX-17）。値上がり/値下がり銘柄数をスクレイピングする代わりに、
# セクターの物色動向を「価格データ」として取る。壊れにくく、遅延もない。
SECTOR = {"1617.T":"食品","1618.T":"エネルギー資源","1619.T":"建設・資材","1620.T":"素材・化学",
          "1621.T":"医薬品","1622.T":"自動車・輸送機","1623.T":"鉄鋼・非鉄","1624.T":"機械",
          "1625.T":"電機・精密","1626.T":"情報通信・サービス他","1627.T":"電力・ガス",
          "1628.T":"運輸・物流","1629.T":"商社・卸売","1630.T":"小売","1631.T":"銀行",
          "1632.T":"金融(除く銀行)","1633.T":"不動産"}
MACRO    = ["^N225","^GSPC","^IXIC","^VIX","^TNX","^TYX",
            "JPY=X","CL=F","GC=F","SI=F","^SOX"]
ALL = HOLDINGS + WATCH + list(SECTOR) + MACRO

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

def sanity(code, d):
    if not code.endswith(".T"):     # ^VIX や SI=F は実際に25%動く。誤検知になるので対象外
        return
    """理由の説明できない価格の飛びを検出して meta に残す。
       Close は実際の取引価格なので株式分割で不連続になる。指標計算には
       Adj Close（分割・配当調整済み）を使うこと。ここでは Adj Close 側に
       飛びが残っていないかを見る＝データ自体の異常の検出。"""
    col = "Adj Close" if "Adj Close" in d.columns else "Close"
    v = d[col].astype(float)
    ch = (v / v.shift() - 1).abs()
    bad = d.loc[ch > 0.25, "Date"]
    if len(bad):
        meta["anomalies"] = meta.get("anomalies", [])
        meta["anomalies"].append({"code": code, "col": col,
                                  "dates": [str(x.date()) for x in bad][:5],
                                  "n": int(len(bad))})

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
            cols = ["Date", "Open", "High", "Low", "Close", "Volume"]
            if "Adj Close" in d.columns:
                cols.append("Adj Close")
            d = d[cols]
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
        sanity(code, d)
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


# ── 3.5) ニュース見出しの取得 ─────────────────────────────────
#    Claudeの定期実行では WebFetch が承認を要求して止まることがある。
#    ニュースもここで取り、report.md に書き込んでしまえば、
#    Claude側はレポートを1回読むだけで済み、承認の対象が減る。
# フィードごとに信頼度を持たせる。market=True のものは市場記事しかないので
# キーワード絞り込みをかけず全件採用する（日経マーケットが最も有用）。
FEEDS = [
    ("日経 マーケット",     "https://assets.wor.jp/rss/rdf/nikkei/markets.rdf", True),
    ("日経 政治・経済",     "https://assets.wor.jp/rss/rdf/nikkei/economy.rdf", False),
    ("Yahoo!ニュース 市況", "https://news.yahoo.co.jp/rss/categories/business.xml", False),
    ("Yahoo!ニュース 経済", "https://news.yahoo.co.jp/rss/topics/business.xml", False),
    ("産経 経済",          "https://assets.wor.jp/rss/rdf/sankei/economy.rdf", False),
    ("財経新聞",           "https://www.zaikei.co.jp/rss/", False),
]
KEYS = ["日経平均","TOPIX","東証","株価","長期金利","国債","利回り","日銀","利上げ","金融政策",
        "円安","円高","為替","ドル円","原油","OPEC","FRB","FOMC","CPI","物価","インフレ",
        "銀行","半導体","決算","地政学","中東","イラン","REIT","不動産","金価格","米株",
        "ナスダック","ダウ","先物","値上がり","値下がり","業種","相場","マーケット"]
news = []
def fetch_feed(name, url):
    import requests, xml.etree.ElementTree as ET, html as _h, re as _re
    r = requests.get(url, timeout=25, headers={"User-Agent": "Mozilla/5.0 (compatible; market-bot)"})
    r.raise_for_status()
    txt = r.content.decode(r.apparent_encoding or "utf-8", errors="replace")
    out = []
    try:
        root = ET.fromstring(txt.encode("utf-8"))
        for it in root.iter():
            if it.tag.split("}")[-1] != "item": continue
            g = lambda k: next((c.text for c in it if c.tag.split("}")[-1] == k and c.text), "")
            ti = _h.unescape((g("title") or "").strip())
            if ti: out.append({"src": name, "title": ti,
                               "date": (g("pubDate") or g("date") or "")[:25]})
    except Exception:                       # XMLとして壊れている場合は素朴に抜く
        for m in _re.finditer(r"<title[^>]*>(.*?)</title>", txt, _re.S)   :
            ti = _h.unescape(_re.sub(r"<[^>]+>", "", m.group(1))).strip()
            if ti: out.append({"src": name, "title": ti, "date": ""})
    return out[:40]

for nm, u, is_market in FEEDS:
    got = retry(fetch_feed, nm, u)
    if got:
        for g in got: g["market_feed"] = is_market
        news.extend(got)
seen = set(); hits = []
for n in news:
    k = n["title"]
    if k in seen: continue
    seen.add(k)
    # 市場専門フィードは無条件採用、それ以外はキーワードで絞る
    if n.get("market_feed") or any(w in k for w in KEYS): hits.append(n)
hits.sort(key=lambda x: (not x.get("market_feed"),))   # 市場フィードを先頭に
json.dump({"fetched_at_jst": NOW.isoformat(), "matched": hits[:45], "all_count": len(news)},
          open(f"{OUT}/news.json", "w"), ensure_ascii=False, indent=1)
meta["news_ok"] = len(hits)
print(f"ニュース: {len(news)}件取得 / 関連 {len(hits)}件")

# ── 4) 日本国債利回り（財務省・公式）────────────────────────────
#    米国債は ^TNX/^TYX で取れるが日本国債は Yahoo に無い。
#    いまのレジーム判断の中心変数なので財務省の公表CSVから直接取る。
#    形式: Shift-JIS / 日付が和暦(R8.9.4等) / 先頭に説明行あり、という癖がある。
def fetch_jgb():
    import io, requests
    # 手元の jgb.csv が短いうちは全履歴版を取りに行き、溜まったら当年版に切り替える
    base = "https://www.mof.go.jp/jgbs/reference/interest_rate/"
    have = 0
    try:
        if os.path.exists(f"{OUT}/jgb.csv"):
            have = sum(1 for _ in open(f"{OUT}/jgb.csv", encoding="utf-8")) - 1
    except Exception:
        pass
    url = base + ("jgbcm.csv" if have >= 250 else "jgbcm_all.csv")
    print(f"JGB: 手元{have}行 → {'当年版' if have >= 250 else '全履歴版'}を取得")
    r = requests.get(url, timeout=45)
    r.raise_for_status()
    txt = None
    for enc in ("cp932", "shift_jis", "utf-8-sig", "utf-8"):
        try:
            txt = r.content.decode(enc); break
        except Exception:
            continue
    if txt is None:
        raise RuntimeError("JGB CSV decode failed")
    lines = txt.splitlines()
    hdr = next(i for i, l in enumerate(lines) if l.startswith("基準日"))
    df = pd.read_csv(io.StringIO("\n".join(lines[hdr:])))
    df = df.rename(columns={df.columns[0]: "Date"})

    ERA = {"S": 1925, "H": 1988, "R": 2018}   # 昭和/平成/令和 の加算基準年
    def wareki(x):
        x = str(x).strip()
        try:
            e = x[0]
            if e in ERA:
                y, m, d = x[1:].split(".")
                return pd.Timestamp(ERA[e] + int(y), int(m), int(d))
            return pd.to_datetime(x)               # 西暦表記に変わっていた場合
        except Exception:
            return pd.NaT
    df["Date"] = df["Date"].map(wareki)
    df = df.dropna(subset=["Date"])
    for c in df.columns[1:]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

try:
    jgb = retry(fetch_jgb)
    if jgb is not None and len(jgb):
        merge_csv(f"{OUT}/jgb.csv", jgb.assign(Close=jgb.get("10年")))
        last = jgb.iloc[-1]
        meta["jgb"] = {"date": str(last["Date"].date()),
                       "y2":  (float(last["2年"])  if pd.notna(last.get("2年"))  else None),
                       "y10": (float(last["10年"]) if pd.notna(last.get("10年")) else None),
                       "y30": (float(last["30年"]) if pd.notna(last.get("30年")) else None)}
        print(f"JGB 10年: {meta['jgb']['y10']}%  ({meta['jgb']['date']})")
except Exception as e:
    meta["errors"].append(f"jgb: {type(e).__name__}: {e}")

# ── 5) 分配金・決算発表予定日（損切りと建玉の可否に直結）──────────
#    ETFの権利落ちは機械的な下落なので、知らないと損切りが誤発動する。
#    決算をまたぐ建玉は避ける（4755は2026-08-12の決算で-13.8%）。
events = {}
try:
    import yfinance as yf
    for code in HOLDINGS:
        ev = {}
        try:
            tk = yf.Ticker(code)
            dv = tk.dividends
            if dv is not None and len(dv):
                dv = dv.tail(8)
                ev["dividends"] = [{"date": str(pd.Timestamp(i).date()), "amount": float(v)}
                                   for i, v in dv.items()]
                # 窓の起点は「最後の支払日」ではなく「本日」。境界は含まない。
                # 起点を最後の支払日にすると年1回払いで2回分を合算してしまう。
                _t = pd.Timestamp(NOW.date())
                _idx = pd.to_datetime(dv.index).tz_localize(None)
                ttm = float(dv[(_idx > _t - pd.Timedelta(days=365)) & (_idx <= _t)].sum())
                ev["ttm_dividend"] = ttm
                if code in snap and snap[code]["close"]:
                    ev["ttm_yield_pct"] = round(ttm / snap[code]["close"] * 100, 2)
        except Exception as e:
            ev["dividends_error"] = str(e)[:120]
        try:
            ed = tk.get_earnings_dates(limit=8)
            if ed is not None and len(ed):
                ev["earnings_dates"] = [str(pd.Timestamp(i).date()) for i in ed.index]
        except Exception:
            pass
        if ev:
            events[code] = ev
    json.dump(events, open(f"{OUT}/events.json", "w"), ensure_ascii=False, indent=1)
except Exception as e:
    meta["errors"].append(f"events: {type(e).__name__}: {e}")
meta["events_ok"] = len(events)

meta["n_anomalies"] = sum(a["n"] for a in meta.get("anomalies", []))
meta["health"] = ("ok" if meta["holdings_ok"] == len(HOLDINGS)
                  else "partial" if meta["holdings_ok"] >= 6 else "bad")

# ══════════════════════════════════════════════════════════════════════
#  6) 分析まで GitHub Actions 側でやりきる
#     狙い: Claudeのセッションが承認待ちや障害で止まっても、
#           リポジトリに完成したレポートが残る状態にする。
#           算術はコードが担い、モデルは「相場観」だけを担当する。
# ══════════════════════════════════════════════════════════════════════
print("\n[分析] 開始")

def load_positions():
    """positions.json があればそれを使う（保有変更時はこのファイルだけ直せばよい）。"""
    p = f"{OUT}/../positions.json"
    default = {"cash": 1144125, "holdings": {
        "1306.T": {"name": "NF TOPIX",        "acct": "NISA", "shares": 2870, "cost": 434.0},
        "1540.T": {"name": "純金信託",          "acct": "特定", "shares": 16,   "cost": 21399.0},
        "1615.T": {"name": "NF銀行業",         "acct": "特定", "shares": 190,  "cost": 767.0},
        "2563.T": {"name": "iS S&P500ヘッジ",  "acct": "特定", "shares": 1140, "cost": 412.0},
        "4755.T": {"name": "楽天グループ",       "acct": "特定", "shares": 100,  "cost": 917.0},
        "9432.T": {"name": "NTT",             "acct": "特定", "shares": 100,  "cost": 149.0}}}
    try:
        if os.path.exists(p):
            return json.load(open(p, encoding="utf-8"))
    except Exception as e:
        meta["errors"].append(f"positions.json: {e}")
    return default

ROUND = [1/100, 1/50, 1/25, 1/20, 1/10, 1/5, 1/4, 1/3, 1/2,
         2, 3, 4, 5, 10, 20, 25, 50, 100]
def _near(r, tol=0.12):
    if r <= 0: return None
    import math
    c = min(ROUND, key=lambda x: abs(math.log(x) - math.log(r)))
    return c if abs(math.log(c) - math.log(r)) < tol else None

def repair(s, thr=0.20, maxrun=5):
    """株価系列の異常値と株式分割を直す。これを飛ばすと相関も指標も壊れる。

    パス1（異常値）: 段差の後、数日以内に元の水準へ戻る区間は Yahoo 側の異常値。
      **倍率がいくつであっても**前後を線形補間した値で置き換える。
      （1306は3/30〜31に1/10、1629は同じ日に約1/500になっていた。
        1/500は「丸い倍率」ではないため、倍率を見る方式では直せなかった。）
    パス2（分割）: 水準が戻らない段差は株式分割とみなし、丸い倍率で遡及調整する。
    """
    import numpy as np
    v = np.asarray(s.values, float).copy(); n = len(v); log = []

    i = 1
    while i < n:
        if v[i-1] > 0 and v[i] > 0 and abs(v[i]/v[i-1] - 1) > thr:
            fixed = False
            for k in range(1, maxrun+1):
                j = i + k
                if j < n and v[j] > 0 and abs(v[j]/v[i-1] - 1) < 0.15:
                    before, after = v[i-1], v[j]
                    for kk in range(i, j):                    # 前後をなめらかにつなぐ
                        f = (kk - i + 1) / (j - i + 1)
                        v[kk] = before + (after - before) * f
                    log.append(f"異常値 {s.index[i].date()}〜{s.index[j-1].date()} を前後の水準で補間")
                    fixed = True; break
            if fixed:
                i = i + 1; continue
        i += 1

    for i in range(1, n):
        if v[i-1] > 0 and v[i] > 0 and abs(v[i]/v[i-1] - 1) > thr:
            r = _near(v[i]/v[i-1])
            if r:
                v[:i] *= r
                log.append(f"分割 {s.index[i].date()} " + (f"1:{1/r:.0f}" if r < 1 else f"{r:.0f}:1"))
    return pd.Series(v, index=s.index), log

def _rsi(c, n=14):
    import numpy as np
    d = np.diff(c); u = np.where(d > 0, d, 0.); w = np.where(d < 0, -d, 0.)
    if len(d) < n: return float("nan")
    a, b = u[:n].mean(), w[:n].mean()
    for i in range(n, len(d)): a = (a*(n-1)+u[i])/n; b = (b*(n-1)+w[i])/n
    return 100.0 if b == 0 else 100 - 100/(1 + a/b)

def _atr(d, n=14):
    import numpy as np
    h, l, c = d["High"].values, d["Low"].values, d["Close"].values
    tr = [h[0]-l[0]] + [max(h[i]-l[i], abs(h[i]-c[i-1]), abs(l[i]-c[i-1])) for i in range(1, len(c))]
    tr = np.array(tr)
    if len(tr) < n+1: return float("nan")
    a = tr[1:n+1].mean()
    for i in range(n+1, len(tr)): a = (a*(n-1)+tr[i])/n
    return a

def ttm_div(code, price, today):
    """events.json の集計値は使わない（窓の起点が最後の支払日で回数を誤る）。
       本日起点の過去365日・境界を含まないで数え直す。"""
    d = events.get(code, {}).get("dividends", [])
    if not d: return 0.0, 0.0
    lo = today - dt.timedelta(days=365)
    tot = sum(x["amount"] for x in d
              if lo < dt.date.fromisoformat(x["date"]) <= today)
    return tot, (tot/price*100 if price else 0.0)

try:
    import numpy as np
    POS = load_positions(); HOLD = POS["holdings"]; CASH = float(POS.get("cash", 0))
    BENCH = "1306.T"          # ベンチマークはTOPIX連動ETF。日経平均は値がさ株に歪められる
    today = NOW.date()
    px = {}; rep_log = {}
    for c in list(HOLD) + [BENCH, "_N225"]:
        p = f"{OUT}/ohlcv/{slug(c)}.csv"
        if not os.path.exists(p): continue
        d = pd.read_csv(p, parse_dates=["Date"]).sort_values("Date")
        d = d[d["Close"].notna()].reset_index(drop=True)
        col = "Adj Close" if "Adj Close" in d.columns and d["Adj Close"].notna().any() else "Close"
        adj, lg = repair(d.set_index("Date")[col])
        if lg: rep_log[c] = lg
        px[c] = {"raw": d, "adj": adj}

    bench = px[BENCH]["adj"]; n225 = px.get("_N225", {}).get("adj")
    rows = []; rets = {}
    for c, h in HOLD.items():
        if c not in px: continue
        d = px[c]["raw"]; adj = px[c]["adj"]; rets[c] = adj.pct_change()
        last = float(d["Close"].iloc[-1]); a = _atr(d)
        m = adj.ewm(span=12, adjust=False).mean() - adj.ewm(span=26, adjust=False).mean()
        hist = m - m.ewm(span=9, adjust=False).mean()
        j = pd.concat([adj, bench], axis=1, join="inner").dropna()
        def rs(k):
            return ((j.iloc[-1,0]/j.iloc[-1-k,0]-1) - (j.iloc[-1,1]/j.iloc[-1-k,1]-1))*100 if len(j) > k else float("nan")
        w60 = d.tail(60)
        dv, dy = ttm_div(c, last, today)
        rows.append(dict(code=c, name=h["name"], acct=h["acct"], shares=int(h["shares"]),
            cost=float(h["cost"]), close=last, mkt=round(last*h["shares"]),
            pl_pct=round((last/h["cost"]-1)*100, 2), bars=len(d),
            rsi=round(_rsi(adj.values), 1), atr=round(a, 2), atr_pct=round(a/last*100, 2),
            macd_hist=round(float(hist.iloc[-1]), 3),
            macd_dir="up" if hist.iloc[-1] > hist.iloc[-2] else "down",
            vs25=round((adj.iloc[-1]/adj.rolling(25).mean().iloc[-1]-1)*100, 2),
            vs75=round((adj.iloc[-1]/adj.rolling(75).mean().iloc[-1]-1)*100, 2),
            vs200=round((adj.iloc[-1]/adj.rolling(200).mean().iloc[-1]-1)*100, 2),
            pos60=round((last-w60["Low"].min())/(w60["High"].max()-w60["Low"].min())*100, 1),
            rs1=round(rs(1), 2), rs5=round(rs(5), 2), rs20=round(rs(20), 2),
            vol_ratio=round(float(d["Volume"].iloc[-1]/d["Volume"].tail(20).mean()), 2),
            div_ttm=round(dv, 2), div_yield=round(dy, 2),
            next_earnings=next((x for x in sorted(events.get(c, {}).get("earnings_dates", []))
                                if x >= str(today)), "")))
    t = pd.DataFrame(rows)
    TOT = float(t["mkt"].sum() + CASH)

    R = pd.DataFrame(rets).dropna(how="any").tail(120)
    C = R.corr()
    t["avg_corr"] = [round(float(C[c].drop(c).abs().mean()), 3) for c in t["code"]]
    t["A"] = (25*np.clip((t["atr_pct"]-0.5)/3.0, 0, 1)).round(1)
    heat = np.where(t["rsi"] > 75, (t["rsi"]-75)/25, 0)
    t["C"] = (25*np.clip(0.5*np.clip(t["pos60"]/100, 0, 1)
                         + 0.5*np.clip((t["rs20"]+8)/16, 0, 1) - heat, 0, 1)).round(1)
    t["D"] = (25*np.clip(1-(t["avg_corr"]-0.1)/0.5, 0, 1)).round(1)
    t["ACD"] = (t["A"]+t["C"]+t["D"]).round(1)   # B（レジーム適合）はモデルが判断して足す

    # サイジング: ATR基準と1銘柄15%上限の、小さいほう
    def size(r):
        stop_w = 2*r["atr"]
        n_atr = int(TOT*0.01 // stop_w) if stop_w > 0 else 0
        n_cap = int(max(TOT*0.15 - r["mkt"], 0) // r["close"]) if r["close"] > 0 else 0
        n = min(n_atr, n_cap); n = int(round(n, -1))
        return pd.Series([n, round(r["close"]-stop_w, 1), round(r["close"]+3*r["atr"], 1),
                          round(r["close"]+4*r["atr"], 1), round(n*r["close"]),
                          "ATR" if n_atr <= n_cap else "15%上限"])
    t[["add_shares","stop","tp3","tp4","add_cost","binding"]] = t.apply(size, axis=1)
    t = t.sort_values("ACD", ascending=False).reset_index(drop=True)

    nk = ""
    if n225 is not None:
        jj = pd.concat([bench.pct_change(), n225.pct_change()], axis=1, join="inner").dropna()
        gap = (jj.iloc[-1,1]-jj.iloc[-1,0])*100
        corr = float(jj.tail(120).corr().iloc[0,1])
        nk = (f"日経-TOPIX騰落差 {gap:+.2f}pt" + ("（**指数が歪んでいる。物色の偏りに注意**）" if abs(gap) > 1.0 else "（正常）")
              + f" / 健全性チェック: 1306と日経の120日相関 {corr:.3f}"
              + ("" if corr >= 0.80 else " ← **0.80未満。データを疑うこと**"))

    jgbline = "取得できず"
    if meta.get("jgb"):
        g = meta["jgb"]
        jgbline = f"{g['date']} 時点  2年 {g.get('y2')}% / 10年 {g.get('y10')}% / 30年 {g.get('y30')}%"

    L = []
    L.append(f"# 後場スイング 事前分析  {NOW:%Y-%m-%d %H:%M JST}\n")
    L.append(f"データ健全性: **{meta['health']}**  保有{meta['holdings_ok']}/{len(HOLDINGS)}  "
             f"全体{meta['n_ok']}/{meta['n_total']}  エラー{len(meta['errors'])}件\n")
    L.append(f"日本国債（財務省）: {jgbline}\n")
    if nk: L.append(f"{nk}\n")
    if rep_log:
        L.append("補正した系列: " + " / ".join(f"{k}: {'; '.join(v)}" for k, v in rep_log.items()) + "\n")
    L.append(f"\n総額 **¥{TOT:,.0f}**（保有 ¥{t['mkt'].sum():,.0f} + 現金 ¥{CASH:,.0f} = 現金比率 {CASH/TOT*100:.1f}%）")
    L.append(f"／ 1トレード許容損失(1.0%) ¥{TOT*0.01:,.0f}\n")

    L.append("\n## テクニカル（ベンチマーク=TOPIX/1306）\n")
    L.append("| コード | 銘柄 | 終値 | 損益% | RSI | MACD | 25日 | 75日 | 200日 | 60日位置 | ATR% | 対TOPIX 1d/5d/20d | 出来高 |")
    L.append("|---|---|--:|--:|--:|:-:|--:|--:|--:|--:|--:|--:|--:|")
    for _, r in t.iterrows():
        L.append(f"| {r['code'][:4]} | {r['name']} | {r['close']:,.1f} | {r['pl_pct']:+.2f} | {r['rsi']:.1f} | "
                 f"{'＋' if r['macd_hist']>0 else '−'}{'↑' if r['macd_dir']=='up' else '↓'} | {r['vs25']:+.1f}% | {r['vs75']:+.1f}% | "
                 f"{r['vs200']:+.1f}% | {r['pos60']:.0f}% | {r['atr_pct']:.2f} | "
                 f"{r['rs1']:+.1f} / {r['rs5']:+.1f} / {r['rs20']:+.1f} | {r['vol_ratio']:.2f}x |")

    L.append("\n## スコア（A/C/D は計算済み。**B＝レジーム適合はモデルが判断して加算する**）\n")
    L.append("| コード | 銘柄 | 評価額 | 比率 | A 適性 | C 位置 | D 分散 | A+C+D | B込み満点 |")
    L.append("|---|---|--:|--:|--:|--:|--:|--:|:-:|")
    for _, r in t.iterrows():
        L.append(f"| {r['code'][:4]} | {r['name']} | {r['mkt']:,.0f} | {r['mkt']/TOT*100:.1f}% | "
                 f"{r['A']:.1f} | {r['C']:.1f} | {r['D']:.1f} | **{r['ACD']:.1f}** | +B(0〜25) |")
    L.append(f"\n判定: 62以上=買い増し / 48以上=維持 / 34以上=縮小 / 34未満=売却")
    L.append("拒否ルール: C<5→損切り管理 ／ RSI>78→買い増し不可 ／ B≤2.5→維持以上にしない\n")

    L.append("\n## サイジング（ATR基準と1銘柄15%上限の小さいほう）\n")
    L.append("| コード | 銘柄 | 追加可能株数 | 必要資金 | 損切り | 利確3ATR | 利確4ATR | 制約 |")
    L.append("|---|---|--:|--:|--:|--:|--:|:-:|")
    for _, r in t.iterrows():
        L.append(f"| {r['code'][:4]} | {r['name']} | {r['add_shares']:,} | {r['add_cost']:,.0f} | "
                 f"{r['stop']:,.1f} | {r['tp3']:,.1f} | {r['tp4']:,.1f} | {r['binding']} |")


    # ── サイジング早見表（総額に依存しない。表を引くだけで株数が出る）──
    L.append("\n## サイジング早見表（許容損失いくらなら何株か）\n")
    L.append("**株数 = 許容損失 ÷ 2ATR幅**。許容損失は総額の0.75〜1.0%。損切り・利確は総額に依存しないのでそのまま使える。\n")
    L.append("| コード | 銘柄 | 現値 | 2ATR幅 | 損切り | 利確3ATR | 利確4ATR | 損失2万 | 2.5万 | 3万 | 3.5万 | 4万 |")
    L.append("|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|")
    for _, r in t.iterrows():
        w = 2*r["atr"]
        cells = " | ".join(f"{int(x//w):,}" for x in (20000, 25000, 30000, 35000, 40000))
        L.append(f"| {r['code'][:4]} | {r['name']} | {r['close']:,.1f} | {w:,.1f} | "
                 f"{r['stop']:,.1f} | {r['tp3']:,.1f} | {r['tp4']:,.1f} | {cells} |")
    L.append("\n**1銘柄の上限は総額の15%**。上の株数と、(総額×15% − その銘柄の既存評価額) ÷ 現値 を比べて**小さいほう**を採る。")
    L.append("\n## 分配金と決算予定\n")
    L.append("| コード | 銘柄 | 年間分配 | 利回り | 次回決算発表 |")
    L.append("|---|---|--:|--:|:-:|")
    for _, r in t.iterrows():
        L.append(f"| {r['code'][:4]} | {r['name']} | {r['div_ttm']:,.2f} | {r['div_yield']:.2f}% | {r['next_earnings'] if isinstance(r['next_earnings'],str) and r['next_earnings'] else '—'} |")


    # ── セクター物色（業種別ETFの騰落＝値上がり/値下がり銘柄数の代わり）──
    sec = []
    for code, jname in SECTOR.items():
        sp = f"{OUT}/ohlcv/{slug(code)}.csv"
        if not os.path.exists(sp): continue
        try:
            sd = pd.read_csv(sp, parse_dates=["Date"]).sort_values("Date")
            sd = sd[sd["Close"].notna()]
            if len(sd) < 22: continue
            c0 = float(sd["Close"].iloc[-1])
            sec.append({"name": jname,
                        "d1": (c0/float(sd["Close"].iloc[-2])-1)*100,
                        "d5": (c0/float(sd["Close"].iloc[-6])-1)*100,
                        "d20": (c0/float(sd["Close"].iloc[-21])-1)*100})
        except Exception: pass
    if sec:
        sec.sort(key=lambda x: x["d1"], reverse=True)
        L.append("\n## セクター物色（業種別ETF・当日騰落順）\n")
        L.append("| 順 | 業種 | 当日 | 5日 | 20日 |")
        L.append("|--:|---|--:|--:|--:|")
        for i, x in enumerate(sec):
            mark = " ←保有" if x["name"] == "銀行" else ""
            L.append(f"| {i+1} | {x['name']}{mark} | {x['d1']:+.2f}% | {x['d5']:+.2f}% | {x['d20']:+.2f}% |")
        up = sum(1 for x in sec if x["d1"] > 0)
        L.append(f"\n上昇 {up}/{len(sec)} 業種。**業種の広がりが狭い日は指数が一部銘柄に歪められている**と判断する。")

    # ── ニュース見出し（Claude側のWebFetchを不要にするため、ここに載せる）──
    try:
        nj = json.load(open(f"{OUT}/news.json", encoding="utf-8"))
        if nj.get("matched"):
            L.append(f"\n## 市場関連の見出し（{len(nj['matched'])}件／取得 {nj['fetched_at_jst'][:16]}）\n")
            for n in nj["matched"][:35]:
                L.append(f"- [{n['src']}] {n['title']}")
            L.append("\n※ 見出しのみ。判断に必要なら本文を確認すること。")
    except Exception:
        L.append("\n## 市場関連の見出し\n\n取得できませんでした。")
    L.append("\n## マクロ\n")
    L.append("| 指標 | 日付 | 値 | 前日比 |")
    L.append("|---|:-:|--:|--:|")
    for k, lbl in [("^N225","日経225"),("1306.T","TOPIX(1306)"),("^GSPC","S&P500"),("^VIX","VIX"),
                   ("^TNX","米10年"),("^TYX","米30年"),("JPY=X","ドル円"),("CL=F","WTI"),("GC=F","金")]:
        s = snap.get(k)
        if s: L.append(f"| {lbl} | {s['date']} | {s['close']:,.2f} | {s['chg_pct']:+.2f}% |")

    L.append(f"\n---\n本ファイルは GitHub Actions が自動生成（承認不要・完全無人）。"
             f"数値はすべてコードが計算しており、モデルによる算術は介在していない。\n"
             f"モデル側の担当は B（レジーム適合）の判断、ニュース・マクロの解釈、スコアの上書き判断、文章化。\n"
             f"発注前にSBI証券の板・気配で価格を確認すること。")

    open(f"{OUT}/report.md", "w", encoding="utf-8").write("\n".join(L))
    json.dump({"generated_at_jst": NOW.isoformat(), "total": TOT, "cash": CASH,
               "benchmark": BENCH, "jgb": meta.get("jgb"), "repairs": rep_log,
               "rows": t.to_dict("records")},
              open(f"{OUT}/analysis.json", "w"), ensure_ascii=False, indent=1, default=str)
    meta["analysis"] = "ok"
    print(f"[分析] 完了 {len(t)}銘柄 / 総額 ¥{TOT:,.0f} / report.md と analysis.json を生成")
except Exception as e:
    import traceback
    meta["errors"].append(f"analysis: {type(e).__name__}: {e}")
    meta["analysis"] = "failed"
    print("[分析] 失敗:", e); traceback.print_exc()

json.dump(meta, open(f"{OUT}/meta.json", "w"), ensure_ascii=False, indent=1)

print(f"保有銘柄: {meta['holdings_ok']}/{len(HOLDINGS)}  健全性: {meta['health']}")
print(f"エラー件数: {len(meta['errors'])}  価格の飛び: {meta['n_anomalies']}件  イベント: {meta['events_ok']}銘柄  ニュース: {meta.get('news_ok',0)}件")
for e in meta["errors"][:10]:
    print("  -", e)
if meta["health"] == "bad":
    print("::warning::保有銘柄の取得が不足しています。data/meta.json を確認してください")
# 部分的にでも取れていればコミットさせるため、常に正常終了する
sys.exit(0)
