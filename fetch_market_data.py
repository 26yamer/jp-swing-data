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
import os, sys, re, time, json, datetime as dt
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
          "1625.T":"電機・精密","1626.T":"情報通信・サービスその他","1627.T":"電力・ガス",
          "1628.T":"運輸・物流","1629.T":"商社・卸売","1630.T":"小売","1631.T":"銀行",
          "1632.T":"金融（除く銀行）","1633.T":"不動産"}
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
JGB_BASE = "https://www.mof.go.jp/jgbs/reference/interest_rate/"
JGB_HDRS = {"User-Agent": "Mozilla/5.0 (compatible; market-bot)"}

def _jgb_text(urls):
    """最初に「基準日」ヘッダを含む本文が取れたURLの中身を返す。
       jgbcm.csv は当月分だけで数百バイトしかないので、サイズで判定してはいけない。"""
    last = None
    for u in urls:
        try:
            rr = __import__("requests").get(u, timeout=60, headers=JGB_HDRS)
            if rr.status_code != 200:
                last = f"{u} -> HTTP {rr.status_code}"; continue
            for enc in ("cp932", "shift_jis", "utf-8-sig", "utf-8"):
                try:
                    t = rr.content.decode(enc); break
                except Exception:
                    t = None
            if t and "基準日" in t:
                return t, u, None
            last = f"{u} -> 基準日ヘッダなし ({len(rr.content)}bytes)"
        except Exception as e:
            last = f"{u} -> {type(e).__name__}"
    return None, None, last

ERA = {"S": 1925, "H": 1988, "R": 2018}   # 昭和/平成/令和 の加算基準年

def _jgb_parse(txt):
    import io
    lines = txt.splitlines()
    hdr = next(i for i, l in enumerate(lines) if l.startswith("基準日"))
    df = pd.read_csv(io.StringIO("\n".join(lines[hdr:])))
    df = df.rename(columns={df.columns[0]: "Date"})

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

def fetch_jgb():
    """★全履歴版は月次更新で、当月の日次が入っていない。
       実際 9/11 の実行で最新が 8/31 のまま（11日遅れ）になっていた。
       金利は保有の判断で最も効く変数（NTTの利回り差・銀行・REIT）なので、
       履歴版と当月版の両方を取って結合する。片方だけでも走る。"""
    frames, used, errs = [], [], []
    for label, urls in [("履歴", [JGB_BASE + "data/jgbcm_all.csv", JGB_BASE + "jgbcm_all.csv"]),
                        ("当月", [JGB_BASE + "jgbcm.csv"])]:
        txt, url, err = _jgb_text(urls)
        if txt is None:
            errs.append(f"{label}: {err}"); continue
        try:
            d = _jgb_parse(txt)
            if len(d):
                frames.append(d); used.append(f"{label}({url.rsplit('/', 1)[-1]}:{len(d)}行)")
        except Exception as e:
            errs.append(f"{label}: parse {type(e).__name__}")
    if not frames:
        raise RuntimeError("JGB: " + " / ".join(errs) if errs else "JGB: 取得先なし")
    df = (pd.concat(frames, ignore_index=True)
            .drop_duplicates(subset=["Date"], keep="last")
            .sort_values("Date").reset_index(drop=True))
    if errs:
        meta["errors"].append("jgb: " + " / ".join(errs))
    print(f"JGB: {' + '.join(used)} → 結合 {len(df)}行（最新 {df['Date'].iloc[-1].date()}）")
    return df

try:
    jgb = retry(fetch_jgb)
    if jgb is None or not len(jgb):
        # 当日取れなくても、蓄積済みの jgb.csv があれば金利は読める。
        # 「取得できず」で金利を丸ごと落とすと、レジーム判断の主材料が消える。
        if os.path.exists(f"{OUT}/jgb.csv"):
            meta["jgb_stale"] = True
            print("JGB: 当日の取得に失敗。蓄積済みの jgb.csv を使う")
    if jgb is not None and len(jgb):
        merge_csv(f"{OUT}/jgb.csv", jgb.assign(Close=jgb.get("10年")))
        last = jgb.iloc[-1]
        age = (pd.Timestamp(NOW.date()) - last["Date"]).days
        meta["jgb"] = {"date": str(last["Date"].date()), "age_days": int(age),
                       "y2":  (float(last["2年"])  if pd.notna(last.get("2年"))  else None),
                       "y10": (float(last["10年"]) if pd.notna(last.get("10年")) else None),
                       "y30": (float(last["30年"]) if pd.notna(last.get("30年")) else None)}
        # 金利が数日古いまま「現在値」として使われると、NTTの利回り差や
        # 銀行の追い風の判断が丸ごとずれる。古ければ必ず表に出す。
        if age > 5:
            meta["jgb_stale"] = True
            meta["errors"].append(f"jgb: 最新が{age}日前（{last['Date'].date()}）")
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
#  7) 全銘柄スクリーニング
#     これまでは保有銘柄しかスコアリングしておらず、東証約4,000のうち
#     0.2%しか俎上に載っていなかった。「何を新しく買うか」を出せる状態にする。
#
#     設計上の要点:
#       ・生データはコミットしない（4,000銘柄×2年をgitに置くと破綻する）。
#         ジョブ内で取得→計算→**上位候補の表だけ**を残す。
#       ・銘柄リストは初回に総当たりで作って universe.json に保存し、
#         以降は再利用する（30日で作り直す）。
#       ・時間予算を持ち、超えたらそこまでの分で結果を出す。止まらない。
# ══════════════════════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════════════════════
#  指標ヘルパ（repair / _rsi / _atr）
#  ★ここに置く理由: 全銘柄スクリーニングがこれらを使う。以前は分析セクション
#    （もっと下）で定義していたため、スクリーニング実行時には未定義で
#    NameError が全銘柄で発生し、except で握りつぶされて通過0件になっていた。
#    定義は必ず最初の呼び出しより前に置くこと。
# ══════════════════════════════════════════════════════════════════════
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

def adjusted_frame(x):
    """OHLC を Adj Close の比率で調整した枠を返す。
       Adj Close が無い／壊れている場合は素通りさせる（生値のまま）。"""
    out = x.copy()
    if "Adj Close" not in out.columns:
        return out
    f = out["Adj Close"] / out["Close"]
    f = f.replace([float("inf"), float("-inf")], float("nan"))
    if f.isna().all():
        return out
    f = f.ffill().bfill()
    for c in ("Open", "High", "Low", "Close"):
        if c in out.columns:
            out[c] = out[c] * f
    return out

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


# ══════════════════════════════════════════════════════════════════════
#  5a) 銘柄マスタ（JPX 東証上場銘柄一覧）と 適時開示（TDnet）
#      狙い: スクリーニング結果に「銘柄名」「業種」「当日の開示」を付ける。
#            コードだけの表は人間が検証できず、なぜ動いているかの裏も取れない。
#      注意: どちらも取得に失敗してもジョブは止めない。マスタは前回の
#            キャッシュにフォールバックし、開示は空で先に進む。
# ══════════════════════════════════════════════════════════════════════
JPX_URLS = [
    "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xlsx",
    "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls",
]
MASTER_MIN_ROWS     = 1000        # これ未満しか取れなければ壊れたファイルとみなす
MASTER_MAX_AGE_DAYS = 25          # 月初の第3営業日に更新されるので25日で取り直す
EXCLUDE_MARKET_KW   = ["pro market", "pro-market", "ｐｒｏ"]
# 2024年以降、新規コードには英字が入る（例 130A）。数字だけで判定してはいけない。
CODE_RE = re.compile(r"^[0-9][0-9A-Za-z]{3}$")

def classify_kind(mkt):
    m = mkt or ""
    if "ETF" in m or "ETN" in m: return "ETF/ETN"
    if "REIT" in m or "インフラ" in m or "ベンチャー" in m or "カントリー" in m: return "REIT等"
    if "出資証券" in m: return "出資証券"
    if "内国株式" in m or "外国株式" in m: return "株式"
    return (m[:8] or "不明")

def _master_bytes():
    import requests
    last = None
    for u in JPX_URLS:
        try:
            r = requests.get(u, timeout=120, headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code == 200 and len(r.content) > 50_000:
                return u, r.content
            last = f"{u} -> HTTP {r.status_code} / {len(r.content)}bytes"
        except Exception as e:
            last = f"{u} -> {type(e).__name__}: {e}"
    raise RuntimeError(last or "URLなし")

def _xlsx_rows(blob):
    """.xlsx を標準ライブラリだけで読む（openpyxl 等が無い環境でも動かすため）。
       xlsx は XML の zip なので、共有文字列表とシートを直接引けば足りる。
       ここが動けば「銘柄名が付かない」という静かな劣化が環境依存でなくなる。"""
    import zipfile, io
    import xml.etree.ElementTree as ET
    NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    z = zipfile.ZipFile(io.BytesIO(blob))

    shared = []
    if "xl/sharedStrings.xml" in z.namelist():
        for si in ET.fromstring(z.read("xl/sharedStrings.xml")):
            # <si> の下は <t> 直下か、書式ごとに分かれた <r><t> の連なり
            shared.append("".join(t.text or "" for t in si.iter(NS + "t")))

    names = [n for n in z.namelist() if n.startswith("xl/worksheets/sheet")]
    if not names:
        raise RuntimeError("シートが見つからない")
    sheet = sorted(names)[0]

    def colnum(ref):
        n = 0
        for ch in ref:
            if not ch.isalpha(): break
            n = n * 26 + (ord(ch.upper()) - 64)
        return n - 1

    rows = []
    root = ET.fromstring(z.read(sheet))
    for r in root.iter(NS + "row"):
        cells, width = {}, 0
        for c in r.iter(NS + "c"):
            i = colnum(c.get("r") or "A")
            t = c.get("t")
            if t == "inlineStr":
                v = "".join(x.text or "" for x in c.iter(NS + "t"))
            else:
                node = c.find(NS + "v")
                v = node.text if node is not None else None
                if t == "s" and v is not None:
                    k = int(v)
                    v = shared[k] if 0 <= k < len(shared) else ""
            cells[i] = v
            width = max(width, i + 1)
        rows.append([cells.get(i) for i in range(width)])
    return rows

def _master_frame(blob):
    """まず標準ライブラリで読む。だめなら pandas のエンジンを順に試す。
       依存を1つ減らすほうが、環境差で静かに壊れる確率が下がる。"""
    errs = []
    try:
        rows = _xlsx_rows(blob)
        rows = [r for r in rows if any(x not in (None, "") for x in r)]
        if len(rows) < 2:
            raise RuntimeError(f"行が足りない: {len(rows)}")
        hdr = [str(x or "").strip() for x in rows[0]]
        w = len(hdr)
        body = [(r + [None] * w)[:w] for r in rows[1:]]
        df = pd.DataFrame(body, columns=hdr).astype("object")
        df = df.where(pd.notna(df), None)      # pandas が None を NaN にするのを戻す
        print(f"[マスタ] 標準ライブラリで読み込み: {len(df):,}行 × {w}列")
        return df
    except Exception as e:
        errs.append(f"stdlib:{type(e).__name__}:{str(e)[:60]}")

    import io
    for eng in ("openpyxl", "calamine", "xlrd"):
        try:
            df = pd.read_excel(io.BytesIO(blob), dtype=str, engine=eng)
            print(f"[マスタ] pandas({eng}) で読み込み: {len(df):,}行")
            return df
        except Exception as e:
            errs.append(f"{eng}:{type(e).__name__}")
    raise RuntimeError("読み込み失敗 " + " / ".join(errs))

def _col(df, *names):
    norm = lambda s: str(s).replace(" ", "").replace("　", "").strip()
    for n in names:
        for c in df.columns:
            if norm(c) == n: return c
    return None

def load_master():
    """コード → 銘柄名・市場区分・17業種 の対照表。"""
    p, cached = f"{OUT}/jpx_master.json", None
    try:
        if os.path.exists(p):
            cached = json.load(open(p, encoding="utf-8"))
            age = (NOW - dt.datetime.fromisoformat(cached["built_at_jst"])).days
            if age < MASTER_MAX_AGE_DAYS and len(cached.get("rows", {})) >= MASTER_MIN_ROWS:
                print(f"[マスタ] jpx_master.json を再利用 ({len(cached['rows']):,}銘柄 / {age}日前)")
                meta["master"] = {"count": len(cached["rows"]), "cached": True}
                return cached["rows"]
    except Exception as e:
        meta["errors"].append(f"master cache: {e}")
    try:
        url, blob = _master_bytes()
        df = _master_frame(blob)
        c_code = _col(df, "コード"); c_name = _col(df, "銘柄名")
        c_mkt  = _col(df, "市場・商品区分"); c_s17 = _col(df, "17業種区分")
        c_s33  = _col(df, "33業種区分");    c_sz  = _col(df, "規模区分")
        if not (c_code and c_name):
            raise RuntimeError(f"想定した列が無い: {list(df.columns)[:12]}")
        def cell(r, c):
            """読み手（標準ライブラリ / pandas）で None にも NaN にもなりうるので、
               どちらでも空文字に寄せる。"""
            if not c: return ""
            v = r[c]
            if v is None or v != v: return ""       # v != v は NaN の判定
            v = str(v).strip()
            return "" if v in ("nan", "None", "-", "－") else v

        rows = {}
        for _, r in df.iterrows():
            code = cell(r, c_code)
            if not CODE_RE.match(code): continue
            mkt = cell(r, c_mkt)
            if any(k in mkt.lower() for k in EXCLUDE_MARKET_KW): continue
            rows[code] = {"name": cell(r, c_name), "market": mkt,
                          "s17": cell(r, c_s17), "s33": cell(r, c_s33),
                          "size": cell(r, c_sz), "kind": classify_kind(mkt)}
        if len(rows) < MASTER_MIN_ROWS:
            raise RuntimeError(f"件数が少なすぎる: {len(rows)}")
        json.dump({"built_at_jst": NOW.isoformat(), "source": url, "rows": rows},
                  open(p, "w"), ensure_ascii=False)
        meta["master"] = {"count": len(rows), "source": url}
        print(f"[マスタ] JPX 東証上場銘柄一覧を取得: {len(rows):,}銘柄")
        return rows
    except Exception as e:
        meta["errors"].append(f"master: {type(e).__name__}: {e}")
        if cached and cached.get("rows"):
            print(f"[マスタ] 取得失敗。古いキャッシュで継続: {e}")
            meta["master"] = {"count": len(cached["rows"]), "stale": True, "error": str(e)[:120]}
            return cached["rows"]
        print(f"[マスタ] 取得失敗・キャッシュ無し: {e}")
        meta["master"] = {"error": str(e)[:120]}
        return {}

# 17業種区分 → 業種別ETF。セクター物色の表と候補を機械的に突き合わせるため。
S17_TO_ETF = {v: k for k, v in SECTOR.items()}

def _s17key(x):
    """括弧の全半角・中黒・空白のゆれを吸収する。文字そのものが違う場合
       （電力/電気 のような取り違え）は吸収せず、未対応として表に出す。"""
    import unicodedata
    x = unicodedata.normalize("NFKC", str(x or ""))
    for ch in "（）()・･ 　":
        x = x.replace(ch, "")
    return x

S17_NORM = {_s17key(v): k for k, v in SECTOR.items()}

def s17_etf(name):
    return S17_TO_ETF.get(name) or S17_NORM.get(_s17key(name), "")

TDNET_MAX_PAGES = 12

def fetch_tdnet(days=4):
    """適時開示（TDnet）の直近4日分。月曜に金曜の開示を拾うため4日みる
       （土日祝のURLは404になるだけなので空振りは安い）。「なぜ動いているか」の裏取りに使う。
       TDnetは約31日分しか保持しないので、履歴の分析には使えない。"""
    import requests, html as _html
    out, pages, errs = {}, 0, 0
    for back in range(days):
        d = (NOW - dt.timedelta(days=back)).strftime("%Y%m%d")
        for pg in range(1, TDNET_MAX_PAGES + 1):
            u = f"https://www.release.tdnet.info/inbs/I_list_{pg:03d}_{d}.html"
            try:
                r = requests.get(u, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
            except Exception as e:
                errs += 1; meta["errors"].append(f"tdnet {d}p{pg}: {type(e).__name__}"); break
            if r.status_code != 200: break
            try:
                txt = r.content.decode("utf-8")
            except UnicodeDecodeError:
                txt = r.content.decode("cp932", errors="replace")
            pages += 1
            before = sum(len(v) for v in out.values())
            for tr in re.split(r"<tr", txt, flags=re.I)[1:]:
                # 属性は " と ' の両方があり得る。要素も td/div の両方が使われてきた。
                mc = re.search(r'kjCode["\']?[^>]*>\s*([0-9][0-9A-Za-z]{3,4})', tr)
                mt = re.search(r'kjTitle["\']?[^>]*>(.*?)</(?:td|th|div)>', tr, re.S | re.I)
                mm = re.search(r'kjTime["\']?[^>]*>\s*([\d:]+)', tr)
                if not (mc and mt): continue
                title = _html.unescape(re.sub(r"<[^>]+>", " ", mt.group(1)))
                title = re.sub(r"\s+", " ", title).strip()
                if not title: continue
                out.setdefault(mc.group(1)[:4], []).append(
                    {"date": d, "time": (mm.group(1) if mm else ""), "title": title[:90]})
            if sum(len(v) for v in out.values()) == before: break   # 空ページ＝終端
    total = sum(len(v) for v in out.values())
    meta["tdnet"] = {"pages": pages, "codes": len(out), "items": total, "errors": errs}
    print(f"[適時開示] {pages}ページ / {len(out)}銘柄 / {total}件")
    return out

SCREEN_ENABLED   = True
SCREEN_BUDGET_S  = 2700      # 取得に使ってよい秒数（超えたら打ち切って結果を出す）
                             # マスタ経由だと対象が約4,000銘柄になるため 30分→45分。
                             # publicリポジトリのActionsは実行時間の上限が無い。
SCREEN_CHUNK     = 180       # 1リクエストあたりの銘柄数
MIN_TURNOVER     = 50_000_000   # 20日平均の売買代金がこの額未満は流動性不足として除外
MIN_PRICE        = 100          # 低位株を除外（呼値の粗さで往復コストが重くなる）
MIN_ATR_PCT      = 1.5          # これ未満は値幅が小さくスイングの期待値が立たない
MAX_ATR_PCT      = 6.0          # これを超えるものは一過性の材料で動いている公算が大きい。
                                # 材料の中身をこの仕組みは見られないので、順位以前に外す。

# ══════════════════════════════════════════════════════════════════════
#  過去検証 ── このスコアに優位性があるのかを、実際に付いた値段で確かめる
#
#  なぜ要るか:
#    台帳は前向きに積み上がるが、平均+0.10Rを判定するには400件規模が要り、
#    しかも同じ日の銘柄は独立でないので、実効的には1年以上かかる。
#    一方、過去2年の実データは今ある。同じ問いに今日答えられる。
#
#  何を比べるのか（先に決めておく。後から良い結果を探さない）:
#    ① 順張り上位10件 vs 同じ母集団からの無作為10件
#    ② 逆張り上位10件 vs 同じ母集団からの無作為10件
#    ③ ①②を「TOPIXが200日線の上/下」で分けたとき
#    比較対象が無作為抽出なのが肝心。「平均Rがプラス」では意味がない。
#    流動性と値幅で絞った母集団から適当に買っても同じ結果なら、
#    順位付けは何もしていないことになる。
#
#  自分を欺かないための約束:
#    ・指標は t 時点までのデータだけで計算する（先読みしない）
#    ・建値は t の終値、決着は t+1 以降の高安。窓は寄値で約定
#    ・価格はすべて調整済み
#    ・保有期間の感度（10/15/25日）は「頑健性の確認」であって、
#      良い数字を選ぶためではない
#
#  どうしても残るバイアス（結果を良い方に歪める）:
#    ・生存バイアス。銘柄一覧は「今上場している会社」なので、
#      この2年で上場廃止になった会社が母集団から抜けている。
#    ・スリッページ・板の薄さ・約定できない場面を含まない。
#    ・税を引いていない。
# ══════════════════════════════════════════════════════════════════════
BT_ENABLED   = True
BT_PERIOD    = "2y"
BT_STEP      = 5        # 何営業日ごとに建てるか
BT_TOP       = 10       # 各サイドの採用数
BT_HOLDS     = (10, 15, 25)
BT_WARMUP    = 80       # 指標が立ち上がるのに要る本数
BT_MAX_AGE_D = 30       # これより新しい結果があれば作り直さない
BT_BUDGET_S  = 1500
BT_CHUNK     = 180
BT_DRAWS     = 5        # 無作為抽出の試行回数（ばらつきを均す）
BT_SEED      = 20260911

def _rsi_series(c, n=14):
    """全期間のRSI。1点ずつ再計算すると検証が終わらないので通しで出す。"""
    import numpy as np
    c = np.asarray(c, float)
    d = np.diff(c, prepend=c[0])
    up = np.where(d > 0, d, 0.0); dn = np.where(d < 0, -d, 0.0)
    au = np.full(len(c), np.nan); ad = np.full(len(c), np.nan)
    if len(c) <= n: return np.full(len(c), np.nan)
    au[n] = up[1:n+1].mean(); ad[n] = dn[1:n+1].mean()
    for i in range(n+1, len(c)):
        au[i] = (au[i-1]*(n-1) + up[i]) / n
        ad[i] = (ad[i-1]*(n-1) + dn[i]) / n
    rs = np.where(ad > 0, au/np.where(ad == 0, np.nan, ad), np.inf)
    return 100 - 100/(1+rs)

def _atr_series(h, l, c, n=14):
    import numpy as np
    h, l, c = map(lambda z: np.asarray(z, float), (h, l, c))
    pc = np.roll(c, 1); pc[0] = c[0]
    tr = np.maximum(h-l, np.maximum(abs(h-pc), abs(l-pc)))
    a = np.full(len(c), np.nan)
    if len(c) <= n: return a
    a[n] = tr[1:n+1].mean()
    for i in range(n+1, len(c)):
        a[i] = (a[i-1]*(n-1) + tr[i]) / n
    return a

def bt_features(ax, vol):
    """調整済みの枠から、全期間ぶんの指標を一度に作る。
       各検証日では添字を引くだけにして、計算量を抑える。"""
    import numpy as np
    cl = ax["Close"].to_numpy(float)
    out = {
        "close": cl,
        "rsi":   _rsi_series(cl),
        "atr":   _atr_series(ax["High"].to_numpy(float), ax["Low"].to_numpy(float), cl),
        "ma25":  ax["Close"].rolling(25).mean().to_numpy(float),
        "ma75":  ax["Close"].rolling(75).mean().to_numpy(float),
        "hi60":  ax["High"].rolling(60).max().to_numpy(float),
        "lo60":  ax["Low"].rolling(60).min().to_numpy(float),
        "open":  ax["Open"].to_numpy(float),
        "high":  ax["High"].to_numpy(float),
        "low":   ax["Low"].to_numpy(float),
        "turn":  (ax["Close"]*vol).rolling(20).mean().to_numpy(float),
        "volr":  (vol / vol.rolling(20).mean()).to_numpy(float),
    }
    r20 = np.full(len(cl), np.nan); r20[21:] = cl[21:]/cl[:-21] - 1
    out["r20"] = r20*100
    return out

def bt_score_at(f, i):
    """日 i 時点の特徴量。i より後のデータは一切使わない。"""
    import numpy as np
    c, a = f["close"][i], f["atr"][i]
    if not (c > 0) or not (a > 0): return None
    rng = f["hi60"][i] - f["lo60"][i]
    if not (rng > 0): return None
    m25, m75 = f["ma25"][i], f["ma75"][i]
    if not (m25 > 0) or not (m75 > 0): return None
    atr_pct = a/c*100
    return dict(close=c, atr=a, atr_pct=atr_pct, rsi=f["rsi"][i],
                vs25=(c/m25-1)*100, vs75=(c/m75-1)*100,
                pos60=(c-f["lo60"][i])/rng*100, r20=f["r20"][i],
                volr=f["volr"][i] if f["volr"][i] == f["volr"][i] else 1.0,
                turn=f["turn"][i])

def bt_simulate(f, i, entry, atr, hold):
    """i+1 以降の実際の高安で決着させる。窓は寄値で約定。"""
    stop, tgt = entry - 2*atr, entry + 3*atr
    n = len(f["close"])
    for k in range(i+1, min(i+1+hold, n)):
        op, hi, lo, cl = f["open"][k], f["high"][k], f["low"][k], f["close"][k]
        if lo <= stop:
            fill = min(op, stop) if op == op else stop
            return (fill-entry)/(2*atr), k-i, "stop"
        if hi >= tgt:
            fill = max(op, tgt) if op == op else tgt
            return (fill-entry)/(2*atr), k-i, "target"
    k = min(i+hold, n-1)
    return (f["close"][k]-entry)/(2*atr), k-i, "timeout"

def run_backtest(codes, bench="1306.T"):
    """過去2年で、順位付けに情報があるかを無作為抽出と比べる。

       ★全銘柄を同じ営業日カレンダーに揃えてから位置で引く。
         揃えないと、配列の位置 i が銘柄ごとに違う日付を指し、
         「同じ日に建てた」という前提が崩れて比較そのものが無意味になる。"""
    import numpy as np, yfinance as yf
    rng = np.random.default_rng(BT_SEED)

    # ① 基準となる営業日カレンダーと、その日のレジーム（200日線の上か）
    b = yf.download(bench, period=BT_PERIOD, interval="1d",
                    auto_adjust=False, progress=False)
    if isinstance(b.columns, pd.MultiIndex): b.columns = b.columns.get_level_values(0)
    b = b.dropna(subset=["Close"])
    cal = b.index
    bc = adjusted_frame(b)["Close"]
    bma = bc.rolling(200).mean()
    above = {i: (bool(bc.iloc[i] > bma.iloc[i]) if bma.iloc[i] == bma.iloc[i] else None)
             for i in range(len(cal))}
    print(f"[検証] 基準カレンダー {len(cal)}営業日（{cal[0].date()}〜{cal[-1].date()}）")

    # ★コード順のまま取ると、時間切れで打ち切られたときに
    #   1300〜4000番台（ETF・REIT・食品・化学）に偏った標本になり、
    #   それを市場全体の結果として報告してしまう。
    #   固定の種で混ぜてから取るので、打ち切られても中立な部分標本になる。
    codes = list(codes)
    rng.shuffle(codes)

    feats, t0, asked = {}, time.time(), 0
    for j in range(0, len(codes), BT_CHUNK):
        if time.time() - t0 > BT_BUDGET_S:
            print(f"[検証] 時間予算に達したため {len(feats)}銘柄で打ち切り"); break
        part = codes[j:j+BT_CHUNK]; asked += len(part)
        try:
            d = yf.download(part, period=BT_PERIOD, interval="1d", auto_adjust=False,
                            group_by="ticker", threads=True, progress=False)
        except Exception as e:
            meta["errors"].append(f"bt chunk {j}: {type(e).__name__}"); continue
        for c in part:
            try:
                x = d[c].dropna(subset=["Close"])
                if len(x) < len(cal)*0.9: continue      # 歯抜けが多い銘柄は外す
                x = x.reindex(cal)                      # ★同じカレンダーに揃える
                if x["Close"].isna().mean() > 0.05: continue
                x = x.ffill().dropna(subset=["Close"])
                if len(x) < len(cal): continue          # 先頭が埋まらないもの（新規上場）は外す
                ax = adjusted_frame(x)
                feats[c] = bt_features(ax, x["Volume"])
            except Exception:
                pass
        if (j // BT_CHUNK) % 4 == 0:
            print(f"  検証データ {len(feats)}銘柄  {time.time()-t0:.0f}秒")
    if len(feats) < 200:
        raise RuntimeError(f"検証に足る銘柄が集まらない: {len(feats)}")

    nbar = len(cal)
    trades = {k: [] for k in ("trend", "revert", "random")}
    n_dates = 0
    for i in range(BT_WARMUP, nbar - max(BT_HOLDS) - 1, BT_STEP):
        pool = []
        for c, f in feats.items():
            s = bt_score_at(f, i)
            if s is None: continue
            if s["close"] < MIN_PRICE: continue
            if not (s["turn"] >= MIN_TURNOVER): continue
            if not (MIN_ATR_PCT <= s["atr_pct"] <= MAX_ATR_PCT): continue
            s["code"] = c; pool.append(s)
        if len(pool) < 50: continue
        n_dates += 1
        df = pd.DataFrame(pool)
        tr, rv = rank_candidates(df)
        picks = {"trend": [r for _, r in tr.head(BT_TOP).iterrows() if r["trend"] > 0],
                 "revert": [r for _, r in rv.head(BT_TOP).iterrows() if r["revert"] > 0]}
        # 無作為は同じ母集団から。これが比較の基準線
        for _ in range(BT_DRAWS):
            idx = rng.choice(len(df), size=min(BT_TOP, len(df)), replace=False)
            picks.setdefault("random", []).extend(df.iloc[k] for k in idx)
        for kind, rows in picks.items():
            w = 1.0/BT_DRAWS if kind == "random" else 1.0
            for r in rows:
                f = feats[r["code"]]
                for hold in BT_HOLDS:
                    R, bars, how = bt_simulate(f, i, float(r["close"]), float(r["atr"]), hold)
                    trades[kind].append({"i": i, "hold": hold, "R": R, "bars": bars,
                                         "how": how, "w": w, "up": above.get(i)})
    meta["backtest_dates"] = n_dates
    cov = round(asked/len(codes)*100, 1) if codes else 0
    meta["backtest_coverage"] = {"asked": asked, "universe": len(codes),
                                 "pct": cov, "usable": len(feats)}
    print(f"[検証] {len(feats)}銘柄（母集団{len(codes):,}中{asked:,}件に照会 = {cov}%）/ "
          f"{n_dates}回の建て日 / 延べ {sum(len(v) for v in trades.values()):,}件")
    return trades, n_dates, len(feats), str(cal[0].date()), str(cal[-1].date()), cov

def bt_summary(trades, hold, regime=None):
    out = {}
    for kind, rows in trades.items():
        d = [r for r in rows if r["hold"] == hold
             and (regime is None or r["up"] == regime)]
        if not d: continue
        w = sum(r["w"] for r in d)
        if w <= 0: continue
        sR = sum(r["R"]*r["w"] for r in d)
        win = sum(r["w"] for r in d if r["R"] > 0)
        out[kind] = {"n": round(w, 1), "avg_r": round(sR/w, 4),
                     "win_pct": round(win/w*100, 1),
                     "target": round(sum(r["w"] for r in d if r["how"] == "target")/w*100, 1),
                     "stop": round(sum(r["w"] for r in d if r["how"] == "stop")/w*100, 1),
                     "timeout": round(sum(r["w"] for r in d if r["how"] == "timeout")/w*100, 1)}
    # 無作為との差が、順位付けが生んでいる値
    base = out.get("random", {}).get("avg_r")
    for k in ("trend", "revert"):
        if k in out and base is not None:
            out[k]["vs_random"] = round(out[k]["avg_r"] - base, 4)
            # 平均の差の粗い有意性（1件あたりのRの散らばりを1.0とみなす）
            n = out[k]["n"]
            out[k]["t_rough"] = round(out[k]["vs_random"] / (1.0/max(n, 1)**0.5), 2) if n else None
    return out

# ══════════════════════════════════════════════════════════════════════
#  シグナル台帳 ── この仕組みに効き目があるのかを実測する
#
#  なぜ必要か:
#    毎日推奨を出しているのに、それが儲かったのかを誰も測っていなかった。
#    測らなければ「前提が崩れたら訂正する」も判断材料が無く、
#    1ヶ月後に20本のレポートと0件の学習が残るだけになる。
#
#  何を測るのか:
#    モデルのB（レジーム判断）は測れない。測れるのは**スクリーニング**の部分。
#    その日の上位候補を、発注したものと見なして台帳に記録し、
#    その後の実際の高値・安値で 2ATR損切り / 3ATR利確 のどちらに先に当たったかを追う。
#
#  乱数は一切使わない。使うのは実際に付いた値段だけ。
#  ただしこれは**執行の記録ではなく、選別ロジックの追跡**である。
#  実際の約定値・スリッページ・板の薄さは含まれない。そこは割り引いて読むこと。
# ══════════════════════════════════════════════════════════════════════
# ── 執行の前提（ここ以外に数字を散らさない）─────────────────────
LOT              = 100      # 日本株の売買単位。10株単位で丸めると発注できない
RISK_PER_TRADE_P = 0.009    # 1トレードの許容損失（総額比）
MAX_WEIGHT       = 0.15     # 1銘柄の上限（総額比）
MIN_CASH_RATIO   = 0.20     # 現金比率の下限。これを割る発注はしない

SIG_PATH      = f"{OUT}/signals.csv"
SIG_TOP_N     = 10       # 各サイドの上位何件を台帳に載せるか
SIG_MAX_HOLD  = 15       # これを超えたら時間切れとして手仕舞う（営業日）
SIG_COLS = ["date", "kind", "rank", "code", "name", "s17", "score",
            "entry", "atr", "stop", "target", "adjf",
            "status", "exit_date", "exit", "r_multiple", "bars"]

def load_signals():
    if not os.path.exists(SIG_PATH):
        return pd.DataFrame(columns=SIG_COLS)
    try:
        df = pd.read_csv(SIG_PATH, dtype=str)
        for c in SIG_COLS:
            if c not in df.columns: df[c] = ""
        return df[SIG_COLS]
    except Exception as e:
        meta["errors"].append(f"signals read: {type(e).__name__}")
        return pd.DataFrame(columns=SIG_COLS)

def open_signal_map(sig):
    """未決着のシグナルを code ごとにまとめる。screen_all が各銘柄の日足を
       持っている最中に、そのまま判定できるようにするため。"""
    out = {}
    if sig.empty: return out
    op = sig[sig["status"].isin(["", "open", "nan"]) | sig["status"].isna()]
    for i, r in op.iterrows():
        out.setdefault(str(r["code"]), []).append(i)
    return out

def settle_signal(row, bars, adjf_now=None):
    """entry日より後の実際の高値・安値で決着を付ける。
       同じ日に損切りと利確の両方に触れた場合は**損切りを優先**する
       （日足では順序が分からないため、都合の良い方を採らない）。

       ★bars は**調整済み**の枠を渡すこと。生値で見ると、建てたあとに
         分割があった銘柄で価格が不連続に下がり、架空の損切りを量産する。
         建値・損切り・利確は建てた時点の調整係数で同じ土俵に移してから比べる。"""
    try:
        entry = float(row["entry"]); stop = float(row["stop"]); tgt = float(row["target"])
        atr = float(row["atr"])
        f0 = float(row.get("adjf") or 1.0)
    except Exception:
        return None
    if not f0 or f0 != f0: f0 = 1.0
    # 建値側を調整済みの土俵へ移す
    entry_a, stop_a, tgt_a, atr_a = entry*f0, stop*f0, tgt*f0, atr*f0
    after = bars[bars.index > pd.Timestamp(row["date"])]
    if not len(after): return None
    for n, (ts, b) in enumerate(after.iterrows(), start=1):
        op, hi, lo, cl = (float(b["Open"]) if "Open" in b else float(b["Close"]),
                          float(b["High"]), float(b["Low"]), float(b["Close"]))
        # ★寄りで損切り水準を飛び越えた日は、損切り値ではなく**寄値**で約定する。
        #   常に -1.00R として記録すると、窓を開けて下げた分の損が消え、
        #   成績が実態より良く出る（窓は下に開くほうが多い）。
        if lo <= stop_a:
            fill = min(op, stop_a)
            return dict(status="stop", exit_date=str(ts.date()), exit=round(fill/f0, 2),
                        r_multiple=round((fill-entry_a)/(2*atr_a), 2), bars=n)
        if hi >= tgt_a:
            fill = max(op, tgt_a)
            return dict(status="target", exit_date=str(ts.date()), exit=round(fill/f0, 2),
                        r_multiple=round((fill-entry_a)/(2*atr_a), 2), bars=n)
        if n >= SIG_MAX_HOLD:
            return dict(status="timeout", exit_date=str(ts.date()), exit=round(cl/f0, 2),
                        r_multiple=round((cl-entry_a)/(2*atr_a), 2), bars=n)
    return None      # まだ決着していない

def append_signals(sig, recs, kind, today):
    """その日の上位を台帳に足す。
       ★未決着の同じ銘柄があるうちは追加しない。
         以前は(日付,銘柄,種別)でしか重複を見ておらず、上位に居座る銘柄が
         連日で別行になっていた。1つの値動きを12回数えれば、件数だけが
         増えて「30件超えたので判断できる」と誤認する。"""
    have_open = set(sig.loc[sig["status"].isin(["", "open"]) | sig["status"].isna(),
                            "code"].astype(str)) if len(sig) else set()
    have_today = set(zip(sig["date"].astype(str), sig["code"].astype(str))) if len(sig) else set()
    add = []
    for i, r in enumerate(recs[:SIG_TOP_N], start=1):
        score = r.get("trend" if kind == "trend" else "revert", 0)
        try:
            score = float(score)
        except (TypeError, ValueError):
            continue
        if not (score > 0): continue          # NaN もここで落ちる
        code = str(r["code"])
        if code in have_open or (today, code) in have_today: continue
        entry, atr = float(r["close"]), float(r["atr"])
        if not (entry > 0 and atr > 0): continue
        have_open.add(code)
        add.append({"date": today, "kind": kind, "rank": i, "code": code,
                    "name": r.get("name", ""), "s17": r.get("s17", ""),
                    "score": score, "entry": round(entry, 2), "atr": round(atr, 2),
                    "stop": round(entry - 2*atr, 2), "target": round(entry + 3*atr, 2),
                    "adjf": r.get("adjf", 1.0),
                    "status": "open", "exit_date": "", "exit": "", "r_multiple": "", "bars": ""})
    if not add: return sig
    return pd.concat([sig, pd.DataFrame(add)], ignore_index=True)[SIG_COLS]

def signal_summary(sig):
    """決着済みだけで集計する。未決着を混ぜると勝率が水増しされる。"""
    if sig.empty: return {}
    d = sig[sig["status"].isin(["stop", "target", "timeout"])].copy()
    if d.empty: return {"closed": 0, "open": int((sig["status"] == "open").sum())}
    d["r"] = pd.to_numeric(d["r_multiple"], errors="coerce")
    d = d.dropna(subset=["r"])
    out = {"closed": int(len(d)), "open": int((sig["status"] == "open").sum()),
           "days": int(sig["date"].astype(str).nunique())}
    for k in ("trend", "revert", None):
        part = d if k is None else d[d["kind"] == k]
        if not len(part): continue
        win = int((part["r"] > 0).sum())
        out[k or "all"] = {
            "n": int(len(part)),
            "win_pct": round(win / len(part) * 100, 1),
            "avg_r": round(float(part["r"].mean()), 3),
            "sum_r": round(float(part["r"].sum()), 2),
            "target": int((part["status"] == "target").sum()),
            "stop": int((part["status"] == "stop").sum()),
            "timeout": int((part["status"] == "timeout").sum()),
            "avg_bars": round(float(pd.to_numeric(part["bars"], errors="coerce").mean()), 1)}
    return out

def load_universe(master=None):
    """スクリーニング対象の銘柄リスト。
       第一候補は JPX の上場銘柄一覧（正確・英字コードも拾える・数秒で済む）。
       取れなかったときだけ、従来どおり Yahoo を総当たりして作る。"""
    if master:
        keep = ("株式", "ETF/ETN", "REIT等", "出資証券")
        codes = [f"{c}.T" for c, v in sorted(master.items()) if v.get("kind") in keep]
        if len(codes) > 1000:
            from collections import Counter
            mix = Counter(v.get("kind") for v in master.values() if v.get("kind") in keep)
            print(f"[全銘柄] JPXマスタから {len(codes):,}銘柄を対象にする "
                  + " / ".join(f"{k}{n:,}" for k, n in mix.most_common()))
            meta["universe_source"] = "jpx_master"
            return codes
        meta["errors"].append(f"universe from master too small: {len(codes)}")

    p = f"{OUT}/universe.json"
    try:
        if os.path.exists(p):
            u = json.load(open(p, encoding="utf-8"))
            age = (NOW - dt.datetime.fromisoformat(u["built_at_jst"])).days
            if age < 30 and len(u.get("codes", [])) > 500:
                print(f"[全銘柄] universe.json を再利用 ({len(u['codes'])}銘柄 / {age}日前に作成)")
                return u["codes"]
    except Exception as e:
        meta["errors"].append(f"universe read: {e}")

    print("[全銘柄] JPXマスタが無いため総当たりで構築（初回のみ数分。英字コードは拾えない）")
    import yfinance as yf
    cands = [f"{c}.T" for c in range(1300, 10000)]
    found, t0, truncated = [], time.time(), False
    for i in range(0, len(cands), SCREEN_CHUNK):
        if time.time() - t0 > SCREEN_BUDGET_S:
            print("[全銘柄] 時間予算に達したため打ち切り"); truncated = True; break
        part = cands[i:i+SCREEN_CHUNK]
        try:
            d = yf.download(part, period="5d", interval="1d", auto_adjust=False,
                            group_by="ticker", threads=True, progress=False)
            for c in part:
                try:
                    if not d[c]["Close"].dropna().empty: found.append(c)
                except Exception:
                    pass
        except Exception as e:
            meta["errors"].append(f"universe chunk {i}: {type(e).__name__}")
        if (i // SCREEN_CHUNK) % 10 == 0:
            print(f"  {i+len(part)}/{len(cands)} 走査  有効 {len(found)}銘柄  {time.time()-t0:.0f}秒")
    # 途中で打ち切ったリストを保存すると、次回それを「完成品」として再利用し、
    # 欠けたユニバースのまま何日も走ることになる。完走したときだけ保存する。
    if truncated:
        meta["errors"].append(f"universe truncated at {len(found)}; not cached")
        print(f"[全銘柄] 打ち切りのため保存しない（{len(found)}銘柄／次回作り直す）")
    else:
        json.dump({"built_at_jst": NOW.isoformat(), "codes": found},
                  open(p, "w"), ensure_ascii=False)
        print(f"[全銘柄] {len(found)}銘柄を universe.json に保存")
    meta["universe_source"] = "bruteforce" + ("(truncated)" if truncated else "")
    return found

def screen_all(codes, sig=None, open_map=None):
    """全銘柄の直近データを取得し、流動性と値幅で絞ってから指標を付ける。"""
    import yfinance as yf
    rows, t0 = [], time.time()
    done = 0
    skipped, skip_msg = {}, {}
    settled = 0
    open_map = open_map or {}
    for i in range(0, len(codes), SCREEN_CHUNK):
        if time.time() - t0 > SCREEN_BUDGET_S:
            print(f"[全銘柄] 時間予算に達したため {done}/{len(codes)} で打ち切り"); break
        part = codes[i:i+SCREEN_CHUNK]
        try:
            d = yf.download(part, period="6mo", interval="1d", auto_adjust=False,
                            group_by="ticker", threads=True, progress=False)
        except Exception as e:
            meta["errors"].append(f"screen chunk {i}: {type(e).__name__}"); continue
        for c in part:
            done += 1
            try:
                x = d[c].dropna(subset=["Close"])

                # ── 台帳の決着（ここでやるのは、この銘柄の日足が今まさに手元にあるから）
                if c in open_map and sig is not None and len(x):
                    _ax = adjusted_frame(x)
                    for idx in open_map[c]:
                        try:
                            out = settle_signal(sig.loc[idx], _ax)
                        except Exception as _e:
                            out = None
                            k = "settle:" + type(_e).__name__
                            skipped[k] = skipped.get(k, 0) + 1
                            if k not in skip_msg: skip_msg[k] = str(_e)[:80]
                        if out:
                            for k, v in out.items(): sig.at[idx, k] = v
                            settled += 1

                if len(x) < 76: continue      # vs75 と r60 に必要な本数を満たすこと
                cl = x["Close"]; last = float(cl.iloc[-1])
                if last < MIN_PRICE: continue
                turnover = float((cl * x["Volume"]).tail(20).mean())
                if turnover < MIN_TURNOVER: continue

                # ★指標は調整済み、発注価格は実際の価格。
                #  以前は変数名だけ adj で中身は Close を使っており、分割も
                #  配当落ちも指標に素通りしていた（配当2.5%落ちの銘柄が
                #  「売られすぎ」として逆張り上位に入る形になっていた）。
                ax = adjusted_frame(x)
                acl, _ = repair(ax["Close"])
                if acl.isna().all(): continue
                # ATRは調整済みで計算し、今日の実際の価格水準に戻す。
                # 分割が窓に入ると生値のATRは桁がずれ、株数と損切り幅が壊れる。
                a_adj = _atr(ax.reset_index())
                if not a_adj or a_adj != a_adj: continue
                f_last = float(ax["Close"].iloc[-1] / last) if last else 1.0
                a = a_adj / f_last if f_last else a_adj
                atr_pct = a / last * 100
                if atr_pct < MIN_ATR_PCT or atr_pct > MAX_ATR_PCT: continue

                w60 = ax.tail(60)                      # レンジ位置も調整済みで揃える
                rng = float(w60["High"].max() - w60["Low"].min())
                if rng <= 0: continue
                rows.append(dict(
                    code=c, close=last, turnover=turnover,
                    atr=round(a, 2), atr_pct=round(atr_pct, 2), adjf=round(f_last, 6),
                    rsi=round(_rsi(acl.values), 1),
                    vs25=round((acl.iloc[-1]/acl.rolling(25).mean().iloc[-1]-1)*100, 2),
                    vs75=round((acl.iloc[-1]/acl.rolling(75).mean().iloc[-1]-1)*100, 2),
                    pos60=round((float(ax["Close"].iloc[-1])-w60["Low"].min())/rng*100, 1),
                    r20=round((acl.iloc[-1]/acl.iloc[-21]-1)*100, 2),
                    r60=round((acl.iloc[-1]/acl.iloc[-61]-1)*100, 2),
                    vol_ratio=round(float(x["Volume"].iloc[-1]/x["Volume"].tail(20).mean()), 2)))
            except Exception as e:
                # 握りつぶすと「全銘柄が同じ理由で落ちている」事故が見えなくなる。
                # 種類ごとに件数を数え、最初の1件はメッセージも残す。
                k = type(e).__name__
                skipped[k] = skipped.get(k, 0) + 1
                if k not in skip_msg: skip_msg[k] = str(e)[:80]
        if (i // SCREEN_CHUNK) % 5 == 0:
            print(f"  {done}/{len(codes)} 処理  通過 {len(rows)}銘柄  {time.time()-t0:.0f}秒")
    if skipped:
        meta["screen_skipped"] = {k: {"n": v, "例": skip_msg.get(k, "")}
                                  for k, v in sorted(skipped.items(), key=lambda x: -x[1])}
        print("[全銘柄] 除外の内訳:", ", ".join(f"{k}×{v}" for k, v in
              sorted(skipped.items(), key=lambda x: -x[1])[:5]))
        # フィルタ落ちではなく例外で全滅している場合は明確に警告する
        if len(rows) == 0 and done > 0:
            print(f"::warning::スクリーニングの通過が0件です。除外の内訳を data/meta.json で確認してください")
    if settled:
        print(f"[台帳] 未決着シグナルのうち {settled}件が決着")
    meta["signals_settled"] = settled
    return pd.DataFrame(rows), done

def rank_candidates(df):
    """順張りと逆張りは別の設定なので、混ぜずに分けて順位を付ける。
       単一の総合スコアにすると、性格の違う銘柄が同じ土俵で比較されて意味を失う。"""
    import numpy as np
    if df.empty: return df, df
    d = df.copy()
    heat = np.where(d["rsi"] > 75, (d["rsi"]-75)/25*20, 0)
    # 出来高を伴わない上昇は続きにくい。初回の実運用では上位15件のうち
    # 9件が出来高比 1.0未満（0.46〜0.79倍）のまま20日+25〜37%という並びだった。
    # 出来高比1.0を基準に ±8点。

    # 順張り: 移動平均の上に並び、60日レンジの上方にいて、20日が伸びている
    # 満点に達する水準が低すぎると、強い銘柄が全部同じ点になって順位が意味を失う。
    # 初回の実運用では上位15件が 97.1〜99.3 の 2.2点差に固まっていた（4項目が飽和）。
    # 実際の分布（25日 +9〜38% / 75日 +15〜84% / 20日 +11〜76%）に合わせて広げる。
    # 配点は 基礎92点 + 出来高±8点 = 0〜100。
    # 以前は基礎100点に出来高を足していたため 101.6 のような値が出て、
    # 「100点満点のスコア」という説明と食い違っていた。
    d["trend"] = np.clip(
        18*np.clip(d["vs25"]/12, 0, 1) +          # 25日線からの上方乖離（12%で満点）
        18*np.clip(d["vs75"]/30, 0, 1) +          # 75日線からの上方乖離（30%で満点）
        23*np.clip(d["pos60"]/100, 0, 1) +        # 60日レンジ内の位置
        18*np.clip(d["r20"]/25, 0, 1) +           # 20日リターン（25%で満点）
        15*np.clip((d["atr_pct"]-1.5)/2.5, 0, 1)  # 値幅（スイング適性）
        - heat
        # 列が欠けても順位付け全体を落とさない（1列の欠損でその日の候補が消えるのは割に合わない）
        + np.clip((d.get("vol_ratio", pd.Series(1.0, index=d.index))-1.0)*8, -8, 8),
        0, 100).round(1)
    # 保有の拒否ルール「RSI>78は買い増し不可」と揃える。
    # 新規の順張りは買い増しそのものなので、同じ線を引かないと整合が取れない。
    # 実測では4174（RSI81・出来高4.73倍・当日に業績開示）が1位に来ていた。
    d.loc[d["rsi"] > 78, "trend"] = 0

    # 逆張り: 長期トレンドは生きている（75日線の上）が、短期で売られすぎ
    #   ★ 以前は「75日線を5%以上割ったら0」としていたが、これはほぼ効かなかった。
    #     初回の実運用では上位8件中6件が75日線の -3.7〜-5.0% に固まり、
    #     見出しの「長期は上向き」が事実と食い違っていた。
    #     下降トレンドの途中を「押し目」と呼ばないよう、75日線の上を必須にする。
    d["revert"] = (
        25*np.clip(-d["vs25"]/8, 0, 1) +         # 25日線を下回るほど高得点
        25*np.clip((45-d["rsi"])/25, 0, 1) +     # RSIが低いほど高得点
        20*np.clip((40-d["pos60"])/40, 0, 1) +   # 60日レンジの下方
        15*np.clip(d["vs75"]/10, 0, 1) +         # ただし長期は上向きであること
        15*np.clip((d["atr_pct"]-1.5)/2.5, 0, 1)
    ).round(1)
    d.loc[d["vs75"] < 0, "revert"] = 0           # 75日線の下にあるものは逆張り対象外

    return (d.sort_values("trend", ascending=False).head(20),
            d.sort_values("revert", ascending=False).head(20))

# ── JPX 決算発表予定日（公式）─────────────────────────────────────
#    yfinance の予定日は日本の中小型で欠けやすく、実測で 40件中10件（25%）しか
#    埋まらなかった。4分の3が「要確認」では、決算跨ぎを避けるルールが働かない。
#    JPXは決算発表予定日を決算期末月ごとにExcelで公開しているので、そちらを主にする。
#    ファイル名の日付部分は更新のたびに変わるため、一覧ページからリンクを拾う。
# ── 決算発表の履歴から次回を推定する ──────────────────────────────
#   なぜ要るか:
#     JPXが予定日を載せるのは「直前の四半期が終わった会社」だけ（実測で
#     7月期末420社＋8月期末236社＝656社）。それ以外は未掲載になる。
#     実測でも候補40件のうち日付が付いたのは13件で、しかも**全部32〜62日後**
#     だった。つまり「予定なし」の多くは本当に遠いだけなのだが、
#     表示上は「不明」と区別できず、毎回あなたが調べ直すことになる。
#
#   やること:
#     TDnetの「決算短信」を日々ためて、その銘柄が最後に決算を出した日を持つ。
#     四半期決算はほぼ91日間隔なので、そこから次回のおおよその時期が出せる。
#     TDnetは約31日しか遡れないので、履歴は今日から前向きに積み上がる。
#
#   扱い:
#     これは**予定ではなく推定**。表示は必ず「推定」と「要確認」を併記し、
#     これだけを根拠に建てさせない。逆に「直近で発表済み」は確定情報として使える。
EHIST_PATH      = f"{OUT}/earnings_hist.json"
EHIST_DAYS      = 31        # TDnetが保持している範囲
EHIST_BUDGET_S  = 240
EHIST_KEEP_DAYS = 500       # 履歴の保持期間
QUARTER_DAYS    = 91        # 四半期の間隔（次回の推定に使う）
RECENT_DAYS     = 45        # これ以内に発表済みなら「当面は決算をまたがない」

def _load_ehist():
    try:
        if os.path.exists(EHIST_PATH):
            h = json.load(open(EHIST_PATH, encoding="utf-8"))
            h.setdefault("days", []); h.setdefault("kessan", {})
            return h
    except Exception as e:
        meta["errors"].append(f"ehist read: {type(e).__name__}")
    return {"days": [], "kessan": {}}

def update_earnings_history():
    """TDnetの決算短信だけを日付ごとに拾って積み上げる。
       取得済みの日は二度と取りに行かないので、初回以降は1日分で済む。"""
    import requests, html as _html
    h = _load_ehist()
    have = set(h["days"])
    want = [(NOW - dt.timedelta(days=i)).strftime("%Y%m%d") for i in range(EHIST_DAYS)]
    todo = [d for d in want if d not in have]
    # 当日分は場中に増えるので、常に取り直す
    today = NOW.strftime("%Y%m%d")
    if today not in todo: todo.insert(0, today)

    t0, fetched, found = time.time(), 0, 0
    for d in todo:
        if time.time() - t0 > EHIST_BUDGET_S:
            print(f"[決算履歴] 時間予算に達したため {fetched}/{len(todo)}日で打ち切り"); break
        got_any = False
        for pg in range(1, TDNET_MAX_PAGES + 1):
            u = f"https://www.release.tdnet.info/inbs/I_list_{pg:03d}_{d}.html"
            try:
                r = requests.get(u, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
            except Exception:
                break
            if r.status_code != 200: break
            try:
                txt = r.content.decode("utf-8")
            except UnicodeDecodeError:
                txt = r.content.decode("cp932", errors="replace")
            n_before = found
            for tr in re.split(r"<tr", txt, flags=re.I)[1:]:
                mc = re.search(r'kjCode["\']?[^>]*>\s*([0-9][0-9A-Za-z]{3,4})', tr)
                mt = re.search(r'kjTitle["\']?[^>]*>(.*?)</(?:td|th|div)>', tr, re.S | re.I)
                if not (mc and mt): continue
                title = _html.unescape(re.sub(r"<[^>]+>", " ", mt.group(1)))
                # 「決算短信」だけを拾う。予想の修正や配当のお知らせは決算発表ではない。
                # 「決算短信」を含んでも、訂正・修正は発表そのものではない。
                # これを発表済みと数えると、本番の決算直前に「安全」と表示してしまう。
                if "決算短信" not in title: continue
                if any(k in title for k in ("訂正", "修正", "差替", "差し替")): continue
                code = mc.group(1)[:4]
                iso = f"{d[:4]}-{d[4:6]}-{d[6:]}"
                lst = h["kessan"].setdefault(code, [])
                if iso not in lst:
                    lst.append(iso); found += 1
            if found == n_before and pg > 1: break
            got_any = True
        fetched += 1
        if got_any and d not in have and d != today:
            h["days"].append(d)
    # 古い履歴を落とす
    cut = str((NOW - dt.timedelta(days=EHIST_KEEP_DAYS)).date())
    for c in list(h["kessan"]):
        h["kessan"][c] = sorted({x for x in h["kessan"][c] if x >= cut})
        if not h["kessan"][c]: del h["kessan"][c]
    h["days"] = sorted(set(h["days"]))[-EHIST_KEEP_DAYS:]
    try:
        json.dump(h, open(EHIST_PATH, "w"), ensure_ascii=False)
    except Exception as e:
        meta["errors"].append(f"ehist write: {type(e).__name__}")
    meta["earnings_hist"] = {"days_cached": len(h["days"]), "codes": len(h["kessan"]),
                             "fetched_now": fetched, "new_items": found}
    print(f"[決算履歴] {fetched}日分を取得（新規{found}件）／"
          f"蓄積 {len(h['kessan']):,}銘柄 / {len(h['days'])}日分")
    return h["kessan"]

def estimate_from_history(code, hist, today):
    """最後の決算短信から次回を推定する。間隔が2つ以上あれば実測の中央値を使う。"""
    ds = sorted(hist.get(str(code)[:4], []))
    if not ds: return None
    last = ds[-1]
    gap = QUARTER_DAYS
    if len(ds) >= 3:
        gaps = [(pd.Timestamp(b) - pd.Timestamp(a)).days for a, b in zip(ds, ds[1:])]
        gaps = [g for g in gaps if 60 <= g <= 130]
        if gaps: gap = int(pd.Series(gaps).median())
    since = (pd.Timestamp(today) - pd.Timestamp(last)).days
    nxt = str((pd.Timestamp(last) + pd.Timedelta(days=gap)).date())
    return {"last": last, "since": int(since), "est": nxt,
            "est_days": int((pd.Timestamp(nxt) - pd.Timestamp(today)).days)}

JPX_EARN_INDEX = "https://www.jpx.co.jp/listing/event-schedules/financial-announcement/index.html"
JPX_EARN_MAX_FILES = 6          # 直近の数ファイルで足りる（決算は期末から45日以内）

def _jpx_earn_links():
    import requests
    from urllib.parse import urljoin
    r = requests.get(JPX_EARN_INDEX, timeout=60, headers=JGB_HDRS)
    r.raise_for_status()
    try:
        html = r.content.decode("utf-8")
    except UnicodeDecodeError:
        html = r.content.decode("cp932", errors="replace")
    hrefs = re.findall(r'href="([^"]*kessan[^"]*\.xlsx?)"', html, re.I)
    seen, out = set(), []
    for h in hrefs:
        u = urljoin(JPX_EARN_INDEX, h)
        if u not in seen:
            seen.add(u); out.append(u)
    # 新しいものから使う（ファイル名の数字が大きいほど新しい）
    out.sort(reverse=True)
    return out[:JPX_EARN_MAX_FILES]

def _parse_jpx_earn(blob):
    """コード→決算発表予定日。列名の正確な表記が分からなくても動くよう、
       『コード』を含む列と『予定日』を含む列を見出し行から探す。"""
    rows = _xlsx_rows(blob)
    hdr_i, c_code, c_date = None, None, None
    for i, r in enumerate(rows[:15]):                 # 見出しは上の方にある
        cells = [str(x or "") for x in r]
        code_i = next((j for j, x in enumerate(cells) if "コード" in x), None)
        date_i = next((j for j, x in enumerate(cells)
                       if "予定" in x and "日" in x and "前回" not in x), None)
        if code_i is not None and date_i is not None:
            hdr_i, c_code, c_date = i, code_i, date_i
            break
    if hdr_i is None:
        raise RuntimeError(f"見出し行が見つからない: {[str(x)[:12] for x in (rows[0] if rows else [])][:8]}")

    def to_date(v):
        if v is None or str(v).strip() == "": return ""
        s = str(v).strip()
        # xlsxの日付はシリアル値で入っていることがある（1900年起点、1900をうるう年とみなす仕様）
        try:
            f = float(s)
            if 30000 < f < 80000:
                return str((dt.date(1899, 12, 30) + dt.timedelta(days=int(f))))
        except ValueError:
            pass
        s = s.replace("年", "/").replace("月", "/").replace("日", "")
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y/%m/%d ", "%m/%d"):
            try:
                d = dt.datetime.strptime(s.strip().split(" ")[0], fmt)
                if fmt == "%m/%d": d = d.replace(year=NOW.year)
                return str(d.date())
            except ValueError:
                continue
        try:
            return str(pd.to_datetime(s).date())
        except Exception:
            return ""

    out = {}
    for r in rows[hdr_i + 1:]:
        if len(r) <= max(c_code, c_date): continue
        code = str(r[c_code] or "").strip()
        if code.endswith(".0"): code = code[:-2]          # 数値として読まれた場合
        code = code.zfill(4) if code.isdigit() and len(code) < 4 else code
        if not CODE_RE.match(code): continue
        d = to_date(r[c_date])
        if d: out[code] = d
    return out

def jpx_earnings():
    """コード→決算発表予定日（公式）。取れなければ空を返し、yfinanceに任せる。"""
    try:
        links = _jpx_earn_links()
    except Exception as e:
        meta["errors"].append(f"jpx_earn index: {type(e).__name__}: {e}")
        print("[決算] JPX一覧ページを取得できず:", e)
        return {}
    if not links:
        meta["errors"].append("jpx_earn: リンクが見つからない")
        return {}
    import requests
    merged, used = {}, []
    for u in links:
        try:
            rr = requests.get(u, timeout=90, headers=JGB_HDRS)
            if rr.status_code != 200 or len(rr.content) < 5000:
                used.append(f"{u.rsplit('/', 1)[-1]}:HTTP{rr.status_code}"); continue
            d = _parse_jpx_earn(rr.content)
            merged.update(d)                    # 新しいファイルから順に、後のもので上書き
            used.append(f"{u.rsplit('/', 1)[-1]}:{len(d)}件")
        except Exception as e:
            used.append(f"{u.rsplit('/', 1)[-1]}:{type(e).__name__}")
    meta["jpx_earnings"] = {"files": used, "codes": len(merged)}
    print(f"[決算] JPX予定日 {len(merged):,}銘柄 ({' / '.join(used)})")
    return merged

EARN_BUDGET_S = 240        # 候補の決算日照会に使ってよい秒数
EARN_SOON_DAYS = 7         # これ以内なら「決算跨ぎ」として扱う

JPX_EARN = {}
EHIST = {}

def next_earnings_for(codes, budget_s=EARN_BUDGET_S):
    """候補の次回決算発表日。
       全4,250銘柄には引けないが、**順位が付いた上位40銘柄だけ**なら
       1銘柄1リクエストで間に合う。ここが埋まって初めて
       「決算をまたぐ建玉は作らない」というルールが候補にも効く。

       yfinance の日本株の決算日は推定値のことがあり、欠けることもある。
       そこで「予定日あり」「照会したが不明」「照会失敗」を区別して返す。
       区別せず空欄にすると『決算が無い』と読めてしまい、ルールが骨抜きになる。"""
    import yfinance as yf
    out, t0, today = {}, time.time(), str(NOW.date())
    n_ok = n_none = n_err = n_jpx = n_recent = n_est = 0
    for c in sorted(codes):
        # ① JPX（公式の予定日）。確度が最も高い。
        j = JPX_EARN.get(str(c)[:4])
        if j and j >= today:
            out[c] = {"status": "ok", "src": "jpx", "next": j,
                      "days": int((pd.Timestamp(j) - pd.Timestamp(today)).days)}
            n_ok += 1; n_jpx += 1
            continue
        # ② 決算短信の履歴。「直近で発表済み」は確定情報として使える
        #    （出したばかりなら当面またがない）。次回は推定にとどめる。
        e = estimate_from_history(c, EHIST, today)
        if e:
            if e["since"] <= RECENT_DAYS:
                out[c] = {"status": "recent", "src": "tdnet", "last": e["last"],
                          "since": e["since"], "next": e["est"], "days": e["est_days"]}
                n_recent += 1
                continue
            if 0 < e["est_days"]:
                out[c] = {"status": "estimate", "src": "tdnet", "last": e["last"],
                          "next": e["est"], "days": e["est_days"]}
                n_est += 1
                continue
        if time.time() - t0 > budget_s:
            out[c] = {"status": "timeout"}
            continue
        try:
            ed = yf.Ticker(c).get_earnings_dates(limit=12)
            ds = ([str(pd.Timestamp(i).date()) for i in ed.index]
                  if ed is not None and len(ed) else [])
            nxt = next((x for x in sorted(ds) if x >= today), "")
            if nxt:
                days = (pd.Timestamp(nxt) - pd.Timestamp(today)).days
                out[c] = {"status": "ok", "src": "yf", "next": nxt, "days": int(days)}
                n_ok += 1
            else:
                out[c] = {"status": "unknown"}      # 照会できたが将来の予定が無い
                n_none += 1
        except Exception as e:
            out[c] = {"status": "error", "why": type(e).__name__}
            n_err += 1
        time.sleep(0.15)                            # 連続照会でのレート制限を避ける
    meta["cand_earnings"] = {"asked": len(codes), "ok": n_ok, "from_jpx": n_jpx,
                             "from_yf": n_ok - n_jpx, "recent": n_recent,
                             "estimate": n_est, "unknown": n_none, "error": n_err}
    print(f"[決算] 候補{len(codes)}銘柄: 確定{n_ok}（JPX {n_jpx} / yfinance {n_ok-n_jpx}）"
          f" 発表済{n_recent} 推定{n_est} 不明{n_none} 失敗{n_err}")
    return out

# 保有している「業種の賭け」。個別株はマスタから、業種ETFは対応表から引く。
HOLD_S17_FIXED = {"1615": "銀行", "1617": "食品", "1618": "エネルギー資源",
                  "1619": "建設・資材", "1620": "素材・化学", "1621": "医薬品",
                  "1622": "自動車・輸送機", "1623": "鉄鋼・非鉄", "1624": "機械",
                  "1625": "電機・精密", "1626": "情報通信・サービスその他",
                  "1627": "電力・ガス", "1628": "運輸・物流", "1629": "商社・卸売",
                  "1630": "小売", "1631": "銀行", "1632": "金融（除く銀行）",
                  "1633": "不動産"}

def held_sectors(holdings, master):
    out = {}
    for c in holdings:
        c4 = str(c)[:4]
        s = HOLD_S17_FIXED.get(c4) or (master.get(c4, {}) or {}).get("s17", "")
        if s: out.setdefault(s, []).append(c4)
    return out

HELD_S17 = {}
CAND_EARN = {}
MASTER = {}
try:
    MASTER = load_master()
    HELD_S17 = held_sectors(HOLDINGS, MASTER)
except Exception as e:
    meta["errors"].append(f"master outer: {type(e).__name__}: {e}")

DISC = {}
try:
    DISC = fetch_tdnet(days=4)
    json.dump({"generated_at_jst": NOW.isoformat(),
               "holdings": {c[:4]: DISC.get(c[:4], []) for c in HOLDINGS},
               "count_codes": len(DISC),
               "count_items": sum(len(v) for v in DISC.values())},
              open(f"{OUT}/disclosures.json", "w"), ensure_ascii=False, indent=1)
except Exception as e:
    meta["errors"].append(f"tdnet outer: {type(e).__name__}: {e}")
    meta["tdnet"] = {"error": str(e)[:120]}
    print("[適時開示] 失敗:", e)

def _decorate(recs):
    """候補の行に 銘柄名・業種・対応する業種ETF・当日の開示 を付ける。
       コードだけの表は人間が検証できず、材料の裏も取れないため必須。"""
    for r in recs:
        c4 = str(r["code"])[:4]
        m = MASTER.get(c4, {})
        r["name"]   = m.get("name", "")
        r["s17"]    = m.get("s17", "")
        r["kind"]   = m.get("kind", "")
        r["size"]   = m.get("size", "")
        s17 = m.get("s17", "")
        etf = s17_etf(s17)
        if s17 and not etf:
            # 表記のゆれで対応が取れないと、その業種の候補すべてでBが付かなくなる。
            # 実際に「情報通信・サービスその他」を1文字違いで書いていて全滅した。
            meta.setdefault("s17_unmapped", {})
            meta["s17_unmapped"][s17] = meta["s17_unmapped"].get(s17, 0) + 1
        r["sector_etf"] = etf[:4]
        r["news"]   = DISC.get(c4, [])[:3]
        # 既に同じ業種を持っているかどうか。持っているなら同じ賭けの上乗せ。
        r["overlap"] = HELD_S17.get(s17, []) if s17 else []
        e = CAND_EARN.get(r["code"], {"status": "not_checked"})
        r["earn_status"] = e.get("status")
        r["earn_next"]   = e.get("next", "")
        r["earn_days"]   = e.get("days")
        # 決算跨ぎは執行の可否そのものを決めるので、行ごとに明示する
        r["earn_src"]    = e.get("src", "")
        r["earn_last"]   = e.get("last", "")
        r["earn_since"]  = e.get("since")
        # 「跨ぎ」は確定した予定日のときだけ立てる。推定で建玉を止めると
        # 機会を失い、推定で建てさせると事故る。推定は情報として出すだけ。
        r["earn_soon"]   = bool(e.get("status") == "ok"
                                and e.get("days") is not None
                                and e["days"] <= EARN_SOON_DAYS)
        r["earn_watch"]  = bool(e.get("status") == "estimate"
                                and e.get("days") is not None
                                and e["days"] <= EARN_SOON_DAYS * 2)
    return recs

try:
    if SCREEN_ENABLED:
        uni = load_universe(MASTER)
        if uni:
            SIG = load_signals()
            sc, scanned = screen_all(uni, SIG, open_signal_map(SIG))
            meta["screen"] = {"universe": len(uni), "scanned": scanned, "passed": len(sc),
                              "named": bool(MASTER),
                              "source": meta.get("universe_source", "?")}
            if not sc.empty:
                trend, revert = rank_candidates(sc)
                tr0, rv0 = trend.to_dict("records"), revert.to_dict("records")
                # 決算日は順位が付いてから、載る銘柄だけ引く（全銘柄には引けない）
                try:
                    EHIST = update_earnings_history()  # 決算短信の履歴を積み上げる
                except Exception as e:
                    meta["errors"].append(f"earnings_hist: {type(e).__name__}: {e}")
                try:
                    JPX_EARN = jpx_earnings()          # 公式の予定日を先に用意する
                except Exception as e:
                    meta["errors"].append(f"jpx_earnings: {type(e).__name__}: {e}")
                try:
                    CAND_EARN = next_earnings_for({r["code"] for r in tr0 + rv0})
                except Exception as e:
                    meta["errors"].append(f"cand_earnings: {type(e).__name__}: {e}")
                    meta["cand_earnings"] = {"error": str(e)[:120]}
                tr = _decorate(tr0)
                rv = _decorate(rv0)
                # ★台帳への追加は大引け後の実行だけ。
                #   11:35の実行では当日足がまだ未完成で、それを「終値」として
                #   建値に使うと、成績の前提（当日終値で建てた）が嘘になる。
                _today = str(NOW.date())
                if NOW.hour >= 15:
                    SIG = append_signals(SIG, tr, "trend", _today)
                    SIG = append_signals(SIG, rv, "revert", _today)
                    meta["signals_appended"] = True
                else:
                    meta["signals_appended"] = False
                    print("[台帳] 場中の実行のため追加は見送り（大引け後の実行で記録する）")
            else:
                tr, rv = [], []
            # ── 過去検証（月1回・大引け後だけ）────────────────────
            try:
                _btp = f"{OUT}/backtest.json"
                _age = 999
                if os.path.exists(_btp):
                    try:
                        _age = (NOW - dt.datetime.fromisoformat(
                            json.load(open(_btp, encoding="utf-8"))["generated_at_jst"])).days
                    except Exception:
                        _age = 999
                if BT_ENABLED and NOW.hour >= 15 and _age >= BT_MAX_AGE_D:
                    print(f"[検証] 過去検証を実行（前回から{_age}日）")
                    tk, nd, nf, d0, d1, cov = run_backtest(uni)
                    bt = {"generated_at_jst": NOW.isoformat(),
                          "from": d0, "to": d1, "tickers": nf, "entry_dates": nd, "coverage_pct": cov,
                          "step": BT_STEP, "top": BT_TOP, "holds": list(BT_HOLDS),
                          "filters": {"min_turnover": MIN_TURNOVER, "min_price": MIN_PRICE,
                                      "min_atr_pct": MIN_ATR_PCT, "max_atr_pct": MAX_ATR_PCT},
                          "all": {str(h): bt_summary(tk, h) for h in BT_HOLDS},
                          "regime": {"above200": bt_summary(tk, 15, True),
                                     "below200": bt_summary(tk, 15, False)}}
                    json.dump(bt, open(_btp, "w"), ensure_ascii=False, indent=1)
                    meta["backtest"] = {"tickers": nf, "dates": nd,
                                        "main": bt["all"][str(15)]}
                    print("[検証] backtest.json を生成")
            except Exception as e:
                import traceback
                meta["errors"].append(f"backtest: {type(e).__name__}: {e}")
                print("[検証] 失敗:", e); traceback.print_exc()

            try:
                SIG.to_csv(SIG_PATH, index=False)
                meta["signals"] = signal_summary(SIG)
                print(f"[台帳] {len(SIG)}件 / 決着済 {meta['signals'].get('closed', 0)} "
                      f"/ 未決着 {meta['signals'].get('open', 0)}")
            except Exception as e:
                meta["errors"].append(f"signals write: {type(e).__name__}: {e}")
            # 通過0件でも必ず書く。書かないと report 側が「未実行」と表示してしまい、
            # 「走らせたが0件だった」という事故が「まだ動かしていない」に見える。
            json.dump({"generated_at_jst": NOW.isoformat(),
                       "universe": len(uni), "scanned": scanned, "passed": len(sc),
                       "master_count": len(MASTER),
                       "universe_source": meta.get("universe_source", "?"),
                       "tdnet": meta.get("tdnet", {}),
                       "skipped": meta.get("screen_skipped", {}),
                       "cand_earnings": meta.get("cand_earnings", {}),
                       "jpx_earnings": meta.get("jpx_earnings", {}),
                       "earnings_hist": meta.get("earnings_hist", {}),
                       "filters": {"min_turnover": MIN_TURNOVER, "min_price": MIN_PRICE,
                                   "min_atr_pct": MIN_ATR_PCT, "max_atr_pct": MAX_ATR_PCT},
                       "s17_unmapped": meta.get("s17_unmapped", {}),
                       "trend": tr, "revert": rv},
                      open(f"{OUT}/candidates.json", "w"), ensure_ascii=False,
                      indent=1, default=str)
            named = sum(1 for r in tr + rv if r["name"])
            print(f"[全銘柄] 走査{scanned} / 通過{len(sc)} / "
                  f"銘柄名あり {named}/{max(len(tr)+len(rv),1)} / candidates.json を生成")
except Exception as e:
    import traceback
    meta["errors"].append(f"screen: {type(e).__name__}: {e}")
    meta["screen"] = {"error": str(e)[:120]}
    print("[全銘柄] 失敗:", e); traceback.print_exc()

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
    RISK_PER_TRADE = TOT * RISK_PER_TRADE_P

    R = pd.DataFrame(rets).dropna(how="any").tail(120)
    C = R.corr()
    t["avg_corr"] = [round(float(C[c].drop(c).abs().mean()), 3) for c in t["code"]]
    def _swing_fit(a):
        """スイングの「やりやすさ」。1.5〜4.0%を満点とし、外れるほど下げる。
           高ければ高いほど良い、ではない（5%超は材料で動いている公算が高い）。"""
        a = np.asarray(a, float)
        lo = np.clip((a - 0.6) / 0.9, 0, 1)      # 1.5%で満点
        hi = np.clip((7.0 - a) / 3.0, 0, 1)      # 4.0%から下がり7.0%で0
        return np.minimum(lo, hi)
    t["A"] = (25*_swing_fit(t["atr_pct"])).round(1)
    heat = np.where(t["rsi"] > 75, (t["rsi"]-75)/25, 0)
    t["C"] = (25*np.clip(0.5*np.clip(t["pos60"]/100, 0, 1)
                         + 0.5*np.clip((t["rs20"]+8)/16, 0, 1) - heat, 0, 1)).round(1)
    t["D"] = (25*np.clip(1-(t["avg_corr"]-0.1)/0.5, 0, 1)).round(1)
    t["ACD"] = (t["A"]+t["C"]+t["D"]).round(1)   # B（レジーム適合）はモデルが判断して足す
    # スイングの土俵に乗るかどうか。乗らないものはスコアで売買を語らない。
    t["swingable"] = t["atr_pct"] >= MIN_ATR_PCT

    # サイジング: ATR基準と1銘柄15%上限の、小さいほう
    def size(r):
        stop_w = 2*r["atr"]
        # 許容損失は RISK_PER_TRADE 一本。以前は表が総額1.0%、運用ルールが0.9%で
        # 食い違い、表どおりに建てると常に11%オーバーサイズになっていた。
        n_atr = int(RISK_PER_TRADE // stop_w) if stop_w > 0 else 0
        n_cap = int(max(TOT*MAX_WEIGHT - r["mkt"], 0) // r["close"]) if r["close"] > 0 else 0
        # 現金で買えない株数を出しても意味がない。現金比率の下限も守る。
        buyable = max(CASH - TOT*MIN_CASH_RATIO, 0)
        n_cash = int(buyable // r["close"]) if r["close"] > 0 else 0
        n = min(n_atr, n_cap, n_cash)
        n = (n // LOT) * LOT          # 単元(100株)で切り捨て。切り上げるとリスク超過
        return pd.Series([n, round(r["close"]-stop_w, 1), round(r["close"]+3*r["atr"], 1),
                          round(r["close"]+4*r["atr"], 1), round(n*r["close"]),
                          min([(n_atr, "ATR"), (n_cap, f"{MAX_WEIGHT:.0%}上限"),
                               (n_cash, "現金")], key=lambda z: z[0])[1]])
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

    # ── レジーム（機械的に出す。Bの出発点にする）────────────────
    regime = {}
    try:
        bser = pd.read_csv(f"{OUT}/ohlcv/{slug(BENCH)}.csv")
        bc = pd.to_numeric(bser.get("Adj Close", bser["Close"]), errors="coerce").dropna()
        if len(bc) >= 200:
            ma200 = float(bc.rolling(200).mean().iloc[-1])
            ma25  = float(bc.rolling(25).mean().iloc[-1])
            px    = float(bc.iloc[-1])
            regime = {"px": px, "vs200": round((px/ma200-1)*100, 2),
                      "vs25": round((px/ma25-1)*100, 2),
                      "above200": bool(px > ma200)}
            # 業種ETF17本のうち何本が25日線の上か（市場の広がり）
            above = tot = 0
            for sc in SECTOR:
                try:
                    q = pd.read_csv(f"{OUT}/ohlcv/{slug(sc)}.csv")
                    qc = pd.to_numeric(q.get("Adj Close", q["Close"]), errors="coerce").dropna()
                    if len(qc) >= 25:
                        tot += 1
                        above += int(float(qc.iloc[-1]) > float(qc.rolling(25).mean().iloc[-1]))
                except Exception:
                    pass
            if tot: regime["breadth"] = f"{above}/{tot}"
            regime["breadth_pct"] = round(above/tot*100, 0) if tot else None
            # 買い持ちで15営業日を狙うなら、指数が200日線の下では素直に縮める
            b = 0.5
            if regime["above200"]: b += 0.2
            else: b -= 0.2
            if (regime.get("breadth_pct") or 50) >= 60: b += 0.1
            elif (regime.get("breadth_pct") or 50) <= 35: b -= 0.1
            regime["b_suggest"] = round(min(max(b, 0.1), 0.9), 2)
        meta["regime"] = regime
    except Exception as e:
        meta["errors"].append(f"regime: {type(e).__name__}")

    jgbline = "取得できず"
    if meta.get("jgb"):
        g = meta["jgb"]
        jgbline = f"{g['date']} 時点  2年 {g.get('y2')}% / 10年 {g.get('y10')}% / 30年 {g.get('y30')}%"
        if (g.get("age_days") or 0) > 5:
            jgbline += (f"  ← **{g['age_days']}日前の値。現在値として使わないこと**"
                        "（NTTの利回り差・銀行の追い風の判断が丸ごとずれる）")

    L = []
    L.append(f"# 後場スイング 事前分析  {NOW:%Y-%m-%d %H:%M JST}\n")
    L.append(f"データ健全性: **{meta['health']}**  保有{meta['holdings_ok']}/{len(HOLDINGS)}  "
             f"全体{meta['n_ok']}/{meta['n_total']}  エラー{len(meta['errors'])}件\n")
    L.append(f"日本国債（財務省）: {jgbline}\n")
    if regime:
        L.append(f"**レジーム（機械計算）**: TOPIX {regime['px']:,.1f} ／ "
                 f"200日線 {regime['vs200']:+.2f}%（{'上' if regime['above200'] else '**下**'}）／ "
                 f"25日線 {regime['vs25']:+.2f}% ／ 25日線の上にある業種 {regime.get('breadth', '—')}\n")
        L.append(f"> **Bの出発点: {regime['b_suggest']:.2f}**（指数の200日線との位置と業種の広がりから機械的に算出）。"
                 "銘柄ごとの材料で上下させてよいが、**動かしたら理由を書くこと**。"
                 + ("\n> **指数が200日線の下にある。** 買い持ち中心の運用では、"
                    "この局面は建玉を減らすか見送るほうが理に適う。" if not regime["above200"] else "") + "\n")
    if nk: L.append(f"{nk}\n")
    if rep_log:
        L.append("補正した系列: " + " / ".join(f"{k}: {'; '.join(v)}" for k, v in rep_log.items()) + "\n")
    L.append(f"\n総額 **¥{TOT:,.0f}**（保有 ¥{t['mkt'].sum():,.0f} + 現金 ¥{CASH:,.0f} = 現金比率 {CASH/TOT*100:.1f}%）")
    L.append(f"／ 1トレード許容損失({RISK_PER_TRADE/TOT:.2%}) ¥{RISK_PER_TRADE:,.0f}"
             f"／ 発注可能現金 ¥{max(CASH - TOT*MIN_CASH_RATIO, 0):,.0f}"
             f"（現金 ¥{CASH:,.0f} − 下限 {MIN_CASH_RATIO:.0%}）\n")

    L.append("\n## テクニカル（ベンチマーク=TOPIX/1306）\n")
    L.append("| コード | 銘柄 | 終値 | 損益% | RSI | MACD | 25日 | 75日 | 200日 | 60日位置 | ATR% | 対TOPIX 1d/5d/20d | 出来高 |")
    L.append("|---|---|--:|--:|--:|:-:|--:|--:|--:|--:|--:|--:|--:|")
    for _, r in t.iterrows():
        L.append(f"| {r['code'][:4]} | {r['name']} | {r['close']:,.1f} | {r['pl_pct']:+.2f} | {r['rsi']:.1f} | "
                 f"{'＋' if r['macd_hist']>0 else '−'}{'↑' if r['macd_dir']=='up' else '↓'} | {r['vs25']:+.1f}% | {r['vs75']:+.1f}% | "
                 f"{r['vs200']:+.1f}% | {r['pos60']:.0f}% | {r['atr_pct']:.2f} | "
                 f"{r['rs1']:+.1f} / {r['rs5']:+.1f} / {r['rs20']:+.1f} | {r['vol_ratio']:.2f}x |")

    L.append("\n## スコア（A/C/D は計算済み。**B＝レジーム適合はモデルが判断して加算する**）\n")
    _ns = t[~t["swingable"]]["code"].tolist()
    if len(_ns):
        L.append("> **スイング対象外（ATR {:.1f}%未満）: {}**".format(
                 MIN_ATR_PCT, "、".join(c[:4] for c in _ns)))
        L.append("> これらは値幅が小さくスイングの道具にならないだけで、**下落を予想しているのではない**。")
        L.append("> スコアは構造上必ず低く出るので、**この点数を根拠に売却・縮小を推奨してはいけない**。")
        L.append("> 判定は「コアとして継続」か「資金を別の用途に回すかの別途判断」のどちらかで書くこと。\n")
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

    # ── 保有銘柄の適時開示（TDnet）──────────────────────────────
    #    決算・上方下方修正・自社株買い・increase/decrease of 配当 など、
    #    価格が動いた理由がここに出る。ニュース見出しより一次情報に近い。
    try:
        dj = json.load(open(f"{OUT}/disclosures.json", encoding="utf-8"))
        hd = {k: v for k, v in dj.get("holdings", {}).items() if v}
        n_items = sum(len(v) for v in hd.values())
        L.append(f"\n## 保有銘柄の適時開示（TDnet 直近4日／全体 {dj.get('count_items',0)}件）\n")
        if hd:
            L.append("| コード | 日付 | 時刻 | 表題 |")
            L.append("|---|:-:|:-:|---|")
            for c, items in sorted(hd.items()):
                for x in items[:4]:
                    L.append(f"| {c} | {x['date'][4:6]}/{x['date'][6:]} | {x.get('time','')} | {x['title']} |")
            L.append(f"\n※ 保有 {len(hd)}銘柄に {n_items}件。**開示が出た銘柄は指標より開示を優先して判断すること。**")
        else:
            L.append("保有銘柄に直近4日の適時開示はなし。")
            L.append("\n※ 「開示なし」は「材料なし」ではない。報道・需給・指数入替はTDnetには載らない。")
    except FileNotFoundError:
        L.append("\n## 保有銘柄の適時開示\n\n取得していません。")
    except Exception as e:
        L.append(f"\n## 保有銘柄の適時開示\n\n生成に失敗: {e}")

    # ── 過去検証 ──────────────────────────────────────────────
    #    順位付けに情報があるのか。無作為抽出との差だけが答え。
    try:
        bt = json.load(open(f"{OUT}/backtest.json", encoding="utf-8"))
        m = bt["all"][str(15)]
        _cov = bt.get("coverage_pct")
        L.append(f"\n## 過去検証（{bt['from']}〜{bt['to']} / {bt['tickers']:,}銘柄 / "
                 f"{bt['entry_dates']}回の建て日）\n")
        if _cov is not None and _cov < 95:
            L.append(f"> **母集団の {_cov}% までしか照会できていない**（時間予算）。"
                     "無作為に混ぜてから取っているので業種の偏りは無いが、"
                     "標本が小さいぶん差の検出力は落ちる。\n")
        L.append("**問い: この順位付けは、同じ母集団から無作為に選ぶより良いのか。**\n")
        L.append("| 選び方 | 件数 | 勝率 | 平均R | 無作為との差 | 粗いt値 | 利確% | 損切% | 時間切れ% |")
        L.append("|---|--:|--:|--:|--:|--:|--:|--:|--:|")
        for k, lbl in [("trend", "順張り上位10"), ("revert", "逆張り上位10"),
                       ("random", "**無作為10（基準）**")]:
            v = m.get(k)
            if not v: continue
            vr = f"{v['vs_random']:+.4f}" if "vs_random" in v else "—"
            tv = f"{v['t_rough']:+.2f}" if v.get("t_rough") is not None else "—"
            L.append(f"| {lbl} | {v['n']:,.0f} | {v['win_pct']:.1f}% | {v['avg_r']:+.4f} | "
                     f"{vr} | {tv} | {v['target']:.0f}% | {v['stop']:.0f}% | {v['timeout']:.0f}% |")

        def _verdict(k, lbl):
            v = m.get(k) or {}
            t = v.get("t_rough")
            if t is None: return None
            if t >= 2.0:  return f"- **{lbl}: 無作為を上回っている（t={t:+.2f}）。** 使う根拠がある。"
            if t <= -2.0: return f"- **{lbl}: 無作為を下回っている（t={t:+.2f}）。使うのをやめること。**"
            return (f"- **{lbl}: 無作為との差は誤差の範囲（t={t:+.2f}）。** "
                    "順位付けに情報があるとは言えない。**この順位を根拠に建てる意味は無い。**")
        L.append("")
        for line in filter(None, [_verdict("trend", "順張り"), _verdict("revert", "逆張り")]):
            L.append(line)

        rg = bt.get("regime", {})
        if rg.get("above200") and rg.get("below200"):
            L.append("\n**TOPIXが200日線の上か下かで分けたとき（保有15日）**\n")
            L.append("| 局面 | 選び方 | 件数 | 勝率 | 平均R | 無作為との差 |")
            L.append("|---|---|--:|--:|--:|--:|")
            for key, lbl in [("above200", "200日線の上"), ("below200", "200日線の下")]:
                for k, kl in [("trend", "順張り"), ("revert", "逆張り"), ("random", "無作為")]:
                    v = (rg.get(key) or {}).get(k)
                    if not v: continue
                    vr = f"{v['vs_random']:+.4f}" if "vs_random" in v else "—"
                    L.append(f"| {lbl} | {kl} | {v['n']:,.0f} | {v['win_pct']:.1f}% | "
                             f"{v['avg_r']:+.4f} | {vr} |")

        L.append("\n**保有期間を変えたとき（頑健性の確認。良い数字を選ぶためではない）**\n")
        L.append("| 保有 | 順張り平均R | 逆張り平均R | 無作為平均R |")
        L.append("|--:|--:|--:|--:|")
        for h in bt["holds"]:
            a = bt["all"][str(h)]
            g = lambda k: f"{a[k]['avg_r']:+.4f}" if k in a else "—"
            L.append(f"| {h}日 | {g('trend')} | {g('revert')} | {g('random')} |")

        L.append("\n**この検証の限界（結果を良い方に歪めるもの）**")
        L.append("- **生存バイアス。** 銘柄一覧は「いま上場している会社」なので、"
                 "この期間に上場廃止になった会社が母集団から抜けている。実際より良く出る。")
        L.append("- **スリッページ・板の薄さ・約定できない場面を含まない。** 建値は終値、損切りは水準どおり（窓は寄値）。")
        L.append("- **税を引いていない。** 特定口座の利益には20.315%かかる。")
        L.append("- 同じ日に建てた10件は相場つきを共有していて独立ではない。**t値は目安であって検定ではない。**")
        L.append("- 比較したのは**この4つだけ**（順張り/逆張り × 全期間/レジーム別）。"
                 "条件を変えて良い結果を探す作業はしていない。")
    except FileNotFoundError:
        L.append("\n## 過去検証\n\n未実施。大引け後の実行で自動的に走る（月1回）。")
    except Exception as e:
        L.append(f"\n## 過去検証\n\n生成に失敗: {e}")

    # ── シグナル台帳の成績 ────────────────────────────────────
    #    「この仕組みは儲かっているのか」に、実際に付いた値段で答える唯一の節。
    try:
        sg = meta.get("signals") or {}
        L.append("\n## シグナルの成績（選別ロジックの追跡）\n")
        if not sg or not sg.get("closed"):
            L.append(f"決着済み 0件 / 未決着 {sg.get('open', 0)}件。"
                     "**判断に使える成績はまだ無い。**最初の決着まで2〜3週間かかる。")
        else:
            L.append("| 種別 | 件数 | 勝率 | 平均R | 累計R | 利確 | 損切 | 時間切れ | 平均保有 |")
            L.append("|---|--:|--:|--:|--:|--:|--:|--:|--:|")
            for k, lbl in [("all", "合計"), ("trend", "順張り"), ("revert", "逆張り")]:
                v = sg.get(k)
                if not v: continue
                L.append(f"| {lbl} | {v['n']} | {v['win_pct']:.1f}% | {v['avg_r']:+.3f} | "
                         f"{v['sum_r']:+.2f} | {v['target']} | {v['stop']} | {v['timeout']} | "
                         f"{v['avg_bars']:.1f}日 |")
            a = sg.get("all", {})
            L.append(f"\n未決着 {sg.get('open', 0)}件／記録した日数 {sg.get('days', 0)}日。")
            # 1件あたりのRのばらつきは概ね1.0。平均+0.10R を t=2 で検出するには
            # n=(2×1.0/0.10)^2≒400件が要る。しかも同じ日の複数銘柄は独立でないので、
            # 実効的な標本数は「件数」ではなく「日数」に近い。
            if a.get("n", 0) < 400:
                L.append(f"> **決着 {a.get('n', 0)}件では、平均Rの正負を統計的に判定できない。** "
                         "1件あたりのRのばらつきが概ね1.0なので、平均+0.10Rを検出するには"
                         "**400件規模**が要る。さらに同じ日に記録した複数銘柄は相場つきを共有していて"
                         f"独立ではないため、実効的な標本は件数より**日数（現在{sg.get('days', 0)}日）**に近い。"
                         "**この表は動作確認であって、優劣の根拠にはまだ使えない。**")
            if a.get("n", 0) >= 60 and a.get("avg_r", 0) <= -0.30:
                L.append("> **平均Rが −0.30 を下回っている。** 統計的な結論には早いが、"
                         "この水準が続くなら選別を止めるか閾値を上げる判断が要る。")
            elif a.get("avg_r", 0) <= 0:
                L.append("> **平均Rがマイナス。この選別で建て続けると負ける。** "
                         "スコアの閾値を上げるか、順張り／逆張りのどちらかを止めることを検討する。")
            L.append("\n**この成績の読み方と限界**")
            L.append("- 建値は**シグナル当日の終値**。実際には後場の板で約定するので、その差は含まれていない。")
            L.append("- 損切り2ATR・利確3ATR・最長15営業日で機械的に決着させている。**実際の執行記録ではない。**")
            L.append("- 同じ日に損切りと利確の両方に触れた場合は**損切りを先**として数えている（日足では順序が分からないため）。")
            L.append("- 手数料は0円だが、**利益には20.315%課税**される。表のRは税引前。")
            L.append("- 測っているのは**スクリーニングの選別**であって、モデルのB（レジーム判断）は含まない。")
    except Exception as e:
        L.append(f"\n## シグナルの成績\n\n生成に失敗: {e}")

    # ── 全銘柄スクリーニングの結果 ─────────────────────────────
    try:
        cj = json.load(open(f"{OUT}/candidates.json", encoding="utf-8"))
        def _nm(r):
            n = (r.get("name") or "").strip()
            return n[:14] if n else "*(名称取得できず)*"
        def _disc(r):
            ns = r.get("news") or []
            if not ns: return "—"
            return " / ".join(f"{x['title'][:26]}" for x in ns[:2])
        def _ov(r):
            o = r.get("overlap") or []
            return ("**" + "/".join(o) + "と重複**") if o else "—"
        def _earn(r):
            """空欄にすると『決算が無い』と読めてしまう。状態を必ず言葉で出す。
               確定・発表済み・推定・不明の4つを取り違えさせないことが要点。"""
            st = r.get("earn_status")
            md = lambda x: str(x)[5:].replace("-", "/")
            if st == "ok":
                mark = "**跨ぎ**" if r.get("earn_soon") else f"{r.get('earn_days')}日後"
                return f"{md(r.get('earn_next'))} {mark}"
            if st == "recent":
                sc = r.get("earn_since")
                return f"{md(r.get('earn_last'))}発表済({sc}日前)" if sc is not None else \
                       f"{md(r.get('earn_last'))}発表済"
            if st == "estimate":
                w = "**要警戒**" if r.get("earn_watch") else "頃"
                return f"推定 {md(r.get('earn_next'))}{w}(要確認)"
            return {"unknown": "予定なし(要確認)", "error": "照会失敗(要確認)",
                    "timeout": "時間切れ(要確認)"}.get(st, "未照会(要確認)")

        L.append(f"\n## 新規候補（全銘柄スクリーニング）\n")
        _uni, _sc = cj.get("universe", 0), cj["scanned"]
        L.append(f"対象 {_uni:,}銘柄 → 走査 {_sc:,} → フィルタ通過 **{cj['passed']:,}銘柄**"
                 f"（売買代金20日平均 {cj['filters']['min_turnover']/1e8:.1f}億円以上／"
                 f"株価{cj['filters']['min_price']}円以上／"
                 f"ATR {cj['filters']['min_atr_pct']}〜{cj['filters'].get('max_atr_pct', 99)}%）")
        _un = cj.get("s17_unmapped") or {}
        if _un:
            L.append(f"\n> **注意: 業種名の対応が取れない値がある** → "
                     + " / ".join(f"「{k}」{v}件" for k, v in list(_un.items())[:5])
                     + "。該当銘柄は業種ETFが空欄になり、Bを機械的に付けられない。\n")
        L.append(f"銘柄マスタ {cj.get('master_count', 0):,}件（JPX上場銘柄一覧／"
                 f"ユニバースの出所: {cj.get('universe_source', '?')}）／"
                 f"適時開示 {cj.get('tdnet', {}).get('items', 0)}件・{cj.get('tdnet', {}).get('codes', 0)}銘柄\n")
        if _uni and _sc < _uni:
            L.append(f"\n> **注意: 時間切れで {_sc:,}/{_uni:,} までしか走査していない。** "
                     f"残り {_uni-_sc:,}銘柄は本日の候補に含まれていない。"
                     f"上位に見えるものが「全体の上位」とは限らないので、採用のハードルを上げること。\n")
        if cj.get("master_count", 0) == 0:
            L.append("\n> **注意: 銘柄マスタが取得できていない。** 候補に銘柄名と業種が付かず、"
                     "候補のBを機械的に付けられない。`openpyxl` が入っているか確認すること。\n")
        if not cj.get("trend") and not cj.get("revert"):
            L.append(f"\n> **本日の通過は0件。** これはフィルタが厳しすぎるか、走査側で例外が出ているかのどちらか。")
            sk = cj.get("skipped") or {}
            if sk:
                L.append("> 除外の内訳（例外の種類ごと）: "
                         + " / ".join(f"`{k}`×{v['n']}（例: {v['例']}）" for k, v in list(sk.items())[:4]))
                L.append("> **例外が大半を占める場合は、フィルタの結果ではなく不具合。** "
                         "この日の候補は信用しないこと。")
            else:
                L.append("> 例外は記録されていないので、フィルタ（売買代金・価格・ATR）で全件が落ちたということ。")
            L.append("")

        L.append("\n### 順張り候補（移動平均の上・レンジ上方・20日が伸びている）\n")
        L.append("| 順 | コード | 銘柄名 | 17業種 | 業種ETF | 重複 | スコア | 終値 | RSI | 25日 | 75日 | 60日位置 | 20日 | ATR% | 売買代金(億) | 出来高比 | 決算 | 当日の開示 |")
        L.append("|--:|---|---|---|:-:|:-:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|:-:|---|")
        for i, r in enumerate(cj["trend"][:15]):
            L.append(f"| {i+1} | {r['code'][:4]} | {_nm(r)} | {r.get('s17') or '—'} | {r.get('sector_etf') or '—'} | "
                     f"{_ov(r)} | **{r['trend']:.1f}** | {r['close']:,.1f} | {r['rsi']:.0f} | "
                     f"{r['vs25']:+.1f}% | {r['vs75']:+.1f}% | {r['pos60']:.0f}% | {r['r20']:+.1f}% | "
                     f"{r['atr_pct']:.2f} | {r['turnover']/1e8:.1f} | {r['vol_ratio']:.2f}x | {_earn(r)} | {_disc(r)} |")

        L.append("\n### 逆張り候補（長期は上向きだが短期で売られすぎ）\n")
        L.append("| 順 | コード | 銘柄名 | 17業種 | 業種ETF | 重複 | スコア | 終値 | RSI | 25日 | 75日 | 60日位置 | 20日 | ATR% | 売買代金(億) | 決算 | 当日の開示 |")
        L.append("|--:|---|---|---|:-:|:-:|--:|--:|--:|--:|--:|--:|--:|--:|--:|:-:|---|")
        for i, r in enumerate(cj["revert"][:15]):
            if r["revert"] <= 0: continue
            L.append(f"| {i+1} | {r['code'][:4]} | {_nm(r)} | {r.get('s17') or '—'} | {r.get('sector_etf') or '—'} | "
                     f"{_ov(r)} | **{r['revert']:.1f}** | {r['close']:,.1f} | {r['rsi']:.0f} | "
                     f"{r['vs25']:+.1f}% | {r['vs75']:+.1f}% | {r['pos60']:.0f}% | {r['r20']:+.1f}% | "
                     f"{r['atr_pct']:.2f} | {r['turnover']/1e8:.1f} | {_earn(r)} | {_disc(r)} |")

        L.append("\n**この表の読み方と限界**")
        L.append("- **スコアの重み・閾値は検証されていない。** 配点も境目も手で決めた値で、"
                 "過去データで当てはめた結果ではない。順張りスコアは実質的に"
                 "「直近1〜3ヶ月の上昇率」を4つの形で測り直したものに近く、"
                 "**独立した4項目を見ているわけではない**。順位に情報があるかどうかは"
                 "「シグナルの成績」で検証中で、現時点では**未証明**。")
        L.append("- したがってこの表は**発注の根拠ではなく、目で見る対象を絞るための入口**として使うこと。")
        L.append("- **「重複」が付いた銘柄は、既に持っている業種の上乗せ。** 1銘柄0.9%のつもりでも、"
                 "業種ショックでは同時に動く。採るなら片方だけにする。")
        L.append("- 順張りと逆張りは別の尺度なので**混ぜて比較しない**。")
        L.append("- 「業種ETF」列は業種別ETF17本の表と突き合わせるための対応コード。**候補のBはこの列の業種の位置から機械的に付けられる。**")
        L.append("- 「当日の開示」は TDnet の直近4日分（新しい順）。**空欄(—)は「開示が無い」であって「材料が無い」ではない**（報道・需給・指数入替は載らない）。")
        ce = cj.get("cand_earnings") or {}
        if ce.get("ok") is not None:
            L.append(f"- **決算列**: 上位候補 {ce.get('asked', 0)}銘柄を照会し、"
                     f"予定日が取れたのは **{ce.get('ok', 0)}銘柄**"
                     f"（不明 {ce.get('unknown', 0)} / 失敗 {ce.get('error', 0)}）。"
                     f"**「跨ぎ」は{EARN_SOON_DAYS}日以内に決算がある。建てないこと。**")
            L.append(f"  内訳: 確定 {ce.get('ok', 0)}（JPX {ce.get('from_jpx', 0)} / yfinance {ce.get('from_yf', 0)}）"
                     f" ／ 直近発表済 {ce.get('recent', 0)} ／ 推定 {ce.get('estimate', 0)}"
                     f" ／ 不明 {ce.get('unknown', 0)}")
            L.append("- **「◯/◯発表済(N日前)」は確定情報**。出したばかりなので当面は決算をまたがない。")
            L.append("- **「推定 ◯/◯頃」は予定ではない。** 過去の決算短信の間隔から割り出した目安で、"
                     "**これだけを根拠に建てない／見送らない**。**要警戒**は推定日が近い印。")
            L.append("- 「予定なし(要確認)」「照会失敗(要確認)」は**決算が無いという意味ではない**。"
                     "発注前にSBIの銘柄ページで必ず確認する。")
            L.append(f"- JPXが予定日を載せるのは**直前の四半期が終わった会社だけ**"
                     f"（今回 {(cj.get('jpx_earnings') or {}).get('codes', 0):,}社）。"
                     "載っていない会社は、直近に決算がある可能性はむしろ低い。")
        else:
            L.append("- **決算発表日の照合ができていない。** 発注前に必ず個別に確認すること。")
        L.append("- ATR%が突出して高いものは一過性の材料で動いている可能性が高い。順位が上でも採用しない。")
    except FileNotFoundError:
        L.append("\n## 新規候補\n\nスクリーニング未実行。")
    except Exception as e:
        L.append(f"\n## 新規候補\n\n生成に失敗: {e}")
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

    # 表の検算: ヘッダと区切り行のセル数が食い違うと、その列は描画されずに消える。
    # 目視では気づけないので機械的に確認し、食い違ったらレポート自身に書く。
    _bad = []
    for _i in range(len(L) - 1):
        _a, _b = L[_i], L[_i + 1]
        if _a.startswith("|") and re.fullmatch(r"\|[\s:|-]+\|", _b or ""):
            _na, _nb = len(_a.split("|")) - 2, len(_b.split("|")) - 2
            if _na != _nb:
                _bad.append(f"ヘッダ{_na}列 / 区切り{_nb}列: {_a[:40]}")
    if _bad:
        meta["table_mismatch"] = _bad
        L.append("\n> **注意: 表の列数が合っていない箇所がある（列が欠けて表示されている）**\n> "
                 + "\n> ".join(_bad))
        print("::warning::表の列数不一致:", " / ".join(_bad))

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
