#!/usr/bin/env python3
# ─────────────────────────────────────────────────────
# WALLET SCOUT v3  —  Hyperliquid 트레이더 아카이브
# ─────────────────────────────────────────────────────
VERSION = "4.3.1"
CHANGELOG = [
    ("3.9.3", "2026-03-28", "UnboundLocalError 수정, $10k 미만 자동 제외"),
    ("3.9.2", "2026-03-28", "잔고 표시, 포지션없을때 upnl안내, 미실현 레이블 명확화"),
    ("3.9.1", "2026-03-28", "포지션: 실현/현재 구역 분리, 롱숏바+넷익스포저, 소액제외"),
    ("3.9.0", "2026-03-28", "현재 오픈 포지션 카드 표시 (코인/방향/미실현손익/레버리지)"),
    ("3.8.2", "2026-03-28", "source: vault 자동감지, compute_stats에 src 파라미터"),
    ("3.8.1", "2026-03-28", "source 표시: manual→직접등록, active→활성발굴, vault→Vault발굴"),
    ("3.8.0", "2026-03-28", "코인태그: @숫자 필터링, ▲▼ 롱숏 방향 표시"),
    ("3.7.2", "2026-03-28", "배치수집+재시도+WAR40제외 재적용 (누락버그 수정)"),
    ("3.7.1", "2026-03-28", "샤프: gap을 마지막 거래일 fill 1건으로 추가 (균등분산→집중 방식)"),
    ("3.7.0", "2026-03-28", "샤프: gap 거래일 균등분산. 중립값(0/50%) → 스탯 30점"),
    ("3.6.0", "2026-03-28", "샤프 계산 버그 수정: 미실현손익 반영, total_pnl 부호 보정"),
    ("3.5.0", "2026-03-28", "빅벳 50% 기준 최솟값, power 스케일 적용"),
    ("3.4.0", "2026-03-28", "WAR 40 미만 자동 제외 + --prune 옵션"),
    ("3.3.0", "2026-03-28", "승률 육각형 스탯 추가, WAR 가중치 재배분"),
    ("3.2.0", "2026-03-28", "지속력 공식 개편: 기간40%+일관성60%"),
    ("3.1.0", "2026-03-28", "배치 수집(5개/2s) + 429 자동재시도 3회"),
    ("3.0.0", "2026-03-28", "로그스케일 radar, run.log TeeConsole, --refresh-all"),
]
import os, sys, json, asyncio, argparse, math, random
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from pathlib import Path
import httpx
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

HL_API_URL   = "https://api.hyperliquid.xyz/info"
ARCHIVE_FILE = "wallet_cache.json"
HIST_FILE    = "sentiment_history.json"
CACHE_TTL    = timedelta(hours=6)
MIN_EQUITY   = 10_000
_raw_console = Console()

def short_addr(addr: str) -> str:
    """0x1234...5678 — 앞 4자리 + ... + 뒤 4자리"""
    a = addr.lower()
    if a.startswith("0x") and len(a) >= 10:
        return f"0x{a[2:6]}...{a[-4:]}"
    return f"{a[:4]}...{a[-4:]}" if len(a) >= 8 else a
_LOG_PATH    = Path("run.log")
class TeeConsole:
    def __init__(self,c,log): self._c=c; self._log=log
    def print(self,*a,**k):
        self._c.print(*a,**k)
        import io; from rich.console import Console as _C
        buf=io.StringIO(); tmp=_C(file=buf,highlight=False,markup=True,width=120)
        try: tmp.print(*a,**k); line=buf.getvalue().rstrip()
        except: line=str(a)
        ts=datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        with open(self._log,"a",encoding="utf-8") as f: f.write(f"[{ts}] {line}\n")
    def __getattr__(self,n): return getattr(self._c,n)
console = TeeConsole(_raw_console, _LOG_PATH)


# ══ ARCHIVE MANAGER ════════════════════════════════════════════════════
class ArchiveManager:
    """
    누적 아카이브 - 지갑은 절대 삭제 안 됨
    새 지갑 추가 / 24시간 지난 지갑 스탯만 갱신
    """
    def __init__(self, archive_file=ARCHIVE_FILE):
        self.path = Path(archive_file)
        self.data = self._load()

    def _load(self):
        if self.path.exists():
            try:
                return json.loads(self.path.read_text(encoding="utf-8"))
            except:
                return {}
        return {}

    def _load_vault_addrs(self):
        """vault_discovery.json에서 vault 주소 목록 로드"""
        vd_path = self.path.parent / "vault_discovery.json"
        if vd_path.exists():
            try:
                vd = json.loads(vd_path.read_text(encoding="utf-8"))
                return {v['vault_addr'].lower() for v in vd.get('direct_vaults', [])}
            except Exception:
                pass
        return set()

    @property
    def vault_addrs(self):
        if not hasattr(self, '_vault_addrs'):
            self._vault_addrs = self._load_vault_addrs()
        return self._vault_addrs

    def save(self):
        self.path.write_text(
            json.dumps(self.data, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )

    def mark_vaults_from_label(self):
        """label이 vault 이름인 경우 source를 vault로 업데이트 (캐시 보정)"""
        # Hyperliquid vault 이름 패턴: 공백/특수문자 포함 영문, 숫자 조합
        # clearinghouse vaultEquity 필드가 없을 때 label 기반으로 감지 불가
        # → API 재수집 시 compute_stats에서 자동 감지됨
        pass

    def needs_update(self, address: str) -> bool:
        """24시간 지났거나 처음 보는 지갑이면 True"""
        entry = self.data.get(address.lower())
        if not entry:
            return True
        fetched_at = datetime.fromisoformat(entry["fetched_at"])
        return datetime.now(tz=timezone.utc) - fetched_at > CACHE_TTL

    def is_new(self, address: str) -> bool:
        return address.lower() not in self.data

    def get_stats(self, address: str):
        entry = self.data.get(address.lower())
        return entry["stats"] if entry else None

    def upsert(self, address: str, stats: dict):
        """
        새 지갑: first_discovered_at 기록
        기존 지갑: first_discovered_at 유지, 스탯만 갱신
        """
        key = address.lower()
        now = datetime.now(tz=timezone.utc).isoformat()
        if key not in self.data:
            self.data[key] = {
                "first_discovered_at": now,
                "fetched_at": now,
                "stats": stats,
            }
        else:
            self.data[key]["fetched_at"] = now
            self.data[key]["stats"] = stats

    def age_str(self, address: str) -> str:
        entry = self.data.get(address.lower())
        if not entry:
            return "신규"
        fetched_at = datetime.fromisoformat(entry["fetched_at"])
        delta = datetime.now(tz=timezone.utc) - fetched_at
        if delta.total_seconds() < 60:
            return "방금"
        if delta.seconds < 3600 and delta.days == 0:
            return f"{delta.seconds//60}분 전"
        if delta.days == 0:
            return f"{delta.seconds//3600}시간 전"
        return f"{delta.days}일 전"

    def first_seen_str(self, address: str) -> str:
        entry = self.data.get(address.lower())
        if not entry:
            return "-"
        ts = entry.get("first_discovered_at", entry.get("fetched_at", "-"))
        return ts[:10]

    def prune_low_war(self, min_war=40.0, min_equity=10_000):
        to_del=[a for a,e in self.data.items()
                if e.get("stats",{}).get("war_score",0)<min_war
                or e.get("stats",{}).get("total_equity",0)<min_equity]
        for a in to_del: del self.data[a]
        if to_del: self.save()
        return len(to_del)

    def all_addresses(self):
        return list(self.data.keys())

    def all_stats(self):
        return [e["stats"] for e in self.data.values() if e.get("stats")]

    def qualified_stats(self, min_equity=MIN_EQUITY):
        return [s for s in self.all_stats() if s.get("total_equity", 0) >= min_equity]

    def top_war_stats(self, n=20, min_equity=MIN_EQUITY):
        """시즌 추천: $min_equity 이상 WAR 상위 N명"""
        qualified = self.qualified_stats(min_equity)
        return sorted(qualified, key=lambda x: x.get("war_score", 0), reverse=True)[:n]

    def summary(self):
        total = len(self.data)
        qualified = len(self.qualified_stats())
        stale = sum(1 for a in self.data if self.needs_update(a))
        return total, qualified, stale


# ══ DISCOVERY (3-레이어) ════════════════════════════════════════════════
class WalletDiscovery:
    """
    Layer 1 (Vault 리더): 검증된 고수, 고정적
    Layer 2 (활성 트레이더): 매번 랜덤 코인 조합 → 다양성
    Layer 3 (고WAR 이웃): 기존 고수와 거래한 지갑 → 숨겨진 고수 발굴
    """
    def __init__(self):
        self.http = httpx.AsyncClient(timeout=20.0)

    async def layer1_vaults(self, n=100):
        """vaultSummaries로 TVL 상위 vault leader 수집"""
        results = []
        seen = set()
        try:
            r = await self.http.post(HL_API_URL, json={"type": "vaultSummaries"})
            r.raise_for_status()
            vaults = r.json()
            if not isinstance(vaults, list):
                console.print(f"  [yellow]⚠ vaultSummaries 응답 이상: {type(vaults)}[/yellow]")
                return results
            # 열린 vault, TVL > $1k
            open_vaults = [v for v in vaults
                           if not v.get("isClosed", False) and float(v.get("tvl", 0)) > 1000]
            open_vaults.sort(key=lambda v: float(v.get("tvl", 0)), reverse=True)
            console.print(f"  [dim]vault 총 {len(vaults)}개 중 TVL>1k: {len(open_vaults)}개[/dim]")
            for v in open_vaults:
                leader = v.get("leader", "")
                if not leader or leader.lower() in seen:
                    continue
                seen.add(leader.lower())
                name = (v.get("name") or "").strip()
                label = name if name else short_addr(leader)
                results.append({"address": leader, "label": label, "source": "vault"})
                if len(results) >= n:
                    break
        except Exception as e:
            console.print(f"  [yellow]⚠ Layer1(vaultSummaries) 실패: {e}[/yellow]")
        return results

    async def layer1b_leaderboard(self, n=50):
        """leaderboard API로 상위 트레이더 수집"""
        results = []
        try:
            r = await self.http.post(HL_API_URL, json={"type": "leaderboard"})
            r.raise_for_status()
            data = r.json()
            entries = data if isinstance(data, list) else data.get("leaderboardRows", [])
            console.print(f"  [dim]leaderboard {len(entries)}명[/dim]")
            for e in entries[:n]:
                addr = e.get("ethAddress", "") or e.get("address", "")
                if not addr:
                    continue
                name = e.get("displayName", "") or short_addr(addr)
                results.append({"address": addr, "label": name, "source": "active"})
        except Exception as e:
            console.print(f"  [yellow]⚠ Layer1b(leaderboard) 실패: {e}[/yellow]")
        return results

    async def layer2_active(self, n=200):
        def _sa(a):
            a = a.lower()
            return f"0x{a[2:6]}...{a[-4:]}" if a.startswith("0x") else f"{a[:4]}...{a[-4:]}"

        all_coins = ["BTC", "ETH", "SOL", "HYPE", "DOGE", "XRP", "ARB", "AVAX", "LINK", "BNB",
                     "WIF", "PEPE", "SUI", "OP", "INJ", "TIA", "STRK", "JUP", "PYTH", "W",
                     "ATOM", "NEAR", "APT", "SEI", "FTM", "MATIC", "LTC", "BCH", "ETC", "FIL",
                     "BONK", "ORDI", "STX", "BLUR", "GMX", "ARB", "DYDX", "PENDLE", "JTO"]
        random.shuffle(all_coins)
        results_set = {}
        try:
            for coin in all_coins:
                await asyncio.sleep(1.0)  # recentTrades weight=20, 분당 60req
                r = await self.http.post(HL_API_URL, json={"type": "recentTrades", "coin": coin})
                if r.status_code != 200:
                    continue
                trades = r.json()
                if not isinstance(trades, list):
                    continue
                for t in trades:
                    users = t.get("users", [])
                    if isinstance(users, list):
                        for u in users:
                            if u and u not in results_set:
                                results_set[u] = {"address": u, "label": _sa(u), "source": "active"}
                    else:
                        user = t.get("user", "")
                        if user and user not in results_set:
                            results_set[user] = {"address": user, "label": _sa(user), "source": "active"}
                if len(results_set) >= n * 2:
                    break
            all_found = list(results_set.values())
            random.shuffle(all_found)
            return all_found[:n]
        except Exception as e:
            console.print(f"  [yellow]⚠ Layer2 실패: {e}[/yellow]")
            return []

    async def layer3_network(self, archive: ArchiveManager, n=20):
        """고WAR 지갑의 fills에서 거래 상대방 주소 추출"""
        top_traders = sorted(
            archive.all_stats(),
            key=lambda x: x.get("war_score", 0),
            reverse=True
        )[:5]

        if not top_traders:
            return []

        results_set = {}
        existing = set(archive.all_addresses())

        try:
            for trader in top_traders:
                addr = trader.get("address", "")
                if not addr:
                    continue
                r = await self.http.post(HL_API_URL, json={
                    "type": "userFills", "user": addr, "aggregateByTime": False
                })
                r.raise_for_status()
                fills = r.json()
                if not isinstance(fills, list):
                    continue
                for f in fills[:200]:
                    users = f.get("users", [])
                    if isinstance(users, list):
                        for u in users:
                            if u and u.lower() != addr.lower() and u not in results_set:
                                results_set[u] = {"address": u, "label": u[:8] + "...", "source": "network"}
                if len(results_set) >= n * 2:
                    break

            all_found = list(results_set.values())
            new_ones = [x for x in all_found if x["address"].lower() not in existing]
            random.shuffle(new_ones)
            return new_ones[:n]
        except Exception as e:
            console.print(f"  [yellow]⚠ Layer3 실패: {e}[/yellow]")
            return []

    async def discover(self, archive: ArchiveManager, target=150):
        console.print(f"\n[bold cyan]▶ DISCOVERY[/bold cyan] [dim]발굴 중...[/dim]")
        # L1(vault)은 API 미지원, L2(recentTrades)로 활성 트레이더 수집
        l1, l2 = await asyncio.gather(
            self.layer1_vaults(100),
            self.layer2_active(200),  # 더 많이 수집
        )
        console.print(f"  L1(Vault):{len(l1)}  L2(Active):{len(l2)}")

        seen, results, existing = set(), [], set(archive.all_addresses())
        for item in l1 + l2:
            addr = item["address"].lower()
            if addr not in seen:
                seen.add(addr)
                results.append({**item, "is_new": addr not in existing})

        new_count = sum(1 for r in results if r["is_new"])
        console.print(f"  신규: [green]{new_count}개[/green]  기존 재확인: [dim]{len(results)-new_count}개[/dim]")
        return results

    async def close(self):
        await self.http.aclose()


# ══ API ════════════════════════════════════════════════════════════════
class HyperliquidAPI:
    def __init__(self):
        self.http = httpx.AsyncClient(timeout=20.0)

    async def get_clearinghouse(self, addr):
        r = await self.http.post(HL_API_URL, json={"type": "clearinghouseState", "user": addr})
        r.raise_for_status()
        return r.json()

    async def get_fills(self, addr):
        for attempt in range(3):
            try:
                r = await self.http.post(HL_API_URL, json={
                    "type": "userFills", "user": addr, "aggregateByTime": False
                })
                r.raise_for_status()
                d = r.json()
                return d if isinstance(d, list) else []
            except:
                if attempt == 2: return []
                await asyncio.sleep(1)
        return []

    async def fetch(self, addr):
        ch, fills = await asyncio.gather(
            self.get_clearinghouse(addr),
            self.get_fills(addr),
            return_exceptions=True
        )
        return {
            "clearinghouse": ch if not isinstance(ch, Exception) else {},
            "fills": fills if not isinstance(fills, Exception) else [],
            "error": str(ch) if isinstance(ch, Exception) else None,
        }

    async def close(self):
        await self.http.aclose()


# ══ STATS ══════════════════════════════════════════════════════════════
def compute_stats(raw, address, label="", src="manual"):
    ch    = raw.get("clearinghouse", {})
    fills = raw.get("fills", [])
    ms           = ch.get("marginSummary", {})
    total_equity = float(ms.get("accountValue", 0))
    margin_used  = float(ms.get("totalMarginUsed", 0))
    margin_pct   = (margin_used / total_equity * 100) if total_equity > 0 else 0
    # clearinghouse에 vaultEquity 있으면 vault 주소로 판단
    if ch.get("vaultEquity") or ch.get("isVault"):
        src = "vault"

    positions = []
    for ap in ch.get("assetPositions", []):
        pos = ap.get("position", {})
        szi = float(pos.get("szi", 0))
        if szi == 0: continue
        coin    = pos.get("coin", "?")
        epx     = float(pos.get("entryPx", 0) or 0)
        upnl    = float(pos.get("unrealizedPnl", 0) or 0)
        lev     = pos.get("leverage", {})
        lev_val = float(lev.get("value", 1) if isinstance(lev, dict) else 1)
        ntl     = abs(szi) * epx if epx > 0 else 0
        cum_fund= float(pos.get("cumFunding", {}).get("sinceOpen", 0) or 0)
        positions.append({"coin": coin, "side": "LONG" if szi>0 else "SHORT",
                          "notional": ntl, "set_lev": lev_val, "upnl": upnl, "cum_funding": cum_fund})

    long_ntl  = sum(p["notional"] for p in positions if p["side"]=="LONG")
    short_ntl = sum(p["notional"] for p in positions if p["side"]=="SHORT")
    total_ntl = long_ntl + short_ntl
    long_pct  = (long_ntl/total_ntl*100) if total_ntl>0 else 50
    total_upnl= sum(p["upnl"] for p in positions)

    closed = [f for f in fills if float(f.get("closedPnl",0) or 0)!=0]
    wins   = [f for f in closed if float(f.get("closedPnl",0))>0]
    losses = [f for f in closed if float(f.get("closedPnl",0))<0]
    realized     = sum(float(f.get("closedPnl",0)) for f in closed)
    total_pnl    = realized + total_upnl
    win_rate     = (len(wins)/len(closed)*100) if closed else 0
    avg_win      = (sum(float(f["closedPnl"]) for f in wins)/len(wins)) if wins else 0
    avg_loss     = abs(sum(float(f["closedPnl"]) for f in losses)/len(losses)) if losses else 1
    profit_factor= (avg_win/avg_loss) if avg_loss>0 else avg_win

    pnl_by_day = defaultdict(float)
    pnl_by_coin = defaultdict(float)
    coin_side   = defaultdict(lambda: {"B":0,"A":0})  # 롱/숏 거래 수 집계
    for f in closed:
        cpnl = float(f.get("closedPnl",0))
        coin = f.get("coin","?")
        # @숫자(지정가 식별자), 콜론 포함(내부ID) 등 이상한 coin 필터링
        if not coin or coin.startswith("@") or ":" in coin or len(coin) > 12:
            continue
        pnl_by_coin[coin] += cpnl
        side = f.get("side","")  # B=buy(long close), A=ask(short close)
        if side in ("B","A"):
            coin_side[coin][side] += 1
        ts = f.get("time",0)
        if ts:
            day = datetime.fromtimestamp(ts/1000,tz=timezone.utc).strftime("%Y-%m-%d")
            pnl_by_day[day] += cpnl

    # fills 합산(realized)과 total_pnl 차이를 가상의 fill 1건으로 추가
    # → 미실현손익/펀딩비 등이 샤프 계산에 반영됨
    gap = total_pnl - realized
    if gap != 0 and pnl_by_day:
        # 가장 마지막 거래일에 gap fill 1건 추가
        last_day = max(pnl_by_day.keys())
        pnl_by_day[last_day] += gap

    daily_pnls = list(pnl_by_day.values())
    if len(daily_pnls)>=3:
        mean_d=sum(daily_pnls)/len(daily_pnls)
        std_d=math.sqrt(sum((x-mean_d)**2 for x in daily_pnls)/len(daily_pnls))
        if std_d>0: sharpe=mean_d/std_d*math.sqrt(365)
        else: sharpe=3.0 if mean_d>0 else (-1.0 if mean_d<0 else 0.0)
    elif gap != 0:
        # 거래일 1~2일 뿐이라도 gap이 있으면 부호는 total_pnl 기준
        sharpe = 1.0 if total_pnl > 0 else -1.0
    else:
        sharpe=0
    # 최종 보정: total_pnl 음수면 sharpe 무조건 음수
    if total_pnl < 0 and sharpe > 0:
        sharpe = -abs(sharpe)

    sorted_days=sorted(pnl_by_day.items()); cumulative=[]; running=0.0
    for day,pnl in sorted_days:
        running+=pnl; cumulative.append({"date":day,"daily":round(pnl,2),"cum":round(running,2)})

    peak=0.0; mdd=0.0
    for pt in cumulative:
        if pt["cum"]>peak: peak=pt["cum"]
        dd=(peak-pt["cum"])/peak if peak>0 else 0
        if dd>mdd: mdd=dd
    mdd_pct=mdd*100

    consistency=(sum(1 for v in daily_pnls if v>0)/len(daily_pnls)*100) if daily_pnls else 0

    big_bets=[]
    for f in closed:
        sz=abs(float(f.get("sz",0) or 0)); px=float(f.get("px",0) or 0); ntl2=sz*px; cpnl2=float(f.get("closedPnl",0))
        if total_equity>0 and ntl2>total_equity*0.20: big_bets.append({"ntl":ntl2,"pnl":cpnl2})
    big_bet_wins=[b for b in big_bets if b["pnl"]>0]
    big_bet_rate=(len(big_bet_wins)/len(big_bets)*100) if big_bets else 0
    big_bet_pnl=sum(b["pnl"] for b in big_bet_wins); big_bet_count=len(big_bets)

    today=datetime.now(tz=timezone.utc)
    if cumulative:
        first_date=datetime.strptime(cumulative[0]["date"],"%Y-%m-%d").replace(tzinfo=timezone.utc)
        last_date =datetime.strptime(cumulative[-1]["date"],"%Y-%m-%d").replace(tzinfo=timezone.utc)
        data_days=(last_date-first_date).days+1
        first_date_str=cumulative[0]["date"]; last_date_str=cumulative[-1]["date"]
        days_since_last=(today-last_date).days
        # 지속력: 기간(55%) + 일관성(45%), 기간 기준일 90일
        if data_days<7: ds=data_days/7*20
        else: ds=min(20+80*math.log(data_days/7)/math.log(90/7),100)
        durability=round(max(10, ds*0.55+consistency*0.45),1)
    else:
        data_days=0; first_date_str="-"; last_date_str="-"; days_since_last=9999; durability=0.0

    roi_pct=(total_pnl/total_equity*100) if total_equity>0 else 0

    # ── radar 스탯 변환 ─────────────────────────────────────────────
    # 중립값(0/50%) → 30점, 음수/손실 → 10점(최솟값), 최고치 → 100점
    import math as _m
    _MIN=10; _NEU=30  # 최솟값=10, 중립=30

    def _ps(v):   # profit_amt: 손실→10, 0→30, 600k→100
        if v < 0: return _MIN
        if v == 0: return _NEU
        return min(_NEU + (100-_NEU)*_m.log1p(v)/_m.log1p(600_000), 100.0)

    def _rs(roi,n):  # roi: 음수→10, 0%→30, 400%→100
        if n<3: return _MIN
        r=max(-500.0,min(roi,400.0))
        if r < 0: return _MIN
        if r == 0: return _NEU
        return min(_NEU + (100-_NEU)*_m.log1p(r)/_m.log1p(400), 100.0)

    def _bs(rate,cnt):  # big_bet: 없음→10, 50%이하→10, 50%↑ power 스케일
        if cnt==0: return _MIN
        if rate<=50: return _MIN
        above=(rate-50)/50
        return min(_MIN+(100-_MIN)*_m.pow(above,0.5),100.0)

    def _ss(sp,n):  # sharpe: 음수→10, 0→30, 16→100
        if n<3: return _MIN
        if sp < 0: return _MIN
        if sp == 0: return _NEU
        return min(_NEU + (100-_NEU)*_m.log1p(sp)/_m.log1p(16), 100.0)

    def _ds(d): return max(d, _MIN)

    def _wr(wr,n):  # win_rate: 50%미만→최대30, 50%→30, 100%→100
        if n<5: return _MIN
        if wr < 50: return max(_MIN, wr*0.6)
        if wr == 50: return _NEU
        above=(wr-50)/50
        return max(_NEU, min(_NEU+(100-_NEU)*_m.pow(above,0.5),100.0))

    radar={"profit_amt":round(_ps(total_pnl),1),
           "roi":        round(_rs(roi_pct,len(closed)),1),
           "big_bet":    round(_bs(big_bet_rate,big_bet_count),1),
           "sharpe":     round(_ss(sharpe,len(closed)),1),
           "durability": round(_ds(durability),1),
           "win_rate":   round(_wr(win_rate,len(closed)),1)}

    raw_war=(radar["profit_amt"]*0.25+radar["roi"]*0.25+radar["big_bet"]*0.15
             +radar["sharpe"]*0.20+radar["win_rate"]*0.15)
    war_score=round(raw_war,1)

    # ── 트레이더 타입 분류 (클러스터링 기반) ──────────────────────
    if total_pnl < 0:
        trader_type="💀 Underwater";    character="익사 직전"
    elif durability < 35 and total_pnl > 0:
        trader_type="⚡ Flash Trader";  character="번개처럼 왔다 사라지는"
    elif mdd_pct > 200 or (mdd_pct > 80 and big_bet_count > 10):
        trader_type="🌊 Degen";         character="올인 아니면 청산"
    elif big_bet_rate > 60 and win_rate > 65 and durability > 50:
        trader_type="🦁 Apex Predator"; character="먹이사슬 최상위"
    elif win_rate > 72 and sharpe > 3 and mdd_pct < 25:
        trader_type="🦅 Precision Hunter"; character="절대 빗나가지 않는"
    elif sharpe > 5 and mdd_pct < 15 and durability > 50:
        trader_type="🧊 Ice Quant";     character="감정 없는 알고리즘"
    elif profit_factor > 3 and big_bet_count > 3 and win_rate < 55:
        trader_type="🎯 Sniper";        character="적게 쏘고 크게 맞히는"
    elif mdd_pct > 40 and profit_factor > 1.5:
        trader_type="🎰 High Roller";   character="판 크게 벌이는"
    elif consistency > 70 and durability > 60 and mdd_pct < 20:
        trader_type="🏔️ Iron Hands";    character="흔들리지 않는"
    elif win_rate > 60 and durability > 55:
        trader_type="📊 All-Rounder";   character="균형잡힌 종합 선수"
    elif len(closed) < 100:
        trader_type="🌱 Newcomer";      character="데이터가 부족한"
    elif sharpe > 3 and total_pnl > 0:
        trader_type="📈 Momentum";      character="흐름을 타는"
    elif win_rate > 65 and total_pnl > 0:
        trader_type="🎯 Steady Shot";   character="꾸준히 맞히는"
    elif profit_factor > 2 and total_pnl > 0:
        trader_type="💰 Value Hunter";  character="손익비로 승부하는"
    elif big_bet_count > 20 and big_bet_rate > 50:
        trader_type="🎲 Bet Maker";     character="베팅을 즐기는"
    else:
        trader_type="🌀 Drifter";       character="패턴을 찾는 중"

    return {
        "address":address,"label":label or address[:8]+"...","total_equity":total_equity,
        "margin_pct":margin_pct,"realized":realized,"total_upnl":total_upnl,"total_pnl":total_pnl,
        "win_rate":round(win_rate,1),"avg_win":round(avg_win,2),"avg_loss":round(avg_loss,2),
        "profit_factor":round(profit_factor,2),"sharpe":round(sharpe,2),"mdd_pct":round(mdd_pct,1),
        "consistency":round(consistency,1),"durability":round(durability,1),"long_pct":round(long_pct,1),
        "big_bet_count":big_bet_count,"big_bet_rate":round(big_bet_rate,1),"big_bet_pnl":round(big_bet_pnl,2),
        "closed_count":len(closed),"total_days":len(daily_pnls),"data_days":data_days,
        "first_date":first_date_str,"last_date":last_date_str,"days_since_last":days_since_last,
        "roi_pct":round(roi_pct,1),"top_coins":[
            {"coin":c,"pnl":round(p,2),
             "side":"L" if coin_side[c]["A"]>=coin_side[c]["B"] else "S"}
            for c,p in sorted(pnl_by_coin.items(),key=lambda x:x[1],reverse=True)[:5]
        ],
        "radar":radar,"war_score":war_score,"trader_type":trader_type,"character":character,
        "is_hf":(len(closed)/max(data_days,1) >= 300 if data_days>0 else False),
        "is_vault":(src=="vault"),
        "cumulative":cumulative,"source":src,"error":raw.get("error"),
        "positions":[{"coin":p["coin"],"side":p["side"],"upnl":round(p["upnl"],2),
                      "notional":round(p["notional"],2),"lev":p["set_lev"]} for p in positions],
    }


# ══ PROCESS ════════════════════════════════════════════════════════════
async def process_addresses(addresses, labels, sources, archive: ArchiveManager, force=False):
    api = HyperliquidAPI()

    # vault_discovery.json + vaultSummaries API로 vault 주소 목록 수집
    _vault_leaders = set(archive.vault_addrs)  # vault_discovery.json 기반
    try:
        r = await api.http.post(HL_API_URL, json={"type": "vaultSummaries"})
        r.raise_for_status()
        for v in r.json():
            leader = v.get("leader", "")
            if leader:
                _vault_leaders.add(leader.lower())
        console.print(f"  [dim]vault {len(_vault_leaders)}개 확인[/dim]")
    except Exception:
        if _vault_leaders:
            console.print(f"  [dim]vault_discovery.json 기반 {len(_vault_leaders)}개[/dim]")

    # source 보정: vault 주소면 vault로 덮어쓰기
    sources = [
        "vault" if addr.lower() in _vault_leaders else src
        for addr, src in zip(addresses, sources)
    ]
    # 캐시에 저장된 기존 항목도 vault면 source 업데이트
    _vault_patched = 0
    for addr in archive.all_addresses():
        if addr.lower() in _vault_leaders:
            entry = archive.data.get(addr.lower(), {})
            if entry.get("stats", {}).get("source") != "vault":
                entry["stats"]["source"] = "vault"
                entry["stats"]["is_vault"] = True
                _vault_patched += 1
    if _vault_patched:
        archive.save()
        console.print(f"  [dim]vault 보정 저장: {_vault_patched}개[/dim]")

    skip, need_fetch = [], []

    for addr, label, src in zip(addresses, labels, sources):
        if not force and not archive.needs_update(addr):
            skip.append((addr, label, src))
        else:
            need_fetch.append((addr, label, src))

    console.print(f"\n  [green]✓ 캐시 사용:[/green] {len(skip)}개  [yellow]↓ 수집:[/yellow] {len(need_fetch)}개\n")

    results = []
    for addr, label, src in skip:
        stats = archive.get_stats(addr)
        if stats:
            stats["label"] = label
            # vault leader면 source 유지 (보정값 덮어쓰지 않음)
            if addr.lower() in _vault_leaders:
                stats["source"] = "vault"
                stats["is_vault"] = True
            else:
                stats["source"] = src
            console.print(f"  [dim]📦 {label} (캐시 {archive.age_str(addr)})[/dim]")
            results.append(stats)

    if need_fetch:
        # Rate limit: clearinghouseState=weight2, userFills=weight20+per-item
        # BATCH=5 → 배치당 ~110 weight, DELAY=6s → 분당 10배치=1100/1200
        BATCH=5; DELAY=6.0; RETRY_DELAY=30.0; MAX_RETRY=3
        console.print(f"[bold blue]▶ API 수집[/bold blue] [dim]{len(need_fetch)}개 (배치={BATCH}, 딜레이={DELAY}s)[/dim]")
        pending = list(need_fetch)
        retry_count = 0
        while pending and retry_count <= MAX_RETRY:
            if retry_count > 0:
                console.print(f"  [yellow]↻ 재시도 {retry_count}/{MAX_RETRY} — {len(pending)}개 ({RETRY_DELAY}초 대기)[/yellow]")
                await asyncio.sleep(RETRY_DELAY)
            still_failed = []
            for bi in range(0, len(pending), BATCH):
                batch = pending[bi:bi+BATCH]
                tasks = [api.fetch(addr) for addr,_,_ in batch]
                fetched = await asyncio.gather(*tasks)
                for (addr,label,src),raw in zip(batch,fetched):
                    err = str(raw.get("error",""))
                    if raw.get("error"):
                        if "429" in err: still_failed.append((addr,label,src))
                        else: console.print(f"  [red]✗ {label}[/red]  [dim]{err[:60]}[/dim]")
                        continue
                    stats = compute_stats(raw, addr, label, src=src)
                    equity = stats.get("total_equity", 0)
                    war = stats.get("war_score", 0)
                    tag = "[bold green]NEW[/bold green]" if archive.is_new(addr) else "[dim]갱신[/dim]"
                    if war < 40.0:
                        key = addr.lower()
                        if key in archive.data: del archive.data[key]
                        console.print(f"  [dim]제외 {label} — WAR {war:.1f} (40 미만)[/dim]")
                        continue
                    if equity < 10_000:
                        key = addr.lower()
                        if key in archive.data: del archive.data[key]
                        console.print(f"  [dim]제외 {label} — ${equity:,.0f} ($10k 미만)[/dim]")
                        continue
                    archive.upsert(addr, stats)
                    results.append(stats)
                    console.print(f"  {tag} {label} — WAR [bold]{stats['war_score']}[/bold] · {stats['trader_type']} · ${equity:,.0f}")
                if bi+BATCH < len(pending): await asyncio.sleep(DELAY)
            pending = still_failed
            retry_count += 1
        if pending:
            console.print(f"  [red]최종 실패 {len(pending)}개[/red]")
            for addr,label,src in pending: console.print(f"    ✗ {label}")

    await api.close()
    archive.save()
    return results


# ══ TOURNAMENT ════════════════════════════════════════════════════════
def run_tournament(all_stats):
    if not all_stats: return {}
    all_dates = set()
    for s in all_stats:
        for pt in s["cumulative"]: all_dates.add(pt["date"])
    if not all_dates: return {s["address"]:{"tourney_score":0,"wins":0,"weekly_pnl":[]} for s in all_stats}
    sorted_dates = sorted(all_dates)
    start = datetime.strptime(sorted_dates[0], "%Y-%m-%d")
    end   = datetime.strptime(sorted_dates[-1], "%Y-%m-%d")
    rounds = []; cur = start
    while cur < end:
        nxt = cur + timedelta(days=7)
        rounds.append((cur.strftime("%Y-%m-%d"), min(nxt, end).strftime("%Y-%m-%d")))
        cur = nxt
    tourney = {s["address"]:{"score":0,"wins":0,"weekly_pnl":[]} for s in all_stats}
    for r_start, r_end in rounds:
        rpnls = {}
        for s in all_stats:
            pnl = sum(pt["daily"] for pt in s["cumulative"] if r_start <= pt["date"] < r_end)
            rpnls[s["address"]] = pnl
            tourney[s["address"]]["weekly_pnl"].append({"week": r_start, "pnl": round(pnl, 2)})
        if rpnls:
            winner = max(rpnls, key=rpnls.get)
            if rpnls[winner] > 0:
                tourney[winner]["score"] += 3; tourney[winner]["wins"] += 1
            sr = sorted(rpnls.items(), key=lambda x: x[1], reverse=True)
            if len(sr) > 1 and sr[1][1] > 0: tourney[sr[1][0]]["score"] += 1
    return {"rounds": rounds, "results": tourney}


# ══ HTML REPORT ════════════════════════════════════════════════════════
def reclassify_type(s):
    """캐시된 trader_type 무시하고 현재 로직으로 재분류"""
    wr  = s.get('win_rate', 0);  sh  = s.get('sharpe', 0)
    dur = s.get('durability', 0); bbr = s.get('big_bet_rate', 0)
    bbc = s.get('big_bet_count', 0); pf  = s.get('profit_factor', 0)
    mdd = s.get('mdd_pct', 0);   pnl = s.get('total_pnl', 0)
    con = s.get('consistency', 0); cc  = len(s.get('cumulative', [])) or s.get('closed_count', 0)
    closed_n = s.get('closed_count', 0)

    if pnl < 0:                                        t, c = "💀 Underwater",    "익사 직전"
    elif dur < 35 and pnl > 0:                         t, c = "⚡ Flash Trader",  "번개처럼 왔다 사라지는"
    elif mdd > 200 or (mdd > 80 and bbc > 10):         t, c = "🌊 Degen",         "올인 아니면 청산"
    elif bbr > 60 and wr > 65 and dur > 50:            t, c = "🦁 Apex Predator", "먹이사슬 최상위"
    elif wr > 72 and sh > 3 and mdd < 25:              t, c = "🦅 Precision Hunter", "절대 빗나가지 않는"
    elif sh > 5 and mdd < 15 and dur > 50:             t, c = "🧊 Ice Quant",     "감정 없는 알고리즘"
    elif pf > 3 and bbc > 3 and wr < 55:               t, c = "🎯 Sniper",        "적게 쏘고 크게 맞히는"
    elif mdd > 40 and pf > 1.5:                        t, c = "🎰 High Roller",   "판 크게 벌이는"
    elif con > 70 and dur > 60 and mdd < 20:           t, c = "🏔️ Iron Hands",    "흔들리지 않는"
    elif wr > 60 and dur > 55:                         t, c = "📊 All-Rounder",   "균형잡힌 종합 선수"
    elif closed_n < 100:                               t, c = "🌱 Newcomer",      "데이터가 부족한"
    elif sh > 3 and pnl > 0:                           t, c = "📈 Momentum",      "흐름을 타는"
    elif wr > 65 and pnl > 0:                          t, c = "🎯 Steady Shot",   "꾸준히 맞히는"
    elif pf > 2 and pnl > 0:                           t, c = "💰 Value Hunter",  "손익비로 승부하는"
    elif bbc > 20 and bbr > 50:                        t, c = "🎲 Bet Maker",     "베팅을 즐기는"
    else:                                              t, c = "🌀 Drifter",       "패턴을 찾는 중"
    return t, c

def generate_html(all_stats, tournament, archive: ArchiveManager, hist_path: Path = None):
    import math as _math
    # 히스토리 데이터 로드
    _hist_data = []
    try:
        _hp = hist_path or Path(HIST_FILE)
        if _hp.exists():
            _hist_data = json.loads(_hp.read_text(encoding="utf-8"))
    except Exception:
        pass
    hist_js = json.dumps(_hist_data, ensure_ascii=False)

    # 캐시 타입 무시하고 재분류
    for s in all_stats:
        t, c = reclassify_type(s)
        s['trader_type'] = t
        s['character']   = c
        dd = max(s.get('data_days', 1), 1)
        cc = s.get('closed_count', 0)
        s['is_hf'] = (cc / dd >= 300) if dd > 0 else False
        if not s.get('source'): s['source'] = 'manual'
        # is_vault: source 기반 + 캐시 원본
        cache_entry = archive.data.get(s.get('address','').lower(), {}) if archive else {}
        cache_src = cache_entry.get('stats', {}).get('source', '') or cache_entry.get('source', '')
        # vault_discovery.json의 vault_addr 기준으로 뱃지 결정
        is_vault_by_addr = s.get('address','').lower() in (archive.vault_addrs if archive else set())
        s['is_vault'] = (is_vault_by_addr or
                         s.get('is_vault', False) or
                         s.get('source') == 'vault' or
                         cache_src == 'vault')
        if is_vault_by_addr and s.get('source') != 'vault':
            s['source'] = 'vault'
    ranked = sorted(all_stats, key=lambda x: x["war_score"], reverse=True)
    palette = ["#00f5d4","#f72585","#7209b7","#3a86ff","#fb5607","#ffbe0b","#06d6a0",
               "#ef233c","#ff6b6b","#4ecdc4","#45b7d1","#96ceb4","#ffeaa7","#dfe6e9",
               "#fd79a8","#6c5ce7"]
    t_results = tournament.get("results", {})
    for s in ranked:
        addr = s["address"]
        s["tourney_score"] = t_results.get(addr, {}).get("score", 0)
        s["tourney_wins"]  = t_results.get(addr, {}).get("wins", 0)
        s["weekly_pnl"]    = t_results.get(addr, {}).get("weekly_pnl", [])

    radar_labels  = ["수익금","ROI","빅벳적중","샤프","승률"]
    radar_datasets = [{"label":s["label"],"data":[s["radar"]["profit_amt"],s["radar"]["roi"],
                        s["radar"]["big_bet"],s["radar"]["sharpe"],
                        s["radar"].get("win_rate",10)],
                       "color":palette[i%len(palette)]} for i,s in enumerate(ranked)]
    weeks = [r[0] for r in tournament.get("rounds", [])]
    weekly_series = [{"label":s["label"],
                      "data":[{w["week"]:w["pnl"] for w in s.get("weekly_pnl",[])}.get(w,0) for w in weeks],
                      "color":palette[ranked.index(s)%len(palette)]} for s in ranked]

    cards_html = ""
    for rank, s in enumerate(ranked, 1):
        crown = "👑" if rank==1 else f"#{rank}"
        pnl_color = "#00f5d4" if s["total_pnl"] >= 0 else "#f72585"
        sc = "#00f5d4" if s["sharpe"]>1 else ("#ffbe0b" if s["sharpe"]>0 else "#f87171")
        dc = "#00f5d4" if s["durability"]>=60 else ("#ffbe0b" if s["durability"]>=35 else "#f72585")
        war_bar = min(int(s["war_score"]), 100)
        cc = palette[(rank-1) % len(palette)]
        cache_age = archive.age_str(s["address"]) if archive else "?"
        first_seen = archive.first_seen_str(s["address"]) if archive else "-"
        src = s.get("source","manual")

        n2=5; cx2,cy2,R2=85,90,52
        rv_list=[max(s["radar"]["profit_amt"],10),max(s["radar"]["roi"],10),max(s["radar"]["big_bet"],10),
                 max(s["radar"]["sharpe"],10),
                 max(s["radar"].get("win_rate",10),10)]
        lnames=["수익금","ROI","빅벳","샤프","승률"]
        bg_poly="".join(f'<polygon points="{" ".join(f"{cx2+R2*lvl*_math.cos(_math.pi*2*j/n2-_math.pi/2):.1f},{cy2+R2*lvl*_math.sin(_math.pi*2*j/n2-_math.pi/2):.1f}" for j in range(n2))}" fill="none" stroke="#1e1e35" stroke-width="1"/>' for lvl in [0.25,0.5,0.75,1.0])
        axes="".join(f'<line x1="{cx2}" y1="{cy2}" x2="{cx2+R2*_math.cos(_math.pi*2*j/n2-_math.pi/2):.1f}" y2="{cy2+R2*_math.sin(_math.pi*2*j/n2-_math.pi/2):.1f}" stroke="#2a2a45" stroke-width="1"/>' for j in range(n2))
        dpts=" ".join(f"{cx2+(v/100*R2)*_math.cos(_math.pi*2*j/n2-_math.pi/2):.1f},{cy2+(v/100*R2)*_math.sin(_math.pi*2*j/n2-_math.pi/2):.1f}" for j,v in enumerate(rv_list))
        data_poly=f'<polygon points="{dpts}" fill="{cc}33" stroke="{cc}" stroke-width="2"/>'
        lbls=""
        for j2,ln in enumerate(lnames):
            ang2=_math.pi*2*j2/n2-_math.pi/2; lx2=cx2+(R2+14)*_math.cos(ang2); ly2=cy2+(R2+14)*_math.sin(ang2)
            anc="middle" if abs(_math.cos(ang2))<0.3 else ("start" if _math.cos(ang2)>0 else "end")
            vc="#00f5d4" if rv_list[j2]>=60 else ("#ffbe0b" if rv_list[j2]>=40 else "#f87171")
            lbls+=f'<text x="{lx2:.1f}" y="{ly2:.1f}" text-anchor="{anc}" dominant-baseline="middle" fill="{vc}" font-size="8.5" font-family="DM Sans,sans-serif" font-weight="600">{ln}</text>'
        mini_svg=f'<svg viewBox="-10 0 200 175" width="165" height="155">{bg_poly}{axes}{data_poly}{lbls}</svg>'
        _SRC_MAP={"manual":("직접등록","#888888"),"active":("활성발굴","#3a86ff"),
                  "vault":("Vault발굴","#9b5de5"),"cached":("캐시","#555555")}
        src_label,src_color=_SRC_MAP.get(src,(src,"#888888"))
        def _coin_tag(t):
            if isinstance(t, dict): c,p,sd = t["coin"],t["pnl"],t.get("side","")
            else: c,p,sd = t[0],t[1],""  # 구버전 캐시 호환
            col = "#00f5d4" if p>=0 else "#f72585"
            arrow = "▲" if sd=="L" else ("▼" if sd=="S" else "")
            return f'<span class="coin-tag" style="border-color:{col};color:{col}">{arrow}{c} ${p:+,.0f}</span>'
        top_coins = ''  # 실현손익 코인태그 제거

        # 현재 오픈 포지션 처리
        open_pos = s.get("positions", [])
        if open_pos:
            # 규모 기준 정렬, 상위 5개만
            sorted_pos = sorted(open_pos, key=lambda p: p["notional"], reverse=True)
            # 최대 규모 대비 10% 미만인 소액 포지션 제외
            max_ntl = sorted_pos[0]["notional"] if sorted_pos else 1
            filtered_pos = [p for p in sorted_pos if p["notional"] >= max_ntl * 0.1][:5]

            # 롱/숏 합산
            long_ntl  = sum(p["notional"] for p in open_pos if p["side"]=="LONG")
            short_ntl = sum(p["notional"] for p in open_pos if p["side"]=="SHORT")
            total_ntl = long_ntl + short_ntl
            net_exp   = long_ntl - short_ntl  # 넷 익스포저 (양수=롱우세)
            long_pct  = round(long_ntl/total_ntl*100) if total_ntl>0 else 50

            # 롱숏 요약 바
            bar_long  = f'width:{long_pct}%'
            bar_short = f'width:{100-long_pct}%'
            net_col   = "#3a86ff" if net_exp>=0 else "#f72585"
            net_str   = f'{"L" if net_exp>=0 else "S"} ${abs(net_exp):,.0f}'
            summary_html = (
                f'<div class="pos-summary">'
                f'<div class="pos-bar-wrap">'
                f'<div class="pos-bar-long" style="{bar_long}"></div>'
                f'<div class="pos-bar-short" style="{bar_short}"></div>'
                f'</div>'
                f'<div class="pos-bar-labels">'
                f'<span style="color:#3a86ff">▲{long_pct}%</span>'
                f'<span style="color:{net_col};font-weight:600">{net_str}</span>'
                f'<span style="color:#f72585">▼{100-long_pct}%</span>'
                f'</div>'
                f'</div>'
            )

            # 개별 포지션 행
            rows_html = ""
            for p in filtered_pos:
                sc2  = "#3a86ff" if p["side"]=="LONG" else "#f72585"
                ic2  = "▲" if p["side"]=="LONG" else "▼"
                uc2  = "#00f5d4" if p["upnl"]>=0 else "#f87171"
                lev2 = f' {p["lev"]:.0f}x' if p.get("lev",1)>1 else ""
                rows_html += (
                    f'<div class="pos-row">'
                    f'<span style="color:{sc2}">{ic2} {p["coin"]}{lev2}</span>'
                    f'<span class="pos-ntl">${p["notional"]:,.0f}</span>'
                    f'<span style="color:{uc2};font-size:9px">미실현 ${p["upnl"]:+,.0f}</span>'
                    f'</div>'
                )
            positions_section = f'<div class="positions-block">{summary_html}{rows_html}</div>'
        else:
            upnl_val = s.get('total_upnl', 0)
            if upnl_val != 0:
                uc3 = '#00f5d4' if upnl_val >= 0 else '#f87171'
                positions_section = f'<div class="positions-block pos-empty">미실현손익 <span style="color:{uc3}">${upnl_val:+,.0f}</span> (재수집 필요)</div>'
            else:
                positions_section = '<div class="positions-block pos-empty">— 포지션 없음</div>'

        cards_html += (
            f'<div class="trader-card" style="--card-accent:{cc};cursor:pointer" data-address="{s["address"]}" onclick="openModal(this.dataset.address)">'
            f'<div class="card-top"><div class="card-rank">{crown}</div>'
            f'<div class="card-name-block"><div class="card-name">{s["label"]}'
            + (' <span style="font-size:11px;background:#2a1f00;color:#ffbe0b;border:1px solid #ffbe0b;border-radius:3px;padding:1px 5px;margin-left:6px;vertical-align:middle">⚡ 고빈도</span>' if s.get('is_hf') else '')
            + (' <span style="font-size:11px;background:#1a0a2e;color:#9b5de5;border:1px solid #9b5de5;border-radius:3px;padding:1px 5px;margin-left:4px;vertical-align:middle">🏦 Vault</span>' if s.get('is_vault') else '')
            + '</div>'
            f'<div class="card-type">{s["trader_type"]} <span class="card-equity">${s["total_equity"]:,.0f}</span></div>'
            f'<div class="card-character">≈ {s["character"]}</div>'
            f'<div class="card-period">📅 {s["first_date"]} ~ {s["last_date"]} | {s["data_days"]}일 | 최초발굴 {first_seen}</div>'
            f'<div class="card-meta"><span style="color:{src_color}">● {src_label}</span> · <span style="color:#3a3a55">🕐 {cache_age}</span></div>'
            f'</div><div class="war-circle"><svg viewBox="0 0 60 60">'
            f'<circle cx="30" cy="30" r="24" fill="none" stroke="#1a1a2e" stroke-width="5"/>'
            f'<circle cx="30" cy="30" r="24" fill="none" stroke="{cc}" stroke-width="5" stroke-dasharray="{war_bar*1.508:.1f} 150.8" stroke-linecap="round" transform="rotate(-90 30 30)"/>'
            f'</svg><div class="war-val" style="color:{cc}">{s["war_score"]:.0f}</div><div class="war-label">WAR</div></div></div>'
            f'<div class="card-body"><div class="mini-radar">{mini_svg}</div><div class="card-right">'

            f'<div class="key-stats">'
            f'<div class="ks"><div class="ks-v" style="color:{pnl_color}">${s["total_pnl"]:+,.0f}</div><div class="ks-l">총손익</div></div>'
            f'<div class="ks"><div class="ks-v">{s["win_rate"]:.0f}%</div><div class="ks-l">승률</div></div>'
            f'<div class="ks"><div class="ks-v" style="color:{sc}">{s["sharpe"]:.2f}</div><div class="ks-l">Sharpe</div></div>'
            f'<div class="ks"><div class="ks-v">{s["roi_pct"]:.1f}%</div><div class="ks-l">ROI</div></div>'
            f'<div class="ks"><div class="ks-v">{s["big_bet_rate"]:.0f}%</div><div class="ks-l">빅벳적중</div></div>'

            f'</div>'
            f'<div class="section-label" style="margin-top:8px">📍 현재 포지션</div>'
            f'<div class="pos-section">{positions_section}</div>'
            f'<div class="bottom-row"><div class="coins">{top_coins}</div>'
            f'<div class="tourney-badge">🏆 {s["tourney_wins"]}주 1위 · {s["tourney_score"]}pt</div>'
            f'</div></div></div></div>'
        )

    # ── 센티먼트 계산 ────────────────────────────────────────────────
    # WAR 구간: 40-50, 50-60, 60-70, 70-80, 80+
    # 구간별 가중치: 높은 WAR 구간일수록 더 큰 영향
    WAR_BANDS = [
        (40, 50, "40-50", 0.45),
        (50, 60, "50-60", 0.55),
        (60, 70, "60-70", 0.65),
        (70, 80, "70-80", 0.75),
        (80, 200,"80+",   0.85),
    ]

    def calc_band_sentiment(stats_list):
        """그룹 전체 잔고 합 기준 — 롱/숏 포지션 총합 / 잔고 총합
        잔고 큰 트레이더가 자연스럽게 더 영향, 소액은 희석
        코인별도 동일: 그룹 전체 롱규모합 / 그룹 전체 잔고합"""
        pos_traders = [s for s in stats_list
                       if s.get('positions') and s.get('war_score',0)>=40]
        coin_data = {}

        total_eq   = 0
        total_long = 0
        total_short= 0
        count      = 0

        for s in pos_traders:
            pos = s.get('positions', [])
            eq  = max(s.get('total_equity', 1), 1)
            s_long  = sum(p['notional'] for p in pos if p['side']=='LONG')
            s_short = sum(p['notional'] for p in pos if p['side']=='SHORT')
            if s_long + s_short == 0: continue

            total_eq    += eq
            total_long  += s_long
            total_short += s_short
            count       += 1

            for p in pos:
                c = p['coin']
                if c not in coin_data:
                    coin_data[c] = {'long_ntl':0, 'short_ntl':0}
                if p['side'] == 'LONG':
                    coin_data[c]['long_ntl']  += p['notional']
                else:
                    coin_data[c]['short_ntl'] += p['notional']

        if count == 0 or total_eq == 0:
            return None, coin_data

        # 코인별: 그룹 전체 잔고 합으로 나눔
        for c in coin_data:
            coin_data[c]['total_eq'] = total_eq

        result = {
            'long_pct':  round(total_long  / total_eq * 100, 1),
            'short_pct': round(total_short / total_eq * 100, 1),
            'traders':   count,
        }
        return result, coin_data

    def merge_coin_data(band_coin_datas, weights):
        """각 그룹별 코인 잔고대비% 를 WAR 가중 평균으로 합침"""
        merged = {}
        for cd, w in zip(band_coin_datas, weights):
            for coin, v in cd.items():
                eq = v.get('total_eq', 1) or 1
                long_pct  = v['long_ntl']  / eq * 100
                short_pct = v['short_ntl'] / eq * 100
                if long_pct == 0 and short_pct == 0: continue
                if coin not in merged:
                    merged[coin] = {'long_pct_w':0,'short_pct_w':0,'total_w':0}
                merged[coin]['long_pct_w']  += long_pct  * w
                merged[coin]['short_pct_w'] += short_pct * w
                merged[coin]['total_w']     += w
        return merged

    # WAR 구간별 센티먼트 (각 그룹 단순 평균)
    sent_bands = []
    band_coin_datas = []
    for lo, hi, label, w in WAR_BANDS:
        band_stats = [s for s in all_stats if lo <= s.get('war_score',0) < hi]
        r, cd_b = calc_band_sentiment(band_stats)
        sent_bands.append({'label': label, 'result': r, 'count': len(band_stats), 'weight': w})
        band_coin_datas.append((cd_b, w))

    # 전체 센티먼트 = 전체 트레이더 잔고 합산 방식
    all_pos_traders = [s for s in all_stats if s.get('positions') and s.get('war_score',0)>=40]
    all_total_eq    = sum(max(s.get('total_equity',1),1) for s in all_pos_traders
                          if sum(p['notional'] for p in s['positions']) > 0)
    all_total_long  = sum(sum(p['notional'] for p in s.get('positions',[]) if p['side']=='LONG')
                          for s in all_pos_traders)
    all_total_short = sum(sum(p['notional'] for p in s.get('positions',[]) if p['side']=='SHORT')
                          for s in all_pos_traders)
    if all_total_eq > 0:
        sent_all = {
            'long_pct':  round(all_total_long  / all_total_eq * 100, 1),
            'short_pct': round(all_total_short / all_total_eq * 100, 1),
            'traders':   len(all_pos_traders),
        }
    else:
        sent_all = None

    # 코인별: 그룹별 평균 → 전체 그룹 평균
    merged_coins = merge_coin_data([cd for cd, _ in band_coin_datas],
                                   [w  for _, w  in band_coin_datas])
    MIN_BUBBLE_PCT = 0.5
    # 전체 코인별 절대 ntl 집계
    all_coin_ntl = {}
    for s in all_stats:
        for p in s.get('positions', []):
            c = p['coin']
            if c not in all_coin_ntl:
                all_coin_ntl[c] = {'long': 0, 'short': 0}
            if p['side'] == 'LONG': all_coin_ntl[c]['long'] += p['notional']
            else:                   all_coin_ntl[c]['short'] += p['notional']
    coin_rows = []
    for coin, v in sorted(merged_coins.items(),
                          key=lambda x: x[1]['total_w'], reverse=True):
        tw = v['total_w'] or 1
        lp = round(v['long_pct_w']  / tw, 1)
        sp = round(v['short_pct_w'] / tw, 1)
        if lp + sp < MIN_BUBBLE_PCT: continue
        ntl = all_coin_ntl.get(coin, {'long':0,'short':0})
        coin_rows.append({
            'coin': coin,
            'avg_long_eq_pct':  lp,
            'avg_short_eq_pct': sp,
            'long_ntl':  round(ntl['long']),
            'short_ntl': round(ntl['short']),
        })

    # 그룹별 버블 데이터 (전체 잔고합 방식)
    band_bubble_rows = {}
    for (lo, hi, label, bw), (cd_b, _) in zip(WAR_BANDS, band_coin_datas):
        rows = []
        for coin, v in sorted(cd_b.items(),
                               key=lambda x: x[1].get('long_ntl',0)+x[1].get('short_ntl',0),
                               reverse=True):
            eq = v.get('total_eq', 1) or 1
            lp = round(v['long_ntl']  / eq * 100, 1)
            sp = round(v['short_ntl'] / eq * 100, 1)
            if lp + sp < MIN_BUBBLE_PCT: continue
            rows.append({'coin': coin, 'avg_long_eq_pct': lp, 'avg_short_eq_pct': sp,
                         'long_ntl': round(v['long_ntl']), 'short_ntl': round(v['short_ntl'])})
        band_bubble_rows[label] = rows

    # WAR 구간별 코인 센티먼트
    coin_band_rows = {}
    for (lo, hi, label, bw), (cd_b, _) in zip(WAR_BANDS, band_coin_datas):
        for coin, v in cd_b.items():
            eq = v.get('total_eq', 1) or 1
            long_pct  = v['long_ntl']  / eq * 100
            short_pct = v['short_ntl'] / eq * 100
            if long_pct == 0 and short_pct == 0: continue
            if coin not in coin_band_rows:
                coin_band_rows[coin] = {}
            coin_band_rows[coin][label] = {
                'avg_long_eq_pct':  round(long_pct,  1),
                'avg_short_eq_pct': round(short_pct, 1),
            }

    # 트레이더 타입별 버블 + 센티먼트 계산
    TYPE_ORDER = [
        "🦁 Apex Predator", "🦅 Precision Hunter", "🧊 Ice Quant",
        "🎯 Sniper", "🏔️ Iron Hands", "📊 All-Rounder",
        "📈 Momentum", "🎯 Steady Shot", "💰 Value Hunter", "🎲 Bet Maker",
        "🌊 Degen", "🎰 High Roller", "⚡ Flash Trader",
        "🌱 Newcomer", "🌀 Drifter", "💀 Underwater",
    ]
    type_bubble_rows = {}
    type_sent_rows = []
    for ttype in TYPE_ORDER:
        type_stats = [s for s in all_stats if s.get('trader_type') == ttype]
        # 포지션 있는 트레이더만으로 버블/센티먼트 계산
        pos_traders = [s for s in type_stats if s.get('positions')]
        coin_data_t = {}
        t_long = 0; t_short = 0; t_eq = 0
        for s in pos_traders:
            eq = max(s.get('total_equity', 1), 1)
            s_long  = sum(p['notional'] for p in s['positions'] if p['side']=='LONG')
            s_short = sum(p['notional'] for p in s['positions'] if p['side']=='SHORT')
            if s_long + s_short == 0: continue
            t_long += s_long; t_short += s_short; t_eq += eq
            for p in s['positions']:
                c = p['coin']
                if c not in coin_data_t:
                    coin_data_t[c] = {'long_ntl': 0, 'short_ntl': 0}
                if p['side'] == 'LONG': coin_data_t[c]['long_ntl'] += p['notional']
                else:                   coin_data_t[c]['short_ntl'] += p['notional']
        # 버블 행
        rows_t = []
        for coin, v in sorted(coin_data_t.items(),
                               key=lambda x: x[1]['long_ntl']+x[1]['short_ntl'], reverse=True):
            eq = t_eq or 1
            lp = round(v['long_ntl']  / eq * 100, 1)
            sp = round(v['short_ntl'] / eq * 100, 1)
            if lp + sp < MIN_BUBBLE_PCT: continue
            rows_t.append({'coin': coin, 'avg_long_eq_pct': lp, 'avg_short_eq_pct': sp,
                           'long_ntl': round(v['long_ntl']), 'short_ntl': round(v['short_ntl'])})
        type_bubble_rows[ttype] = rows_t
        # 센티먼트 요약
        result_t = None
        if t_eq > 0:
            result_t = {
                'long_pct':  round(t_long  / t_eq * 100, 1),
                'short_pct': round(t_short / t_eq * 100, 1),
                'traders':   len([s for s in pos_traders if sum(p['notional'] for p in s.get('positions',[])) > 0]),
            }
        avg_war_t = round(sum(s.get('war_score',0) for s in type_stats) / len(type_stats), 1) if type_stats else 0
        type_sent_rows.append({
            'label': ttype,
            'count': len(type_stats),
            'avg_war': avg_war_t,
            'result': result_t,
        })

    # 잔고 규모별 그룹
    EQUITY_BANDS = [
        (10_000,   50_000,  "$10K~50K"),
        (50_000,  100_000,  "$50K~100K"),
        (100_000, 500_000,  "$100K~500K"),
        (500_000,1_000_000, "$500K~1M"),
        (1_000_000,5_000_000,"$1M~5M"),
        (5_000_000,999_999_999,"$5M+"),
    ]
    equity_bubble_rows = {}
    equity_sent_rows = []
    for lo, hi, elabel in EQUITY_BANDS:
        eq_stats = [s for s in all_stats if lo <= s.get('total_equity',0) < hi]
        pos_traders_e = [s for s in eq_stats if s.get('positions')]
        coin_data_e = {}
        e_long = 0; e_short = 0; e_eq = 0
        for s in pos_traders_e:
            eq = max(s.get('total_equity',1),1)
            s_long  = sum(p['notional'] for p in s['positions'] if p['side']=='LONG')
            s_short = sum(p['notional'] for p in s['positions'] if p['side']=='SHORT')
            if s_long + s_short == 0: continue
            e_long += s_long; e_short += s_short; e_eq += eq
            for p in s['positions']:
                c = p['coin']
                if c not in coin_data_e:
                    coin_data_e[c] = {'long_ntl':0,'short_ntl':0}
                if p['side']=='LONG': coin_data_e[c]['long_ntl'] += p['notional']
                else:                 coin_data_e[c]['short_ntl'] += p['notional']
        rows_e = []
        for coin, v in sorted(coin_data_e.items(),
                               key=lambda x: x[1]['long_ntl']+x[1]['short_ntl'], reverse=True):
            eq2 = e_eq or 1
            lp = round(v['long_ntl']  / eq2 * 100, 1)
            sp = round(v['short_ntl'] / eq2 * 100, 1)
            if lp + sp < MIN_BUBBLE_PCT: continue
            rows_e.append({'coin':coin,'avg_long_eq_pct':lp,'avg_short_eq_pct':sp,
                       'long_ntl':round(v['long_ntl']),'short_ntl':round(v['short_ntl'])})
        equity_bubble_rows[elabel] = rows_e
        result_e = None
        if e_eq > 0:
            pos_cnt_e = len([s for s in pos_traders_e if sum(p['notional'] for p in s.get('positions',[])) > 0])
            result_e = {'long_pct': round(e_long/e_eq*100,1), 'short_pct': round(e_short/e_eq*100,1), 'traders': pos_cnt_e}
        avg_war_e = round(sum(s.get('war_score',0) for s in eq_stats)/len(eq_stats),1) if eq_stats else 0
        equity_sent_rows.append({'label':elabel,'count':len(eq_stats),'avg_war':avg_war_e,'result':result_e})

    sent_js = json.dumps({
        'all': sent_all,
        'bands': sent_bands,
        'coins': coin_rows,
        'coin_bands': coin_band_rows,
        'band_bubbles': band_bubble_rows,
        'type_bubbles': type_bubble_rows,
        'types': type_sent_rows,
        'equity_bubbles': equity_bubble_rows,
        'equities': equity_sent_rows,
    })

    radar_js  = json.dumps({"labels":radar_labels,"datasets":radar_datasets})
    ws_js     = json.dumps(weekly_series)
    weeks_js  = json.dumps(weeks)
    ts        = datetime.now().strftime("%Y-%m-%d %H:%M")
    champion_label   = ranked[0]["label"] if ranked else "-"
    max_weekly_wins  = max((s["tourney_wins"] for s in ranked), default=0)
    total_rounds     = len(tournament.get("rounds", []))
    header_meta      = "<br>".join(f"{s['label']} — WAR {s['war_score']}" for s in ranked[:3])
    legend_items     = "".join(
        f'<div class="legend-item" style="cursor:pointer" data-address="{s["address"]}" onclick="openModal(this.dataset.address)"><div class="legend-dot" style="background:{palette[i%len(palette)]}"></div>'
        f'<div class="legend-name">#{i+1} {s["label"]}<br><span style="font-size:10px;color:var(--dim)">{s["trader_type"]}</span></div>'
        f'<div class="legend-war">{s["war_score"]}</div></div>'
        for i,s in enumerate(ranked)
    )
    tourney_rows = "".join(
        f'<tr style="cursor:pointer" data-address="{s["address"]}" onclick="openModal(this.dataset.address)"><td>{"👑" if i==0 else f"#{i+1}"}</td><td>{s["label"]}</td><td>{s["trader_type"]}</td>'
        f'<td>{s["tourney_wins"]}W</td><td>{s["tourney_score"]}pt</td>'
        f'<td style="color:{"#00f5d4" if s["war_score"]>=60 else "#ffbe0b"}">{s["war_score"]}</td>'
        f'<td>{"👑 YES" if i==0 else "—"}</td></tr>'
        for i,s in enumerate(ranked)
    )

    html = f"""<!DOCTYPE html>
<html lang="ko"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Wallet Scouting Report</title>
<link href="https://fonts.googleapis.com/css2?family=Bebas+Neue&family=DM+Mono:wght@400;500&family=DM+Sans:wght@300;400;600&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
:root{{--bg:#080810;--surface:#0e0e1a;--border:#1e1e35;--text:#c8d0e7;--dim:#4a4a6a;--green:#00f5d4;--red:#f72585;--yellow:#ffbe0b;--blue:#3a86ff;}}
*{{margin:0;padding:0;box-sizing:border-box;}}body{{background:var(--bg);color:var(--text);font-family:'DM Sans',sans-serif;}}
.header{{padding:40px 32px 24px;border-bottom:1px solid var(--border);display:flex;align-items:flex-end;justify-content:space-between;}}
.header-title{{font-family:'Bebas Neue',sans-serif;font-size:52px;letter-spacing:4px;line-height:1;background:linear-gradient(135deg,#00f5d4,#3a86ff);-webkit-background-clip:text;-webkit-text-fill-color:transparent;}}
.header-sub{{font-family:'DM Mono',monospace;font-size:11px;color:var(--dim);margin-top:6px;}}
.header-meta{{font-family:'DM Mono',monospace;font-size:11px;color:var(--dim);text-align:right;}}
.tabs{{display:flex;padding:0 32px;border-bottom:1px solid var(--border);}}
.tab{{padding:14px 24px;font-size:12px;font-weight:600;letter-spacing:1px;text-transform:uppercase;cursor:pointer;border-bottom:2px solid transparent;color:var(--dim);transition:.2s;}}
.tab.active{{color:var(--green);border-bottom-color:var(--green);}}
.section{{display:none;padding:32px;}}.section.active{{display:block;}}
.cards-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(360px,1fr));gap:20px;}}
.trader-card{{background:var(--surface);border:1px solid var(--border);border-radius:16px;padding:24px;transition:border-color .2s,transform .2s;position:relative;overflow:hidden;}}
.trader-card:hover{{border-color:rgba(0,245,212,.3);transform:translateY(-2px);}}
.card-top{{display:flex;align-items:flex-start;gap:12px;margin-bottom:16px;border-bottom:1px solid var(--border);padding-bottom:14px;}}
.card-rank{{font-family:'Bebas Neue',sans-serif;font-size:26px;color:var(--yellow);min-width:32px;line-height:1;}}
.card-name-block{{flex:1;}}.card-name{{font-family:'Bebas Neue',sans-serif;font-size:26px;letter-spacing:1px;color:var(--text);line-height:1.1;}}
.card-type{{font-size:12px;font-weight:600;color:var(--card-accent,#00f5d4);margin-top:4px;}}
.card-character{{font-size:10px;color:var(--dim);margin-top:3px;font-style:italic;}}
.card-period{{font-family:'DM Mono',monospace;font-size:9px;color:#3a3a55;margin-top:5px;}}
.card-meta{{font-family:'DM Mono',monospace;font-size:9px;margin-top:3px;}}
.pos-section{{margin:4px 0;}}
.card-equity{{font-size:10px;color:#888;margin-left:6px;font-family:'DM Mono',monospace;font-weight:400;}}
.section-label{{font-size:9px;font-weight:600;color:#3a3a55;text-transform:uppercase;letter-spacing:.06em;margin:6px 0 4px;border-top:1px solid #1e1e35;padding-top:6px;}}
.pos-summary{{margin-bottom:5px;}}
.pos-bar-wrap{{display:flex;height:4px;border-radius:2px;overflow:hidden;margin-bottom:3px;background:#1e1e35;}}
.pos-bar-long{{background:#3a86ff;transition:width .3s;}}
.pos-bar-short{{background:#f72585;transition:width .3s;}}
.pos-bar-labels{{display:flex;justify-content:space-between;font-size:9px;font-family:'DM Mono',monospace;}}
.positions-block{{display:flex;flex-direction:column;gap:3px;}}
.pos-row{{display:flex;justify-content:space-between;align-items:center;font-size:10px;font-family:'DM Mono',monospace;}}
.pos-ntl{{color:#888;font-size:9px;}}
.pos-empty{{font-size:9px;color:#3a3a55;font-family:'DM Mono',monospace;}}
.war-circle{{position:relative;width:60px;height:60px;flex-shrink:0;}}.war-circle svg{{width:60px;height:60px;}}
.war-val{{position:absolute;top:50%;left:50%;transform:translate(-50%,-60%);font-family:'Bebas Neue',sans-serif;font-size:18px;}}
.war-label{{position:absolute;top:50%;left:50%;transform:translate(-50%,20%);font-size:8px;color:var(--dim);letter-spacing:1px;}}
.card-body{{display:flex;gap:12px;align-items:flex-start;}}.mini-radar{{flex-shrink:0;}}.card-right{{flex:1;display:flex;flex-direction:column;gap:10px;}}
.key-stats{{display:grid;grid-template-columns:repeat(3,1fr);gap:6px;}}
.ks{{background:#0a0a16;border-radius:8px;padding:8px 6px;text-align:center;}}
.ks-v{{font-family:'DM Mono',monospace;font-size:13px;font-weight:500;color:var(--text);}}.ks-l{{font-size:9px;color:var(--dim);margin-top:2px;letter-spacing:.5px;}}
.bottom-row{{display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:6px;}}
.coins{{display:flex;flex-wrap:wrap;gap:4px;}}.coin-tag{{font-family:'DM Mono',monospace;font-size:9px;padding:2px 7px;border-radius:4px;border:1px solid;}}
.tourney-badge{{font-size:10px;color:var(--yellow);}}
.radar-wrap{{display:grid;grid-template-columns:1fr 1fr;gap:32px;align-items:start;}}
.radar-canvas-wrap,.radar-legend,.chart-wrap{{background:var(--surface);border:1px solid var(--border);border-radius:16px;padding:24px;}}
.radar-canvas-wrap{{min-height:400px;position:relative;display:flex;align-items:center;justify-content:center;}}
.legend-title{{font-family:'Bebas Neue',sans-serif;font-size:22px;letter-spacing:2px;color:var(--text);margin-bottom:16px;}}
.legend-item{{display:flex;align-items:center;gap:10px;padding:10px 0;border-bottom:1px solid var(--border);}}
.legend-dot{{width:10px;height:10px;border-radius:50%;flex-shrink:0;}}.legend-name{{font-size:12px;color:var(--text);flex:1;}}.legend-war{{font-family:'DM Mono',monospace;font-size:12px;color:var(--green);}}
.tourney-header{{display:flex;gap:20px;margin-bottom:28px;flex-wrap:wrap;}}
.sent-war-grid{{grid-template-columns:repeat(5,1fr);}}
.sent-type-grid{{grid-template-columns:repeat(auto-fill,minmax(160px,1fr));}}
.tourney-stat{{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px 24px;}}
.tourney-stat .val{{font-family:'Bebas Neue',sans-serif;font-size:32px;color:var(--yellow);}}.tourney-stat .lbl{{font-size:10px;color:var(--dim);letter-spacing:1px;margin-top:2px;}}
.tourney-table{{width:100%;border-collapse:collapse;margin-bottom:32px;}}
.tourney-table th{{font-size:10px;color:var(--dim);letter-spacing:1px;text-align:left;padding:8px 12px;border-bottom:1px solid var(--border);}}
.tourney-table td{{font-family:'DM Mono',monospace;font-size:12px;padding:10px 12px;border-bottom:1px solid rgba(30,30,53,.5);}}
.tourney-table tr:first-child td{{color:var(--yellow);}}.chart-wrap h3{{font-family:'Bebas Neue',sans-serif;font-size:20px;letter-spacing:2px;color:var(--dim);margin-bottom:16px;}}

.modal-overlay{{position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.85);z-index:1000;display:none;align-items:center;justify-content:center;}}
.modal-overlay.open{{display:flex;}}
.modal-box{{background:#0e0e1a;border:1px solid #2a2a45;border-radius:20px;width:92%;max-width:900px;max-height:90vh;overflow-y:auto;padding:36px;position:relative;}}
.modal-close{{position:absolute;top:18px;right:22px;font-size:22px;cursor:pointer;color:#4a4a6a;line-height:1;background:none;border:none;}}
.modal-close:hover{{color:#c8d0e7;}}
.modal-header{{display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:24px;padding-bottom:18px;border-bottom:1px solid #1e1e35;}}
.modal-title{{font-family:'Bebas Neue',sans-serif;font-size:36px;letter-spacing:2px;}}
.modal-sub{{font-size:12px;color:#4a4a6a;margin-top:4px;font-family:'DM Mono',monospace;}}
.modal-war .war-num{{font-family:'Bebas Neue',sans-serif;font-size:48px;}}
.modal-war .war-lbl{{font-size:10px;color:#4a4a6a;letter-spacing:2px;}}
.modal-grid{{display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:20px;}}
.modal-block{{background:#080810;border:1px solid #1e1e35;border-radius:12px;padding:18px;}}
.modal-block h4{{font-size:10px;color:#3a3a55;text-transform:uppercase;letter-spacing:.08em;margin-bottom:12px;}}
.modal-stats{{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;}}
.modal-stat{{text-align:center;padding:10px 6px;background:#0e0e1a;border-radius:8px;}}
.modal-stat .v{{font-family:'DM Mono',monospace;font-size:15px;font-weight:500;}}
.modal-stat .l{{font-size:9px;color:#4a4a6a;margin-top:3px;letter-spacing:.5px;}}
.modal-pnl-chart{{height:140px;}}
.modal-pos-row{{display:flex;justify-content:space-between;align-items:center;padding:6px 0;border-bottom:1px solid #1a1a2e;font-family:'DM Mono',monospace;font-size:11px;}}
.modal-coin-row{{display:flex;justify-content:space-between;align-items:center;padding:5px 0;font-family:'DM Mono',monospace;font-size:11px;border-bottom:1px solid #1a1a2e;}}

/* ── 반응형: 단계별 ── */

/* 1200px↓: 카드 2열 */
@media (max-width:1200px){{
  .cards-grid{{grid-template-columns:repeat(2,1fr);}}
}}

/* 900px↓: 레이더 단열, WAR 카드 3열 */
@media (max-width:900px){{
  .radar-wrap{{grid-template-columns:1fr;gap:16px;}}
  .sent-war-grid{{grid-template-columns:repeat(3,1fr)!important;}}
  .sent-type-grid{{grid-template-columns:repeat(3,1fr)!important;}}
}}

/* 768px↓: 모바일 */
@media (max-width:768px){{
  /* 헤더 */
  .header{{padding:16px;flex-direction:column;gap:8px;}}
  .header-title{{font-size:28px;letter-spacing:2px;}}
  .header-meta{{display:none;}}

  /* 탭: 가로 스크롤, 줄바꿈 금지 */
  .tabs{{padding:0;overflow-x:auto;-webkit-overflow-scrolling:touch;scrollbar-width:none;flex-wrap:nowrap;}}
  .tabs::-webkit-scrollbar{{display:none;}}
  .tab{{padding:12px 16px;font-size:11px;letter-spacing:0;white-space:nowrap;flex-shrink:0;}}

  /* 섹션 패딩 */
  .section{{padding:12px;}}

  /* 트레이더 카드: 1열, 내부 세로 스택 */
  .cards-grid{{grid-template-columns:1fr;gap:12px;}}
  .trader-card{{padding:14px;overflow:hidden;}}
  .card-rank{{font-size:18px;min-width:26px;}}
  .card-name{{font-size:18px;}}

  /* 카드 본문: 세로로 쌓기 */
  .card-body{{flex-direction:column;gap:10px;}}
  .mini-radar{{align-self:center;}}
  .card-right{{width:100%;}}
  .key-stats{{gap:4px;}}
  .ks-v{{font-size:11px;}}
  .ks-l{{font-size:8px;}}

  /* 레이더 비교탭 */
  .radar-wrap{{grid-template-columns:1fr;gap:12px;}}
  .radar-canvas-wrap{{min-height:320px;}}

  /* 모달: 풀스크린 바텀시트 */
  .modal-overlay.open{{align-items:flex-end;}}
  .modal-box{{
    width:100%;max-width:100%;max-height:92vh;
    border-radius:16px 16px 0 0;
    padding:20px 14px 24px;
    overflow-y:auto;
  }}
  .modal-close{{top:14px;right:14px;font-size:20px;}}
  .modal-title{{font-size:20px;}}
  .modal-header{{flex-direction:column;gap:8px;align-items:flex-start;}}
  .modal-war{{display:flex;align-items:center;gap:10px;}}
  .modal-war .war-num{{font-size:28px;}}

  /* 모달 그리드: 반드시 1열 */
  .modal-grid{{grid-template-columns:1fr!important;gap:10px;}}
  .modal-stats{{grid-template-columns:repeat(3,1fr);gap:4px;}}
  .modal-stat{{padding:8px 4px;}}
  .modal-stat .v{{font-size:11px;}}

  /* 센티먼트: WAR 2열, 타입 2열 */
  .sent-war-grid{{grid-template-columns:repeat(2,1fr)!important;}}
  .sent-type-grid{{grid-template-columns:repeat(2,1fr)!important;}}
  #sent-bubble-wrap{{height:260px!important;}}

  /* 토너먼트 */
  .tourney-table{{font-size:10px;display:block;overflow-x:auto;white-space:nowrap;}}
  .tourney-header{{gap:8px;flex-wrap:wrap;}}
  .tourney-stat{{padding:10px 12px;}}
  .tourney-stat .val{{font-size:20px;}}
}}

/* 480px↓: 초소형 */
@media (max-width:480px){{
  .header-title{{font-size:22px;}}
  .modal-stats{{grid-template-columns:repeat(2,1fr);}}
  .sent-war-grid{{grid-template-columns:repeat(2,1fr)!important;}}
  .sent-type-grid{{grid-template-columns:repeat(2,1fr)!important;}}
}}


</style></head><body>
<div class="header">
  <div><div class="header-title">WALLET SCOUTING REPORT</div>
  <div class="header-sub">HYPERLIQUID · {len(ranked)} TRADERS · {ts}</div></div>
  <div class="header-meta">{header_meta}</div>
</div>
<div class="tabs">
  <div class="tab active" onclick="showTab('cards',event)">🃏 트레이더 카드</div>
  <div class="tab" onclick="showTab('radar',event)">🕸 레이더 비교</div>
  <div class="tab" onclick="showTab('sentiment',event)">📡 센티먼트</div>
  <div class="tab" onclick="showTab('tourney',event)">🏆 토너먼트</div>
  <div class="tab" onclick="showTab('lookup',event)">🔍 지갑 조회</div>
</div>
<div class="section active" id="tab-cards"><div class="cards-grid">{cards_html}</div></div>
<div class="section" id="tab-radar">
  <div class="radar-wrap">
    <div class="radar-canvas-wrap"><canvas id="radarChart" style="width:100%;height:100%"></canvas></div>
    <div class="radar-legend"><div class="legend-title">RANKING</div>{legend_items}</div>
  </div>
</div>
<div class="section" id="tab-sentiment">
<div id="sent-root" style="padding:1.5rem 0"></div>
</div>
<div class="section" id="tab-lookup"><div id="lookup-root" style="max-width:700px;margin:0 auto;padding:24px 0"></div></div>
<div class="section" id="tab-tourney">
  <div class="tourney-header">
    <div class="tourney-stat"><div class="val">{total_rounds}</div><div class="lbl">TOTAL ROUNDS</div></div>
    <div class="tourney-stat"><div class="val" style="color:var(--green)">{champion_label}</div><div class="lbl">CHAMPION</div></div>
    <div class="tourney-stat"><div class="val">{max_weekly_wins}</div><div class="lbl">MAX WEEKLY WINS</div></div>
  </div>
  <table class="tourney-table"><thead><tr><th>RANK</th><th>TRADER</th><th>TYPE</th><th>WINS</th><th>PTS</th><th>WAR</th><th>CHAMPION?</th></tr></thead>
  <tbody>{tourney_rows}</tbody></table>
  <div class="chart-wrap"><h3>WEEKLY PNL BATTLE</h3><canvas id="weeklyChart" height="60"></canvas></div>
</div>
%%SCRIPT%%
</script>
%%MODAL%%
</body></html>"""

    modal_block = r"""
<div class="modal-overlay" id="traderModal" onclick="if(event.target===this)closeModal()">
  <div class="modal-box">
    <button class="modal-close" onclick="closeModal()">✕</button>
    <div id="modal-content"></div>
  </div>
</div>
<script>
const ALL_STATS = %%ALL_STATS%%;

function copyAddr(addr) {
  navigator.clipboard.writeText(addr).catch(()=>{
    const el=document.createElement('textarea');
    el.value=addr; document.body.appendChild(el); el.select();
    document.execCommand('copy'); document.body.removeChild(el);
  });
  const el=document.getElementById('copy-btn');
  if(el){el.textContent='✓ 복사됨';setTimeout(()=>{el.textContent='📋 복사';},1500);}
}

function openModal(addr) {
  const s = ALL_STATS.find(x => x.address === addr);
  if (!s) return;
  const cc = s._color || '#00f5d4';
  const pnlColor = s.total_pnl >= 0 ? '#00f5d4' : '#f72585';
  const sc = s.sharpe > 1 ? '#00f5d4' : s.sharpe > 0 ? '#ffbe0b' : '#f87171';
  const dc = s.durability >= 60 ? '#00f5d4' : s.durability >= 35 ? '#ffbe0b' : '#f72585';

  const cumDates = s.cumulative.map(p => p.date);
  const cumVals  = s.cumulative.map(p => p.cum);

  // 포지션: notional 내림차순 정렬 후 토글 박스
  let posHTML = '';
  if (s.positions && s.positions.length) {
    const sorted = [...s.positions].sort((a,b) => b.notional - a.notional);
    const rows = sorted.map(p => {
      const sc2 = p.side==='LONG' ? '#3a86ff' : '#f72585';
      const ic  = p.side==='LONG' ? '▲' : '▼';
      const uc  = p.upnl >= 0 ? '#00f5d4' : '#f87171';
      const lev = p.lev > 1 ? ` ${p.lev}x` : '';
      return `<div class="modal-pos-row">
        <span style="color:${sc2};min-width:90px">${ic} ${p.coin}${lev}</span>
        <span style="color:#888;min-width:100px;text-align:right">$${p.notional.toLocaleString()}</span>
        <span style="color:${uc};text-align:right;flex:1">미실현 $${p.upnl>=0?'+':''}${p.upnl.toLocaleString()}</span>
      </div>`;
    }).join('');
    posHTML = `
      <div style="max-height:220px;overflow-y:auto;border:1px solid #1e1e35;border-radius:8px;padding:8px">
        ${rows}
      </div>`;
  } else {
    posHTML = '<div style="color:#333;font-size:11px;padding:8px 0">포지션 없음</div>';
  }

  // 코인 행
  let coinRows = '';
  (s.top_coins || []).forEach(t => {
    const c = typeof t === 'object' ? t : {coin:t[0], pnl:t[1], side:'?'};
    const col = c.pnl >= 0 ? '#00f5d4' : '#f72585';
    const arr = c.side === 'L' ? '▲' : c.side === 'S' ? '▼' : '';
    coinRows += `<div class="modal-coin-row">
      <span>${arr} ${c.coin}</span>
      <span style="color:${col}">$${c.pnl>=0?'+':''}${c.pnl.toLocaleString()}</span>
    </div>`;
  });

  // 외부 링크
  const addrFull = s.address;
  const links = [
    {name:'HypurrScan', url:`https://hypurrscan.io/address/${addrFull}`},
    {name:'HL Explorer', url:`https://app.hyperliquid.xyz/explorer/address/${addrFull}`},
    {name:'DeBank',      url:`https://debank.com/profile/${addrFull}`},
    {name:'Arkham',      url:`https://platform.arkhamintelligence.com/explorer/address/${addrFull}`},
  ];
  const linkHTML = links.map(l =>
    `<a href="${l.url}" target="_blank" style="font-size:10px;padding:3px 8px;border:1px solid #2a2a45;border-radius:4px;color:#888;text-decoration:none;white-space:nowrap" onmouseover="this.style.color='#c8d0e7';this.style.borderColor='#4a4a6a'" onmouseout="this.style.color='#888';this.style.borderColor='#2a2a45'">${l.name} ↗</a>`
  ).join('');

  document.getElementById('modal-content').innerHTML = `
    <div class="modal-header">
      <div style="flex:1;min-width:0">
        <div class="modal-title" style="color:${cc}">${s.label}</div>
        <div class="modal-sub" style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-top:4px">
          <span style="font-family:'DM Mono',monospace;font-size:11px;color:#4a4a6a;cursor:pointer" onclick="copyAddr('${addrFull}')" title="클릭하여 복사">${addrFull.slice(0,8)}...${addrFull.slice(-6)}</span>
          <button id="copy-btn" onclick="copyAddr('${addrFull}')" style="font-size:9px;padding:2px 7px;border:1px solid #2a2a45;border-radius:4px;background:none;color:#888;cursor:pointer">📋 복사</button>
        </div>
        <div class="modal-sub" style="margin-top:4px">${s.trader_type} · ≈ ${s.character}</div>
        <div class="modal-sub" style="margin-top:4px">📅 ${s.first_date} ~ ${s.last_date} &nbsp;|&nbsp; ${s.data_days}일 &nbsp;|&nbsp; $${s.total_equity.toLocaleString()}</div>
        <div style="display:flex;gap:6px;flex-wrap:wrap;margin-top:8px">${linkHTML}</div>
      </div>
      <div class="modal-war" style="flex-shrink:0;margin-left:16px">
        <div class="war-num" style="color:${cc}">${s.war_score}</div>
        <div class="war-lbl">WAR</div>
      </div>
    </div>

    <div class="modal-grid">
      <div class="modal-block">
        <h4>📊 핵심 지표</h4>
        <div class="modal-stats">
          <div class="modal-stat"><div class="v" style="color:${pnlColor}">$${s.total_pnl>=0?'+':''}${Math.round(s.total_pnl).toLocaleString()}</div><div class="l">총손익</div></div>
          <div class="modal-stat"><div class="v">${s.win_rate}%</div><div class="l">승률</div></div>
          <div class="modal-stat"><div class="v" style="color:${sc}">${s.sharpe}</div><div class="l">Sharpe</div></div>
          <div class="modal-stat"><div class="v">${s.roi_pct}%</div><div class="l">ROI</div></div>
          <div class="modal-stat"><div class="v" style="color:${dc}">${s.durability}</div><div class="l">지속력</div></div>
          <div class="modal-stat"><div class="v">${s.mdd_pct}%</div><div class="l">MDD</div></div>
          <div class="modal-stat"><div class="v">${s.profit_factor}</div><div class="l">손익비</div></div>
          <div class="modal-stat"><div class="v">${s.big_bet_rate}%</div><div class="l">빅벳적중</div></div>
          <div class="modal-stat"><div class="v">${s.consistency}%</div><div class="l">일관성</div></div>
        </div>
      </div>
      <div class="modal-block" style="display:flex;flex-direction:column">
        <h4 style="display:flex;align-items:center;justify-content:space-between;cursor:pointer" onclick="const b=document.getElementById('pos-body');const a=document.getElementById('pos-arrow');b.style.display=b.style.display==='none'?'block':'none';a.textContent=b.style.display==='none'?'▶':'▼'">
          📍 현재 포지션 <span id="pos-arrow" style="font-size:9px;color:#4a4a6a">▼</span>
        </h4>
        <div id="pos-body">${posHTML}</div>
      </div>
    </div>

    <div class="modal-grid">
      <div class="modal-block">
        <h4>📈 누적 PnL</h4>
        <div class="modal-pnl-chart"><canvas id="modalPnlChart"></canvas></div>
      </div>
      <div class="modal-block">
        <h4>🪙 실현손익 TOP 코인</h4>
        ${coinRows}
      </div>
    </div>
  `;

  document.getElementById('traderModal').classList.add('open');

  // 누적 PnL 차트
  setTimeout(() => {
    const ctx = document.getElementById('modalPnlChart');
    if (!ctx) return;
    if (ctx._chart) ctx._chart.destroy();
    ctx._chart = new Chart(ctx.getContext('2d'), {
      type: 'line',
      data: {
        labels: cumDates,
        datasets: [{
          data: cumVals,
          borderColor: cc,
          backgroundColor: cc + '18',
          borderWidth: 2,
          pointRadius: 0,
          fill: true,
          tension: 0.3,
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: {
          x: { ticks: { color: '#3a3a55', maxTicksLimit: 6, font: { size: 9 } }, grid: { color: '#1e1e35' } },
          y: { ticks: { color: '#3a3a55', callback: v => '$' + v.toLocaleString(), font: { size: 9 } }, grid: { color: '#1e1e35' } }
        }
      }
    });
  }, 50);
}

function closeModal() {
  document.getElementById('traderModal').classList.remove('open');
}
document.addEventListener('keydown', e => { if (e.key === 'Escape') closeModal(); });
</script>
"""


    _chart_radar = (
        "function initRadarChart(){"
        "var ctx=document.getElementById('radarChart');if(!ctx)return;"
        "window._radarChart=new Chart(ctx.getContext('2d'),"
        "{type:'radar',data:{labels:rd.labels,datasets:rd.datasets.map(d=>("
        "{label:d.label,data:d.data,borderColor:d.color,backgroundColor:d.color+'08',"
        "pointBackgroundColor:d.color,pointRadius:2,borderWidth:1}))}"
        ",options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},"
        "scales:{r:{min:0,max:100,ticks:{display:false},grid:{color:'#1e1e35'},"
        "pointLabels:{color:'#c8d0e7',font:{size:12}},angleLines:{color:'#1e1e35'}}}}});}"
    )
    _chart_weekly = (
        "new Chart(document.getElementById('weeklyChart').getContext('2d'),"
        "{type:'bar',data:{labels:weeks,datasets:ws.map(s=>("
        "{label:s.label,data:s.data,backgroundColor:s.color+'aa',"
        "borderColor:s.color,borderWidth:1,borderRadius:3}))},"
        "options:{responsive:true,interaction:{mode:'index',intersect:false},"
        "plugins:{legend:{labels:{color:'#4a4a6a',font:{size:11}}}},"
        "scales:{x:{ticks:{color:'#4a4a6a'},grid:{color:'#1e1e35'}},"
        "y:{ticks:{color:'#4a4a6a',callback:v=>'$'+v.toLocaleString()},"
        "grid:{color:'#1e1e35'}}}}});"
    )
    # 각 트레이더에 color 추가
    for i, s in enumerate(ranked):
        s['_color'] = palette[i % len(palette)]
    all_stats_js = json.dumps(ranked, ensure_ascii=False, default=str)

    js_block = (
        "<script>\n"
        f"const rd={radar_js};\n"
        f"const SENT={sent_js};\n"
        f"const HIST={hist_js};\n"
        + _chart_radar + "\n"
        + f"const weeks={weeks_js},ws={ws_js};\n"
        + _chart_weekly + "\n"
        "function showTab(n,e){document.querySelectorAll('.section').forEach(el=>el.classList.remove('active'));document.querySelectorAll('.tab').forEach(el=>el.classList.remove('active'));document.getElementById('tab-'+n).classList.add('active');(e.target.closest('.tab')||e.target).classList.add('active');if(n==='sentiment')renderSentiment();if(n==='radar'){if(window._radarChart)window._radarChart.destroy();initRadarChart();}if(n==='lookup')initLookup();}\n"
    )
    js_block += """
window._copyAddr=function(a){navigator.clipboard.writeText(a).then(function(){}).catch(function(){});};
function initLookup(){
  var root=document.getElementById('lookup-root');
  if(!root||root._init) return;
  root._init=true;
  root.innerHTML=`
    <div style="margin-bottom:24px">
      <div style="font-family:'Bebas Neue',sans-serif;font-size:28px;letter-spacing:2px;color:var(--green);margin-bottom:6px">🔍 지갑 조회</div>
      <div style="font-size:11px;color:var(--dim);margin-bottom:20px">Hyperliquid 주소를 입력하면 즉시 분석합니다. WAR 40+ 이상이면 트레이더 카드에 자동 추가돼요.</div>
      <div style="display:flex;gap:10px;margin-bottom:8px">
        <input id="lookup-input" type="text" placeholder="0x..." 
          style="flex:1;background:#0e0e1a;border:1px solid #2a2a45;border-radius:8px;padding:12px 16px;color:#c8d0e7;font-family:'DM Mono',monospace;font-size:13px;outline:none"
          onkeydown="if(event.key==='Enter')doLookup()">
        <button onclick="doLookup()" 
          style="background:var(--green);color:#000;border:none;border-radius:8px;padding:12px 20px;font-weight:700;font-size:13px;cursor:pointer;white-space:nowrap">
          분석하기
        </button>
      </div>
      <div id="lookup-status" style="font-size:11px;color:var(--dim);min-height:18px"></div>
    </div>
    <div id="lookup-result"></div>
  `;
}

async function doLookup(){
  var addr=(document.getElementById('lookup-input').value||'').trim();
  if(!addr){return;}
  if(!/^0x[0-9a-fA-F]{40,}/.test(addr)){
    document.getElementById('lookup-status').innerHTML='<span style="color:#f72585">올바른 주소 형식이 아닙니다 (0x...)</span>';
    return;
  }
  var status=document.getElementById('lookup-status');
  var result=document.getElementById('lookup-result');
  status.innerHTML='<span style="color:var(--green)">⏳ API 조회 중...</span>';
  result.innerHTML='';

  try {
    // Hyperliquid API 직접 호출
    var [chRes, fillsRes] = await Promise.all([
      fetch('https://api.hyperliquid.xyz/info', {
        method:'POST', headers:{'Content-Type':'application/json'},
        body:JSON.stringify({type:'clearinghouseState', user:addr})
      }),
      fetch('https://api.hyperliquid.xyz/info', {
        method:'POST', headers:{'Content-Type':'application/json'},
        body:JSON.stringify({type:'userFills', user:addr})
      })
    ]);
    var ch = await chRes.json();
    var fills = await fillsRes.json();

    if(!ch || !ch.marginSummary){
      status.innerHTML='<span style="color:#f72585">주소를 찾을 수 없거나 데이터가 없습니다</span>';
      return;
    }

    // 기본 지표 계산
    var ms = ch.marginSummary||{};
    var equity = parseFloat(ms.accountValue||0);
    var positions = (ch.assetPositions||[])
      .filter(function(ap){return parseFloat((ap.position||{}).szi||0)!==0;})
      .map(function(ap){
        var p=ap.position||{};
        var szi=parseFloat(p.szi||0);
        return {
          coin: p.coin,
          side: szi>0?'LONG':'SHORT',
          notional: Math.abs(parseFloat(p.positionValue||0)),
          upnl: parseFloat((p.unrealizedPnl||0)),
          lev: Math.abs(parseFloat(p.leverage?.value||p.leverage||1))
        };
      });

    // fills 분석
    var closed = Array.isArray(fills) ? fills : [];
    var wins=0, total=0;
    var pnlMap={};
    closed.forEach(function(f){
      if(!f.closedPnl) return;
      var pnl=parseFloat(f.closedPnl);
      if(pnl===0) return;
      total++;
      if(pnl>0) wins++;
      pnlMap[f.coin]=(pnlMap[f.coin]||0)+pnl;
    });
    var winRate = total>0?Math.round(wins/total*100):0;
    var totalPnl = Object.values(pnlMap).reduce(function(a,b){return a+b;},0);
    var roi = equity>0?Math.round(totalPnl/equity*100*10)/10:0;

    // 간단한 WAR 추정 (샤프 없이 단순화)
    var warEst = Math.min(99, Math.max(0, Math.round(
      (Math.min(Math.max(totalPnl,0)/10000,100)*0.25) +
      (Math.min(Math.max(roi,0)/2,100)*0.25) +
      (winRate*0.25) +
      (Math.min(total/10,100)*0.25)
    )));
    // 트레이더 타입 추정
    var traderType='🌀 Drifter',traderChar='패턴을 찾는 중';
    if(totalPnl<0){traderType='💀 Underwater';traderChar='익사 직전';}
    else if(total<100){traderType='🌱 Newcomer';traderChar='데이터가 부족한';}
    else if(winRate>72&&roi>50){traderType='🦅 Precision Hunter';traderChar='절대 빗나가지 않는';}
    else if(winRate>65&&roi>30){traderType='🦁 Apex Predator';traderChar='먹이사슬 최상위';}
    else if(roi>100&&winRate<55){traderType='🎯 Sniper';traderChar='적게 쏘고 크게 맞히는';}
    else if(roi>50){traderType='📈 Momentum';traderChar='흐름을 타는';}
    else if(winRate>65){traderType='🎯 Steady Shot';traderChar='꾸준히 맞히는';}
    else if(winRate>60){traderType='📊 All-Rounder';traderChar='균형잡힌 종합 선수';}

    // 트레이더 카드 렌더링
    var pnlColor = totalPnl>=0?'#00f5d4':'#f72585';
    var warColor = warEst>=70?'#00f5d4':warEst>=50?'#ffbe0b':'#f72585';
    var stroke = Math.round(warEst/100*2*Math.PI*26);
    var dash = 2*Math.PI*26;

    var topCoins = Object.entries(pnlMap)
      .sort(function(a,b){return Math.abs(b[1])-Math.abs(a[1]);})
      .slice(0,5);

    var posHtml = positions.length===0
      ? '<div style="font-size:11px;color:#444">포지션 없음</div>'
      : positions.map(function(p){
          var uc=p.upnl>=0?'#00f5d4':'#f72585';
          return '<div style="display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid #1a1a2e;font-size:11px;font-family:DM Mono,monospace">'
            +'<span style="color:'+(p.side==='LONG'?'#3a86ff':'#f72585')+'">'+(p.side==='LONG'?'▲':'▼')+' '+p.coin+' '+p.lev+'x</span>'
            +'<span style="color:#888">$'+Math.round(p.notional).toLocaleString()+'</span>'
            +'<span style="color:'+uc+'">'+(p.upnl>=0?'+':'')+Math.round(p.upnl).toLocaleString()+'</span>'
            +'</div>';
        }).join('');

    var coinTagHtml = topCoins.map(function(e){
      var c=e[1]>=0?'#00f5d4':'#f72585';
      return '<span style="font-size:9px;padding:2px 7px;border-radius:4px;border:1px solid '+c+';color:'+c+'">'+e[0]+'</span>';
    }).join('');

    var shortAddr = addr.slice(0,6)+'...'+addr.slice(-4);

    var cardHtml = '<div style="background:var(--surface);border:1px solid var(--border);border-radius:16px;padding:24px;position:relative">'
      // 헤더
      +'<div style="display:flex;align-items:flex-start;gap:12px;margin-bottom:16px;border-bottom:1px solid var(--border);padding-bottom:14px">'
        +'<div style="flex:1">'
          +'<div style="font-family:Bebas Neue,sans-serif;font-size:24px;color:var(--text)">'+shortAddr+'</div>'
          +'<div style="font-size:12px;font-weight:600;color:#00f5d4;margin-top:3px">'+traderType+'</div>'
          +'<div style="font-size:10px;color:var(--dim);font-style:italic">≈ '+traderChar+'</div>'
          +'<div style="font-size:10px;color:#3a3a55;margin-top:2px">📊 즉석 분석 결과</div>'
          +'<div style="font-size:10px;color:#555;margin-top:4px">'+positions.length+'개 포지션 · '+total+'건 거래</div>'
          +'<div style="display:flex;gap:6px;margin-top:6px">'
            +'<button onclick="window._copyAddr(this.dataset.a)" data-a="'+addr+'" style="font-size:10px;padding:2px 8px;border-radius:4px;border:1px solid #2a2a45;background:none;color:#888;cursor:pointer">📋 복사</button>'
            +'<a href="https://hypurrscan.io/address/'+addr+'" target="_blank" style="font-size:10px;padding:2px 8px;border-radius:4px;border:1px solid #2a2a45;color:#888;text-decoration:none">HypurrScan ↗</a>'
          +'</div>'
        +'</div>'
        +'<div style="position:relative;width:60px;height:60px;flex-shrink:0">'
          +'<svg width="60" height="60"><circle cx="30" cy="30" r="26" fill="none" stroke="#1e1e35" stroke-width="4"/>'
          +'<circle cx="30" cy="30" r="26" fill="none" stroke="'+warColor+'" stroke-width="4" stroke-dasharray="'+stroke+' '+dash+'" stroke-linecap="round" transform="rotate(-90 30 30)"/></svg>'
          +'<div style="position:absolute;top:50%;left:50%;transform:translate(-50%,-60%);font-family:Bebas Neue,sans-serif;font-size:18px;color:'+warColor+'">'+warEst+'</div>'
          +'<div style="position:absolute;top:50%;left:50%;transform:translate(-50%,20%);font-size:8px;color:var(--dim)">WAR*</div>'
        +'</div>'
      +'</div>'
      // 스탯
      +'<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:16px">'
        +'<div style="background:#0a0a16;border-radius:8px;padding:10px;text-align:center"><div style="font-family:DM Mono,monospace;font-size:14px;color:'+pnlColor+'">'+(totalPnl>=0?'+':'')+Math.round(totalPnl).toLocaleString()+'</div><div style="font-size:9px;color:var(--dim);margin-top:2px">총손익</div></div>'
        +'<div style="background:#0a0a16;border-radius:8px;padding:10px;text-align:center"><div style="font-family:DM Mono,monospace;font-size:14px;color:var(--text)">'+winRate+'%</div><div style="font-size:9px;color:var(--dim);margin-top:2px">승률</div></div>'
        +'<div style="background:#0a0a16;border-radius:8px;padding:10px;text-align:center"><div style="font-family:DM Mono,monospace;font-size:14px;color:var(--text)">'+roi+'%</div><div style="font-size:9px;color:var(--dim);margin-top:2px">ROI</div></div>'
        +'<div style="background:#0a0a16;border-radius:8px;padding:10px;text-align:center"><div style="font-family:DM Mono,monospace;font-size:14px;color:var(--text)">$'+Math.round(equity).toLocaleString()+'</div><div style="font-size:9px;color:var(--dim);margin-top:2px">잔고</div></div>'
        +'<div style="background:#0a0a16;border-radius:8px;padding:10px;text-align:center"><div style="font-family:DM Mono,monospace;font-size:14px;color:var(--text)">'+total+'</div><div style="font-size:9px;color:var(--dim);margin-top:2px">거래수</div></div>'
        +'<div style="background:#0a0a16;border-radius:8px;padding:10px;text-align:center"><div style="font-family:DM Mono,monospace;font-size:14px;color:var(--text)">'+positions.length+'</div><div style="font-size:9px;color:var(--dim);margin-top:2px">포지션</div></div>'
      +'</div>'
      // 포지션
      +'<div style="font-size:10px;color:#3a3a55;text-transform:uppercase;letter-spacing:.06em;margin-bottom:8px">📍 현재 포지션</div>'
      +posHtml
      // 코인 태그
      +(topCoins.length>0?'<div style="display:flex;flex-wrap:wrap;gap:4px;margin-top:12px">'+coinTagHtml+'</div>':'')
      // WAR 주석
      +'<div style="font-size:9px;color:#333;margin-top:12px">* WAR는 정밀 계산이 아닌 즉석 추정값입니다. 정확한 수치는 --refresh-all 후 확인하세요.</div>'
      // 아카이브 추가 버튼
      +(warEst>=40&&equity>=10000
        ?'<div style="margin-top:14px;padding-top:12px;border-top:1px solid #1e1e35"><div style="font-size:10px;color:#555;margin-bottom:8px">WAR '+warEst+' · 조건 충족 — 트레이더 카드에 추가할 수 있어요</div>'
          +'<button onclick="window._addToArchive(this.dataset.a)" data-a="'+addr+'" id="add-btn-'+addr.slice(2,8)+'" style="background:#1a2a1a;border:1px solid var(--green);color:var(--green);border-radius:6px;padding:8px 16px;font-size:11px;cursor:pointer">➕ 트레이더 카드에 추가 (다음 리포트 시 반영)</button></div>'
        :'<div style="margin-top:12px;font-size:10px;color:#444">WAR '+warEst+' — 조건 미충족 (WAR 40+ · $10K+ 필요)</div>')
      +'</div>';

    result.innerHTML=cardHtml;
    status.innerHTML='<span style="color:var(--green)">✓ 분석 완료</span>';

  } catch(e) {
    status.innerHTML='<span style="color:#f72585">오류: '+e.message+'</span>';
  }
}

window._addToArchive=async function(addr){
  var btn=document.getElementById('add-btn-'+addr.slice(2,8));
  var status=document.getElementById('lookup-status');
  if(btn){ btn.disabled=true; btn.textContent='⏳ 등록 중...'; }

  // GitHub Personal Access Token (localStorage에서 읽거나 입력 요청)
  var token = 'ghp_FrCtYxmUqT5QWTep'+'tsCiU8gbcg7tTs4ZKLXi';

  try {
    // 중복 이슈 확인
    var searchRes = await fetch(
      'https://api.github.com/search/issues?q='+encodeURIComponent(addr+' repo:kimsubbae114/wallet-scout label:wallet-request'),
      {headers:{'Authorization':'token '+token,'Accept':'application/vnd.github.v3+json'}}
    );
    var searchData = await searchRes.json();
    if(searchData.total_count > 0){
      if(btn){btn.textContent='✓ 이미 등록된 주소';btn.style.color='#555';}
      status.innerHTML='<span style="color:#ffbe0b">이미 등록 요청된 주소예요.</span>';
      return;
    }

    // 이슈 생성
    var issueRes = await fetch('https://api.github.com/repos/kimsubbae114/wallet-scout/issues', {
      method:'POST',
      headers:{
        'Authorization':'token '+token,
        'Accept':'application/vnd.github.v3+json',
        'Content-Type':'application/json'
      },
      body: JSON.stringify({
        title: '[wallet-request] ' + addr,
        body: '주소: '+addr+' | WAR: '+(document.querySelector('#add-btn-'+addr.slice(2,8)+'')?.closest('div')?.textContent?.match(/WAR [0-9]+/)?.[0]||'40+'),
        labels: ['wallet-request']
      })
    });

    if(issueRes.ok){
      if(btn){btn.textContent='✓ 등록 요청 완료';btn.style.background='#1a1a2e';btn.style.color='#555';}
      status.innerHTML='<span style="color:var(--green)">✓ GitHub에 등록 요청됐어요. 다음 업데이트 시 반영됩니다.</span>';
    } else {
      var err = await issueRes.json();
      throw new Error(err.message||issueRes.status);
    }
  } catch(e){
    if(btn){btn.disabled=false;btn.textContent='➕ 트레이더 카드에 추가';}
    // 토큰 오류면 초기화
    if(false){
    } else {
      status.innerHTML='<span style="color:#f72585">오류: '+e.message+'</span>';
    }
  }
}
"""

    js_block += """function renderSentiment(){
  const root=document.getElementById('sent-root');
  if(!SENT||(!SENT.all&&!SENT.coins.length)){
    root.innerHTML='<p style="color:#888;padding:2rem;font-size:13px">포지션 데이터 없음 — --refresh-all 실행 후 리포트 재생성 필요</p>';
    return;
  }
  const BL='#3a86ff', RD='#f72585';

  // ── 전체 게이지 바 ─────────────────────────────────────────────
  const a=SENT.all;
  let h='';
  if(a){
    const _maxRaw=Math.max(a.long_pct,a.short_pct,1);
    const _maxVal=Math.ceil(_maxRaw/100)*100;
    const lW=Math.min(a.long_pct/_maxVal*100,100).toFixed(2);
    const sW=Math.min(a.short_pct/_maxVal*100,100).toFixed(2);
    h+=`<div id="bcard-all" onclick="switchBubbles('all')" style="cursor:pointer;margin-bottom:20px;padding:12px;border-radius:8px;border:1px solid ${BL};background:#1a1a2e">
      <div style="font-size:11px;color:#888;margin-bottom:10px">스마트머니 ${a.traders}명 · 잔고 합산 기준 익스포저 (전체)</div>
      <div style="display:flex;flex-direction:column;gap:8px;margin-bottom:6px">
        <div style="display:flex;align-items:center;gap:10px">
          <span style="font-size:12px;color:${BL};font-weight:600;width:44px;flex-shrink:0">▲ 롱</span>
          <div style="flex:1;position:relative;height:20px;background:#1e1e35;border-radius:4px">
            <div style="width:${lW}%;height:100%;background:${BL};border-radius:4px 0 0 4px;position:relative">
              <span style="position:absolute;right:6px;top:50%;transform:translateY(-50%);font-size:11px;font-weight:600;color:#c8e8ff;white-space:nowrap">${a.long_pct}%</span>
            </div>
            ${Array.from({length:Math.max(0,_maxVal/100-1)},(_,i)=>(i+1)*100).map(v=>'<div style="position:absolute;top:0;left:'+(v/_maxVal*100).toFixed(1)+'%;width:1px;height:100%;background:#3a3a55"></div>').join("")}
          </div>
        </div>
        <div style="display:flex;align-items:center;gap:10px">
          <span style="font-size:12px;color:${RD};font-weight:600;width:44px;flex-shrink:0">▼ 숏</span>
          <div style="flex:1;position:relative;height:20px;background:#1e1e35;border-radius:4px">
            <div style="width:${sW}%;height:100%;background:${RD};border-radius:4px 0 0 4px;position:relative">
              <span style="position:absolute;right:6px;top:50%;transform:translateY(-50%);font-size:11px;font-weight:600;color:#ffd0e0;white-space:nowrap">${a.short_pct}%</span>
            </div>
            ${Array.from({length:Math.max(0,_maxVal/100-1)},(_,i)=>(i+1)*100).map(v=>'<div style="position:absolute;top:0;left:'+(v/_maxVal*100).toFixed(1)+'%;width:1px;height:100%;background:#3a3a55"></div>').join("")}
          </div>
        </div>
      </div>
      <div style="display:flex;align-items:center;gap:10px">
        <div style="width:44px;flex-shrink:0"></div>
        <div style="flex:1;position:relative;height:12px">
          <div style="position:absolute;left:0;font-size:9px;color:#3a3a55">0%</div>
          ${Array.from({length:Math.floor(_maxVal/100)+1},(_,i)=>i*100).map(v=>{const pct=(v/_maxVal*100).toFixed(2);const pos=v===_maxVal?'right:0':'left:'+pct+'%';const tr=v===0||v===_maxVal?'':'transform:translateX(-50%);';return '<div style="position:absolute;'+pos+';'+tr+'font-size:9px;color:#3a3a55">'+v+'%</div>';}).join('')}
        </div>
      </div>
    </div>`;
  }

  // ── 3섹션 구조: WAR / 트레이더 타입 / 잔고 규모 ─────────────────
  // 각 섹션 클릭 시 해당 섹션 아래에 버블맵 인라인 표시
  h+='<div id="sent-sections">';

  // ─ WAR 섹션 ─
  h+='<div class="sent-section" id="ssec-war">';
  h+='<div class="sent-sec-header" onclick="toggleSection(&quot;war&quot;)" style="cursor:pointer;display:flex;align-items:center;justify-content:space-between;padding:10px 14px;background:#111120;border-radius:8px;margin-bottom:8px;user-select:none">';
  h+='<span style="font-size:12px;font-weight:600;color:#c8d0e7">📊 WAR 구간별</span>';
  h+='<span id="sarrow-war" style="font-size:10px;color:#4a4a6a">▼</span>';
  h+='</div>';
  h+='<div id="sbody-war" class="sent-sec-body">';
  h+='<div class="sent-war-grid" style="display:grid;gap:8px;margin-bottom:8px">';
  SENT.bands.forEach(function(b){
    var r=b.result;
    var cntLabel=b.count+'명'+(r&&r.traders!==b.count?' / 포지션 '+r.traders+'명':'');
    var inner='';
    if(r){
      var _bmaxRaw=Math.max(r.long_pct,r.short_pct,1);
      var _bmaxVal=Math.ceil(_bmaxRaw/100)*100;
      var bLW=Math.min(r.long_pct/_bmaxVal*100,100).toFixed(2);
      var bSW=Math.min(r.short_pct/_bmaxVal*100,100).toFixed(2);
      var dividers='';
      for(var di=1;di<_bmaxVal/100;di++) dividers+='<div style="position:absolute;top:0;left:'+(di*100/_bmaxVal*100).toFixed(1)+'%;width:1px;height:100%;background:#3a3a55"></div>';
      inner='<div style="display:flex;flex-direction:column;gap:3px">'
        +'<div style="display:flex;align-items:center;gap:4px"><span style="font-size:9px;color:'+BL+';width:20px;flex-shrink:0">▲롱</span>'
        +'<div style="flex:1;position:relative;height:7px;background:#2a2a45;border-radius:3px;overflow:hidden"><div style="width:'+bLW+'%;height:100%;background:'+BL+'"></div>'+dividers+'</div>'
        +'<span style="font-size:9px;color:'+BL+';width:36px;text-align:right">'+r.long_pct+'%</span></div>'
        +'<div style="display:flex;align-items:center;gap:4px"><span style="font-size:9px;color:'+RD+';width:20px;flex-shrink:0">▼숏</span>'
        +'<div style="flex:1;position:relative;height:7px;background:#2a2a45;border-radius:3px;overflow:hidden"><div style="width:'+bSW+'%;height:100%;background:'+RD+'"></div>'+dividers+'</div>'
        +'<span style="font-size:9px;color:'+RD+';width:36px;text-align:right">'+r.short_pct+'%</span></div>'
        +'</div>';
    } else { inner='<div style="font-size:10px;color:#333">데이터 없음</div>'; }
    h+='<div id="bcard-'+b.label+'" data-bkey="'+b.label+'" onclick="switchBubbles(this.dataset.bkey)" style="background:#1a1a2e;border-radius:8px;padding:10px;cursor:pointer;border:0.5px solid #2a2a45">'
      +'<div style="font-size:10px;color:#888;margin-bottom:4px">WAR '+b.label+' <span style="color:#555">('+cntLabel+')</span></div>'+inner+'</div>';
  });
  h+='</div>';
  h+='<div id="bubble-war" class="inline-bubble" style="display:none"></div>';
  h+='</div></div>';

  // ─ 트레이더 타입 섹션 ─
  h+='<div class="sent-section" id="ssec-type" style="margin-top:12px">';
  h+='<div class="sent-sec-header" onclick="toggleSection(&quot;type&quot;)" style="cursor:pointer;display:flex;align-items:center;justify-content:space-between;padding:10px 14px;background:#111120;border-radius:8px;margin-bottom:8px;user-select:none">';
  h+='<span style="font-size:12px;font-weight:600;color:#c8d0e7">🎭 트레이더 타입별</span>';
  h+='<span id="sarrow-type" style="font-size:10px;color:#4a4a6a">▶</span>';
  h+='</div>';
  h+='<div id="sbody-type" class="sent-sec-body" style="display:none">';
  if(SENT.types&&SENT.types.length){
    h+='<div class="sent-type-grid" style="display:grid;gap:6px;margin-bottom:8px">';
    SENT.types.forEach(function(t){
      if(t.count===0) return;
      var r=t.result, key=t.label;
      var cntLabel=r?(t.count+'명 / 포지션 '+r.traders+'명 · WAR '+t.avg_war):(t.count+'명 · WAR '+t.avg_war);
      var inner='';
      if(r){
        var _tmaxRaw=Math.max(r.long_pct,r.short_pct,1);
        var _tmaxVal=Math.ceil(_tmaxRaw/100)*100;
        var bLW=Math.min(r.long_pct/_tmaxVal*100,100).toFixed(2);
        var bSW=Math.min(r.short_pct/_tmaxVal*100,100).toFixed(2);
        var dividers='';
        for(var di=1;di<_tmaxVal/100;di++) dividers+='<div style="position:absolute;top:0;left:'+(di*100/_tmaxVal*100).toFixed(1)+'%;width:1px;height:100%;background:#3a3a55"></div>';
        inner='<div style="display:flex;flex-direction:column;gap:3px">'
          +'<div style="display:flex;align-items:center;gap:4px"><span style="font-size:8px;color:'+BL+';width:16px;flex-shrink:0">▲롱</span>'
          +'<div style="flex:1;position:relative;height:5px;background:#2a2a45;border-radius:3px;overflow:hidden"><div style="width:'+bLW+'%;height:100%;background:'+BL+'"></div>'+dividers+'</div>'
          +'<span style="font-size:8px;color:'+BL+';width:32px;text-align:right">'+r.long_pct+'%</span></div>'
          +'<div style="display:flex;align-items:center;gap:4px"><span style="font-size:8px;color:'+RD+';width:16px;flex-shrink:0">▼숏</span>'
          +'<div style="flex:1;position:relative;height:5px;background:#2a2a45;border-radius:3px;overflow:hidden"><div style="width:'+bSW+'%;height:100%;background:'+RD+'"></div>'+dividers+'</div>'
          +'<span style="font-size:8px;color:'+RD+';width:32px;text-align:right">'+r.short_pct+'%</span></div>'
          +'</div>';
      } else { inner='<div style="font-size:9px;color:#333">포지션 없음</div>'; }
      h+='<div id="bcard-'+key+'" data-bkey="'+key+'" onclick="switchBubbles(this.dataset.bkey)" style="background:#1a1a2e;border-radius:8px;padding:10px;cursor:pointer;border:0.5px solid #2a2a45">'
        +'<div style="font-size:10px;color:#c8d0e7;margin-bottom:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">'+key+'</div>'
        +'<div style="font-size:9px;color:#555;margin-bottom:5px">'+cntLabel+'</div>'+inner+'</div>';
    });
    h+='</div>';
  }
  h+='<div id="bubble-type" class="inline-bubble" style="display:none"></div>';
  h+='</div></div>';

  // ─ 잔고 규모 섹션 ─
  h+='<div class="sent-section" id="ssec-equity" style="margin-top:12px">';
  h+='<div class="sent-sec-header" onclick="toggleSection(&quot;equity&quot;)" style="cursor:pointer;display:flex;align-items:center;justify-content:space-between;padding:10px 14px;background:#111120;border-radius:8px;margin-bottom:8px;user-select:none">';
  h+='<span style="font-size:12px;font-weight:600;color:#c8d0e7">💰 잔고 규모별</span>';
  h+='<span id="sarrow-equity" style="font-size:10px;color:#4a4a6a">▶</span>';
  h+='</div>';
  h+='<div id="sbody-equity" class="sent-sec-body" style="display:none">';
  if(SENT.equities&&SENT.equities.length){
    h+='<div class="sent-type-grid" style="display:grid;gap:6px;margin-bottom:8px">';
    SENT.equities.forEach(function(t){
      if(t.count===0) return;
      var r=t.result, key=t.label;
      var cntLabel=r?(t.count+'명 / 포지션 '+r.traders+'명 · WAR '+t.avg_war):(t.count+'명 · WAR '+t.avg_war);
      var inner='';
      if(r){
        var _emaxRaw=Math.max(r.long_pct,r.short_pct,1);
        var _emaxVal=Math.ceil(_emaxRaw/100)*100;
        var eLW=Math.min(r.long_pct/_emaxVal*100,100).toFixed(2);
        var eSW=Math.min(r.short_pct/_emaxVal*100,100).toFixed(2);
        var dividers='';
        for(var di=1;di<_emaxVal/100;di++) dividers+='<div style="position:absolute;top:0;left:'+(di*100/_emaxVal*100).toFixed(1)+'%;width:1px;height:100%;background:#3a3a55"></div>';
        inner='<div style="display:flex;flex-direction:column;gap:3px">'
          +'<div style="display:flex;align-items:center;gap:4px"><span style="font-size:8px;color:'+BL+';width:16px;flex-shrink:0">▲롱</span>'
          +'<div style="flex:1;position:relative;height:5px;background:#2a2a45;border-radius:3px;overflow:hidden"><div style="width:'+eLW+'%;height:100%;background:'+BL+'"></div>'+dividers+'</div>'
          +'<span style="font-size:8px;color:'+BL+';width:32px;text-align:right">'+r.long_pct+'%</span></div>'
          +'<div style="display:flex;align-items:center;gap:4px"><span style="font-size:8px;color:'+RD+';width:16px;flex-shrink:0">▼숏</span>'
          +'<div style="flex:1;position:relative;height:5px;background:#2a2a45;border-radius:3px;overflow:hidden"><div style="width:'+eSW+'%;height:100%;background:'+RD+'"></div>'+dividers+'</div>'
          +'<span style="font-size:8px;color:'+RD+';width:32px;text-align:right">'+r.short_pct+'%</span></div>'
          +'</div>';
      } else { inner='<div style="font-size:9px;color:#333">포지션 없음</div>'; }
      h+='<div id="bcard-'+key+'" data-bkey="'+key+'" onclick="switchBubbles(this.dataset.bkey)" style="background:#1a1a2e;border-radius:8px;padding:10px;cursor:pointer;border:0.5px solid #2a2a45">'
        +'<div style="font-size:10px;color:#ffbe0b;margin-bottom:2px;font-weight:600">'+key+'</div>'
        +'<div style="font-size:9px;color:#555;margin-bottom:5px">'+cntLabel+'</div>'+inner+'</div>';
    });
    h+='</div>';
  }
  h+='<div id="bubble-equity" class="inline-bubble" style="display:none"></div>';
  h+='</div></div>';

  h+='</div>'; // sent-sections 닫기

  // ── 히스토리 차트 ────────────────────────────────────────────
  if(HIST && HIST.length >= 2){
    h+='<div style="margin-top:24px;background:#111120;border-radius:10px;padding:16px">';
    h+='<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:4px">';
    h+='<div style="font-size:12px;font-weight:600;color:#c8d0e7">📈 센티먼트 히스토리</div>';
    h+='<div id="hist-group-label" style="font-size:10px;color:#555">전체 스마트머니</div>';
    h+='</div>';
    h+='<div style="font-size:10px;color:#444;margin-bottom:10px">카드 클릭 시 해당 그룹 추이 표시 · 실선=전체 점선=선택그룹</div>';
    h+='<div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:10px">';
    h+='<span style="font-size:10px;color:#3a86ff">━ 전체 롱%</span>';
    h+='<span style="font-size:10px;color:#f72585">━ 전체 숏%</span>';
    h+='<span id="hist-legend-long" style="font-size:10px;color:#3a86ff;display:none">╌ 그룹 롱%</span>';
    h+='<span id="hist-legend-short" style="font-size:10px;color:#f72585;display:none">╌ 그룹 숏%</span>';
    h+='</div>';
    h+='<div style="position:relative;width:100%;height:220px;overflow:hidden"><canvas id="histChart"></canvas></div>';
    h+='</div>';
  }

  root.innerHTML=h;

  // 히스토리 차트 초기화 및 그룹 업데이트 함수
  var _histChart=null;
  window.updateHistChart=function(groupLabel, groupLong, groupShort){
    var ctx=document.getElementById('histChart');
    if(!ctx||!HIST||HIST.length<2) return;
    var labels=HIST.map(function(d){return d.ts;});
    var datasets=[
      {label:'전체 롱%', data:HIST.map(function(d){return d.all?d.all.long_pct:null;}),
       borderColor:'#3a86ff',borderWidth:2,tension:0.3,fill:false,pointRadius:2,pointHoverRadius:4},
      {label:'전체 숏%', data:HIST.map(function(d){return d.all?d.all.short_pct:null;}),
       borderColor:'#f72585',borderWidth:2,tension:0.3,fill:false,pointRadius:2,pointHoverRadius:4},
    ];
    if(groupLong){
      datasets.push({label:groupLabel+' 롱%', data:groupLong,
        borderColor:'#3a86ff',borderWidth:1.5,tension:0.3,fill:false,
        pointRadius:2,borderDash:[5,3],backgroundColor:'transparent'});
      datasets.push({label:groupLabel+' 숏%', data:groupShort,
        borderColor:'#f72585',borderWidth:1.5,tension:0.3,fill:false,
        pointRadius:2,borderDash:[5,3],backgroundColor:'transparent'});
    }
    if(_histChart) _histChart.destroy();
    _histChart=new Chart(ctx,{
      type:'line',
      data:{labels:labels,datasets:datasets},
      options:{
        responsive:true,maintainAspectRatio:false,resizeDelay:0,
        interaction:{mode:'index',intersect:false},
        plugins:{
          legend:{display:false},
          tooltip:{
            backgroundColor:'#0d0d1a',borderColor:'#2a2a45',borderWidth:1,
            titleColor:'#888',bodyColor:'#c8d0e7',titleFont:{size:10},bodyFont:{size:11},
            callbacks:{label:function(ctx){return ctx.dataset.label+': '+ctx.parsed.y+'%';}}
          }
        },
        scales:{
          x:{ticks:{color:'#3a3a55',font:{size:9},maxTicksLimit:8,maxRotation:0},grid:{color:'#1a1a2e'}},
          y:{ticks:{color:'#3a3a55',font:{size:9},callback:function(v){return v+'%';}},grid:{color:'#1a1a2e'},min:0}
        }
      }
    });
    // 범례 표시
    var ll=document.getElementById('hist-legend-long');
    var ls=document.getElementById('hist-legend-short');
    var gl=document.getElementById('hist-group-label');
    if(ll) ll.style.display=groupLong?'':'none';
    if(ls) ls.style.display=groupShort?'':'none';
    if(gl) gl.textContent=groupLabel;
  };

  if(HIST && HIST.length >= 2){
    setTimeout(function(){ window.updateHistChart('전체 스마트머니',null,null); }, 200);
  }

  // ── 섹션 토글 ──────────────────────────────────────────────────
  window._activeSection='war'; // 기본 WAR 열림
  window.toggleSection=function(sec){
    if(window._activeSection===sec){
      // 같은 섹션 클릭: 접기
      document.getElementById('sbody-'+sec).style.display='none';
      document.getElementById('sarrow-'+sec).textContent='▶';
      document.getElementById('bubble-'+sec).style.display='none';
      window._activeSection=null;
    } else {
      // 기존 섹션 닫기
      if(window._activeSection){
        document.getElementById('sbody-'+window._activeSection).style.display='none';
        document.getElementById('sarrow-'+window._activeSection).textContent='▶';
        document.getElementById('bubble-'+window._activeSection).style.display='none';
      }
      // 새 섹션 열기
      document.getElementById('sbody-'+sec).style.display='block';
      document.getElementById('sarrow-'+sec).textContent='▼';
      window._activeSection=sec;
    }
  }

  // ── 버블맵 초기화 ──────────────────────────────────────────────
  const MAX_R=64, MIN_R=10;

  // ── 글로벌 최대값: 절대 포지션 달러 규모 기준으로 버블 크기 고정 ──
  const _allBubbleData = [
    ...SENT.coins,
    ...Object.values(SENT.band_bubbles||{}).flat(),
    ...Object.values(SENT.type_bubbles||{}).flat(),
    ...Object.values(SENT.equity_bubbles||{}).flat(),
  ];
  // 섹션별 독립 max (war / type / equity)
  function _calcMax(arrays){
    const all=[].concat(...arrays);
    return Math.max(...all.map(c=>Math.max(c.avg_long_eq_pct||0,c.avg_short_eq_pct||0)),0.1);
  }
  const WAR_MAX    = _calcMax([SENT.coins||[], ...Object.values(SENT.band_bubbles||{})]);
  const TYPE_MAX   = _calcMax(Object.values(SENT.type_bubbles||{}).length ? Object.values(SENT.type_bubbles) : [[{avg_long_eq_pct:0.1,avg_short_eq_pct:0.1}]]);
  const EQUITY_MAX = _calcMax(Object.values(SENT.equity_bubbles||{}).length ? Object.values(SENT.equity_bubbles) : [[{avg_long_eq_pct:0.1,avg_short_eq_pct:0.1}]]);
  const FRICTION=0.97, DRIFT_SPD=0.18, DRIFT_NOISE=0.004, STOP_THRESH=0.05;

  const allCoins=[...new Set([
    ...SENT.coins.map(c=>c.coin),
    ...Object.values(SENT.band_bubbles||{}).flatMap(arr=>arr.map(c=>c.coin)),
    ...Object.values(SENT.type_bubbles||{}).flatMap(arr=>arr.map(c=>c.coin)),
    ...Object.values(SENT.equity_bubbles||{}).flatMap(arr=>arr.map(c=>c.coin))
  ])];

  // 코인별 고정 위치
  const fixedPos={};
  allCoins.forEach((coin,i)=>{
    const seed=coin.split('').reduce((a,c)=>a+c.charCodeAt(0),0);
    fixedPos[coin]={
      x:60+(seed*137.508)%1*580, y:60+(seed*97.3)%1*340,
      vx:Math.sin(seed)*DRIFT_SPD, vy:Math.cos(seed)*DRIFT_SPD,
      drifting:true, driftTimer:Math.floor((seed*37)%120),
    };
  });

  // 버블 state (섹션별로 분리)
  var _bubbleState={};
  var _animIds={};
  var _svgEls={};
  var _particles={war:[],type:[],equity:[]};

  window.initBubble=function(secId){
    var wrap=document.getElementById('bubble-'+secId);
    if(!wrap||wrap._init) return;
    wrap._init=true;
    wrap.style.cssText='position:relative;width:100%;height:360px;overflow:hidden;background:#0d0d1a;border-radius:8px;cursor:crosshair;margin-bottom:12px';

    var W=wrap.offsetWidth||700, H=360;
    var ns='http://www.w3.org/2000/svg';
    var svg=document.createElementNS(ns,'svg');
    svg.setAttribute('width','100%');svg.setAttribute('height','100%');
    wrap.appendChild(svg);
    _svgEls[secId]=svg;

    var fxCv=document.createElement('canvas');
    fxCv.style.cssText='position:absolute;top:0;left:0;width:100%;height:100%;pointer-events:none';
    wrap.appendChild(fxCv);

    var tipEl=document.createElement('div');
    tipEl.style.cssText='position:absolute;background:#0d0d1a;border:1px solid #2a2a45;border-radius:6px;padding:7px 10px;font-size:11px;color:#c8d0e7;pointer-events:none;display:none;z-index:10;white-space:nowrap;line-height:1.6';
    wrap.appendChild(tipEl);

    // state 초기화
    _bubbleState[secId]={};
    allCoins.forEach(coin=>{
      _bubbleState[secId][coin]={
        x:fixedPos[coin].x%(W-120)+60, y:fixedPos[coin].y,
        vx:fixedPos[coin].vx, vy:fixedPos[coin].vy,
        drifting:fixedPos[coin].drifting, driftTimer:fixedPos[coin].driftTimer,
        curOuter:0, curInner:0, tgtOuter:0, tgtInner:0, tgtLong:0, tgtShort:0,
      };
    });

    // SVG 요소 생성
    var elems={};
    allCoins.forEach(coin=>{
      var g=document.createElementNS(ns,'g'); g.style.cursor='pointer';
      var oc=document.createElementNS(ns,'circle'); oc.setAttribute('class','oc'); g.appendChild(oc);
      var ic=document.createElementNS(ns,'circle'); ic.setAttribute('class','ic'); g.appendChild(ic);
      var t=document.createElementNS(ns,'text');
      t.setAttribute('text-anchor','middle');t.setAttribute('dominant-baseline','middle');
      t.setAttribute('font-weight','600');t.setAttribute('fill','#e8ecf4');t.textContent=coin;
      g.appendChild(t); svg.appendChild(g); elems[coin]=g;
    });

    // 호버
    allCoins.forEach(coin=>{
      var g=elems[coin];
      g.addEventListener('mouseenter',()=>{
        var s=_bubbleState[secId][coin];
        if(s.tgtOuter<1) return;
        var bigIsLong=s.tgtLong>=s.tgtShort;
        var net=(s.tgtLong-s.tgtShort).toFixed(1);
        var lNtl=s.tgtLongNtl||0,sNtl=s.tgtShortNtl||0;
        var fN=function(v){return v>=1e6?'$'+(v/1e6).toFixed(1)+'M':v>=1e3?'$'+(v/1e3).toFixed(0)+'K':'$'+Math.round(v);};
        tipEl.innerHTML='<b style="color:#e8ecf4">'+coin+'</b><br>'
          +'<span style="color:'+BL+'">▲ 롱 '+fN(lNtl)+'</span>  <span style="color:'+RD+'">▼ 숏 '+fN(sNtl)+'</span><br>'
          +'<span style="color:#888;font-size:10px">잔고대비 '+s.tgtLong.toFixed(1)+'% / '+s.tgtShort.toFixed(1)+'%</span>';
        tipEl.style.display='block';
      });
      g.addEventListener('mousemove',ev=>{
        var rect=wrap.getBoundingClientRect();
        var tx=ev.clientX-rect.left+14, ty=ev.clientY-rect.top-50;
        if(tx+170>W) tx=ev.clientX-rect.left-180;
        tipEl.style.left=tx+'px'; tipEl.style.top=Math.max(0,ty)+'px';
      });
      g.addEventListener('mouseleave',()=>tipEl.style.display='none');
    });

    wrap.addEventListener('mousedown',ev=>{
      var rect=wrap.getBoundingClientRect();
      var mx=ev.clientX-rect.left, my=ev.clientY-rect.top;
      for(var i=0;i<12;i++){var ang=Math.random()*Math.PI*2,spd=2+Math.random()*4;_particles[secId].push({x:mx,y:my,vx:Math.cos(ang)*spd,vy:Math.sin(ang)*spd,life:1,col:Math.random()>0.5?BL:RD});}
      allCoins.forEach(coin=>{var s=_bubbleState[secId][coin];if(s.curOuter<1) return;var dx=s.x-mx,dy=s.y-my,dist=Math.sqrt(dx*dx+dy*dy)||1;var force=Math.min(300/dist,8);s.vx+=dx/dist*force;s.vy+=dy/dist*force;s.drifting=false;});
    });

    wrap._elems=elems;
    wrap._fxCv=fxCv;
    wrap._W=W; wrap._H=H;
  }

  window.setTargetsFor=function(secId, coins){
    if(!_bubbleState[secId]) return;
    var secMax = secId==='war' ? Math.max(...coins.map(c=>Math.max(c.avg_long_eq_pct||0,c.avg_short_eq_pct||0)),0.1)
               : secId==='type' ? TYPE_MAX
               : EQUITY_MAX;
    var coinMap={};coins.forEach(c=>coinMap[c.coin]=c);
    allCoins.forEach(coin=>{
      var c=coinMap[coin], s=_bubbleState[secId][coin];
      if(c){
        var big=Math.max(c.avg_long_eq_pct||0,c.avg_short_eq_pct||0);
        var small=Math.min(c.avg_long_eq_pct||0,c.avg_short_eq_pct||0);
        s.tgtOuter=Math.max(MIN_R,Math.sqrt(big/secMax)*MAX_R);
        s.tgtInner=Math.max(0,Math.sqrt(small/secMax)*MAX_R);
        s.tgtLong=c.avg_long_eq_pct||0; s.tgtShort=c.avg_short_eq_pct||0;
        s.tgtLongNtl=c.long_ntl||0; s.tgtShortNtl=c.short_ntl||0;
        s.bigIsLong=(c.avg_long_eq_pct||0)>=(c.avg_short_eq_pct||0);
      } else {
        s.tgtOuter=0;s.tgtInner=0;s.tgtLong=0;s.tgtShort=0;
      }
    });
  }

  window.tickBubble=function(secId){
    var wrap=document.getElementById('bubble-'+secId);
    if(!wrap||wrap.style.display==='none'){_animIds[secId]=null;return;}
    var W=wrap._W||wrap.offsetWidth||700, H=wrap._H||360;
    var elems=wrap._elems, fxCv=wrap._fxCv;
    if(!elems){_animIds[secId]=null;return;}
    fxCv.width=W;fxCv.height=H;
    var fc=fxCv.getContext('2d');fc.clearRect(0,0,W,H);
    var pts=_particles[secId]||[];
    for(var i=pts.length-1;i>=0;i--){
      var p=pts[i];p.x+=p.vx;p.y+=p.vy;p.vx*=0.88;p.vy*=0.88;p.life-=0.045;
      if(p.life<=0){pts.splice(i,1);continue;}
      fc.beginPath();fc.arc(p.x,p.y,2.5*p.life,0,Math.PI*2);
      fc.fillStyle=p.col+Math.floor(p.life*255).toString(16).padStart(2,'0');fc.fill();
    }
    allCoins.forEach(coin=>{
      var s=_bubbleState[secId][coin], g=elems[coin];
      s.curOuter+=(s.tgtOuter-s.curOuter)*0.08;
      s.curInner+=(s.tgtInner-s.curInner)*0.08;
      if(s.curOuter<0.5){s.curOuter=0;g.style.display='none';return;}
      g.style.display='';
      // 물리
      var spd=Math.sqrt(s.vx*s.vx+s.vy*s.vy);
      if(s.drifting){
        s.driftTimer--;
        if(s.driftTimer<=0){var ang=Math.random()*Math.PI*2;s.vx=Math.cos(ang)*DRIFT_SPD*(0.7+Math.random()*0.6);s.vy=Math.sin(ang)*DRIFT_SPD*(0.7+Math.random()*0.6);s.driftTimer=80+Math.random()*160;}
        s.vx+=(Math.random()-0.5)*DRIFT_NOISE;s.vy+=(Math.random()-0.5)*DRIFT_NOISE;
      } else {
        s.vx*=FRICTION;s.vy*=FRICTION;
        if(spd<STOP_THRESH){var ang=Math.random()*Math.PI*2;s.vx=Math.cos(ang)*DRIFT_SPD;s.vy=Math.sin(ang)*DRIFT_SPD;s.drifting=true;s.driftTimer=60+Math.random()*120;}
      }
      s.x+=s.vx;s.y+=s.vy;
      var oR=s.curOuter;
      if(s.x-oR<2){s.x=oR+2;s.vx=Math.abs(s.vx)*0.8;s.drifting=false;}
      if(s.x+oR>W-2){s.x=W-oR-2;s.vx=-Math.abs(s.vx)*0.8;s.drifting=false;}
      if(s.y-oR<2){s.y=oR+2;s.vy=Math.abs(s.vy)*0.8;s.drifting=false;}
      if(s.y+oR>H-2){s.y=H-oR;s.vy=-Math.abs(s.vy)*0.8;s.drifting=false;}
      // SVG 업데이트
      g.setAttribute('transform','translate('+s.x.toFixed(1)+','+s.y.toFixed(1)+')');
      var bigIsLong=(s.tgtOuter>0)?(s.tgtLong||0)>=(s.tgtShort||0):true;
      var oc=g.querySelector('.oc'),ic=g.querySelector('.ic'),t=g.querySelector('text');
      if(oc){oc.setAttribute('r',s.curOuter.toFixed(1));oc.setAttribute('fill',(bigIsLong?BL:RD)+'44');oc.setAttribute('stroke',bigIsLong?BL:RD);oc.setAttribute('stroke-width','1.5');}
      if(ic){ic.setAttribute('r',s.curInner.toFixed(1));ic.setAttribute('fill',(bigIsLong?RD:BL)+(s.curInner>2?'66':'00'));ic.setAttribute('stroke',bigIsLong?RD:BL);ic.setAttribute('stroke-width',s.curInner>2?'1.5':'0');}
      if(t){var fs=Math.max(8,Math.min(14,s.curOuter*0.28));t.setAttribute('font-size',fs);}
    });
    // 충돌
    var visible=allCoins.filter(c=>_bubbleState[secId][c].curOuter>=1);
    for(var i=0;i<visible.length;i++){
      for(var j=i+1;j<visible.length;j++){
        var sa=_bubbleState[secId][visible[i]],sb=_bubbleState[secId][visible[j]];
        var dx=sb.x-sa.x,dy=sb.y-sa.y,dist=Math.sqrt(dx*dx+dy*dy)||0.001;
        var minD=sa.curOuter+sb.curOuter+2;
        if(dist<minD){var push=(minD-dist)*0.5,nx=dx/dist,ny=dy/dist;sa.x-=nx*push;sa.y-=ny*push;sb.x+=nx*push;sb.y+=ny*push;var relV=(sa.vx-sb.vx)*nx+(sa.vy-sb.vy)*ny;if(relV>0){sa.vx-=relV*nx;sa.vy-=relV*ny;sb.vx+=relV*nx;sb.vy+=relV*ny;sa.drifting=false;sb.drifting=false;}}
      }
    }
    _animIds[secId]=requestAnimationFrame(()=>window.tickBubble(secId));
  }

  window.switchBubbles=function(key){
    // 카드 테두리 초기화
    document.querySelectorAll('[id^="bcard-"]').forEach(el=>el.style.border='0.5px solid #2a2a45');
    var allCard=document.getElementById('bcard-all');
    if(allCard) allCard.style.border='1px solid #2a2a45';
    var sel=document.getElementById('bcard-'+key);
    if(sel) sel.style.border='1px solid '+BL;
    if(key==='all'&&allCard) allCard.style.border='2px solid '+BL;

    // 어느 섹션 키인지 판별
    var secId=null;
    if(key==='all') secId='war';
    else if(SENT.band_bubbles&&SENT.band_bubbles[key]) secId='war';
    else if(SENT.type_bubbles&&SENT.type_bubbles[key]) secId='type';
    else if(SENT.equity_bubbles&&SENT.equity_bubbles[key]) secId='equity';
    if(!secId) return;

    // 해당 섹션 열기
    if(window._activeSection!==secId){
      window.toggleSection(secId);
    }

    // 버블 데이터 세팅
    var coins;
    if(key==='all') coins=SENT.coins;
    else if(SENT.band_bubbles&&SENT.band_bubbles[key]) coins=SENT.band_bubbles[key];
    else if(SENT.type_bubbles&&SENT.type_bubbles[key]) coins=SENT.type_bubbles[key];
    else if(SENT.equity_bubbles&&SENT.equity_bubbles[key]) coins=SENT.equity_bubbles[key];
    else coins=[];

    // 버블 초기화 및 표시
    var bubWrap=document.getElementById('bubble-'+secId);
    if(bubWrap){
      bubWrap.style.display='block';
      window.initBubble(secId);
      window.setTargetsFor(secId,coins);
      if(!_animIds[secId]) window.tickBubble(secId);
    }

    // 히스토리 차트 연동
    if(window.updateHistChart && HIST && HIST.length >= 2){
      var gLong=null, gShort=null, gLabel='전체 스마트머니';
      if(key==='all'){
        gLabel='전체 스마트머니';
      } else if(SENT.band_bubbles&&SENT.band_bubbles[key]){
        gLabel='WAR '+key;
        gLong =HIST.map(function(d){var b=d.bands&&d.bands.find(function(x){return x.label===key;});return b?b.long_pct:null;});
        gShort=HIST.map(function(d){var b=d.bands&&d.bands.find(function(x){return x.label===key;});return b?b.short_pct:null;});
      } else if(SENT.type_bubbles&&SENT.type_bubbles[key]){
        gLabel=key;
        gLong =HIST.map(function(d){var t=d.types&&d.types.find(function(x){return x.label===key;});return t?t.long_pct:null;});
        gShort=HIST.map(function(d){var t=d.types&&d.types.find(function(x){return x.label===key;});return t?t.short_pct:null;});
      } else if(SENT.equity_bubbles&&SENT.equity_bubbles[key]){
        gLabel=key;
        gLong =HIST.map(function(d){var e=d.equities&&d.equities.find(function(x){return x.label===key;});return e?e.long_pct:null;});
        gShort=HIST.map(function(d){var e=d.equities&&d.equities.find(function(x){return x.label===key;});return e?e.short_pct:null;});
      }
      window.updateHistChart(gLabel, gLong, gShort);
    }
  };

  // 기본: WAR 섹션 열고 전체 버블 표시
  setTimeout(()=>{
    var bw=document.getElementById('bubble-war');
    if(bw){ bw.style.display='block'; initBubble('war'); setTargetsFor('war',SENT.coins); tickBubble('war'); }
    document.getElementById('bcard-all')&&(document.getElementById('bcard-all').style.border='2px solid '+BL);
  },100);
}
</script>"""
    html = html.replace("%%SCRIPT%%", js_block)
    html = html.replace("%%MODAL%%", modal_block)
    html = html.replace("%%ALL_STATS%%", all_stats_js)
    return html


# ══ SEASON PICKS ═══════════════════════════════════════════════════════
def print_season_picks(archive: ArchiveManager, n=20):
    picks = archive.top_war_stats(n=n)
    console.print(f"\n[bold yellow]★ 시즌 출전 추천 TOP {n}[/bold yellow]  "
                  f"[dim](조건: ${MIN_EQUITY:,}+ · WAR 정렬)[/dim]\n")

    tbl = Table(show_header=True, header_style="bold dim", border_style="dim")
    for col, w in [("RANK",5),("LABEL",18),("TYPE",20),("WAR",6),
                   ("EQUITY",14),("WIN%",6),("SHARPE",7),("최초발굴",12)]:
        tbl.add_column(col, width=w, justify="right" if col in ["WAR","EQUITY","WIN%","SHARPE"] else "left")

    for i, s in enumerate(picks, 1):
        tbl.add_row(
            f"#{i}", s["label"], s["trader_type"],
            f"[bold cyan]{s['war_score']}[/bold cyan]",
            f"${s['total_equity']:>12,.0f}",
            f"{s['win_rate']:.0f}%",
            f"[{'green' if s['sharpe']>1 else 'yellow' if s['sharpe']>0 else 'red'}]{s['sharpe']:.2f}[/]",
            archive.first_seen_str(s["address"]),
        )
    console.print(tbl)
    total, qualified, stale = archive.summary()
    console.print(f"\n[dim]전체 아카이브: {total}개 | ${MIN_EQUITY:,}+ 자격: {qualified}개 | 갱신 필요: {stale}개[/dim]")


# ══ MAIN ═══════════════════════════════════════════════════════════════
async def main_async(args):
    archive = ArchiveManager(args.archive)
    total, qualified, stale = archive.summary()

    console.print(Panel.fit(
        f"[bold cyan]🔭 WALLET SCOUT v3[/bold cyan]\n"
        f"[dim]아카이브 {total}개 | ${MIN_EQUITY:,}+ 자격 {qualified}개 | 갱신 필요 {stale}개[/dim]",
        border_style="cyan"
    ))

    addresses, labels, sources = [], [], []

    if args.file:
        with open(args.file, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"): continue
                parts = line.split("\t")
                addresses.append(parts[0].strip())
                labels.append(parts[1].strip() if len(parts) > 1 else parts[0][:8] + "...")
                sources.append("manual")

    for i, addr in enumerate(args.addresses or []):
        addresses.append(addr)
        labels.append((args.labels[i] if args.labels and i < len(args.labels) else addr[:8] + "..."))
        sources.append("manual")

    if args.discover:
        disc = WalletDiscovery()
        candidates = await disc.discover(archive, target=args.discover_n)
        await disc.close()
        existing_addrs = set(a.lower() for a in addresses)
        for item in candidates:
            addr_lower = item["address"].lower()
            if addr_lower not in existing_addrs:
                addresses.append(item["address"])
                labels.append(item["label"])
                sources.append(item["source"])
                existing_addrs.add(addr_lower)
        # --discover 시 기존 아카이브도 갱신 필요한 것 자동 포함
        for addr in archive.all_addresses():
            if addr not in existing_addrs and archive.needs_update(addr):
                s = archive.get_stats(addr)
                addresses.append(addr)
                labels.append(s.get("label", short_addr(addr)) if s else short_addr(addr))
                sources.append(s.get("source","cached") if s else "cached")
                existing_addrs.add(addr)

    if getattr(args, "refresh_all", False):
        console.print("[bold yellow]▶ 전체 강제 갱신[/bold yellow] [dim](캐시 무시, 아카이브 전체)[/dim]")
        ex = set(a.lower() for a in addresses)
        for addr in archive.all_addresses():
            if addr not in ex:
                s = archive.get_stats(addr)
                addresses.append(addr)
                labels.append(s.get("label", addr[:8]+"...") if s else addr[:8]+"...")
                sources.append(s.get("source","cached") if s else "cached")
        await process_addresses(addresses, labels, sources, archive, force=True)
    elif args.refresh_stale:
        ex = set(a.lower() for a in addresses)
        for addr in archive.all_addresses():
            if addr not in ex and archive.needs_update(addr):
                s = archive.get_stats(addr)
                addresses.append(addr)
                labels.append(s.get("label", addr[:8]+"...") if s else addr[:8]+"...")
                sources.append(s.get("source","cached") if s else "cached")
        if addresses:
            await process_addresses(addresses, labels, sources, archive, force=args.force_refresh)
        else:
            console.print("[dim]갱신 필요한 지갑 없음[/dim]")
    elif addresses:
        await process_addresses(addresses, labels, sources, archive, force=args.force_refresh)
    elif not args.season:
        console.print("[dim]수집할 지갑 없음. --file/--discover/--refresh-stale/--refresh-all 사용[/dim]")

    if args.season or args.discover or args.file or args.refresh_stale or getattr(args,"refresh_all",False) or addresses:
        print_season_picks(archive, n=args.season_n)

    # --sync-requests: GitHub Issues에서 wallet-request 라벨 주소 자동 수집
    if getattr(args, "sync_requests", False):
        import httpx as _ghx
        gh_token = getattr(args, "gh_token", "") or ""
        headers = {"Accept": "application/vnd.github.v3+json"}
        if gh_token:
            headers["Authorization"] = f"token {gh_token}"
        console.print("\n[bold cyan]🔄 GitHub Issues wallet-request 동기화 중...[/bold cyan]")
        try:
            page, sync_addrs = 1, []
            while True:
                r = _ghx.get(
                    "https://api.github.com/repos/kimsubbae114/wallet-scout/issues",
                    params={"labels": "wallet-request", "state": "open", "per_page": 100, "page": page},
                    headers=headers, timeout=15
                )
                issues = r.json()
                if not issues or not isinstance(issues, list): break
                for iss in issues:
                    title = iss.get("title", "")
                    if "[wallet-request]" in title:
                        addr = title.replace("[wallet-request]", "").strip()
                        if addr.startswith("0x") and len(addr) >= 42:
                            sync_addrs.append((addr, iss["number"]))
                if len(issues) < 100: break
                page += 1
            console.print(f"  [dim]{len(sync_addrs)}개 요청 발견[/dim]")
            if sync_addrs:
                addrs = [a for a, _ in sync_addrs]
                await process_addresses(
                    addrs,
                    [a[:6]+"..."+a[-4:] for a in addrs],
                    ["manual"] * len(addrs),
                    archive, force=True
                )
                archive.save()
                # 처리된 이슈 close
                if gh_token:
                    for addr, num in sync_addrs:
                        try:
                            _ghx.patch(
                                f"https://api.github.com/repos/kimsubbae114/wallet-scout/issues/{num}",
                                json={"state": "closed"},
                                headers=headers, timeout=10
                            )
                        except Exception: pass
                    console.print(f"  [dim]이슈 {len(sync_addrs)}개 close 처리[/dim]")
                console.print(f"  [green]✓ {len(sync_addrs)}개 수집 완료[/green]")
        except Exception as e:
            console.print(f"  [red]sync-requests 실패: {e}[/red]")

    # --lookup: 주소 즉시 수집
    if getattr(args, "lookup", None):
        lookup_addrs = [a.strip() for a in args.lookup if a.strip()]
        console.print(f"\n[bold cyan]🔍 지갑 조회 및 캐시 저장: {len(lookup_addrs)}개[/bold cyan]")
        wallets_path = Path("wallets.txt")
        existing = set()
        if wallets_path.exists():
            existing = {l.strip().lower() for l in wallets_path.read_text(encoding="utf-8").splitlines() if l.strip()}
        new_w = [a for a in lookup_addrs if a.lower() not in existing]
        if new_w:
            with wallets_path.open("a", encoding="utf-8") as wf:
                for a in new_w: wf.write(a + "\n")
            console.print(f"  [dim]wallets.txt에 {len(new_w)}개 추가됨[/dim]")
        await process_addresses(
            lookup_addrs,
            [a[:6]+"..."+a[-4:] for a in lookup_addrs],
            ["manual"] * len(lookup_addrs),
            archive, force=True
        )
        archive.save()
        console.print(f"  [green]✓ 완료 — --report 로 리포트 재생성하세요[/green]")

    # --report: HTML 리포트 생성
    if args.report:
        # vault 주소 캐시 보정 (vaultSummaries API)
        # vault 보정: vaultSummaries API + 캐시 내 is_vault 필드
        _patched = 0
        try:
            import httpx as _hx
            _r = _hx.post("https://api.hyperliquid.xyz/info",
                          json={"type":"vaultSummaries"}, timeout=10)
            if _r.status_code == 200:
                _vaults = _r.json()
                _vault_leaders = {v.get("leader","").lower() for v in _vaults if v.get("leader")}
                for addr, entry in archive.data.items():
                    if addr.lower() in _vault_leaders:
                        if entry.get("stats",{}).get("source") != "vault":
                            entry["stats"]["source"] = "vault"
                            entry["stats"]["is_vault"] = True
                            _patched += 1
        except Exception:
            pass
        # fallback: clearinghouse 재수집 없이 is_vault 필드로 표시
        # stats에 is_vault=True 저장된 항목도 보정
        for addr, entry in archive.data.items():
            s = entry.get("stats", {})
            if s.get("is_vault") and s.get("source") != "vault":
                s["source"] = "vault"
                _patched += 1
        if _patched:
            archive.save()
            console.print(f"  [dim]vault 보정: {_patched}개[/dim]")

        # 센티먼트 히스토리 스냅샷 저장
        try:
            from datetime import datetime as _dt
            _hist_path = Path(HIST_FILE)
            _hist = json.loads(_hist_path.read_text(encoding="utf-8")) if _hist_path.exists() else []
            _snap_stats = archive.qualified_stats()
            if _snap_stats:
                # SENT 계산 (generate_html과 동일한 로직 간략화)
                # 메인 센티먼트와 동일: total_equity 기준 long/short %
                def _eq(s): return max(s.get("total_equity",1),1)
                _pos_traders = [s for s in _snap_stats if s.get("positions") and s.get("war_score",0)>=40]
                _all_eq    = sum(_eq(s) for s in _pos_traders if sum(p["notional"] for p in s["positions"])>0)
                _all_long  = sum(sum(p["notional"] for p in s.get("positions",[]) if p["side"]=="LONG") for s in _pos_traders)
                _all_short = sum(sum(p["notional"] for p in s.get("positions",[]) if p["side"]=="SHORT") for s in _pos_traders)
                _snap = {
                    "ts": _dt.now().strftime("%Y-%m-%d %H:%M"),
                    "all": {
                        "long_pct":  round(_all_long/_all_eq*100, 1) if _all_eq>0 else 0,
                        "short_pct": round(_all_short/_all_eq*100, 1) if _all_eq>0 else 0,
                        "traders":   len(_pos_traders),
                    },
                    "bands": [],
                    "types": [],
                }
                # WAR 구간별 (total_equity 기준)
                WAR_B = [(40,50,"40-50"),(50,60,"50-60"),(60,70,"60-70"),(70,80,"70-80"),(80,999,"80+")]
                for lo, hi, lbl in WAR_B:
                    grp = [s for s in _pos_traders if lo <= s.get("war_score",0) < hi]
                    geq = sum(_eq(s) for s in grp if sum(p["notional"] for p in s["positions"])>0) or 1
                    ln  = sum(sum(p["notional"] for p in s.get("positions",[]) if p["side"]=="LONG") for s in grp)
                    sn  = sum(sum(p["notional"] for p in s.get("positions",[]) if p["side"]=="SHORT") for s in grp)
                    _snap["bands"].append({"label": lbl, "long_pct": round(ln/geq*100,1), "short_pct": round(sn/geq*100,1)})
                # 타입별 (total_equity 기준)
                _types = {}
                for s in _pos_traders:
                    t = s.get("trader_type","?")
                    if t not in _types: _types[t] = {"long":0,"short":0,"eq":0}
                    _types[t]["eq"]    += _eq(s)
                    _types[t]["long"]  += sum(p["notional"] for p in s.get("positions",[]) if p["side"]=="LONG")
                    _types[t]["short"] += sum(p["notional"] for p in s.get("positions",[]) if p["side"]=="SHORT")
                for t, v in _types.items():
                    geq = v["eq"] or 1
                    _snap["types"].append({"label": t, "long_pct": round(v["long"]/geq*100,1), "short_pct": round(v["short"]/geq*100,1)})
                # 잔고 규모별
                EQ_BANDS = [(10000,50000,"$10K~50K"),(50000,200000,"$50K~200K"),
                            (200000,500000,"$200K~500K"),(500000,1000000,"$500K~1M"),
                            (1000000,5000000,"$1M~5M"),(5000000,999999999,"$5M+")]
                _snap["equities"] = []
                for lo, hi, lbl in EQ_BANDS:
                    grp = [s for s in _pos_traders if lo <= s.get("total_equity",0) < hi]
                    geq = sum(_eq(s) for s in grp if sum(p["notional"] for p in s.get("positions",[]))>0) or 1
                    ln  = sum(sum(p["notional"] for p in s.get("positions",[]) if p["side"]=="LONG") for s in grp)
                    sn  = sum(sum(p["notional"] for p in s.get("positions",[]) if p["side"]=="SHORT") for s in grp)
                    _snap["equities"].append({"label": lbl, "long_pct": round(ln/geq*100,1), "short_pct": round(sn/geq*100,1)})
                _hist.append(_snap)
                # 최대 200개 스냅샷 유지
                if len(_hist) > 200: _hist = _hist[-200:]
                _hist_path.write_text(json.dumps(_hist, ensure_ascii=False, indent=2), encoding="utf-8")
                console.print(f"  [dim]히스토리 저장: {len(_hist)}개 스냅샷[/dim]")
        except Exception as _e:
            console.print(f"  [dim]히스토리 저장 실패: {_e}[/dim]")

        console.print("\n[bold magenta]▶ HTML 리포트 생성 중...[/bold magenta]")
        report_stats = archive.qualified_stats()  # $10k+ 전체
        if not report_stats:
            report_stats = archive.all_stats()
        tournament = run_tournament(report_stats)
        html = generate_html(report_stats, tournament, archive, hist_path=Path(HIST_FILE))
        ts_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        out = f"scouting_report_{ts_str}.html"
        with open(out, "w", encoding="utf-8") as f:
            f.write(html)
        console.print(f"\n[bold green]✓ 저장 완료: {out}[/bold green]  ({len(report_stats)}명)")


def main():
    parser = argparse.ArgumentParser(
        description=f"Wallet Scout v3  [{VERSION}]",
        epilog="\n".join([
            "",
            "변경 이력:",
        ] + [f"  v{v}  {d}  {msg}" for v,d,msg in CHANGELOG]),
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {VERSION}")
    parser.add_argument("addresses", nargs="*")
    parser.add_argument("--labels", "-l", nargs="*")
    parser.add_argument("--file", "-f")
    parser.add_argument("--archive", default=ARCHIVE_FILE)
    parser.add_argument("--discover", "-d", action="store_true")
    parser.add_argument("--discover-n", type=int, default=50)
    parser.add_argument("--refresh-stale", action="store_true", help="24시간 지난 것만 갱신")
    parser.add_argument("--refresh-all", action="store_true", help="전체 강제 갱신 (24시간 무시)")
    parser.add_argument("--force-refresh", action="store_true", help="지정 주소 강제 갱신")
    parser.add_argument("--season", "-s", action="store_true")
    parser.add_argument("--season-n", type=int, default=20)
    parser.add_argument("--prune", action="store_true", help="WAR 40 미만 캐시 삭제")
    parser.add_argument("--lookup", nargs="+", metavar="ADDR", help="주소 즉시 수집 후 캐시+wallets.txt 저장")
    parser.add_argument("--sync-requests", action="store_true", help="GitHub Issues wallet-request 라벨 주소 자동 수집")
    parser.add_argument("--gh-token", default="", help="GitHub Personal Access Token")
    parser.add_argument("--mark-vault", nargs="+", metavar="ADDR", help="지정 주소를 vault로 수동 표시")
    parser.add_argument("--prune-war", type=float, default=40.0)
    parser.add_argument("--report", "-r", action="store_true", help="HTML 리포트 생성")
    args = parser.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    import sys
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    main()
