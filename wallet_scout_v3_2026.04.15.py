# -*- coding: utf-8 -*-
#!/usr/bin/env python3
# ─────────────────────────────────────────────────────
# WALLET SCOUT v3  —  Hyperliquid Trader 아카이브
# ─────────────────────────────────────────────────────
VERSION = "4.3.1"
SCHEMA_VERSION = "3"  # v3: durability removed from radar/classification  # Bump when WAR formulas change to force cache refresh

def _he(s):
    """HTML-escape value for safe insertion into HTML."""
    return str(s).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;").replace('"','&quot;').replace("'","&#39;")

CHANGELOG = [
    ("3.9.3", "2026-03-28", "UnboundLocalError 수정, $10k 미만 자동 제외"),
    ("3.9.2", "2026-03-28", "Portfolio 표시, Positions없을때 upnl안내, uPnL 레이블 확화"),
    ("3.9.1", "2026-03-28", "Positions: 실현/현재 구역 분리, 롱숏바+넷익스포저, 소액제외"),
    ("3.9.0", "2026-03-28", "현재 오픈 Positions Card 표시 (코인/방향/uPnL손익/레버리지)"),
    ("3.8.2", "2026-03-28", "source: vault 자동감지, compute_stats에 src 파라미터"),
    ("3.8.1", "2026-03-28", "source 표시: manual→직접Register, active→활성발굴, vault→Vault발굴"),
    ("3.8.0", "2026-03-28", "코인태그: @숫자 필터링, ▲▼ 롱숏 방향 표시"),
    ("3.7.2", "2026-03-28", "배치수집+재시도+WAR40제외 재적용 (누락버그 수정)"),
    ("3.7.1", "2026-03-28", "샤프: gap을 마지막 거래일 fill 1건으로 추가 (균등분산→집중 방식)"),
    ("3.7.0", "2026-03-28", "샤프: gap 거래일 균등분산. 중립값(0/50%) → 스탯 30점"),
    ("3.6.0", "2026-03-28", "샤프 계산 버그 수정: uPnL손익 반영, total_pnl 부호 보정"),
    ("3.5.0", "2026-03-28", "빅벳 50% 기준 최솟값, power 스케일 적용"),
    ("3.4.0", "2026-03-28", "WAR 40 미만 자동 제외 + --prune 옵션"),
    ("3.3.0", "2026-03-28", "Win Rate 육각형 스탯 추가, WAR 가중치 재배분"),
    ("3.2.0", "2026-03-28", "Durability 공식 개편: 기간40%+Consistency60%"),
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
WAR_HIST_FILE = "war_history.json"
EXCLUDED_FILE = "war_excluded.json"   # WAR 40 미만 backoff 관리
META_FILE    = "wallets_meta.json"    # 커스텀 태그/이름/링크
CACHE_TTL    = timedelta(hours=6)
MIN_EQUITY   = 50_000
_raw_console = Console()

def short_addr(addr: str) -> str:
    """0x1234...5678 — 앞 4자리 + ... + 뒤 4자리"""
    a = addr.lower()
    if a.startswith("0x") and len(a) >= 10:
        return f"0x{a[2:6]}...{a[-4:]}"
    return f"{a[:4]}...{a[-4:]}" if len(a) >= 8 else a

def fmt_compact(v: float) -> str:
    """숫자를 K/M 단위로 압축. 예: 1203698 → +$1.2M, -45000 → -$45K"""
    sign = "+" if v >= 0 else "-"
    av = abs(v)
    if av >= 1_000_000:
        return f"{sign}${av/1_000_000:.1f}M"
    if av >= 1_000:
        return f"{sign}${av/1_000:.0f}K"
    return f"{sign}${av:.0f}"
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



# ══ EXCLUDED MANAGER ═══════════════════════════════════════════════════
# WAR 40 미만으로 제외된 지갑의 backoff 관리
# 제외 횟수에 따라 점차 체크 간격을 늘림 (최대 30일)
BACKOFF_DAYS = {1:1, 2:3, 3:7, 4:14, 5:21, 6:30, 7:30, 8:30, 9:30, 10:30}
BACKOFF_MAX_DAYS = 30

class ExcludedManager:
    def __init__(self, path=None):
        self.path = Path(path or EXCLUDED_FILE)
        self.data = self._load()

    def _load(self):
        if self.path.exists():
            try:
                return json.loads(self.path.read_text(encoding="utf-8"))
            except Exception:
                return {}
        return {}

    def save(self):
        self.path.write_text(
            json.dumps(self.data, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )

    def should_skip(self, address: str) -> bool:
        """next_check_at이 아직 안 지났으면 True (스킵)"""
        entry = self.data.get(address.lower())
        if not entry:
            return False
        next_check = entry.get("next_check_at")
        if not next_check:
            return False
        try:
            nc = datetime.fromisoformat(next_check)
            if nc.tzinfo is None:
                nc = nc.replace(tzinfo=timezone.utc)
            return datetime.now(tz=timezone.utc) < nc
        except Exception:
            return False

    def skip_info(self, address: str) -> str:
        """로그용 스킵 정보 문자열"""
        entry = self.data.get(address.lower())
        if not entry:
            return ""
        cnt    = entry.get("exclude_count", 0)
        nc     = entry.get("next_check_at", "")[:10]
        war    = entry.get("last_war", 0)
        reason = entry.get("reason", "war")
        reason_str = "WAR 미달" if reason == "war" else "Equity 부족"
        return f"{reason_str} (WAR {war:.1f}) · 제외 {cnt}회 · 다음체크 {nc}"

    def record_exclusion(self, address: str, war: float, reason: str = "war"):
        """WAR 40 미만 또는 equity 부족으로 제외될 때 호출 — backoff 갱신"""
        key = address.lower()
        now = datetime.now(tz=timezone.utc)
        cnt = self.data.get(key, {}).get("exclude_count", 0) + 1
        days = BACKOFF_DAYS.get(cnt, BACKOFF_MAX_DAYS)
        next_check = (now + timedelta(days=days)).isoformat()
        self.data[key] = {
            "exclude_count":  cnt,
            "last_excluded_at": now.isoformat(),
            "next_check_at":  next_check,
            "last_war":       round(war, 1),
            "reason":         reason,   # "war" | "equity"
        }

    def clear(self, address: str):
        """WAR 40 이상으로 통과 시 backoff 초기화"""
        self.data.pop(address.lower(), None)

    def summary(self) -> dict:
        """현재 backoff 중인 지갑 통계"""
        now = datetime.now(tz=timezone.utc)
        active = 0
        for entry in self.data.values():
            nc = entry.get("next_check_at", "")
            try:
                t = datetime.fromisoformat(nc)
                if t.tzinfo is None:
                    t = t.replace(tzinfo=timezone.utc)
                if now < t:
                    active += 1
            except Exception:
                pass
        return {"total": len(self.data), "active_skip": active}

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
        """CACHE_TTL expired, first-seen wallet, or schema version mismatch → True"""
        entry = self.data.get(address.lower())
        if not entry:
            return True
        if entry.get("schema_ver") != SCHEMA_VERSION:
            return True
        try:
            fetched_at = datetime.fromisoformat(entry["fetched_at"])
            return datetime.now(tz=timezone.utc) - fetched_at > CACHE_TTL
        except Exception:
            return True

    def is_new(self, address: str) -> bool:
        return address.lower() not in self.data

    def get_stats(self, address: str):
        entry = self.data.get(address.lower())
        return entry["stats"] if entry else None

    def upsert(self, address: str, stats: dict):
        """
        New wallet: record first_discovered_at.
        Existing wallet: keep first_discovered_at, update stats + schema_ver.
        prev_positions: 24시간 이상 지난 스냅샷만 교체 (최근 변화 추적용)
        """
        key = address.lower()
        now = datetime.now(tz=timezone.utc).isoformat()
        if key not in self.data:
            self.data[key] = {
                "first_discovered_at": now,
                "fetched_at": now,
                "schema_ver": SCHEMA_VERSION,
                "stats": stats,
            }
        else:
            prev_entry = self.data[key]
            prev_stats = prev_entry.get("stats", {})
            # prev_positions 갱신: 24시간 이상 지난 경우 현재→prev로 이동
            prev_ts = prev_stats.get("prev_positions_ts")
            cur_positions = prev_stats.get("positions", [])
            should_rotate = True
            if prev_ts:
                try:
                    t = datetime.fromisoformat(prev_ts)
                    if t.tzinfo is None:
                        t = t.replace(tzinfo=timezone.utc)
                    should_rotate = (datetime.now(tz=timezone.utc) - t).total_seconds() >= 86400
                except Exception:
                    should_rotate = True
            if should_rotate and cur_positions:
                stats["prev_positions"]    = cur_positions
                stats["prev_positions_ts"] = prev_entry.get("fetched_at", now)
            else:
                # 24시간 안 지났으면 기존 prev 유지
                stats["prev_positions"]    = prev_stats.get("prev_positions", [])
                stats["prev_positions_ts"] = prev_stats.get("prev_positions_ts", "")
            self.data[key]["fetched_at"]  = now
            self.data[key]["schema_ver"]  = SCHEMA_VERSION
            self.data[key]["stats"]       = stats

    def age_str(self, address: str) -> str:
        entry = self.data.get(address.lower())
        if not entry:
            return "신규"
        fetched_at = datetime.fromisoformat(entry["fetched_at"])
        delta = datetime.now(tz=timezone.utc) - fetched_at
        if delta.total_seconds() < 60:
            return "just now"
        if delta.seconds < 3600 and delta.days == 0:
            return f"{delta.seconds//60}m ago"
        if delta.days == 0:
            return f"{delta.seconds//3600}h ago"
        return f"{delta.days}d ago"

    def first_seen_str(self, address: str) -> str:
        entry = self.data.get(address.lower())
        if not entry:
            return "-"
        ts = entry.get("first_discovered_at", entry.get("fetched_at", "-"))
        return ts[:10]

    def prune_low_war(self, min_war=50.0, min_equity=10_000):
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

    def qualified_stats(self, min_equity=MIN_EQUITY, min_war=50.0):
        return [s for s in self.all_stats()
                if s.get("total_equity", 0) >= min_equity and s.get("war_score", 0) >= min_war]

    def top_war_stats(self, n=20, min_equity=MIN_EQUITY):
        """시즌 추천: $min_equity 이상 WAR 상위 N"""
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
    Layer 2 (활성 Trader): 매번 랜덤 코인 조합 → 다양성
    Layer 3 (고WAR 이웃): 기존 고수와 거래한 지갑 → 숨겨진 고수 발굴
    """
    def __init__(self):
        self.http = httpx.AsyncClient(timeout=30.0)

    async def layer1_vaults(self, n=200):
        """stats-data.hyperliquid.xyz/Mainnet/vaults 로 TVL 상위 vault leader 수집
        (vaultSummaries API는 공식 문서에 있으나 항상 빈 배열 반환 — non-functional)
        """
        results = []
        seen = set()
        STATS_URL = "https://stats-data.hyperliquid.xyz/Mainnet/vaults"
        try:
            r = await self.http.get(STATS_URL, timeout=15)
            r.raise_for_status()
            raw = r.json()

            # 응답: list of vault objects
            if not isinstance(raw, list):
                console.print(f"  [yellow]⚠ stats-data vault 응답 이상: {type(raw)}[/yellow]")
                return results

            # TVL 필드: "tvl" (string) 또는 숫자
            # stats-data 구조: {"apr":..., "summary": {"name":..., "leader":..., "tvl":..., "isClosed":...}}
            def _summary(v):
                return v.get("summary") or v  # summary 중첩 또는 flat 모두 대응

            def _tvl(v):
                try: return float(_summary(v).get("tvl") or 0)
                except: return 0.0

            open_vaults = [v for v in raw
                           if not _summary(v).get("isClosed", False) and _tvl(v) > 10000]
            open_vaults.sort(key=_tvl, reverse=True)
            console.print(f"  [dim]stats-data vault 총 {len(raw)}개 중 TVL>10k: {len(open_vaults)}개[/dim]")

            for v in open_vaults:
                s = _summary(v)
                # vault address 우선 사용 (leader address 아님)
                vault_addr = s.get("vaultAddress", "") or s.get("vault_address", "")
                leader = s.get("leader", "")
                addr = vault_addr or leader
                if not addr or addr.lower() in seen:
                    continue
                seen.add(addr.lower())
                name = (s.get("name") or "").strip()
                label = name if name else short_addr(addr)
                results.append({"address": addr, "label": label, "source": "vault"})
                if len(results) >= n:
                    break
        except Exception as e:
            console.print(f"  [yellow]⚠ Layer1(stats-data vaults) 실패: {e}[/yellow]")
        return results

    async def layer1b_leaderboard(self, n=50):
        """[DEPRECATED - not used in discover()] leaderboard API로 상위 Trader 수집"""
        results = []
        try:
            r = await self.http.post(HL_API_URL, json={"type": "leaderboard"})
            r.raise_for_status()
            data = r.json()
            entries = data if isinstance(data, list) else data.get("leaderboardRows", [])
            console.print(f"  [dim]leaderboard {len(entries)}[/dim]")
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
        """[DEPRECATED - not used in discover()] 고WAR 지갑의 fills에서 거래 상대방 주소 추출"""
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
                    "type": "userFills", "user": addr
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

    async def discover(self, archive: ArchiveManager, target=150, excluded: "ExcludedManager | None" = None):
        console.print(f"\n[bold cyan]▶ DISCOVERY[/bold cyan] [dim]발굴 중...[/dim]")
        l1, l2 = await asyncio.gather(
            self.layer1_vaults(200),
            self.layer2_active(300),
        )
        console.print(f"  L1(VaultSummaries):{len(l1)}  L2(ActiveTrades):{len(l2)}  [Total candidates: {len(l1)+len(l2)}]")

        seen, results, existing = set(), [], set(archive.all_addresses())
        for item in l1 + l2:
            addr = item["address"].lower()
            if addr not in seen:
                seen.add(addr)
                results.append({**item, "is_new": addr not in existing})

        new_count = sum(1 for r in results if r["is_new"])
        console.print(f"  신규: [green]{new_count}개[/green]  기존 재확인: [dim]{len(results)-new_count}개[/dim]")

        # backoff 중인 지갑 미리 제외 → target 잘라도 실제 수집 가능한 것만 포함
        if excluded:
            before = len(results)
            results_active  = [r for r in results if not excluded.should_skip(r["address"])]
            results_backoff = [r for r in results if excluded.should_skip(r["address"])]
            skipped = before - len(results_active)
            if skipped:
                console.print(f"  [dim]backoff 제외: {skipped}개 → 실수집 후보 {len(results_active)}개[/dim]")
            results = results_active

        # target 적용: 신규 우선, 그 다음 기존 갱신 필요한 것
        new_ones = [r for r in results if r["is_new"]]
        old_ones = [r for r in results if not r["is_new"]]
        trimmed  = (new_ones + old_ones)[:target]
        console.print(f"  후보 {len(results)}개 → API 대상 최대 {target}개로 제한")
        return trimmed

    async def close(self):
        await self.http.aclose()


# ══ API ════════════════════════════════════════════════════════════════
class HyperliquidAPI:
    def __init__(self):
        self.http = httpx.AsyncClient(timeout=30.0)

    async def get_clearinghouse(self, addr):
        r = await self.http.post(HL_API_URL, json={"type": "clearinghouseState", "user": addr})
        r.raise_for_status()
        return r.json()

    async def get_vault_name(self, addr):
        """vault 주소면 vault 이름 반환, 아니면 None"""
        info = await self.get_vault_info(addr)
        return info.get("name") if info else None

    async def get_vault_info(self, addr):
        """vault 상세정보 반환: name, leader 주소 등"""
        try:
            r = await self.http.post(HL_API_URL, json={"type": "vaultDetails", "vaultAddress": addr})
            r.raise_for_status()
            d = r.json()
            if isinstance(d, dict) and d.get("name"):
                return {"name": d["name"], "leader": d.get("leader", "")}
        except:
            pass
        return None

    async def get_spot(self, addr):
        try:
            r = await self.http.post(HL_API_URL, json={"type": "spotClearinghouseState", "user": addr})
            r.raise_for_status()
            return r.json()
        except:
            return {}

    async def get_spot_prices(self):
        """spot 코인별 USD 가격 반환"""
        try:
            r = await self.http.post(HL_API_URL, json={"type": "spotMetaAndAssetCtxs"})
            r.raise_for_status()
            data = r.json()
            prices = {}
            if isinstance(data, list) and len(data) >= 2:
                tokens = data[0].get("tokens", [])
                ctxs   = data[1]
                for i, ctx in enumerate(ctxs):
                    if i < len(tokens):
                        coin = tokens[i].get("name", "")
                        px   = float(ctx.get("midPx", 0) or 0)
                        if coin and px > 0:
                            prices[coin] = px
            return prices
        except:
            return {}

    async def get_fills(self, addr):
        for attempt in range(3):
            try:
                r = await self.http.post(HL_API_URL, json={
                    "type": "userFills", "user": addr, "aggregateByTime": True
                })
                r.raise_for_status()
                d = r.json()
                if isinstance(d, list):
                    return d
                # 빈 응답이지만 요청 자체는 성공 → 진짜 빈 fills
                return []
            except Exception as e:
                err = str(e)
                if "429" in err or "rate" in err.lower():
                    # rate limit → 재시도 대기
                    await asyncio.sleep(5 * (attempt + 1))
                elif attempt == 2:
                    return None  # 실패 → None (빈 배열과 구분)
                else:
                    await asyncio.sleep(1)
        return None  # 최종 실패

    async def fetch(self, addr):
        ch, fills, spot = await asyncio.gather(
            self.get_clearinghouse(addr),
            self.get_fills(addr),
            self.get_spot(addr),
            return_exceptions=True
        )
        ch_data = ch if not isinstance(ch, Exception) else {}
        vault_name = None
        # vault면 이름만 가져오기 (fills는 입력 주소 그대로 사용 — lookup과 동일)
        if isinstance(ch_data, dict) and (ch_data.get("vaultEquity") or ch_data.get("isVault")):
            vault_info = await self.get_vault_info(addr)
            if vault_info:
                vault_name = vault_info.get("name")
        fills_data = fills if not isinstance(fills, Exception) else None
        return {
            "clearinghouse": ch_data,
            "fills": fills_data if fills_data is not None else [],
            "fills_ok": fills_data is not None,  # fills 수집 성공 여부
            "spot": spot if not isinstance(spot, Exception) else {},
            "vault_name": vault_name,
            "error": str(ch) if isinstance(ch, Exception) else None,
        }

    async def close(self):
        await self.http.aclose()


# ══ STATS ══════════════════════════════════════════════════════════════
def classify_trader_type(wr, sh, bbr, bbc, pf, mdd, pnl, roi, cc):
    """Single source of truth for trader type classification.
    Used by both compute_stats() and generate_html() pre-processing.
    5-stat based: win_rate, sharpe, big_bet_rate/count, profit_factor, mdd_pct, roi_pct
    """
    if pnl < 0:                                    return "💀 Underwater",       "Drowning in losses"
    elif mdd > 100:                                return "🌊 Degen",            "All in or liquidated"
    elif bbr > 60 and wr > 65:                     return "🦁 Apex Predator",    "Top of the food chain"
    elif wr > 72 and sh > 4:                       return "🦅 Precision Hunter", "Never misses a shot"
    elif sh > 8 and bbr < 30:                      return "🧊 Ice Quant",        "Cold, emotionless algo"
    elif pf > 3 and bbc > 3 and wr < 55:           return "🎯 Sniper",           "Few shots, big hits"
    elif mdd > 40 and pf > 1.5:                    return "🎰 High Roller",      "Goes big or goes home"
    elif bbr > 50 and bbc > 10:                    return "🎲 Bet Maker",        "Loves to bet big"
    elif wr > 60 and sh > 2 and roi > 10:          return "📊 All-Rounder",      "Balanced all-around"
    elif sh > 4 and roi > 10:                      return "📈 Momentum",         "Riding the momentum"
    elif wr > 65 and pf > 1.5:                     return "🎯 Steady Shot",      "Consistently on target"
    elif pf > 2.5 and bbr < 20:                    return "💰 Value Hunter",     "Wins on risk/reward ratio"
    elif wr > 55 and pf > 1.2 and pnl > 0:        return "📦 Consistent",       "Quietly stacking wins"
    elif pnl > 0 and wr > 50 and pf > 1.0:        return "⚙️ Grinder",          "Shows up every day"
    elif cc < 300:                                 return "🌱 Newcomer",         "Not enough data yet"
    else:                                          return "🌀 Drifter",          "Still finding a pattern"

def compute_stats(raw, address, label="", src="manual"):
    ch    = raw.get("clearinghouse", {})
    fills = raw.get("fills", [])
    # vault 이름 있으면 label로 사용
    vault_name = raw.get("vault_name")
    if vault_name and not label:
        label = vault_name
    elif vault_name:
        label = vault_name
    ms           = ch.get("marginSummary", {})
    # vault는 marginSummary.accountValue 대신 vaultEquity 사용
    vault_equity = float(ch.get("vaultEquity", 0) or 0)
    account_value = float(ms.get("accountValue", 0) or 0)
    total_equity = vault_equity if vault_equity > 0 else account_value
    margin_used  = float(ms.get("totalMarginUsed", 0))
    margin_pct   = (margin_used / total_equity * 100) if total_equity > 0 else 0
    # vaultEquity>0은 LP 예치자도 해당되어 오탐 발생 → isVault 명시 필드만 신뢰
    # vault 주소 판단은 process_addresses의 _vault_leaders 기준으로 처리
    if ch.get("isVault") is True:
        src = "vault"

    # spot 보유 현황 (표시용만, total_equity에는 합산 안 함)
    spot        = raw.get("spot", {})
    spot_holdings = []
    for bal in spot.get("balances", []):
        coin  = bal.get("coin", "")
        hold  = float(bal.get("total", 0))
        if hold <= 0:
            continue
        spot_holdings.append({"coin": coin, "amount": round(hold, 6), "usd": 0})
    # 필터 기준:
    # - BTC/ETH/SOL/HYPE 포함: 1개 이상
    # - USD 포함: 1000개 이상
    # - 나머지: 1000개 이상
    # 우선순위: BTC/ETH/SOL/HYPE > USD > 나머지, 최대 6개
    def _spot_filter(h):
        c = h["coin"].upper()
        amt = h["amount"]
        if any(k in c for k in ["BTC","ETH","SOL","HYPE"]):
            return amt >= 1
        return amt >= 1000
    def _spot_priority(h):
        c = h["coin"].upper()
        if any(k in c for k in ["BTC","ETH","SOL","HYPE"]): return 0
        if "USD" in c: return 1
        return 2
    spot_holdings = sorted(
        [h for h in spot_holdings if _spot_filter(h)],
        key=lambda x: (_spot_priority(x), -x["amount"])
    )[:6]
    # spot USDC/USDT를 total_equity에 합산 (lookup JS와 동일 기준)
    spot_raw = raw.get("spot", {})
    for bal in spot_raw.get("balances", []):
        coin = bal.get("coin","")
        if coin in ("USDC","USDT"):
            total_equity += float(bal.get("total",0))

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
    avg_loss     = abs(sum(float(f["closedPnl"]) for f in losses)/len(losses)) if losses else 0
    # profit_factor: no losses → very high (capped at 99), normal → avg_win/avg_loss
    profit_factor = round(avg_win / avg_loss, 2) if avg_loss > 0 else (99.0 if avg_win > 0 else 1.0)

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
    # → uPnL손익/펀딩비 등이 샤프 계산에 반영됨
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
        # Durability: 기간(55%) + Consistency(45%), 기간 기준일 90일
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

    def _bs(rate,cnt):  # big_bet: N/A→10, 50%이하→10, 50%↑ power 스케일
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

    # 5-stat radar: Profit, ROI, Big Bet, Sharpe, Win Rate (durability removed)
    radar={"profit_amt":round(_ps(total_pnl),1),
           "roi":        round(_rs(roi_pct,len(closed)),1),
           "big_bet":    round(_bs(big_bet_rate,big_bet_count),1),
           "sharpe":     round(_ss(sharpe,len(closed)),1),
           "win_rate":   round(_wr(win_rate,len(closed)),1)}

    # WAR 컴포넌트 기여도 계산 (가중치 × 점수)
    war_components = {
        "Profit":   round(radar["profit_amt"] * 0.25, 1),
        "ROI":      round(radar["roi"]         * 0.25, 1),
        "Big Bet":  round(radar["big_bet"]     * 0.15, 1),
        "Sharpe":   round(radar["sharpe"]      * 0.20, 1),
        "Win Rate": round(radar["win_rate"]    * 0.15, 1),
    }
    raw_war  = sum(war_components.values())
    war_score = round(raw_war, 1)

    # ── Followability Score ──────────────────────────────────────────
    # "잘하는 지갑"이 아닌 "따라가기 좋은 지갑" 점수
    # MDD낮음 20% · big_bet낮음 20% · consistency높음 25% · sample충분 15% · profit_factor높음 20%
    def _follow_score():
        # MDD: 낮을수록 좋음 (0%=100점, 50%+=0점)
        mdd_s   = max(0, 1 - mdd_pct / 50) * 20

        # Big Bet Rate: 낮을수록 안정적 (0%=100점, 60%+=0점)
        bb_s    = max(0, 1 - big_bet_rate / 60) * 20

        # Consistency: 높을수록 좋음 (0=0점, 80+=100점)
        con_s   = min(1, consistency / 80) * 25

        # Sample: 거래 수 충분한지 (300개+ = 만점)
        samp_s  = min(1, len(closed) / 300) * 15

        # Profit Factor: 높을수록 좋음 (1=0점, 3+=100점)
        pf_capped = min(profit_factor, 10)  # 99.0 등 이상치 제거
        pf_s    = max(0, min(1, (pf_capped - 1) / 2)) * 20

        return round(mdd_s + bb_s + con_s + samp_s + pf_s, 1)

    follow_score = _follow_score()

    # ── Trader Type 분류 — classify_trader_type() 단일 소스 사용 ──
    trader_type, character = classify_trader_type(
        wr=win_rate, sh=sharpe, bbr=big_bet_rate, bbc=big_bet_count,
        pf=profit_factor, mdd=mdd_pct, pnl=total_pnl,
        roi=roi_pct, cc=len(closed)
    )

    # ── 분류 근거 (Why this type?) ──────────────────────────────────
    def _make_type_reasons():
        t = trader_type
        reasons = []
        if "Underwater" in t:
            reasons.append(f"Total PnL negative (${total_pnl:,.0f})")
        elif "Degen" in t:
            reasons.append(f"PnL Drawdown {mdd_pct:.0f}% > 100%")
            reasons.append(f"{big_bet_count} large positions taken")
        elif "Apex Predator" in t:
            reasons.append(f"Big Bet Rate {big_bet_rate:.0f}% > 60%")
            reasons.append(f"Win Rate {win_rate:.0f}% > 65%")
        elif "Precision Hunter" in t:
            reasons.append(f"Win Rate {win_rate:.0f}% > 72%")
            reasons.append(f"Sharpe* {sharpe:.2f} > 4")
        elif "Ice Quant" in t:
            reasons.append(f"Sharpe* {sharpe:.2f} > 8")
            reasons.append(f"Big Bet Rate {big_bet_rate:.0f}% < 30%")
        elif "Sniper" in t:
            reasons.append(f"Profit Factor {profit_factor:.1f}x > 3")
            reasons.append(f"{big_bet_count} big bets placed")
            reasons.append(f"Win Rate {win_rate:.0f}% < 55% (quality over quantity)")
        elif "High Roller" in t:
            reasons.append(f"PnL Drawdown {mdd_pct:.0f}% > 40%")
            reasons.append(f"Profit Factor {profit_factor:.1f}x > 1.5")
        elif "Bet Maker" in t:
            reasons.append(f"{big_bet_count} big bets > 10")
            reasons.append(f"Big Bet Rate {big_bet_rate:.0f}% > 50%")
        elif "All-Rounder" in t:
            reasons.append(f"Win Rate {win_rate:.0f}% > 60%")
            reasons.append(f"Sharpe* {sharpe:.2f} > 2")
            reasons.append(f"ROI {roi_pct:.1f}% > 10%")
        elif "Momentum" in t:
            reasons.append(f"Sharpe* {sharpe:.2f} > 4")
            reasons.append(f"ROI {roi_pct:.1f}% > 10%")
        elif "Steady Shot" in t:
            reasons.append(f"Win Rate {win_rate:.0f}% > 65%")
            reasons.append(f"Profit Factor {profit_factor:.1f}x > 1.5")
        elif "Value Hunter" in t:
            reasons.append(f"Profit Factor {profit_factor:.1f}x > 2.5")
            reasons.append(f"Big Bet Rate {big_bet_rate:.0f}% < 20% (selective)")
        elif "Consistent" in t:
            reasons.append(f"Win Rate {win_rate:.0f}% > 55%")
            reasons.append(f"Profit Factor {profit_factor:.1f}x > 1.2")
        elif "Grinder" in t:
            reasons.append(f"Profitable with Win Rate {win_rate:.0f}% > 50%")
            reasons.append(f"Profit Factor {profit_factor:.1f}x > 1.0")
        elif "Newcomer" in t:
            reasons.append(f"Only {len(closed)} closed trades (< 300)")
        else:
            reasons.append("No dominant pattern — fallback classification")
        return reasons[:3]

    # ── Confidence 배지 ───────────────────────────────────────────────
    def _make_confidence():
        if len(closed) >= 200 and data_days >= 60 and consistency >= 55:
            return "High Confidence"
        elif len(closed) >= 80 and data_days >= 20:
            return "Medium Confidence"
        elif len(closed) < 50 or data_days < 14:
            return "Early Read"
        else:
            return "Medium Confidence"

    type_reasons = _make_type_reasons()
    confidence   = _make_confidence()

    # 규칙 기반 서머리 생성
    def _make_summary():
        # 지표 해석 헬퍼
        hi_wr  = win_rate >= 72
        ok_wr  = win_rate >= 58
        hi_sh  = sharpe >= 3
        ok_sh  = sharpe >= 1.2
        hi_roi = roi_pct >= 100
        ok_roi = roi_pct >= 25
        hi_bb  = big_bet_rate >= 70 and big_bet_count >= 3
        hi_mdd = mdd_pct >= 50
        ok_mdd = mdd_pct >= 25
        hi_con = consistency >= 65
        lo_con = consistency < 40
        # durability removed from classification
        hi_pf  = profit_factor >= 2.5
        few_trades = len(closed) < 50 or data_days < 20
        ttype = trader_type  # emoji + name string

        # ── Apex Predator ─────────────────────────────────────────
        if "Apex Predator" in ttype:
            if hi_wr and hi_sh and ok_roi:
                body = ("This wallet does not dominate through chaos — it dominates through control. "
                        "High accuracy combined with strong return efficiency points to a trader who identifies real edge early, "
                        "then presses it with unusual confidence. It is not just winning often; it is winning in a way that looks repeatable.")
                bottom = "Overall: a top-tier conviction wallet with both precision and force."
            elif hi_roi and hi_bb:
                body = ("This wallet does not need a perfect hit rate to lead. "
                        "Its edge comes from recognizing when the opportunity is big enough to matter — "
                        "then sizing into it hard. The profile suggests a trader who makes fewer but heavier statements, "
                        "and gets paid when conviction is right.")
                bottom = "Overall: a dominance-driven wallet built on size, timing, and selective aggression."
            elif hi_mdd:
                body = ("This wallet clearly knows how to win, but it does not win quietly. "
                        "Strong upside and elite efficiency point to real skill, yet the drawdown profile suggests "
                        "that dominance sometimes comes with sharp periods of exposure. "
                        "When this wallet presses, it can look unstoppable — but it does not always leave itself much room to be wrong.")
                bottom = "Overall: elite upside, elite conviction, and a risk curve that stays aggressive."
            else:
                body = ("This wallet wins through authority, not hesitation. "
                        "It tends to identify where its edge is strongest, then commit with enough conviction "
                        "to shape outcomes rather than merely participate in them.")
                bottom = "Overall: a high-conviction operator built to lead, not follow."

        # ── Precision Hunter ──────────────────────────────────────
        elif "Precision Hunter" in ttype:
            if hi_wr and hi_sh and not few_trades:
                body = ("This wallet wins by staying selective and being right more often than most. "
                        "The combination of high accuracy and disciplined return quality suggests a trader "
                        "who waits for clean setups, enters without hesitation, and avoids wasting risk on low-conviction noise.")
                bottom = "Overall: a sharp, disciplined wallet that lets precision compound."
            elif hi_wr and few_trades:
                body = ("The read looks promising: this wallet appears selective, patient, and difficult to bait into weak trades. "
                        "But the sample is still light, so the current profile says more about style than proof.")
                bottom = "Overall: early signs of a clean operator, with more history needed before calling it proven."
            elif hi_wr and not ok_roi:
                body = ("This wallet rarely looks sloppy, but it may also leave money on the table. "
                        "Accuracy is strong, yet the overall payoff suggests a trader who protects quality "
                        "so carefully that the upside can feel capped.")
                bottom = "Overall: precise and disciplined, though sometimes more careful than forceful."
            else:
                body = ("This wallet is selective by design. It prefers clean entries, disciplined timing, "
                        "and high-probability setups over constant participation — "
                        "which usually results in a profile that feels sharp rather than noisy.")
                bottom = "Overall: a wallet that waits longer, misses less, and lets accuracy do the heavy lifting."

        # ── Ice Quant ─────────────────────────────────────────────
        elif "Ice Quant" in ttype:
            if hi_sh and hi_con and not hi_mdd:
                body = ("This wallet trades like emotion has been engineered out of the loop. "
                        "Strong efficiency and stable execution suggest a process-led operator "
                        "that wins through repeatability, not drama. "
                        "The edge appears systematic, measured, and resilient across normal market noise.")
                bottom = "Overall: a clean, process-first wallet with unusually strong discipline."
            elif hi_sh and ok_roi:
                body = ("There is very little waste in this profile. "
                        "The wallet combines frequency, quality, and efficiency in a way that feels engineered rather than improvised. "
                        "It does not need flashy swings to impress — the edge shows up in how consistently it extracts value.")
                bottom = "Overall: quiet on the surface, lethal in the numbers."
            elif hi_mdd or lo_con:
                body = ("The profile looks systematic, but not fully robust. "
                        "There is evidence of real structure in the way this wallet trades, "
                        "though the drawdown behavior suggests that the process may become fragile "
                        "when conditions stop matching the model.")
                bottom = "Overall: smart, efficient, and somewhat regime-dependent."
            else:
                body = ("This wallet trades with a distinctly systematic profile. "
                        "Its edge seems to come from repeatability, risk-adjusted execution, and emotional detachment "
                        "rather than discretionary bursts of conviction.")
                bottom = "Overall: cold, clean, and mechanical — trades like a model, not a mood."

        # ── Sniper ────────────────────────────────────────────────
        elif "Sniper" in ttype:
            if hi_wr and hi_bb:
                body = ("This wallet is built on restraint. "
                        "It stays quiet for long stretches, then steps in only when the setup looks worth real attention. "
                        "High hit quality suggests that timing matters more here than trade count.")
                bottom = "Overall: low-noise, high-intent trading with sharp entry discipline."
            elif hi_roi and hi_bb:
                body = ("This wallet does not need many shots to matter. "
                        "It appears to focus on high-payoff opportunities where one clean decision "
                        "can do the work of many average trades.")
                bottom = "Overall: a selective wallet that hunts upside, not activity."
            elif few_trades:
                body = ("The style is clear, but the evidence is still thin. "
                        "This wallet looks selective and deliberate, though there is not yet enough history "
                        "to separate real precision from a short hot streak.")
                bottom = "Overall: good shape, light proof."
            else:
                body = ("This wallet does not spray trades — it tends to stay quiet, then strike with intent. "
                        "The profile fits a trader who would rather take fewer shots with better odds "
                        "than stay active for the sake of activity.")
                bottom = "Overall: quiet, then lethal. Patience is the edge."

        # ── All-Rounder ───────────────────────────────────────────
        elif "All-Rounder" in ttype:
            if hi_con and ok_roi:
                body = ("This wallet is defined less by one dominant trait than by balance across multiple ones. "
                        "It appears capable of adapting to different market conditions without becoming overly dependent "
                        "on a single style — which makes the profile resilient even when the environment changes.")
                bottom = "Overall: versatile, balanced, and hard to expose in just one regime."
            elif hi_con:
                body = ("The strength here is not flash — it is survivability. "
                        "This wallet looks built to keep functioning even when conditions shift, "
                        "with enough flexibility to avoid being trapped inside one market personality.")
                bottom = "Overall: a reliable operator with fewer obvious weaknesses than most."
            else:
                body = ("This wallet does many things reasonably well, but nothing yet defines it as special. "
                        "The balance is useful, though the lack of standout edge makes the profile "
                        "feel more safe than dangerous.")
                bottom = "Overall: steady and usable, but not yet distinctive."

        # ── Momentum ──────────────────────────────────────────────
        elif "Momentum" in ttype:
            if hi_roi and hi_bb and hi_sh:
                body = ("This wallet does not just catch trend — it knows how to press it. "
                        "The profile suggests a trader who gets more out of winning conditions than most "
                        "by adding conviction when the market confirms the move.")
                bottom = "Overall: a momentum wallet that extracts more than just the first leg."
            elif hi_roi and ok_wr:
                body = ("This wallet looks most comfortable when the market is already moving. "
                        "Its edge appears to come from recognizing strength early enough to join it, "
                        "then staying with the trade long enough to let trend do the heavy lifting.")
                bottom = "Overall: a trend-led wallet that performs best when follow-through is real."
            elif hi_mdd or lo_con:
                body = ("When the tape is clean, this wallet can look excellent. "
                        "But the risk profile suggests it may lose shape when strength turns into noise — "
                        "which is common for strategies that rely on continuation more than mean reversion.")
                bottom = "Overall: dangerous in trend, vulnerable in chop."
            else:
                body = ("This wallet looks most comfortable when price is already moving. "
                        "It tends to trust strength, ride continuation, and press the trade "
                        "while the market is still carrying it forward.")
                bottom = "Overall: built for trend — gets on it, stays on it, squeezes it."

        # ── Steady Shot ───────────────────────────────────────────
        elif "Steady Shot" in ttype:
            if ok_wr and ok_sh and hi_con and not hi_mdd:
                body = ("This wallet wins through repetition, not spectacle. "
                        "The profile suggests disciplined trade selection, consistent execution, "
                        "and a preference for stacking good decisions rather than depending on standout home runs.")
                bottom = "Overall: a quiet compounding wallet with dependable rhythm."
            elif hi_sh and not hi_mdd:
                body = ("The edge here is efficiency. "
                        "This wallet may not post the loudest upside, but it wastes very little motion — "
                        "which often matters more over time than one-off bursts of performance.")
                bottom = "Overall: clean, repeatable, and built for durability."
            elif ok_wr and not ok_roi:
                body = ("This wallet looks disciplined and reliable, though not especially explosive. "
                        "It tends to avoid major mistakes, but may also lack the aggression needed "
                        "to turn consistency into standout upside.")
                bottom = "Overall: dependable, but not built to overwhelm."
            else:
                body = ("This wallet is built for consistency. "
                        "The edge does not come from heroic trades, but from stacking solid decisions "
                        "over and over again with relatively little drama.")
                bottom = "Overall: no fireworks, just results — quietly compounds through consistency."

        # ── Degen ─────────────────────────────────────────────────
        elif "Degen" in ttype:
            if hi_wr and ok_roi and ok_sh:
                body = ("This wallet lives close to the edge, but not blindly. "
                        "The aggression is obvious, yet the numbers suggest it is being directed "
                        "with real timing skill rather than random risk appetite. "
                        "A rare combination for a high-speed, high-risk profile.")
                bottom = "Overall: controlled chaos with genuine trading edge."
            elif hi_roi and hi_bb:
                body = ("This wallet does not need clean symmetry to produce outcomes. "
                        "It looks built for explosive upside, where a few aggressive, well-timed swings "
                        "can do outsized damage in the right direction.")
                bottom = "Overall: high-voltage trading with payoff concentrated in the biggest hits."
            elif hi_mdd or lo_con:
                body = ("The aggression is clear, but the discipline is harder to trust. "
                        "This wallet may still catch spectacular upside, though the drawdown behavior "
                        "suggests that the line between conviction and overexposure can get thin very quickly.")
                bottom = "Overall: exciting when right, fragile when pressed."
            else:
                body = ("This wallet likes chaos more than comfort. It moves fast, takes bigger swings, "
                        "and plays closer to the edge than most — but when the results are strong, "
                        "that usually means there is real skill underneath the aggression.")
                bottom = "Overall: wild on the surface, calculated underneath."

        # ── High Roller ───────────────────────────────────────────
        elif "High Roller" in ttype:
            if total_pnl > 0 and hi_bb and ok_sh:
                body = ("This wallet turns edge into impact through size. "
                        "It is not just identifying opportunities — it is scaling them aggressively enough "
                        "for the wins to matter in a meaningful way.")
                bottom = "Overall: a size-driven wallet with conviction and control."
            elif hi_roi and ok_mdd:
                body = ("This wallet is comfortable making the position size part of the strategy. "
                        "When conviction clears the bar, it tends to act decisively — "
                        "which can make performance look powerful even without extreme frequency.")
                bottom = "Overall: strong upside driven by capital pressure, not just signal quality."
            elif hi_mdd and not hi_bb:
                body = ("The wallet clearly likes size, but the payoff profile suggests "
                        "that aggression may sometimes outrun edge. "
                        "Large-position styles can look brilliant in good conditions, "
                        "but they do not hide mistakes when timing slips.")
                bottom = "Overall: high stakes, uneven control."
            else:
                body = ("This wallet is comfortable playing with size. "
                        "Its profile suggests a trader whose edge is amplified not by frequency alone, "
                        "but by the ability to deploy capital aggressively when conviction is high.")
                bottom = "Overall: bold, size-driven, and built to win large — not grind small."

        # ── Consistent ────────────────────────────────────────────
        elif "Consistent" in ttype:
            if ok_wr and ok_sh:
                body = ("This wallet does not rely on a single dramatic edge to stay ahead. "
                        "It shows up, executes with quiet discipline, and stacks positive outcomes "
                        "across a wide range of conditions. The strength is not in the peak — it is in the floor.")
                bottom = "Overall: a reliable performer that earns through repetition, not spectacle."
            else:
                body = ("This wallet builds results through consistency rather than conviction. "
                        "It is not trying to be the biggest or the fastest — "
                        "it is trying to be the one that is still profitable when others are not.")
                bottom = "Overall: steady, unspectacular, and quietly effective."

        # ── Grinder ───────────────────────────────────────────────
        elif "Grinder" in ttype:
            body = ("This wallet earns its results through volume and persistence. "
                    "It does not rely on one outsized call or a high-conviction cluster of wins — "
                    "it operates on a steady loop of modest gains that accumulate over time. "
                    "The edge is not dramatic, but it is real and consistently applied.")
            bottom = "Overall: patient, process-driven, and grinding out results one session at a time."


        # ── Newcomer ──────────────────────────────────────────────
        elif "Newcomer" in ttype:
            if ok_roi and ok_wr:
                body = ("This wallet has started with enough structure to be interesting. "
                        "Early results suggest there may be a real style here, "
                        "but the sample is still too fresh to know whether the edge survives "
                        "different market conditions.")
                bottom = "Overall: promising signal, pending proof."
            elif hi_roi:
                body = ("The upside is real, but so is the uncertainty. "
                        "This wallet may be catching a strong opening run, "
                        "though it is too early to tell whether the current pace reflects durable skill "
                        "or favorable timing.")
                bottom = "Overall: exciting start, incomplete evidence."
            else:
                body = ("There are hints of potential, but not enough identity yet. "
                        "The profile is still forming, and the next stretch matters more than the last one "
                        "in deciding whether this wallet has a repeatable edge.")
                bottom = "Overall: worth tracking, not yet worth trusting."

        # ── Underwater ────────────────────────────────────────────
        elif "Underwater" in ttype:
            body = ("This wallet is currently in the red, and the profile does not yet show "
                    "a clear path back to consistency. "
                    "Whether the losses come from a bad strategy, a bad streak, or a style still forming "
                    "is hard to tell — but the risk picture does not look clean right now.")
            bottom = "Overall: not a wallet to follow at this stage, but potentially worth watching for a turnaround."

        # ── Drifter / fallback ────────────────────────────────────
        else:
            if lo_con:
                body = ("This wallet does not yet show a stable rhythm. "
                        "The results may include isolated wins, but the broader profile suggests "
                        "shifting behavior rather than a repeatable process.")
                bottom = "Overall: still searching for a durable way to win."
            elif total_pnl > 0 and not ok_sh:
                body = ("Some outcomes may look better than the process behind them. "
                        "The wallet can still post wins, but the underlying profile makes it hard to tell "
                        "whether those gains came from real edge or favorable randomness.")
                bottom = "Overall: better headline than foundation."
            else:
                body = ("This wallet does not yet show a strong, repeatable identity. "
                        "Results may be coming from inconsistent execution, changing style, "
                        "or a lack of clear edge — which makes performance harder to trust "
                        "even when occasional wins look strong.")
                bottom = "Overall: no stable rhythm, no clear edge — still searching."

        # 리스크 경고 (공통)
        risk = ""
        if hi_mdd and "Underwater" not in ttype:
            risk = f" The main watchout is the drawdown profile — this wallet does not protect capital gently when things go wrong."
        elif lo_con and ok_roi:
            risk = " That said, consistency is not its strong suit, so results can be uneven across different stretches."
        # durability risk warning removed (durability no longer tracked)

        # 데이터 부족 경고
        if few_trades:
            risk += " Note: the sample size is still light, so all signals should be read with some caution."

        return f"{body}{risk} {bottom}"

    ai_summary = _make_summary()
    return {


        "address":address,"label":label or address[:8]+"...","total_equity":total_equity,
        "margin_pct":margin_pct,"realized":realized,"total_upnl":total_upnl,"total_pnl":total_pnl,
        "win_rate":round(win_rate,1),"avg_win":round(avg_win,2),"avg_loss":round(avg_loss,2),
        "profit_factor":round(profit_factor,2),"sharpe":round(sharpe,2),"mdd_pct":round(mdd_pct,1),
        "consistency":round(consistency,1),"durability":round(durability,1),"long_pct":round(long_pct,1),"war_components":war_components,"follow_score":follow_score,
        "big_bet_count":big_bet_count,"big_bet_rate":round(big_bet_rate,1),"big_bet_pnl":round(big_bet_pnl,2),
        "closed_count":len(closed),"total_days":len(daily_pnls),"data_days":data_days,
        "first_date":first_date_str,"last_date":last_date_str,"days_since_last":days_since_last,
        "roi_pct":round(roi_pct,1),"top_coins":[
            {"coin":c,"pnl":round(p,2),
             "side":"L" if coin_side[c]["A"]>=coin_side[c]["B"] else "S"}
            for c,p in sorted(pnl_by_coin.items(),key=lambda x:x[1],reverse=True)
        ],
        "radar":radar,"war_score":war_score,"trader_type":trader_type,"character":character,
        "is_hf":(len(closed)/max(data_days,1) >= 300 if data_days>0 else False),
        "spot_holdings": spot_holdings,
        "is_vault":(src=="vault"),
        "cumulative":cumulative,"source":src,"error":raw.get("error"),
        "positions":[{"coin":p["coin"],"side":p["side"],"upnl":round(p["upnl"],2),
                      "notional":round(p["notional"],2),"lev":round(p["notional"]/max(total_equity,1),2)} for p in positions],
        "ai_summary":ai_summary,
        "type_reasons":type_reasons,
        "confidence":confidence,
    }


# ══ PROCESS ════════════════════════════════════════════════════════════
async def process_addresses(addresses, labels, sources, archive: ArchiveManager, force=False):
    api = HyperliquidAPI()

    # vault_discovery.json + vaultSummaries API로 vault 주소 목록 수집
    _vault_leaders = set(archive.vault_addrs)  # vault_discovery.json 기반
    try:
        r = await api.http.get("https://stats-data.hyperliquid.xyz/Mainnet/vaults", timeout=15)
        r.raise_for_status()
        raw = r.json()
        if isinstance(raw, list):
            for v in raw:
                s = v.get("summary") or v
                # isClosed 아니고 TVL 10k 이상인 활성 vault leader만 포함
                if s.get("isClosed", False):
                    continue
                try:
                    tvl = float(s.get("tvl") or 0)
                except Exception:
                    tvl = 0.0
                if tvl < 10000:
                    continue
                leader = s.get("leader", "")
                if leader:
                    _vault_leaders.add(leader.lower())
        console.print(f"  [dim]활성 vault leader {len(_vault_leaders)}개 확인[/dim]")
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

    # vault_discovery.json으로 label 보정 (0x 주소로 된 vault 이름 교체)
    try:
        _vd_path = Path("vault_discovery.json")
        if _vd_path.exists():
            _vd = json.loads(_vd_path.read_text(encoding="utf-8"))
            _vname_map = {v["vault_addr"].lower(): v["name"] for v in _vd.get("direct_vaults", []) if v.get("name")}
            for addr in archive.all_addresses():
                vname = _vname_map.get(addr.lower())
                if vname:
                    entry = archive.data.get(addr.lower(), {})
                    lbl = entry.get("stats", {}).get("label", "")
                    if lbl.startswith("0x") or lbl == "":
                        entry["stats"]["label"] = vname
                        entry["stats"]["source"] = "vault"
                        entry["stats"]["is_vault"] = True
            archive.save()
    except Exception:
        pass

    excluded = ExcludedManager()
    skip, need_fetch, backoff_skip = [], [], []

    for addr, label, src in zip(addresses, labels, sources):
        if not force and not archive.needs_update(addr):
            skip.append((addr, label, src))
        elif not force and excluded.should_skip(addr):
            backoff_skip.append((addr, label, src))
        else:
            need_fetch.append((addr, label, src))

    if backoff_skip:
        console.print(f"  [dim]⏭ backoff 스킵: {len(backoff_skip)}개[/dim]")
        for addr, label, src in backoff_skip:
            console.print(f"    [dim]↷ {label} — {excluded.skip_info(addr)}[/dim]")
    console.print(f"\n  [green]✓ 캐시 사용:[/green] {len(skip)}개  [yellow]↓ 수집:[/yellow] {len(need_fetch)}개  [dim]⏭ backoff:[/dim] {len(backoff_skip)}개\n")

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
        # BATCH=5 → 배치당 ~110 weight, DELAY=5s → 분당 12배치=1320/1200 (rate limit 주의)
        BATCH=3; DELAY=5.0; RETRY_DELAY=40.0; MAX_RETRY=3; MAX_SAVE=50
        console.print(f"[bold blue]▶ API 수집[/bold blue] [dim]{len(need_fetch)}개 대상 (배치={BATCH}, 딜레이={DELAY}s, 최대저장={MAX_SAVE}개)[/dim]")
        pending = list(need_fetch)
        retry_count = 0
        saved_count = 0   # NEW + 갱신 카운터
        _hit_limit = False
        while pending and retry_count <= MAX_RETRY and not _hit_limit:
            if retry_count > 0:
                console.print(f"  [yellow]↻ 재시도 {retry_count}/{MAX_RETRY} — {len(pending)}개 ({RETRY_DELAY}초 대기)[/yellow]")
                await asyncio.sleep(RETRY_DELAY)
            still_failed = []
            for bi in range(0, len(pending), BATCH):
                if _hit_limit:
                    break
                batch = pending[bi:bi+BATCH]
                tasks = [api.fetch(addr) for addr,_,_ in batch]
                fetched = await asyncio.gather(*tasks)
                for (addr,label,src),raw in zip(batch,fetched):
                    err = str(raw.get("error",""))
                    if raw.get("error"):
                        if "429" in err: still_failed.append((addr,label,src))
                        else: console.print(f"  [red]✗ {label}[/red]  [dim]{err[:60]}[/dim]")
                        continue
                    # fills 수집 실패 → WAR 신뢰 불가, 재시도 목록에
                    if not raw.get("fills_ok", True) and not raw.get("fills"):
                        console.print(f"  [yellow]⚠ {label} — fills 수집 실패, 재시도 예약[/yellow]")
                        still_failed.append((addr, label, src))
                        continue
                    stats = compute_stats(raw, addr, label, src=src)
                    equity = stats.get("total_equity", 0)
                    war = stats.get("war_score", 0)
                    # 디버그: WAR 계산 근거 로그
                    _fills_raw = raw.get("fills", [])
                    _closed_n  = len([f for f in _fills_raw if float(f.get("closedPnl",0) or 0)!=0])
                    _is_vault  = bool(raw.get("clearinghouse",{}).get("vaultEquity") or raw.get("clearinghouse",{}).get("isVault"))
                    _ch        = raw.get("clearinghouse", {})
                    _ms        = _ch.get("marginSummary", {})
                    _vault_eq  = float(_ch.get("vaultEquity", 0) or 0)
                    _acct_eq   = float(_ms.get("accountValue", 0) or 0)
                    console.print(
                        f"  [dim]  └ fills={len(_fills_raw)} closed={_closed_n} "
                        f"equity=${equity:,.0f} vault={_is_vault} "
                        f"vaultEq={_vault_eq:,.0f} acctEq={_acct_eq:,.0f} "
                        f"pnl={stats.get('total_pnl',0):,.0f} roi={stats.get('roi_pct',0):.1f}% "
                        f"realized={stats.get('realized',0):,.0f} upnl={stats.get('total_upnl',0):,.0f} "
                        f"war_components={stats.get('war_components',{})}[/dim]"
                    )
                    tag = "[bold green]NEW[/bold green]" if archive.is_new(addr) else "[dim]갱신[/dim]"
                    if war < 50.0:
                        key = addr.lower()
                        if key in archive.data: del archive.data[key]
                        excluded.record_exclusion(addr, war)
                        cnt = excluded.data[addr.lower()]["exclude_count"]
                        days = BACKOFF_DAYS.get(cnt, BACKOFF_MAX_DAYS)
                        console.print(f"  [dim]제외 {label} — WAR {war:.1f} (50 미만) · {cnt}회째 · {days}일 후 재체크[/dim]")
                        continue
                    if equity < 50_000:
                        key = addr.lower()
                        if key in archive.data: del archive.data[key]
                        excluded.record_exclusion(addr, war, reason="equity")
                        cnt = excluded.data[addr.lower()]["exclude_count"]
                        days = BACKOFF_DAYS.get(cnt, BACKOFF_MAX_DAYS)
                        console.print(f"  [dim]제외 {label} — ${equity:,.0f} ($50k 미만) · {cnt}회째 · {days}일 후 재체크[/dim]")
                        continue
                    excluded.clear(addr)  # backoff 초기화 (WAR 회복)
                    archive.upsert(addr, stats)
                    results.append(stats)
                    saved_count += 1
                    console.print(f"  {tag} {label} — WAR [bold]{stats['war_score']}[/bold] · {stats['trader_type']} · ${equity:,.0f}  [dim]({saved_count}/{MAX_SAVE})[/dim]")
                    if saved_count >= MAX_SAVE:
                        _hit_limit = True
                        console.print(f"  [bold cyan]✓ 저장 {MAX_SAVE}개 도달 — 수집 중단[/bold cyan]")
                        break
                if bi+BATCH < len(pending) and not _hit_limit: await asyncio.sleep(DELAY)
            pending = still_failed
            retry_count += 1
        if _hit_limit:
            _remaining = len(need_fetch) - (pending.__len__() + saved_count)
            console.print(f"  [dim]미처리 {len(need_fetch) - saved_count}개는 다음 실행에서 수집[/dim]")
        elif pending:
            console.print(f"  [red]최종 실패 {len(pending)}개[/red]")
            for addr,label,src in pending: console.print(f"    ✗ {label}")

    await api.close()
    archive.save()
    excluded.save()
    _ex_sum = excluded.summary()
    console.print(f"  [dim]backoff 관리: 총 {_ex_sum['total']}개 지갑 · 현재 스킵 중 {_ex_sum['active_skip']}개[/dim]")
    return results


# ══ TOURNAMENT ════════════════════════════════════════════════════════
def run_tournament(all_stats):
    """Always returns {"rounds": [...], "results": {...}} for consistent downstream handling."""
    _empty = {"rounds": [], "results": {s["address"]: {"score": 0, "wins": 0, "weekly_pnl": []} for s in all_stats}}
    if not all_stats: return _empty
    all_dates = set()
    for s in all_stats:
        for pt in s["cumulative"]: all_dates.add(pt["date"])
    if not all_dates: return _empty
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
    """Thin wrapper kept for backward compat — delegates to classify_trader_type()."""
    return classify_trader_type(
        wr=s.get('win_rate',0), sh=s.get('sharpe',0),
        bbr=s.get('big_bet_rate',0), bbc=s.get('big_bet_count',0),
        pf=s.get('profit_factor',0), mdd=s.get('mdd_pct',0),
        pnl=s.get('total_pnl',0), roi=s.get('roi_pct',0),
        cc=s.get('closed_count',0) or len(s.get('cumulative',[]))
    )

def load_wallets_meta() -> dict:
    """wallets_meta.json 로드. 주소 키는 소문자로 정규화."""
    p = Path(META_FILE)
    if not p.exists():
        return {}
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
        return {k.lower(): v for k, v in raw.items()}
    except Exception:
        return {}


def make_avatar_svg(trader_type: str, address: str, color: str, size: int = 48) -> str:
    """트레이더 타입 + 지갑 주소 기반 아바타 SVG 생성.
    타입이 기본 캐릭터를, 주소가 보조 색상/패턴을 결정."""
    import hashlib
    h = hashlib.md5(address.lower().encode()).digest()
    # 주소에서 보조색 추출
    r2 = h[0]; g2 = h[1]; b2 = h[2]
    accent2 = f"#{r2:02x}{g2:02x}{b2:02x}"
    # 패턴용 값
    p = [h[i] % 4 for i in range(8)]  # 0~3 랜덤
    sz = size
    half = sz // 2
    bg = "#0a0a16"

    def apex_predator():
        # 사자: 갈기 색상이 주소에 따라 변함
        mane_l = f"#{max(r2,80):02x}{max(g2//3,30):02x}{10:02x}"
        mane_d = f"#{min(r2+40,220):02x}{min(g2//3+20,100):02x}{10:02x}"
        face   = f"#{min(r2+80,245):02x}{min(g2//2+80,192):02x}{106:02x}"
        ear_x1, ear_x2 = int(half*0.55), int(half*1.45)
        return f"""
  <ellipse cx="{half}" cy="{half+2}" rx="{int(sz*0.34)}" ry="{int(sz*0.34)}" fill="{mane_l}"/>
  <ellipse cx="{half}" cy="{half+2}" rx="{int(sz*0.26)}" ry="{int(sz*0.26)}" fill="{mane_d}"/>
  <polygon points="{ear_x1},{int(sz*0.28)} {int(half*0.38)},{int(sz*0.08)} {int(half*0.8)},{int(sz*0.2)}" fill="{mane_d}"/>
  <polygon points="{ear_x2},{int(sz*0.28)} {int(half*1.62)},{int(sz*0.08)} {int(half*1.2)},{int(sz*0.2)}" fill="{mane_d}"/>
  <ellipse cx="{half}" cy="{half+3}" rx="{int(sz*0.2)}" ry="{int(sz*0.19)}" fill="{face}"/>
  <ellipse cx="{int(half*0.82)}" cy="{int(half*0.92)}" rx="{int(sz*0.06)}" ry="{int(sz*0.07)}" fill="#1a0a00"/>
  <ellipse cx="{int(half*1.18)}" cy="{int(half*0.92)}" rx="{int(sz*0.06)}" ry="{int(sz*0.07)}" fill="#1a0a00"/>
  <circle cx="{int(half*0.83)}" cy="{int(half*0.9)}" r="{max(int(sz*0.02),1)}" fill="white"/>
  <circle cx="{int(half*1.19)}" cy="{int(half*0.9)}" r="{max(int(sz*0.02),1)}" fill="white"/>
  <ellipse cx="{half}" cy="{int(half*1.08)}" rx="{int(sz*0.05)}" ry="{int(sz*0.03)}" fill="{mane_l}"/>"""

    def ice_quant():
        # 눈결정: 가지 색이 주소에 따라 변함
        c1 = color; c2 = accent2
        r = int(sz*0.38); cx = half; cy = half
        arms = ""
        for angle_deg in [0, 45, 90, 135]:
            import math
            a = math.radians(angle_deg)
            x1 = int(cx + r*math.cos(a)); y1 = int(cy + r*math.sin(a))
            x2 = int(cx - r*math.cos(a)); y2 = int(cy - r*math.sin(a))
            # 가지
            blen = int(r*0.35)
            for sign in [1,-1]:
                ba = a + sign*math.radians(60)
                bx = int(cx + (r*0.55)*math.cos(a) + blen*0.5*math.cos(ba))
                by = int(cy + (r*0.55)*math.sin(a) + blen*0.5*math.sin(ba))
                arms += f'<line x1="{int(cx+(r*0.55)*math.cos(a))}" y1="{int(cy+(r*0.55)*math.sin(a))}" x2="{bx}" y2="{by}" stroke="{c2}" stroke-width="{max(int(sz*0.015),1)}"/>'
                bx2 = int(cx - (r*0.55)*math.cos(a) + blen*0.5*math.cos(ba+math.pi))
                by2 = int(cy - (r*0.55)*math.sin(a) + blen*0.5*math.sin(ba+math.pi))
                arms += f'<line x1="{int(cx-(r*0.55)*math.cos(a))}" y1="{int(cy-(r*0.55)*math.sin(a))}" x2="{bx2}" y2="{by2}" stroke="{c2}" stroke-width="{max(int(sz*0.015),1)}"/>'
            arms += f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{c1}" stroke-width="{max(int(sz*0.025),1.5)}"/>'
            arms += f'<circle cx="{x1}" cy="{y1}" r="{max(int(sz*0.04),2)}" fill="{c2}"/>'
            arms += f'<circle cx="{x2}" cy="{y2}" r="{max(int(sz*0.04),2)}" fill="{c2}"/>'
        return arms + f'<circle cx="{cx}" cy="{cy}" r="{max(int(sz*0.08),3)}" fill="{c1}" opacity="0.9"/>'

    def degen():
        # 불꽃: 주소에 따라 불꽃 형태 변형
        cx = half; base = int(sz*0.85)
        tip = int(sz*0.1) + p[0]*2
        lx = int(cx - sz*0.22) + p[1]*2
        rx_ = int(cx + sz*0.22) - p[2]*2
        mid = int(sz*0.45) + p[3]*3
        c1 = color; c2 = accent2
        return f"""
  <path d="M{cx} {base} Q{lx} {int(sz*0.65)} {int(cx-sz*0.18)} {mid} Q{int(cx-sz*0.08)} {int(sz*0.3)} {cx} {tip} Q{int(cx+sz*0.08)} {int(sz*0.3)} {int(cx+sz*0.18)} {mid} Q{rx_} {int(sz*0.65)} {cx} {base}Z" fill="{c1}" opacity="0.9"/>
  <path d="M{cx} {base} Q{int(cx-sz*0.12)} {int(sz*0.68)} {int(cx-sz*0.1)} {int(sz*0.5)} Q{int(cx-sz*0.04)} {int(sz*0.35)} {cx} {int(tip+int(sz*0.12))} Q{int(cx+sz*0.04)} {int(sz*0.35)} {int(cx+sz*0.1)} {int(sz*0.5)} Q{int(cx+sz*0.12)} {int(sz*0.68)} {cx} {base}Z" fill="#ff6b6b" opacity="0.85"/>
  <path d="M{cx} {base} Q{int(cx-sz*0.06)} {int(sz*0.7)} {int(cx-sz*0.04)} {int(sz*0.55)} Q{cx} {int(sz*0.42)} {cx} {int(tip+int(sz*0.22))} Q{cx} {int(sz*0.42)} {int(cx+sz*0.04)} {int(sz*0.55)} Q{int(cx+sz*0.06)} {int(sz*0.7)} {cx} {base}Z" fill="#ffbe0b" opacity="0.8"/>
  <circle cx="{cx}" cy="{int(sz*0.62)}" r="{max(int(sz*0.06),2)}" fill="white" opacity="0.25"/>"""

    def steady_shot():
        # 과녁: 링 개수/색이 주소에 따라 변함
        cx = half; cy = half
        c1 = color; c2 = accent2
        rings = ""
        for i, rad in enumerate([int(sz*0.42), int(sz*0.3), int(sz*0.18)]):
            op = 0.3 + i*0.2
            rings += f'<circle cx="{cx}" cy="{cy}" r="{rad}" fill="none" stroke="{c1}" stroke-width="1.2" opacity="{op}"/>'
        rings += f'<line x1="{cx}" y1="{int(sz*0.06)}" x2="{cx}" y2="{int(sz*0.94)}" stroke="{c1}" stroke-width="0.6" opacity="0.3"/>'
        rings += f'<line x1="{int(sz*0.06)}" y1="{cy}" x2="{int(sz*0.94)}" y2="{cy}" stroke="{c1}" stroke-width="0.6" opacity="0.3"/>'
        rings += f'<circle cx="{cx}" cy="{cy}" r="{max(int(sz*0.07),3)}" fill="{c1}" opacity="0.9"/>'
        # 화살
        ax1 = int(cx + sz*0.3 + p[4]*2); ay1 = int(cy - sz*0.28 - p[5]*2)
        ax2 = int(cx + sz*0.06); ay2 = int(cy - sz*0.06)
        rings += f'<line x1="{ax1}" y1="{ay1}" x2="{ax2}" y2="{ay2}" stroke="{c2}" stroke-width="{max(int(sz*0.04),2)}"/>'
        rings += f'<polygon points="{ax2},{ay2} {ax2+6},{ay2-10} {ax2+10},{ay2+4}" fill="{c2}"/>'
        return rings

    def momentum():
        # 상승 화살표 + 추세선
        c1 = color; c2 = accent2
        pts = []
        import random; rng = random.Random(int.from_bytes(h[:4], 'big'))
        base_y = int(sz*0.75)
        for i in range(6):
            x = int(sz*0.12 + i*sz*0.15)
            y = base_y - int(i*sz*0.09) - rng.randint(0, int(sz*0.06))
            pts.append((x,y))
        line_d = " ".join(f"{'M' if i==0 else 'L'}{x},{y}" for i,(x,y) in enumerate(pts))
        dots = "".join(f'<circle cx="{x}" cy="{y}" r="{max(int(sz*0.04),2)}" fill="{c2}"/>' for x,y in pts)
        # 큰 화살표
        ax = int(sz*0.5); ay_tip = int(sz*0.12)
        return f"""
  <path d="{line_d}" fill="none" stroke="{c1}" stroke-width="{max(int(sz*0.04),2)}" stroke-linejoin="round"/>
  {dots}
  <polygon points="{ax},{ay_tip} {int(ax-sz*0.18)},{int(ay_tip+sz*0.22)} {int(ax+sz*0.18)},{int(ay_tip+sz*0.22)}" fill="{c1}" opacity="0.9"/>"""

    def bet_maker():
        # 주사위: 점 위치가 주소에 따라 변함
        c1 = color
        margin = int(sz*0.1)
        body = f'<rect x="{margin}" y="{margin}" width="{sz-2*margin}" height="{sz-2*margin}" rx="{int(sz*0.12)}" fill="#1a1a2a" stroke="{c1}" stroke-width="{max(int(sz*0.03),1.5)}"/>'
        dot_positions = [(0.28,0.28),(0.72,0.28),(0.28,0.72),(0.72,0.72),(0.5,0.5)]
        dots = ""
        for i,(dx,dy) in enumerate(dot_positions[:3+p[6]%3]):
            dots += f'<circle cx="{int(sz*dx)}" cy="{int(sz*dy)}" r="{max(int(sz*0.07),3)}" fill="{c1}" opacity="0.9"/>'
        return body + dots

    def sniper():
        # 조준경 + 십자선
        c1 = color; c2 = accent2
        cx=half; cy=half; r=int(sz*0.38)
        return f"""
  <circle cx="{cx}" cy="{cy}" r="{r}" fill="none" stroke="{c1}" stroke-width="{max(int(sz*0.04),2)}"/>
  <circle cx="{cx}" cy="{cy}" r="{int(r*0.55)}" fill="none" stroke="{c1}" stroke-width="1" opacity="0.5"/>
  <line x1="{cx-r}" y1="{cy}" x2="{cx+r}" y2="{cy}" stroke="{c1}" stroke-width="{max(int(sz*0.03),1.5)}"/>
  <line x1="{cx}" y1="{cy-r}" x2="{cx}" y2="{cy+r}" stroke="{c1}" stroke-width="{max(int(sz*0.03),1.5)}"/>
  <circle cx="{cx}" cy="{cy}" r="{max(int(sz*0.06),3)}" fill="{c2}"/>
  <circle cx="{cx+int(r*0.7)}" cy="{cy-int(r*0.7)}" r="{max(int(sz*0.04),2)}" fill="{c1}" opacity="0.6"/>"""

    def default_char():
        # 기본: 다이아몬드
        c1 = color
        cx=half; cy=half; r=int(sz*0.38)
        return f'<polygon points="{cx},{cy-r} {cx+r},{cy} {cx},{cy+r} {cx-r},{cy}" fill="none" stroke="{c1}" stroke-width="{max(int(sz*0.04),2)}" opacity="0.9"/><circle cx="{cx}" cy="{cy}" r="{max(int(sz*0.08),3)}" fill="{c1}" opacity="0.8"/>'

    type_map = {
        "Apex Predator": apex_predator,
        "Precision Hunter": steady_shot,
        "Ice Quant": ice_quant,
        "Sniper": sniper,
        "Steady Shot": steady_shot,
        "All-Rounder": steady_shot,
        "Momentum": momentum,
        "Value Hunter": momentum,
        "Bet Maker": bet_maker,
        "Degen": degen,
        "High Roller": degen,
        "Consistent": ice_quant,
        "Grinder": bet_maker,
        "Newcomer": default_char,
        "Drifter": default_char,
        "Underwater": default_char,
    }

    # 트레이더 타입에서 이모지 제거
    ttype = trader_type
    for emoji in ["🦁","🦅","🧊","🎯","📊","📈","💰","🎲","🌊","🎰","📦","⚙️","🌱","🌀","💀"]:
        ttype = ttype.replace(emoji,"").strip()

    fn = type_map.get(ttype, default_char)
    inner = fn()

    # identicon 패턴 (우하단 미니 뱃지)
    badge_size = max(sz // 4, 8)
    bx = sz - badge_size - 2; by = sz - badge_size - 2
    badge_cells = ""
    cell = badge_size // 3
    for row in range(3):
        for col in range(3):
            if h[(row*3+col) % 16] % 2 == 0:
                badge_cells += f'<rect x="{bx+col*cell}" y="{by+row*cell}" width="{cell}" height="{cell}" fill="{accent2}" opacity="0.8"/>'

    return (
        f'<svg viewBox="0 0 {sz} {sz}" width="{sz}" height="{sz}" xmlns="http://www.w3.org/2000/svg" '
        f'style="border-radius:8px;flex-shrink:0">'
        f'<rect width="{sz}" height="{sz}" rx="8" fill="{bg}" stroke="{color}" stroke-width="1.2"/>'
        f'{inner}'
        f'<rect x="{bx-1}" y="{by-1}" width="{badge_size+2}" height="{badge_size+2}" rx="3" fill="{bg}" stroke="{color}" stroke-width="0.8" opacity="0.7"/>'
        f'{badge_cells}'
        f'</svg>'
    )


def generate_html(all_stats, tournament, archive: ArchiveManager, hist_path: Path = None, war_hist_path: Path = None):
    import math as _math
    # 히스토리 Data 로드
    _hist_data = []
    try:
        _hp = hist_path or Path(HIST_FILE)
        if _hp.exists():
            _hist_data = json.loads(_hp.read_text(encoding="utf-8"))
    except Exception:
        pass
    hist_js = json.dumps(_hist_data, ensure_ascii=False)

    # WAR 히스토리 Data 로드
    _war_hist_data = []
    try:
        _whp = war_hist_path or Path(WAR_HIST_FILE)
        if _whp.exists():
            _war_hist_data = json.loads(_whp.read_text(encoding="utf-8"))
    except Exception:
        pass
    war_hist_js = json.dumps(_war_hist_data, ensure_ascii=False)

    # compute_stats()가 classify_trader_type() 단일 소스로 이미 분류 완료
    # → 강제 재분류 제거. 캐시된 trader_type을 그대로 신뢰.
    # 단, type/character가 아예 없는 경우(구버전 캐시)만 보정
    for s in all_stats:
        if not s.get('trader_type'):
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
        # 리포트에 며칠째 있는지
        try:
            _fs = archive.first_seen_str(addr) if archive else "-"
            _fs_date = datetime.strptime(_fs, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            s["days_in_report"] = (datetime.now(timezone.utc) - _fs_date).days
        except Exception:
            s["days_in_report"] = None

    radar_labels  = ["Profit","ROI","Big Bet","Sharpe","Win Rate"]
    radar_datasets = [{"label":s["label"],"data":[s["radar"]["profit_amt"],s["radar"]["roi"],
                        s["radar"]["big_bet"],s["radar"]["sharpe"],
                        s["radar"].get("win_rate",10)],
                       "color":palette[i%len(palette)]} for i,s in enumerate(ranked)]
    weeks = [r[0] for r in tournament.get("rounds", [])]
    weekly_series = [{"label":s["label"],
                      "data":[{w["week"]:w["pnl"] for w in s.get("weekly_pnl",[])}.get(w,0) for w in weeks],
                      "color":palette[ranked.index(s)%len(palette)]} for s in ranked]

    # wallets_meta.json 로드
    _meta = load_wallets_meta()

    cards_html = ""
    for rank, s in enumerate(ranked, 1):
        crown = "👑" if rank==1 else f"#{rank}"
        pnl_color = "#00f5d4" if s["total_pnl"] >= 0 else "#f72585"
        sc = "#00f5d4" if s["sharpe"]>1 else ("#ffbe0b" if s["sharpe"]>0 else "#f87171")
        dc = "#00f5d4" if s["durability"]>=60 else ("#ffbe0b" if s["durability"]>=35 else "#f72585")
        war_bar = min(int(s["war_score"]), 100)
        fs = s.get("follow_score", 0)
        fc = "#00f5d4" if fs >= 70 else ("#ffbe0b" if fs >= 45 else "#f72585")
        cc = palette[(rank-1) % len(palette)]
        cache_age = archive.age_str(s["address"]) if archive else "?"
        first_seen = archive.first_seen_str(s["address"]) if archive else "-"
        # first_seen 기준 7일 이내면 NEW 뱃지, days_in_report 계산
        days_in_report = None
        try:
            _fs_date = datetime.strptime(first_seen, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            _days_alive = (datetime.now(timezone.utc) - _fs_date).days
            is_new = _days_alive <= 7
            days_in_report = _days_alive
        except Exception:
            is_new = False
        src = s.get("source","manual")

        # ── wallets_meta 조회 ───────────────────────────────────────
        _m = _meta.get(s["address"].lower(), {})
        _meta_name    = _m.get("name", "")
        _meta_tags    = _m.get("tags", [])
        _meta_twitter = _m.get("twitter", "")
        _meta_note    = _m.get("note", "")
        # 메타 태그 뱃지 HTML
        _tag_colors = {"KOL":"#3a86ff","Whale":"#9b5de5","Dev":"#00f5d4",
                       "Fund":"#ffbe0b","Degen":"#f72585","Bot":"#888888"}
        _meta_tag_html = "".join(
            f'<span style="font-size:10px;background:#0a0a1e;color:{_tag_colors.get(t,"#aaaaaa")};border:0.5px solid {_tag_colors.get(t,"#aaaaaa")};border-radius:3px;padding:1px 6px;margin-left:4px;vertical-align:middle">{_he(t)}</span>'
            for t in _meta_tags
        )
        # 트위터 링크 버튼
        _twitter_btn = (
            f'<a href="{_he(_meta_twitter)}" target="_blank" onclick="event.stopPropagation()" '
            f'style="font-size:9px;padding:2px 7px;border-radius:4px;border:0.5px solid #1d9bf0;background:transparent;color:#1d9bf0;text-decoration:none;white-space:nowrap;font-family:DM Mono,monospace">𝕏 Twitter</a>'
        ) if _meta_twitter else ""

        # ── 팔로우 경고 배지 ──────────────────────────────────────
        _warn_badges = []
        if s.get("mdd_pct", 0) >= 40:
            _warn_badges.append(('<span style="font-size:9px;padding:2px 6px;border-radius:4px;'
                'background:#2a0010;color:#f72585;border:0.5px solid #f72585;white-space:nowrap">'
                '⚠ MDD {:.0f}%</span>').format(s["mdd_pct"]))
        if s.get("days_since_last", 0) >= 14:
            _warn_badges.append(('<span style="font-size:9px;padding:2px 6px;border-radius:4px;'
                'background:#1a1a00;color:#888800;border:0.5px solid #666600;white-space:nowrap">'
                '💤 Inactive {}d</span>').format(s["days_since_last"]))
        if s.get("closed_count", 0) < 50:
            _warn_badges.append(('<span style="font-size:9px;padding:2px 6px;border-radius:4px;'
                'background:#0d0d1a;color:#3a3a55;border:0.5px solid #2a2a45;white-space:nowrap">'
                '📉 Low Sample {}trades</span>').format(s["closed_count"]))
        _warn_html = ('<div style="display:flex;flex-wrap:wrap;gap:4px;margin-top:4px;margin-bottom:2px">'
                      + "".join(_warn_badges) + "</div>") if _warn_badges else ""

        n2=5; cx2,cy2,R2=110,115,75
        rv_list=[max(s["radar"]["profit_amt"],10),max(s["radar"]["roi"],10),max(s["radar"]["big_bet"],10),
                 max(s["radar"]["sharpe"],10),
                 max(s["radar"].get("win_rate",10),10)]
        lnames=["Profit","ROI","BigBet","Sharpe","WinRate"]
        bg_poly="".join(f'<polygon points="{" ".join(f"{cx2+R2*lvl*_math.cos(_math.pi*2*j/n2-_math.pi/2):.1f},{cy2+R2*lvl*_math.sin(_math.pi*2*j/n2-_math.pi/2):.1f}" for j in range(n2))}" fill="none" stroke="#1e1e35" stroke-width="1"/>' for lvl in [0.25,0.5,0.75,1.0])
        axes="".join(f'<line x1="{cx2}" y1="{cy2}" x2="{cx2+R2*_math.cos(_math.pi*2*j/n2-_math.pi/2):.1f}" y2="{cy2+R2*_math.sin(_math.pi*2*j/n2-_math.pi/2):.1f}" stroke="#2a2a45" stroke-width="1"/>' for j in range(n2))
        dpts=" ".join(f"{cx2+(v/100*R2)*_math.cos(_math.pi*2*j/n2-_math.pi/2):.1f},{cy2+(v/100*R2)*_math.sin(_math.pi*2*j/n2-_math.pi/2):.1f}" for j,v in enumerate(rv_list))
        data_poly=f'<polygon points="{dpts}" fill="{cc}33" stroke="{cc}" stroke-width="2"/>'
        lbls=""
        for j2,ln in enumerate(lnames):
            ang2=_math.pi*2*j2/n2-_math.pi/2; lx2=cx2+(R2+18)*_math.cos(ang2); ly2=cy2+(R2+18)*_math.sin(ang2)
            anc="middle" if abs(_math.cos(ang2))<0.3 else ("start" if _math.cos(ang2)>0 else "end")
            vc="#00f5d4" if rv_list[j2]>=60 else ("#ffbe0b" if rv_list[j2]>=40 else "#f87171")
            lbls+=f'<text x="{lx2:.1f}" y="{ly2:.1f}" text-anchor="{anc}" dominant-baseline="middle" fill="{vc}" font-size="13" font-family="DM Sans,sans-serif" font-weight="600">{ln}</text>'
        mini_svg=f'<svg viewBox="-50 -5 280 250" width="250" height="230">{bg_poly}{axes}{data_poly}{lbls}</svg>'

        # 스파크라인 SVG (최근 30일 누적 PnL)
        _cum = s.get('cumulative', [])
        # 최근 30일치만
        from datetime import datetime as _dt2, timedelta as _td
        _cutoff = (_dt2.utcnow() - _td(days=30)).strftime('%Y-%m-%d')
        _pts = [p for p in _cum if p['date'] >= _cutoff] or _cum[-30:]
        if len(_pts) >= 2:
            _vals = [p['cum'] for p in _pts]
            _min_v, _max_v = min(_vals), max(_vals)
            _rng = _max_v - _min_v or 1
            _W, _H = 200, 36
            _coords = ' '.join(
                f'{i*_W/(len(_pts)-1):.1f},{_H - (_v-_min_v)/_rng*_H:.1f}'
                for i, _v in enumerate(_vals)
            )
            _col = '#00f5d4' if _vals[-1] >= _vals[0] else '#f72585'
            _fill_pts = f'0,{_H} ' + _coords + f' {_W},{_H}'
            sparkline_svg = (
                f'<svg viewBox="0 0 {_W} {_H}" width="100%" height="36" preserveAspectRatio="none" style="display:block">'
                f'<defs><linearGradient id="sg{s["address"][-4:]}" x1="0" y1="0" x2="0" y2="1">'
                f'<stop offset="0%" stop-color="{_col}" stop-opacity="0.25"/>'
                f'<stop offset="100%" stop-color="{_col}" stop-opacity="0.02"/>'
                f'</linearGradient></defs>'
                f'<polygon points="{_fill_pts}" fill="url(#sg{s["address"][-4:]})" stroke="none"/>'
                f'<polyline points="{_coords}" fill="none" stroke="{_col}" stroke-width="1.5" stroke-linejoin="round"/>'
                f'</svg>'
            )
        else:
            sparkline_svg = '<div style="height:36px;background:#0a0a16;border-radius:4px"></div>'

        _SRC_MAP={"manual":("Manual","#888888"),"active":("Active","#3a86ff"),
                  "vault":("Vault","#9b5de5"),"cached":("Cached","#555555")}
        src_label,src_color=_SRC_MAP.get(src,(src,"#888888"))
        def _coin_tag(t):
            if isinstance(t, dict): c,p,sd = t["coin"],t["pnl"],t.get("side","")
            else: c,p,sd = t[0],t[1],""  # 구버전 캐시 호환
            col = "#00f5d4" if p>=0 else "#f72585"
            arrow = "▲" if sd=="L" else ("▼" if sd=="S" else "")
            return f'<span class="coin-tag" style="border-color:{col};color:{col}">{arrow}{c} ${p:+,.0f}</span>'
        top_coins = ''  # 실현손익 코인태그 제거

        # 현재 오픈 Positions 처리
        open_pos = s.get("positions", [])
        if open_pos:
            # 규모 기준 정렬, 상위 5개만
            sorted_pos = sorted(open_pos, key=lambda p: p["notional"], reverse=True)
            # 최대 규모 대비 10% 미만인 소액 Positions 제외
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

            # 개별 Positions 행
            rows_html = ""
            for p in filtered_pos:
                sc2  = "#3a86ff" if p["side"]=="LONG" else "#f72585"
                ic2  = "▲" if p["side"]=="LONG" else "▼"
                uc2  = "#00f5d4" if p["upnl"]>=0 else "#f87171"
                lev_ratio = p.get("lev", 0)
                if lev_ratio >= 1:
                    lev2 = f' x{lev_ratio:.1f}'
                elif lev_ratio > 0.01:
                    lev2 = f' x{lev_ratio:.2f}'
                else:
                    lev2 = ""
                rows_html += (
                    f'<div class="pos-row">'
                    f'<span style="color:{sc2};white-space:nowrap;overflow:hidden;text-overflow:ellipsis">{ic2} {p["coin"]}{lev2}</span>'
                    f'<span class="pos-ntl" style="white-space:nowrap;text-align:right">${p["notional"]:,.0f}</span>'
                    f'<span style="color:{uc2};font-size:9px;text-align:right;white-space:nowrap">uPnL {fmt_compact(p["upnl"])}</span>'
                    f'</div>'
                )
            positions_section = f'<div class="positions-block">{summary_html}{rows_html}</div>'
        else:
            upnl_val = s.get('total_upnl', 0)
            if upnl_val != 0:
                uc3 = '#00f5d4' if upnl_val >= 0 else '#f87171'
                positions_section = f'<div class="positions-block pos-empty">uPnL손익 <span style="color:{uc3}">${upnl_val:+,.0f}</span> (재수집 필요)</div>'
            else:
                positions_section = '<div class="positions-block pos-empty">— No positions</div>'

        cards_html += (
            f'<div class="trader-card" style="--card-accent:{cc};cursor:pointer" data-address="{s["address"]}" onclick="openModal(this.dataset.address)">'
            f'<div class="card-top"><div class="card-rank">{crown}</div>'
            f'<div class="card-name-block"><div class="card-name">{_he(_meta_name or s["label"])}'
            + (' <span style="font-size:11px;background:#2a1f00;color:#ffbe0b;border:1px solid #ffbe0b;border-radius:3px;padding:1px 5px;margin-left:6px;vertical-align:middle">⚡ HF</span>' if s.get('is_hf') else '')
            + (' <span style="font-size:11px;background:#1a0a2e;color:#9b5de5;border:1px solid #9b5de5;border-radius:3px;padding:1px 5px;margin-left:4px;vertical-align:middle">🏦 Vault</span>' if s.get('is_vault') else '')
            + (' <span style="font-size:11px;background:#001a0f;color:#00f5d4;border:1px solid #00f5d4;border-radius:3px;padding:1px 5px;margin-left:4px;vertical-align:middle;font-weight:700;letter-spacing:0.5px">NEW</span>' if is_new else '')
            + _meta_tag_html
            + '</div>'
            f'<div class="card-type">{s["trader_type"]} <span class="card-equity">${s["total_equity"]:,.0f}</span></div>'
            f'<div class="card-character">≈ {s["character"]}</div>'
            f'<div class="card-period">📅 {s["first_date"]} ~ {s["last_date"]} | {s["data_days"]}d | First seen {first_seen}'
            + (f' | <span style="color:#ffbe0b;font-weight:600">{days_in_report}d in report</span>' if days_in_report is not None else '')
            + '</div>'
            f'{_warn_html}'
            f'<div class="card-meta"><span style="color:{src_color}">● {src_label}</span> · <span style="color:#3a3a55">🕐 {cache_age}</span>'
            + (f' · {_twitter_btn}' if _twitter_btn else '')
            + '</div>'
            f'</div><div class="war-circle"><svg viewBox="0 0 60 60">'
            f'<circle cx="30" cy="30" r="24" fill="none" stroke="#1a1a2e" stroke-width="5"/>'
            f'<circle cx="30" cy="30" r="24" fill="none" stroke="{cc}" stroke-width="5" stroke-dasharray="{war_bar*1.508:.1f} 150.8" stroke-linecap="round" transform="rotate(-90 30 30)"/>'
            f'</svg><div class="war-val" style="color:{cc}">{s["war_score"]:.0f}</div><div class="war-label">WAR</div></div>'
            f'<div style="display:flex;flex-direction:column;gap:4px;margin-left:6px;align-self:flex-start;padding-top:4px">'
            f'<button class="like-btn" data-addr="{s["address"]}" onclick="event.stopPropagation();toggleLike(this)" style="font-size:10px;padding:3px 7px;border-radius:5px;border:0.5px solid #2a2a45;background:transparent;color:#555;cursor:pointer;display:flex;align-items:center;gap:3px;font-family:DM Mono,monospace">'
            f'<span class="like-icon">🤍</span><span class="like-count" style="font-size:9px">·</span></button>'
            f'<button class="save-btn" data-addr="{s["address"]}" onclick="event.stopPropagation();toggleSave(this)" style="font-size:10px;padding:3px 7px;border-radius:5px;border:0.5px solid #2a2a45;background:transparent;color:#555;cursor:pointer;font-family:DM Mono,monospace">'
            f'<span class="save-icon">🔖</span></button>'
            f'<div style="padding:3px 7px;border-radius:5px;border:0.5px solid {fc};background:transparent;text-align:center" title="Followability Score: low MDD, low big-bet, high consistency">'
            f'<div style="font-size:8px;color:#3a3a55;font-family:DM Mono,monospace">📋</div>'
            f'<div style="font-size:11px;font-weight:700;font-family:DM Mono,monospace;color:{fc};line-height:1.2">{fs:.0f}</div>'
            f'</div>'
            f'<div class="comment-count-badge" style="font-size:9px;padding:2px 5px;border-radius:4px;color:#3a86ff;min-height:18px;font-family:DM Mono,monospace;text-align:center"></div>'
            f'</div></div>'
            f'<div class="card-body"><div class="mini-radar" style="align-self:center">{mini_svg}</div><div class="card-right">'

            f'<div class="key-stats">'
            f'<div class="ks"><div class="ks-v" style="color:{pnl_color}">{fmt_compact(s["total_pnl"])}</div><div class="ks-l">Total PnL</div></div>'
            f'<div class="ks"><div class="ks-v">{s["win_rate"]:.0f}%</div><div class="ks-l">Win Rate</div></div>'
            f'<div class="ks"><div class="ks-v" style="color:{sc}">{s["sharpe"]:.2f}</div><div class="ks-l" title="Estimated risk-adjusted return. Non-standard calculation — use as relative indicator only.">Sharpe* ⓘ</div></div>'
            f'<div class="ks"><div class="ks-v">{s["roi_pct"]:.1f}%</div><div class="ks-l">ROI</div></div>'
            f'<div class="ks"><div class="ks-v">{s["big_bet_rate"]:.0f}%</div><div class="ks-l" title="% of large positions (>15% of equity) that were profitable. High = knows when to go big.">Big Bet Hit ⓘ</div></div>'
            f'</div>'
            f'<div class="section-label" style="margin-top:8px">📈 Recent PnL</div>'
            f'<div style="margin-bottom:8px">{sparkline_svg}</div>'
            f'<div class="section-label" style="margin-top:4px">📍 Current Positions</div>'
            f'<div class="pos-section">{positions_section}</div>'
            f'<div class="bottom-row"><div class="coins">{top_coins}'
            + ((''.join(
                f'<span class="coin-tag" style="color:#ffbe0b;border-color:#ffbe0b">📦{h["coin"]} '
                + (f'{h["amount"]:,.2f}' if any(k in h['coin'].upper() for k in ['BTC','ETH','SOL','HYPE']) else f'{h["amount"]:,.0f}')
                + '</span>'
                for h in s.get('spot_holdings',[]))) if s.get('spot_holdings') else '')
            + '</div>'
            f'<div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">'
            f'<div class="tourney-badge">🏆 {s["tourney_wins"]}wk #1 · {s["tourney_score"]}pt</div>'
            + (f'<span style="font-size:8px;padding:1px 6px;border-radius:3px;border:0.5px solid {"#00f5d4" if s.get("confidence")=="High Confidence" else "#ffbe0b" if s.get("confidence")=="Medium Confidence" else "#4a4a6a"};color:{"#00f5d4" if s.get("confidence")=="High Confidence" else "#ffbe0b" if s.get("confidence")=="Medium Confidence" else "#888"};font-family:DM Mono,sans-serif">{s.get("confidence","")}</span>' if s.get('confidence') else '')
            + '</div>'

            f'</div></div></div></div>'
        )

    # ── 센티먼트 계산 ────────────────────────────────────────────────
    # WAR 구간: 50-60, 60-70, 70-80, 80+
    # 구간별 가중치: 높은 WAR 구간일수록 더 큰 영향
    WAR_BANDS = [
        (50, 60, "50-60", 0.55),
        (60, 70, "60-70", 0.65),
        (70, 80, "70-80", 0.75),
        (80, 200,"80+",   0.85),
    ]

    def calc_band_sentiment(stats_list):
        """그룹 전체 Portfolio 합 기준 — 롱/숏 Positions 총합 / Portfolio 총합
        Portfolio 큰 Trader가 자연스럽게 더 영향, 소액은 희석
        코인별도 동일: 그룹 전체 롱규모합 / 그룹 전체 Portfolio합"""
        pos_traders = [s for s in stats_list
                       if s.get('positions') and s.get('war_score',0)>=50]
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

        # 그룹 전체 equity를 특수 키로 저장 (포지션 없는 트레이더 포함한 전체)
        # pos_traders 뿐 아니라 stats_list 전체 equity 합산
        group_total_eq = sum(max(s.get('total_equity',1),1) for s in stats_list) or total_eq
        coin_data['__total_eq__'] = group_total_eq
        for c in coin_data:
            if c != '__total_eq__':
                coin_data[c]['total_eq'] = group_total_eq

        result = {
            'long_pct':  round(total_long  / total_eq * 100, 1),
            'short_pct': round(total_short / total_eq * 100, 1),
            'traders':   count,
        }
        return result, coin_data

    def merge_coin_data(band_coin_datas, weights):
        """전체 그룹 합산 ntl / 전체 그룹 합산 equity 방식
        포지션 없는 트레이더도 분모에 포함 → 희석 왜곡 방지"""
        merged = {}
        for cd, w in zip(band_coin_datas, weights):
            eq = cd.get('__total_eq__', 1) or 1  # 그룹 전체 equity
            for coin, v in cd.items():
                if coin == '__total_eq__': continue
                long_pct  = v['long_ntl']  / eq * 100
                short_pct = v['short_ntl'] / eq * 100
                if long_pct == 0 and short_pct == 0: continue
                if coin not in merged:
                    merged[coin] = {'long_ntl_w':0,'short_ntl_w':0,'eq_w':0}
                merged[coin]['long_ntl_w']  += v['long_ntl']  * w
                merged[coin]['short_ntl_w'] += v['short_ntl'] * w
                merged[coin]['eq_w']        += eq * w
        # 최종 pct 계산
        result = {}
        for coin, v in merged.items():
            eq_w = v['eq_w'] or 1
            result[coin] = {
                'long_pct_w':  v['long_ntl_w']  / eq_w * 100 * (sum(weights) or 1),
                'short_pct_w': v['short_ntl_w'] / eq_w * 100 * (sum(weights) or 1),
                'total_w':     sum(weights) or 1,
            }
        return result

    # By WAR Band 센티먼트 (각 그룹 단순 평균)
    sent_bands = []
    band_coin_datas = []
    for lo, hi, label, w in WAR_BANDS:
        band_stats = [s for s in all_stats if lo <= s.get('war_score',0) < hi]
        r, cd_b = calc_band_sentiment(band_stats)
        sent_bands.append({'label': label, 'result': r, 'count': len(band_stats), 'weight': w})
        band_coin_datas.append((cd_b, w))

    # 전체 센티먼트 = 전체 Trader Portfolio 합산 방식
    all_pos_traders = [s for s in all_stats if s.get('positions') and s.get('war_score',0)>=50]
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

    # 그룹별 Bubble Data (전체 Portfolio합 방식)
    def _norm_coin_dir(v):
        if isinstance(v, dict):
            return {
                'long_ntl': float(v.get('long_ntl', 0) or 0),
                'short_ntl': float(v.get('short_ntl', 0) or 0),
                'total_eq': float(v.get('total_eq', 1) or 1),
            }
        if isinstance(v, (int, float)):
            fv = float(v)
            return {'long_ntl': max(fv, 0.0), 'short_ntl': max(-fv, 0.0), 'total_eq': 1.0}
        return {'long_ntl': 0.0, 'short_ntl': 0.0, 'total_eq': 1.0}

    band_bubble_rows = {}
    for (lo, hi, label, bw), (cd_b, _) in zip(WAR_BANDS, band_coin_datas):
        rows = []
        cd_b = cd_b or {}
        for coin, raw_v in sorted(cd_b.items(),
                               key=lambda x: _norm_coin_dir(x[1]).get('long_ntl',0)+_norm_coin_dir(x[1]).get('short_ntl',0),
                               reverse=True):
            if coin == '__total_eq__':
                continue
            v = _norm_coin_dir(raw_v)
            eq = v.get('total_eq', 1) or 1
            lp = round(v['long_ntl']  / eq * 100, 1)
            sp = round(v['short_ntl'] / eq * 100, 1)
            if lp + sp < MIN_BUBBLE_PCT: continue
            rows.append({'coin': coin, 'avg_long_eq_pct': lp, 'avg_short_eq_pct': sp,
                         'long_ntl': round(v['long_ntl']), 'short_ntl': round(v['short_ntl'])})
        band_bubble_rows[label] = rows

    # By WAR Band 코인 센티먼트
    coin_band_rows = {}
    for (lo, hi, label, bw), (cd_b, _) in zip(WAR_BANDS, band_coin_datas):
        cd_b = cd_b or {}
        for coin, raw_v in cd_b.items():
            if coin == '__total_eq__':
                continue
            v = _norm_coin_dir(raw_v)
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

    # By Type Bubble + 센티먼트 계산
    TYPE_ORDER = [
        "🦁 Apex Predator", "🦅 Precision Hunter", "🧊 Ice Quant",
        "🎯 Sniper", "📊 All-Rounder",
        "📈 Momentum", "🎯 Steady Shot", "💰 Value Hunter", "🎲 Bet Maker",
        "🌊 Degen", "🎰 High Roller", "📦 Consistent", "⚙️ Grinder",
        "🌱 Newcomer", "🌀 Drifter", "💀 Underwater",
    ]
    type_bubble_rows = {}
    type_sent_rows = []
    for ttype in TYPE_ORDER:
        type_stats = [s for s in all_stats if s.get('trader_type') == ttype]
        # Positions 있는 Trader만으로 Bubble/센티먼트 계산
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
        # Bubble 행
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

    # By Portfolio Size 그룹
    EQUITY_BANDS = [
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
        f'<div class="legend-name">#{i+1} {_he(s["label"])}<br><span style="font-size:10px;color:var(--dim)">{s["trader_type"]}</span></div>'
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
<!-- Google tag (gtag.js) -->
<script async src="https://www.googletagmanager.com/gtag/js?id=G-4VY35WKN8Z"></script>
<script>
  window.dataLayer = window.dataLayer || [];
  function gtag(){{dataLayer.push(arguments);}}
  gtag('js', new Date());
  gtag('config', 'G-4VY35WKN8Z');
</script>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Wallet Scouting Report</title>
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>⚡</text></svg>">
<link href="https://fonts.googleapis.com/css2?family=Bebas+Neue&family=DM+Mono:wght@400;500&family=DM+Sans:wght@300;400;600&display=swap" rel="stylesheet">
<style>
:root{{--bg:#080810;--surface:#0e0e1a;--border:#1e1e35;--text:#c8d0e7;--dim:#4a4a6a;--green:#00f5d4;--red:#f72585;--yellow:#ffbe0b;--blue:#3a86ff;}}
*{{margin:0;padding:0;box-sizing:border-box;}}body{{background:var(--bg);color:var(--text);font-family:'DM Sans',sans-serif;}}
.header{{padding:40px 32px 24px;border-bottom:1px solid var(--border);display:flex;align-items:flex-end;justify-content:space-between;}}
.header-title{{font-family:'Bebas Neue',sans-serif;font-size:52px;letter-spacing:4px;line-height:1;background:linear-gradient(135deg,#00f5d4,#3a86ff);-webkit-background-clip:text;-webkit-text-fill-color:transparent;}}
.header-sub{{font-family:'DM Mono',monospace;font-size:11px;color:var(--dim);margin-top:6px;}}
.header-meta{{font-family:'DM Mono',monospace;font-size:11px;color:var(--dim);text-align:right;}}
.tabs{{display:flex;flex-wrap:wrap;padding:0 24px;border-bottom:1px solid var(--border);gap:0;}}
.tab{{padding:12px 18px;font-size:11px;font-weight:600;letter-spacing:0.5px;text-transform:uppercase;cursor:pointer;border-bottom:2px solid transparent;color:var(--dim);transition:.2s;white-space:nowrap;}}
.tab.active{{color:var(--green);border-bottom-color:var(--green);}}
.section{{display:none;padding:32px;}}.section.active{{display:block;}}
.cards-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(400px,1fr));gap:20px;}}
.trader-card{{background:var(--surface);border:1px solid var(--border);border-radius:16px;padding:24px;transition:border-color .2s,transform .2s;position:relative;}}
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
.pos-row{{display:grid;grid-template-columns:80px 1fr 1fr;align-items:center;font-size:10px;font-family:'DM Mono',monospace;gap:4px;min-width:0;overflow:hidden;}}
.pos-ntl{{color:#888;font-size:9px;text-align:right;}}
.pos-empty{{font-size:9px;color:#3a3a55;font-family:'DM Mono',monospace;}}
.war-circle{{position:relative;width:60px;height:60px;flex-shrink:0;}}.war-circle svg{{width:60px;height:60px;}}
.war-val{{position:absolute;top:50%;left:50%;transform:translate(-50%,-60%);font-family:'Bebas Neue',sans-serif;font-size:18px;}}
.war-label{{position:absolute;top:50%;left:50%;transform:translate(-50%,20%);font-size:8px;color:var(--dim);letter-spacing:1px;}}
.card-body{{display:flex;flex-direction:column;gap:10px;align-items:stretch;}}.mini-radar{{align-self:center;flex-shrink:0;}}.card-right{{flex:1;min-width:0;overflow:hidden;display:flex;flex-direction:column;gap:10px;}}
.key-stats{{display:grid;grid-template-columns:repeat(5,1fr);gap:6px;}}
.ks{{background:#0a0a16;border-radius:8px;padding:8px 6px;text-align:center;min-width:0;overflow:hidden;}}
.ks-v{{font-family:'DM Mono',monospace;font-size:13px;font-weight:500;color:var(--text);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}}.ks-l{{font-size:9px;color:var(--dim);margin-top:2px;letter-spacing:.5px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}}
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
.modal-pos-row{{display:grid;grid-template-columns:100px 1fr 1fr;align-items:center;padding:6px 0;border-bottom:1px solid #1a1a2e;font-family:'DM Mono',monospace;font-size:11px;gap:4px;}}
.modal-coin-row{{display:flex;justify-content:space-between;align-items:center;padding:5px 0;font-family:'DM Mono',monospace;font-size:11px;border-bottom:1px solid #1a1a2e;}}

/* ── responsive: step-by-step ── */

/* 1200px↓: Card 2col */
@media (max-width:1200px){{
}}

/* 900px: single col radar */
@media (max-width:900px){{
  .radar-wrap{{grid-template-columns:1fr;gap:16px;}}
  .sent-war-grid{{grid-template-columns:repeat(3,1fr)!important;}}
  .sent-type-grid{{grid-template-columns:repeat(3,1fr)!important;}}
}}

/* 768px: mobile */
@media (max-width:768px){{
  /* header */
  .header{{padding:16px;flex-direction:column;gap:8px;}}
  .header-title{{font-size:28px;letter-spacing:2px;}}
  .header-meta{{display:none;}}

  /* tabs: horizontal scroll */
  .tabs{{padding:0;overflow-x:auto;-webkit-overflow-scrolling:touch;scrollbar-width:none;flex-wrap:nowrap;}}
  .tabs::-webkit-scrollbar{{display:none;}}
  .tab{{padding:12px 16px;font-size:11px;letter-spacing:0;white-space:nowrap;flex-shrink:0;}}

  /* section padding */
  .section{{padding:12px;}}

  /* trader card: vertical stack */
  .trader-card{{padding:14px;overflow:hidden;}}
  .card-rank{{font-size:18px;min-width:26px;}}
  .card-name{{font-size:18px;}}

  /* card body: vertical */
  .card-body{{flex-direction:column;gap:10px;}}
  .mini-radar{{align-self:center;}}
  .card-right{{width:100%;}}
  .key-stats{{gap:4px;grid-template-columns:repeat(3,1fr);}}
  .ks-v{{font-size:11px;}}
  .ks-l{{font-size:8px;}}

  /* radar compare tab */
  .radar-wrap{{grid-template-columns:1fr;gap:12px;}}
  .radar-canvas-wrap{{min-height:320px;}}

  /* modal: fullscreen */
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

  /* modal grid: 1col */
  .modal-grid{{grid-template-columns:1fr!important;gap:10px;}}
  .modal-stats{{grid-template-columns:repeat(3,1fr);gap:4px;}}
  .modal-stat{{padding:8px 4px;}}
  .modal-stat .v{{font-size:11px;}}

  /* sentiment: 2col grid */
  .sent-war-grid{{grid-template-columns:repeat(2,1fr)!important;}}
  .sent-type-grid{{grid-template-columns:repeat(2,1fr)!important;}}
  #sent-bubble-wrap{{height:260px!important;}}

  /* tournament */
  .tourney-table{{font-size:10px;display:block;overflow-x:auto;white-space:nowrap;}}
  .tourney-header{{gap:8px;flex-wrap:wrap;}}
  .tourney-stat{{padding:10px 12px;}}
  .tourney-stat .val{{font-size:20px;}}
}}

/* 480px: extra small */
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
  <div class="tab active" onclick="showTab('signal',event)">⚡ Signal</div>
  <div class="tab" onclick="showTab('cards',event)">🃏 Traders</div>
  <div class="tab" onclick="showTab('styles',event)">🗺 Styles</div>
  <div class="tab" onclick="showTab('radar',event)">🕸 Compare</div>
  <div class="tab" onclick="showTab('sentiment',event)">📡 Sentiment</div>
  <div class="tab" onclick="showTab('tourney',event)">🏆 Tournament</div>
  <div class="tab" onclick="showTab('lookup',event)">🔍 Lookup</div>
  <div class="tab" onclick="showTab('watchlist',event)">⭐ Watchlist</div>
  <div class="tab" onclick="showTab('guestbook',event)">📝 Guestbook</div>
</div>
<div class="section active" id="tab-signal">
  <div id="signal-root" style="max-width:900px;margin:0 auto"></div>
</div>
<div class="section" id="tab-cards">
  <div id="war-alert-banner" style="margin-bottom:12px"></div>
  <div style="padding:10px 0 14px;border-bottom:0.5px solid #1e1e35;margin-bottom:16px">
    <div id="type-filter-bar" style="display:flex;flex-wrap:wrap;gap:5px;margin-bottom:10px;min-height:28px"></div>
    <div style="display:flex;flex-wrap:wrap;gap:8px;align-items:center">
      <select id="filter-source" onchange="applyCardFilters()" style="font-size:11px;padding:4px 8px;border-radius:6px;border:0.5px solid #2a2a45;background:#111120;color:#c8d0e7;font-family:DM Mono,monospace">
        <option value="">All Sources</option>
        <option value="vault">Vault</option>
        <option value="active">Active</option>
        <option value="manual">Manual</option>
      </select>
      <select id="filter-conf" onchange="applyCardFilters()" style="font-size:11px;padding:4px 8px;border-radius:6px;border:0.5px solid #2a2a45;background:#111120;color:#c8d0e7;font-family:DM Mono,monospace">
        <option value="">All Confidence</option>
        <option value="High Confidence">High Confidence</option>
        <option value="Medium Confidence">Medium Confidence</option>
        <option value="Early Read">Early Read</option>
      </select>
      <select id="sort-by" onchange="applyCardFilters()" style="font-size:11px;padding:4px 8px;border-radius:6px;border:0.5px solid #2a2a45;background:#111120;color:#c8d0e7;font-family:DM Mono,monospace">
        <option value="war">Sort: WAR</option>
        <option value="pnl">Sort: Total PnL</option>
        <option value="winrate">Sort: Win Rate</option>
        <option value="sharpe">Sort: Sharpe*</option>
        <option value="roi">Sort: ROI</option>
        <option value="bigbet">Sort: Big Bet Hit</option>
        <option value="equity">Sort: Equity</option>
        <option value="follow">Sort: Followability</option>
        <option value="likes">Sort: Likes</option>
      </select>
      <span id="filter-count" style="font-size:10px;color:#4a6a4a;font-family:DM Mono,monospace;font-weight:500"></span>
      <button onclick="resetCardFilters()" style="font-size:10px;padding:3px 8px;border-radius:6px;border:0.5px solid #2a2a45;background:transparent;color:#555;cursor:pointer;margin-left:auto">↺ Reset</button>
    </div>
  </div>
  <div class="cards-grid" id="cards-grid-inner">{cards_html}</div>
  <div id="cards-pagination" style="display:flex;justify-content:center;align-items:center;gap:6px;padding:24px 0;flex-wrap:wrap"></div>
</div>
<div class="section" id="tab-styles">
  <div style="background:#111120;border-radius:10px;padding:20px">
    <div style="font-size:12px;font-weight:600;color:#c8d0e7;margin-bottom:2px">🗺 Playstyle Map</div>
    <div style="font-size:10px;color:#3a3a55;margin-bottom:10px">x: Big Bet Rate (passive → aggressive) · y: Sharpe* (inefficient → efficient) · positions based on actual classification rules · hover to inspect</div>
    <div id="playstyle-map" style="position:relative;width:100%;height:400px;background:#0a0a16;border-radius:8px;border:0.5px solid #1e1e35;overflow:hidden">
      <div id="psm-tip" style="position:absolute;background:#0d0d1a;border:0.5px solid #2a2a45;border-radius:6px;padding:8px 12px;font-size:11px;color:#c8d0e7;pointer-events:none;display:none;z-index:20;max-width:220px;line-height:1.6;font-family:DM Mono,monospace;"></div>
    </div>
    <div style="display:flex;flex-wrap:wrap;gap:10px;margin-top:12px;font-family:DM Mono,monospace" id="psm-legend"></div>
  </div>

  <div style="margin-top:24px;background:#111120;border-radius:10px;padding:20px">
    <div style="font-size:12px;font-weight:600;color:#c8d0e7;margin-bottom:4px">📖 Type Guide</div>
    <div style="font-size:10px;color:#3a3a55;margin-bottom:14px">Classification rules for each trader type</div>
    <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:8px">
      <div style="background:#0a0a16;border-radius:8px;padding:10px 12px;border:0.5px solid #1e1e35"><div style="font-size:11px;font-weight:600;color:#ffbe0b;margin-bottom:4px">🦁 Apex Predator</div><div style="font-size:10px;color:#555;font-family:DM Mono,monospace">big_bet_rate &gt; 60% · win_rate &gt; 65% · durability &gt; 50</div></div>
      <div style="background:#0a0a16;border-radius:8px;padding:10px 12px;border:0.5px solid #1e1e35"><div style="font-size:11px;font-weight:600;color:#00f5d4;margin-bottom:4px">🦅 Precision Hunter</div><div style="font-size:10px;color:#555;font-family:DM Mono,monospace">win_rate &gt; 72% · Sharpe* &gt; 3 · PnL Draw &lt; 25%</div></div>
      <div style="background:#0a0a16;border-radius:8px;padding:10px 12px;border:0.5px solid #1e1e35"><div style="font-size:11px;font-weight:600;color:#3a86ff;margin-bottom:4px">🧊 Ice Quant</div><div style="font-size:10px;color:#555;font-family:DM Mono,monospace">Sharpe* &gt; 5 · PnL Draw &lt; 15% · durability &gt; 50</div></div>
      <div style="background:#0a0a16;border-radius:8px;padding:10px 12px;border:0.5px solid #1e1e35"><div style="font-size:11px;font-weight:600;color:#00f5d4;margin-bottom:4px">🎯 Sniper</div><div style="font-size:10px;color:#555;font-family:DM Mono,monospace">profit_factor &gt; 3 · big_bet_count &gt; 3 · win_rate &lt; 55%</div></div>
      <div style="background:#0a0a16;border-radius:8px;padding:10px 12px;border:0.5px solid #1e1e35"><div style="font-size:11px;font-weight:600;color:#f72585;margin-bottom:4px">🎰 High Roller</div><div style="font-size:10px;color:#555;font-family:DM Mono,monospace">PnL Draw &gt; 40% · profit_factor &gt; 1.5</div></div>
      <div style="background:#0a0a16;border-radius:8px;padding:10px 12px;border:0.5px solid #1e1e35"><div style="font-size:11px;font-weight:600;color:#9b5de5;margin-bottom:4px">📊 All-Rounder</div><div style="font-size:10px;color:#555;font-family:DM Mono,monospace">win_rate &gt; 60% · durability &gt; 55</div></div>
      <div style="background:#0a0a16;border-radius:8px;padding:10px 12px;border:0.5px solid #1e1e35"><div style="font-size:11px;font-weight:600;color:#ffbe0b;margin-bottom:4px">📈 Momentum</div><div style="font-size:10px;color:#555;font-family:DM Mono,monospace">Sharpe* &gt; 3 · total_pnl &gt; 0</div></div>
      <div style="background:#0a0a16;border-radius:8px;padding:10px 12px;border:0.5px solid #1e1e35"><div style="font-size:11px;font-weight:600;color:#9b5de5;margin-bottom:4px">🎯 Steady Shot</div><div style="font-size:10px;color:#555;font-family:DM Mono,monospace">win_rate &gt; 65% · total_pnl &gt; 0</div></div>
      <div style="background:#0a0a16;border-radius:8px;padding:10px 12px;border:0.5px solid #1e1e35"><div style="font-size:11px;font-weight:600;color:#3a86ff;margin-bottom:4px">💰 Value Hunter</div><div style="font-size:10px;color:#555;font-family:DM Mono,monospace">profit_factor &gt; 2 · total_pnl &gt; 0</div></div>
      <div style="background:#0a0a16;border-radius:8px;padding:10px 12px;border:0.5px solid #1e1e35"><div style="font-size:11px;font-weight:600;color:#ffbe0b;margin-bottom:4px">🎲 Bet Maker</div><div style="font-size:10px;color:#555;font-family:DM Mono,monospace">big_bet_count &gt; 20 · big_bet_rate &gt; 50%</div></div>
      <div style="background:#0a0a16;border-radius:8px;padding:10px 12px;border:0.5px solid #1e1e35"><div style="font-size:11px;font-weight:600;color:#f72585;margin-bottom:4px">🌊 Degen</div><div style="font-size:10px;color:#555;font-family:DM Mono,monospace">PnL Draw &gt; 200% or (PnL Draw &gt; 80% + big_bets &gt; 10)</div></div>
      
      <div style="background:#0a0a16;border-radius:8px;padding:10px 12px;border:0.5px solid #1e1e35"><div style="font-size:11px;font-weight:600;color:#4a4a6a;margin-bottom:4px">🌱 Newcomer</div><div style="font-size:10px;color:#555;font-family:DM Mono,monospace">closed_count &lt; 100</div></div>
      <div style="background:#0a0a16;border-radius:8px;padding:10px 12px;border:0.5px solid #1e1e35"><div style="font-size:11px;font-weight:600;color:#4a4a6a;margin-bottom:4px">🌀 Drifter</div><div style="font-size:10px;color:#555;font-family:DM Mono,monospace">fallback — no dominant pattern</div></div>
    </div>
  </div>
</div>

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
<div class="section" id="tab-watchlist"><div id="watchlist-root" style="padding:24px 0"></div></div>
<div class="section" id="tab-guestbook"><div id="guestbook-root" style="max-width:700px;margin:0 auto;padding:24px 0"></div></div>
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
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/luxon@3.4.4/build/global/luxon.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-luxon@1.3.1/dist/chartjs-adapter-luxon.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/html2canvas@1.4.1/dist/html2canvas.min.js"></script>
<script>
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
window.ALL_STATS = window.ALL_STATS || [];
window.WALLET_META = window.WALLET_META || {};

function copyAddr(addr) {
  navigator.clipboard.writeText(addr).catch(()=>{
    const el=document.createElement('textarea');
    el.value=addr; document.body.appendChild(el); el.select();
    document.execCommand('copy'); document.body.removeChild(el);
  });
  const el=document.getElementById('copy-btn');
  if(el){el.textContent='✓ Copied';setTimeout(()=>{el.textContent='📋 Copy';},1500);}
}

function openModal(addr) {
  if(!addr) return;
  // 대소문자 무관 매칭
  const s = ALL_STATS.find(x => x.address && x.address.toLowerCase() === addr.toLowerCase());
  if (!s) return;
  // WAR 등수 계산
  const _sortedByWar = ALL_STATS.slice().sort((a,b)=>(b.war_score||0)-(a.war_score||0));
  const _warRank = _sortedByWar.findIndex(x=>x.address===addr) + 1;
  const cc = s._color || '#00f5d4';
  const pnlColor = s.total_pnl >= 0 ? '#00f5d4' : '#f72585';
  const sc = s.sharpe > 1 ? '#00f5d4' : s.sharpe > 0 ? '#ffbe0b' : '#f87171';
  const dc = s.durability >= 60 ? '#00f5d4' : s.durability >= 35 ? '#ffbe0b' : '#f72585';

  const cumDates = s.cumulative.map(p => p.date);
  const cumVals  = s.cumulative.map(p => p.cum);

  // Positions: notional 내림차순 정렬 후 토글 박스
  let posChangeHTML = buildPosChangeHTML(s);
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
        <span style="color:${uc};text-align:right;flex:1">uPnL $${p.upnl>=0?'+':''}${p.upnl.toLocaleString()}</span>
      </div>`;
    }).join('');
    posHTML = `
      <div style="max-height:220px;overflow-y:auto;border:1px solid #1e1e35;border-radius:8px;padding:8px">
        ${rows}
      </div>`;
  } else {
    posHTML = '<div style="color:#333;font-size:11px;padding:8px 0">Positions N/A</div>';
  }

  // 코인 행
  let coinRows = '';
  (s.top_coins || []).forEach(t => {
    const c = typeof t === 'object' ? t : {coin:t[0], pnl:t[1], side:'?'};
    const pnlCol = c.pnl >= 0 ? '#00f5d4' : '#f72585';
    const sideCol = c.side === 'L' ? '#3a86ff' : c.side === 'S' ? '#f72585' : '#888';
    const arr = c.side === 'L' ? '▲' : c.side === 'S' ? '▼' : '';
    coinRows += `<div class="modal-coin-row">
      <span style="color:${sideCol}">${arr} ${c.coin}</span>
      <span style="color:${pnlCol}">$${c.pnl>=0?'+':''}${c.pnl.toLocaleString()}</span>
    </div>`;
  });

  // 외부 링크
  const addrFull = s.address;
  const links = [
    {name:'HypurrScan', url:`https://hypurrscan.io/address/${addrFull}`},
    {name:'HL Explorer', url:`https://app.hyperliquid.xyz/explorer/address/${addrFull}`},
    {name:'DeBank',      url:`https://debank.com/profile/${addrFull}`},
    {name:'Arkham',      url:`https://platform.arkhamintelligence.com/explorer/address/${addrFull}`},
    {name:'Search / 𝕏', url:`https://x.com/search?q=${addrFull}&src=typed_query`},
  ];
  const linkHTML = links.map(l =>
    `<a href="${l.url}" target="_blank" style="font-size:10px;padding:3px 8px;border:1px solid #2a2a45;border-radius:4px;color:#888;text-decoration:none;white-space:nowrap" onmouseover="this.style.color='#c8d0e7';this.style.borderColor='#4a4a6a'" onmouseout="this.style.color='#888';this.style.borderColor='#2a2a45'">${l.name} ↗</a>`
  ).join('');

  document.getElementById('modal-content').innerHTML = `
    <div class="modal-header">
      <div style="flex:1;min-width:0">
        <div style="display:flex;align-items:baseline;gap:8px;flex-wrap:wrap">
          <div class="modal-title" style="color:${cc}">${(WALLET_META[s.address.toLowerCase()]||{}).name || s.label}</div>
          <span style="font-size:11px;font-weight:600;padding:2px 8px;border-radius:4px;background:#1a1a2e;border:0.5px solid #2a2a45;color:#888;font-family:DM Mono,monospace;white-space:nowrap">#${_warRank} WAR</span>
        </div>
        <div class="modal-sub" style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-top:4px">
          <span style="font-family:'DM Mono',monospace;font-size:11px;color:#4a4a6a;cursor:pointer" onclick="copyAddr('${addrFull}')" title="Click to copy">${addrFull.slice(0,8)}...${addrFull.slice(-6)}</span>
          <button id="copy-btn" onclick="copyAddr('${addrFull}')" style="font-size:9px;padding:2px 7px;border:1px solid #2a2a45;border-radius:4px;background:none;color:#888;cursor:pointer">📋 Copy</button>
        </div>
        <div class="modal-sub" style="margin-top:4px;display:flex;align-items:center;gap:8px;flex-wrap:wrap">
          <span>${s.trader_type} · ≈ ${s.character}</span>
          <span style="font-size:9px;padding:2px 7px;border-radius:4px;border:0.5px solid ${
            s.confidence==='High Confidence'?'#00f5d4':s.confidence==='Medium Confidence'?'#ffbe0b':'#4a4a6a'
          };color:${
            s.confidence==='High Confidence'?'#00f5d4':s.confidence==='Medium Confidence'?'#ffbe0b':'#888'
          };background:${
            s.confidence==='High Confidence'?'#001a0f':s.confidence==='Medium Confidence'?'#2a1f00':'#111120'
          };font-family:DM Mono,monospace">${s.confidence||''}</span>
        </div>
        <div class="modal-sub" style="margin-top:4px">📅 ${s.first_date} ~ ${s.last_date} &nbsp;|&nbsp; ${s.data_days}d &nbsp;|&nbsp; $${s.total_equity.toLocaleString()}${s.days_in_report!=null?' &nbsp;|&nbsp; <span style="color:#ffbe0b;font-weight:600">'+s.days_in_report+'d in report</span>':''}</div>
        <div style="display:flex;gap:6px;flex-wrap:wrap;margin-top:8px">${linkHTML}</div>
      </div>
      <div style="flex-shrink:0;margin-left:16px;display:flex;flex-direction:column;align-items:center;gap:8px">
        <div class="modal-war">
          <div class="war-num" style="color:${cc}">${s.war_score}</div>
          <div class="war-lbl">WAR</div>
        </div>
        <div style="display:flex;gap:6px">
          <button id="modal-like-btn" data-addr="${addrFull}"
            onclick="toggleLike(this)"
            style="font-size:11px;padding:4px 9px;border-radius:6px;border:0.5px solid #2a2a45;background:transparent;color:#555;cursor:pointer;display:flex;align-items:center;gap:3px;font-family:DM Mono,monospace">
            <span class="like-icon">🤍</span><span class="like-count" style="font-size:10px">·</span>
          </button>
          <button id="modal-save-btn" data-addr="${addrFull}"
            onclick="toggleSave(this)"
            style="font-size:11px;padding:4px 9px;border-radius:6px;border:0.5px solid #2a2a45;background:transparent;color:#555;cursor:pointer;font-family:DM Mono,monospace"
            title="Add to Watchlist">
            <span class="save-icon">🔖</span>
          </button>
        </div>
      </div>
    </div>

    <div class="modal-block" style="margin-bottom:16px;background:#0d0d1a;border-radius:10px;padding:14px 16px;border:0.5px solid #1e1e35">
      <div style="display:flex;align-items:center;gap:10px;margin-bottom:10px;flex-wrap:wrap">
        <span style="font-size:11px;font-weight:600;color:#c8d0e7">🔍 Why ${s.trader_type.replace(/^\S+\s/,'')}?</span>
        <span style="font-size:9px;padding:2px 7px;border-radius:4px;border:0.5px solid ${
          s.confidence==='High Confidence'?'#00f5d4':s.confidence==='Medium Confidence'?'#ffbe0b':'#4a4a6a'
        };color:${
          s.confidence==='High Confidence'?'#00f5d4':s.confidence==='Medium Confidence'?'#ffbe0b':'#888'
        };background:${
          s.confidence==='High Confidence'?'#001a0f':s.confidence==='Medium Confidence'?'#2a1f00':'#111120'
        };font-family:DM Mono,monospace">${s.confidence||''}</span>
      </div>
      <div style="display:flex;flex-wrap:wrap;gap:6px;margin-bottom:4px">
        ${(s.type_reasons||[]).map(r=>`<span style="font-size:10px;font-family:DM Mono,monospace;background:#111120;border:0.5px solid #2a2a45;border-radius:4px;padding:3px 8px;color:#00f5d4">✓ ${_esc(r)}</span>`).join('')}
      </div>
    </div>

    <!-- posChangeHTML injected by JS below -->
    <div class="modal-grid">
      <div class="modal-block">
        <h4>📊 Key Stats</h4>
        <div class="modal-stats">
          <div class="modal-stat"><div class="v" style="color:${pnlColor}">$${s.total_pnl>=0?'+':''}${Math.round(s.total_pnl).toLocaleString()}</div><div class="l">Total PnL</div></div>
          <div class="modal-stat"><div class="v">${s.win_rate}%</div><div class="l">Win Rate</div></div>
          <div class="modal-stat"><div class="v" style="color:${sc}">${s.sharpe}</div><div class="l" title="Estimated Sharpe — non-standard calculation used as relative WAR signal.">Sharpe* ⓘ</div></div>
          <div class="modal-stat"><div class="v">${s.roi_pct}%</div><div class="l">ROI</div></div>
          <div class="modal-stat"><div class="v" style="color:${dc}">${s.durability}</div><div class="l" title="Composite of active trading days and result consistency. High = style holds across time.">Durability ⓘ</div></div>
          <div class="modal-stat"><div class="v">${s.mdd_pct}%</div><div class="l" title="Profit Drawdown: peak-to-trough of cumulative PnL curve. Not standard equity drawdown.">PnL Draw ⓘ</div></div>
          <div class="modal-stat"><div class="v">${s.profit_factor}</div><div class="l">PnL Ratio</div></div>
          <div class="modal-stat"><div class="v">${s.big_bet_rate}%</div><div class="l" title="% of large positions (notional >15% of equity) that were profitable.">Big Bet Hit ⓘ</div></div>
          <div class="modal-stat"><div class="v">${s.consistency}%</div><div class="l" title="% of active trading days that were profitable. High = wins more days than loses.">Consistency ⓘ</div></div>
          <div class="modal-stat" style="grid-column:1/-1;background:#111120;padding:8px 12px;border-radius:8px;display:flex;align-items:center;justify-content:space-between">
            <div style="font-size:10px;color:#4a4a6a" title="Followability: low MDD · low big-bet · high consistency · sufficient sample · high profit factor">📋 Followability Score</div>
            <div style="font-family:DM Mono,monospace;font-size:16px;font-weight:700;color:${(s.follow_score||0)>=70?'#00f5d4':(s.follow_score||0)>=45?'#ffbe0b':'#f72585'}">${(s.follow_score||0).toFixed(0)}<span style="font-size:10px;color:#3a3a55">/100</span></div>
          </div>
        </div>
      </div>
      <div class="modal-block" style="display:flex;flex-direction:column">
        <h4 style="display:flex;align-items:center;justify-content:space-between;cursor:pointer" onclick="const b=document.getElementById('pos-body');const a=document.getElementById('pos-arrow');b.style.display=b.style.display==='none'?'block':'none';a.textContent=b.style.display==='none'?'▶':'▼'">
          📍 Current Positions <span id="pos-arrow" style="font-size:9px;color:#4a4a6a">▼</span>
        </h4>
        <div id="pos-body">${posHTML}</div>
      </div>
    </div>

    <div class="modal-grid">
      <div class="modal-block">
        <h4>📈 Cumulative PnL</h4>
        <div class="modal-pnl-chart"><canvas id="modalPnlChart"></canvas></div>
      </div>
      <div class="modal-block">
        <h4>💰 Realized PnL</h4>
        <div style="max-height:200px;overflow-y:auto;scrollbar-width:thin;scrollbar-color:#2a2a45 transparent">${coinRows}</div>
      </div>
    </div>

    <div class="modal-block" style="margin-top:16px;background:#0d0d1a;border-radius:10px;padding:14px 16px;border:0.5px solid #1e1e35">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px">
        <h4 style="margin:0;font-size:11px;font-weight:600;color:#c8d0e7">⚡ WAR Score Breakdown</h4>
        <span style="font-size:13px;font-weight:700;font-family:DM Mono,monospace;color:${cc}">${s.war_score}</span>
      </div>
      ${(function(){
        // war_components 없으면 radar 점수에서 역산 (구버전 캐시 폴백)
        var wc = s.war_components;
        if(!wc || Object.keys(wc).length === 0){
          var r = s.radar || {};
          wc = {
            'Profit':   parseFloat(((r.profit_amt||0)*0.25).toFixed(1)),
            'ROI':      parseFloat(((r.roi||0)*0.25).toFixed(1)),
            'Big Bet':  parseFloat(((r.big_bet||0)*0.15).toFixed(1)),
            'Sharpe':   parseFloat(((r.sharpe||0)*0.20).toFixed(1)),
            'Win Rate': parseFloat(((r.win_rate||0)*0.15).toFixed(1))
          };
        }
        var weights = {Profit:0.25, ROI:0.25, 'Big Bet':0.15, Sharpe:0.20, 'Win Rate':0.15};
        var keys = ['Profit','ROI','Sharpe','Win Rate','Big Bet'];
        return keys.map(function(k){
          var score = wc[k] || 0;
          var maxScore = weights[k] * 100;
          var pct = Math.min(100, (score / maxScore * 100)).toFixed(0);
          var barColor = score >= maxScore*0.7 ? '#00f5d4' : score >= maxScore*0.4 ? '#ffbe0b' : '#f72585';
          return '<div style="margin-bottom:7px">'
            + '<div style="display:flex;justify-content:space-between;font-size:9px;color:#888;font-family:DM Mono,monospace;margin-bottom:3px">'
            + '<span>'+k+'</span>'
            + '<span style="color:#c8d0e7">'+score.toFixed(1)+' / '+maxScore.toFixed(0)+'</span>'
            + '</div>'
            + '<div style="height:5px;background:#1e1e35;border-radius:3px;overflow:hidden">'
            + '<div style="height:100%;width:'+pct+'%;background:'+barColor+';border-radius:3px;transition:width .4s"></div>'
            + '</div></div>';
        }).join('');
      })()}
    </div>

    <div class="modal-block" style="margin-top:16px">
      <h4 style="margin-bottom:10px">🤖 AI Analysis</h4>
      <div style="font-size:12px;color:#c8d0e7;line-height:1.7">${_esc(s.ai_summary) || 'Analysis unavailable.'}</div>
    </div>

    <div class="modal-block" style="margin-top:16px;background:#0d0d1a;border-radius:10px;padding:14px 16px;border:0.5px solid #1e2a3a">
      <div style="font-size:11px;font-weight:600;color:#c8d0e7;margin-bottom:10px;display:flex;align-items:center;gap:8px">
        &#x1F4AC; Comments
        <span id="wc-count-${addrFull.slice(2,8)}" style="font-size:10px;color:#3a86ff;font-family:DM Mono,monospace"></span>
      </div>
      <div style="display:flex;flex-direction:column;gap:8px;margin-bottom:10px">
        <input id="wc-name-${addrFull.slice(2,8)}" type="text" placeholder="Name (optional)" maxlength="30"
          style="background:#0a0a16;border:1px solid #2a2a45;border-radius:8px;padding:8px 12px;color:#c8d0e7;font-size:12px;outline:none">
        <textarea id="wc-msg-${addrFull.slice(2,8)}" placeholder="Leave a comment about this wallet..." maxlength="300" rows="2"
          style="background:#0a0a16;border:1px solid #2a2a45;border-radius:8px;padding:8px 12px;color:#c8d0e7;font-size:12px;outline:none;resize:vertical"></textarea>
        <button onclick="submitGuestbook('${addrFull}')"
          style="background:#0a1a2a;color:#3a86ff;border:1px solid #3a86ff;border-radius:8px;padding:7px 16px;font-weight:700;font-size:12px;cursor:pointer;align-self:flex-start">
          Post Comment
        </button>
        <div id="wc-status-${addrFull.slice(2,8)}" style="font-size:10px;color:var(--dim);min-height:14px"></div>
      </div>
      <div id="wc-list-${addrFull.slice(2,8)}" style="display:flex;flex-direction:column;gap:8px">
        <div style="font-size:11px;color:#444">Loading...</div>
      </div>
    </div>
  `;

  document.getElementById('traderModal').classList.add('open');

  // 모달 하트/즐겨찾기 상태 초기화
  (function(){
    var likeBtn = document.getElementById('modal-like-btn');
    var saveBtn = document.getElementById('modal-save-btn');
    if(likeBtn){
      var today = new Date().toISOString().slice(0,10);
      var liked = localStorage.getItem(LIKE_KEY(addrFull.toLowerCase())) === today;
      var cnt = (window._likeCounts && window._likeCounts[addrFull.toLowerCase()]) || 0;
      likeBtn.querySelector('.like-icon').textContent = liked ? '❤️' : '🤍';
      likeBtn.querySelector('.like-count').textContent = cnt || '·';
      if(liked){ likeBtn.style.borderColor='#f72585'; likeBtn.style.color='#f72585'; }
    }
    if(saveBtn){
      var wl = getWatchlist();
      var saved = wl.indexOf(addrFull) >= 0;
      saveBtn.querySelector('.save-icon').textContent = saved ? '⭐' : '🔖';
      if(saved){ saveBtn.style.borderColor='#ffbe0b'; saveBtn.style.color='#ffbe0b'; saveBtn.title='Remove from Watchlist'; }
    }
  })();

  // Position Changes 블록 삽입 (f-string 충돌 우회)
  (function(){
    var ph = buildPosChangeHTML(s);
    if(ph){
      var grid = document.querySelector('#modal-content .modal-grid');
      if(grid){
        var div = document.createElement('div');
        div.innerHTML = ph;
        grid.parentNode.insertBefore(div.firstChild, grid);
      }
    }
  })();

  // 지갑 댓글 로드
  (function(){
    var shortId = addrFull.slice(2,8);
    var listEl  = document.getElementById('wc-list-'+shortId);
    var cntEl   = document.getElementById('wc-count-'+shortId);
    if(listEl) loadWalletComments(addrFull, listEl, cntEl);
  })();

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

function buildPosChangeHTML(s){
  if(!s.prev_positions||!s.prev_positions.length) return '';
  var equity = s.total_equity || 1;
  var prevMap={}, curMap={};
  s.prev_positions.forEach(function(p){prevMap[p.coin]=p;});
  (s.positions||[]).forEach(function(p){curMap[p.coin]=p;});
  var changes=[];

  function fmtLev(ntl){
    var r=ntl/equity;
    return r>=1?'x'+(Math.round(r*10)/10).toFixed(1):'x'+(Math.round(r*100)/100).toFixed(2);
  }

  // 현재 포지션 기준 변화 (신규 진입 / 증감)
  (s.positions||[]).forEach(function(cur){
    var prev=prevMap[cur.coin];
    var lev='';
    var ic=cur.side==='LONG'?'&#x25B2;':'&#x25BC;';
    var sc=cur.side==='LONG'?'#3a86ff':'#f72585';

    if(!prev){
      var dc = cur.side==='LONG' ? '#3a86ff' : '#f72585';
      changes.push({html:
        '<div style="display:flex;align-items:center;gap:8px;padding:5px 8px;border-radius:6px;background:#0a1520;border:0.5px solid #1e3a5a">'
        +'<span style="color:'+sc+';font-weight:700;min-width:90px">'+ic+' '+cur.coin+lev+'</span>'
        +'<span style="color:'+dc+';font-family:DM Mono,monospace;font-size:11px;font-weight:600">'+fmtLev(cur.notional)+'</span>'
        +'<span style="font-size:9px;background:#003020;color:#00f5d4;border-radius:3px;padding:1px 5px">NEW</span>'
        +'<span style="color:#555;margin-left:auto;font-size:10px">$'+Math.round(cur.notional).toLocaleString()+'</span>'
        +'</div>', n:cur.notional});
    } else {
      var flipped = cur.side !== prev.side;
      var diff = cur.notional - prev.notional;
      var absDiff = Math.abs(diff);

      if(flipped){
        changes.push({html:
          '<div style="display:flex;align-items:center;gap:8px;padding:5px 8px;border-radius:6px;background:#1a0a20;border:0.5px solid #9b5de5">'
          +'<span style="color:'+sc+';font-weight:700;min-width:90px">'+ic+' '+cur.coin+lev+'</span>'
          +'<span style="font-size:9px;background:#2a0030;color:#9b5de5;border-radius:3px;padding:1px 5px">FLIP</span>'
          +'<span style="color:#9b5de5;font-family:DM Mono,monospace;font-size:10px">'+fmtLev(cur.notional)+'</span>'
          +'<span style="color:#555;margin-left:auto;font-size:10px">$'+Math.round(prev.notional).toLocaleString()+' &#x2192; $'+Math.round(cur.notional).toLocaleString()+'</span>'
          +'</div>', n:cur.notional+prev.notional});
      } else if(absDiff / equity >= 0.05){
        var increased = diff > 0;
        var sign = increased ? '+' : '-';
        var dc = (ic === '&#x25B2;') ? '#3a86ff' : '#f72585';
        changes.push({html:
          '<div style="display:flex;align-items:center;gap:8px;padding:5px 8px;border-radius:6px;background:#0d0d1a;border:0.5px solid #2a2a45">'
          +'<span style="color:'+sc+';font-weight:700;min-width:90px">'+ic+' '+cur.coin+lev+'</span>'
          +'<span style="color:'+dc+';font-family:DM Mono,monospace;font-size:11px;font-weight:600">'+fmtLev(cur.notional)+'</span>'
          +'<span style="color:#555;font-family:DM Mono,monospace;font-size:9px">'+sign+'$'+Math.abs(Math.round(diff)).toLocaleString()+'</span>'
          +'<span style="color:#333;margin-left:auto;font-size:10px">$'+Math.round(cur.notional).toLocaleString()+'</span>'
          +'</div>', n:absDiff});
      }
    }
  });

  // 청산된 포지션
  s.prev_positions.forEach(function(prev){
    if(!curMap[prev.coin]){
      var ic=prev.side==='LONG'?'&#x25B2;':'&#x25BC;';
      var sc=prev.side==='LONG'?'#3a86ff':'#f72585';
      var lev='';
      changes.push({html:
        '<div style="display:flex;align-items:center;gap:8px;padding:5px 8px;border-radius:6px;background:#1a0808;border:0.5px solid #3a1a1a">'
        +'<span style="color:'+sc+';font-weight:700;min-width:90px;opacity:0.6">'+ic+' '+prev.coin+lev+'</span>'
        +'<span style="color:#888;font-family:DM Mono,monospace;font-size:11px;font-weight:600">'+fmtLev(prev.notional)+'</span>'
        +'<span style="font-size:9px;background:#2a0000;color:#f87171;border-radius:3px;padding:1px 5px">CLOSED</span>'
        +'<span style="color:#333;margin-left:auto;font-size:10px">was $'+Math.round(prev.notional).toLocaleString()+'</span>'
        +'</div>', n:prev.notional});
    }
  });

  if(!changes.length) return '';
  // 변화 큰 순 정렬
  changes.sort(function(a,b){ return b.n - a.n; });
  var ts=s.prev_positions_ts ? new Date(s.prev_positions_ts).toLocaleString('ko-KR') : '24h ago';
  return '<div style="margin-bottom:16px;background:#0d0d1a;border-radius:10px;padding:14px 16px;border:0.5px solid #1e2a3a">'
    +'<div style="font-size:11px;font-weight:600;color:#c8d0e7;margin-bottom:10px;display:flex;align-items:center;gap:8px">'
    +'&#x1F4CA; Position Changes'
    +'<span style="font-size:9px;color:#3a3a55;font-family:DM Mono,monospace">vs '+ts+'</span>'
    +'</div>'
    +'<div style="display:flex;flex-direction:column;gap:5px">'+changes.map(function(c){return c.html;}).join('')+'</div>'
    +'</div>';
}

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
        "setTimeout(function(){"
        "new Chart(document.getElementById('weeklyChart').getContext('2d'),"
        "{type:'bar',data:{labels:weeks,datasets:ws.map(s=>("
        "{label:s.label,data:s.data,backgroundColor:s.color+'aa',"
        "borderColor:s.color,borderWidth:1,borderRadius:3}))},"
        "options:{responsive:true,interaction:{mode:'index',intersect:false},"
        "plugins:{legend:{labels:{color:'#4a4a6a',font:{size:11}}}},"
        "scales:{x:{ticks:{color:'#4a4a6a'},grid:{color:'#1e1e35'}},"
        "y:{ticks:{color:'#4a4a6a',callback:v=>'$'+v.toLocaleString()},"
        "grid:{color:'#1e1e35'}}}}});},0);"
    )
    # 각 Trader에 color 추가
    for i, s in enumerate(ranked):
        s['_color'] = palette[i % len(palette)]
    all_stats_js = json.dumps(ranked, ensure_ascii=False, default=str)

    # ══ SIGNAL TAB — Python data computation ════════════════════════════
    def _sig_parse_ts(ts_str):
        try:
            t = datetime.fromisoformat(ts_str)
            return t.replace(tzinfo=timezone.utc) if t.tzinfo is None else t
        except Exception:
            return datetime(1970, 1, 1, tzinfo=timezone.utc)

    def _sig_time_ago(ts_str):
        if not ts_str:
            return "~24h ago"
        try:
            delta = datetime.now(timezone.utc) - _sig_parse_ts(ts_str)
            if delta.total_seconds() < 3600:
                return f"{int(delta.total_seconds()//60)}m ago"
            if delta.days == 0:
                return f"{int(delta.total_seconds()//3600)}h ago"
            return f"{delta.days}d ago"
        except Exception:
            return "~24h ago"

    def _sig_dir_for(stats_list):
        long_ntl  = sum(sum(p["notional"] for p in s.get("positions",[]) if p["side"]=="LONG")  for s in stats_list)
        short_ntl = sum(sum(p["notional"] for p in s.get("positions",[]) if p["side"]=="SHORT") for s in stats_list)
        total = long_ntl + short_ntl
        if total == 0:
            return {"long_pct":50.0,"short_pct":50.0,"trader_count":len(stats_list),"change_24h":0.0,"net_bias":"neutral"}
        lp = round(long_ntl/total*100, 1)
        sp = round(short_ntl/total*100, 1)
        bias = "long" if lp > 55 else ("short" if sp > 55 else "neutral")
        return {"long_pct":lp,"short_pct":sp,"trader_count":len(stats_list),"change_24h":0.0,"net_bias":bias}

    _s_all = [s for s in ranked if s.get("positions")]
    expert_direction = {"all": _sig_dir_for(_s_all)}
    # 24h change from sentiment_history (use most recent two snapshots, all wallets)
    if len(_hist_data) >= 2:
        _lsnap, _psnap = _hist_data[-1], _hist_data[-2]
        _lb = next((b for b in _lsnap.get("bands",[]) if b.get("label")), None)
        _pb = next((b for b in _psnap.get("bands",[]) if b.get("label")), None)
        if _lb and _pb:
            _lt = _lb["long_pct"] + _lb["short_pct"]
            _pt = _pb["long_pct"] + _pb["short_pct"]
            if _lt > 0 and _pt > 0:
                expert_direction["all"]["change_24h"] = round(
                    _lb["short_pct"]/_lt*100 - _pb["short_pct"]/_pt*100, 1)

    # coin_consensus
    from collections import defaultdict as _dd
    _coin_data = _dd(lambda: {"long_ntl":0.0,"short_ntl":0.0,"wallets":set(),"wars":[],"real_levs":[]})
    for _s in _s_all:
        _s_eq = max(_s.get("total_equity", 1) or 1, 1)
        for _p in _s.get("positions",[]):
            _cd = _coin_data[_p["coin"]]
            if _p["side"]=="LONG": _cd["long_ntl"] += _p["notional"]
            else: _cd["short_ntl"] += _p["notional"]
            _cd["wallets"].add(_s["address"])
            _cd["wars"].append(_s["war_score"])
            # 실제 레버리지 = 포지션 규모 / balance
            _cd["real_levs"].append(_p["notional"] / _s_eq)
    coin_consensus = []
    for _coin, _cd in _coin_data.items():
        _tot = _cd["long_ntl"] + _cd["short_ntl"]
        if _tot == 0: continue
        _lp = round(_cd["long_ntl"]/_tot*100, 1)
        _sp = round(_cd["short_ntl"]/_tot*100, 1)
        _dir = "long" if _lp > 60 else ("short" if _sp > 60 else "neutral")
        _avgwar = round(sum(_cd["wars"])/len(_cd["wars"]),1) if _cd["wars"] else 0
        # avg_lev = 평균 (포지션규모/balance) — 실제 자본 대비 비중
        _avglev = round(sum(_cd["real_levs"])/len(_cd["real_levs"]),2) if _cd["real_levs"] else 0
        coin_consensus.append({"coin":_coin,"long_pct":_lp,"short_pct":_sp,"direction":_dir,
            "wallet_count":len(_cd["wallets"]),"avg_war":_avgwar,"avg_lev":_avglev,
            "total_ntl":_tot,"delta_24h":"N/A"})
    coin_consensus.sort(key=lambda x: x["total_ntl"], reverse=True)
    for _c in coin_consensus: _c.pop("total_ntl", None)
    coin_consensus = coin_consensus[:5]

    # hot_moves
    _now_ts = datetime.now(timezone.utc)
    _now_iso = _now_ts.isoformat()
    hot_moves = []
    for _s in ranked:
        if not _s.get("positions") or not _s.get("prev_positions"): continue
        _eq = _s.get("total_equity",1) or 1
        _ts = _sig_parse_ts(_s.get("prev_positions_ts","")).timestamp()
        _cur_map = {_p["coin"]:_p for _p in _s["positions"]}
        _prev_map = {_p["coin"]:_p for _p in _s["prev_positions"]}
        for _coin, _cur in _cur_map.items():
            if _cur["notional"] < 100_000: continue
            _lev = _cur["notional"] / _eq if _eq > 0 else 1
            _prev = _prev_map.get(_coin)
            if _prev:
                _diff = _cur["notional"] - _prev["notional"]
                if abs(_diff) >= _eq * 0.15:
                    _sign = "+" if _diff > 0 else "-"
                    _dstr = "Long" if _cur["side"]=="LONG" else "Short"
                    hot_moves.append({"name":_s.get("label",short_addr(_s["address"])),
                        "addr":_s["address"],"war":_s["war_score"],
                        "action":f"{_coin} {_dstr}","change":_sign,
                        "time_ago":_sig_time_ago(_now_iso),
                        "notional":round(_cur["notional"]),"upnl":round(_cur.get("upnl",0),1),
                        "equity":round(_eq),
                        "_ts":_ts,"_size":abs(_diff)*_lev})
            else:
                _dstr = "Long" if _cur["side"]=="LONG" else "Short"
                hot_moves.append({"name":_s.get("label",short_addr(_s["address"])),
                    "addr":_s["address"],"war":_s["war_score"],
                    "action":f"{_coin} {_dstr}","change":"new",
                    "time_ago":_sig_time_ago(_now_iso),
                    "notional":round(_cur["notional"]),"upnl":round(_cur.get("upnl",0),1),
                    "equity":round(_eq),
                    "_ts":_ts,"_size":_cur["notional"]*_lev})
    hot_moves.sort(key=lambda x: (x["_ts"], x["_size"]), reverse=True)
    for _m in hot_moves: _m.pop("_ts", None); _m.pop("_size", None)
    hot_moves = hot_moves[:6]

    # sim_returns — 각 밴드: lo <= war_score < hi (80은 80+)
    def _sig_ret(lo, hi=None, follow=False):
        if follow:
            _wallets = [s for s in ranked if s.get("follow_score",0)>=80 and s.get("positions")]
        elif hi is None:
            _wallets = [s for s in ranked if s.get("war_score",0)>=lo and s.get("positions")]
        else:
            _wallets = [s for s in ranked if lo<=s.get("war_score",0)<hi and s.get("positions")]
        if not _wallets:
            return {"1d":0.0,"7d":0.0,"30d":0.0,"90d":0.0}
        _ln = sum(sum(p["notional"] for p in s.get("positions",[]) if p["side"]=="LONG") for s in _wallets)
        _sn = sum(sum(p["notional"] for p in s.get("positions",[]) if p["side"]=="SHORT") for s in _wallets)
        _tot = _ln + _sn
        _net = (_ln - _sn) / _tot if _tot > 0 else 0
        _now = datetime.now(timezone.utc)
        _ret = {}
        for _pname, _days in [("1d",1),("7d",7),("30d",30),("90d",90)]:
            _cutoff = _now - timedelta(days=_days)
            _snaps = [s for s in _hist_data if _sig_parse_ts(s.get("ts","")) >= _cutoff]
            if len(_snaps) >= 2:
                _f, _l = _snaps[0].get("btc_price",0), _snaps[-1].get("btc_price",0)
                _ret[_pname] = round(_net * (_l-_f)/_f * 100, 2) if _f else 0.0
            else:
                _ret[_pname] = 0.0
        return _ret
    sim_returns = {
        "war80": _sig_ret(80),
        "war70": _sig_ret(70, 80),
        "war60": _sig_ret(60, 70),
        "war50": _sig_ret(50, 60),
        "follow": _sig_ret(0, follow=True),
    }

    # easy_signals
    easy_signals = []
    for _c in coin_consensus:
        _cons = max(_c["long_pct"], _c["short_pct"])
        _avglev = _c.get("avg_lev", 5)
        _delta_str = _c.get("delta_24h","N/A")
        _vol = 0.0
        try:
            _vol = min(abs(float(_delta_str.replace("%p","").replace("+","").replace("-","") or 0))/20.0,1.0)
        except Exception:
            pass
        _ds = (1-_cons/100)*40 + min(_avglev, 1)*40 + _vol*20
        _diff = "easy" if _ds<=33 else ("medium" if _ds<=66 else "hard")
        _risk = "low" if _ds<=33 else ("medium" if _ds<=66 else "high")
        _dir = _c["direction"] if _c["direction"]!="neutral" else ("long" if _c["long_pct"]>=50 else "short")
        if _diff=="easy":
            _note = f"Strong {_cons:.0f}% smart money consensus on {_c['coin']} {_dir}s — low disagreement, manageable leverage."
        elif _diff=="medium":
            _note = f"{_c['coin']} shows {_cons:.0f}% {_dir} lean but leverage and mixed signals warrant caution."
        else:
            _note = f"High leverage and low consensus on {_c['coin']} — not recommended for beginners."
        easy_signals.append({"coin":_c["coin"],"direction":_dir,"difficulty":_diff,
            "consensus_pct":_cons,"avg_leverage":_avglev,"risk_level":_risk,"note":_note})
    easy_signals.sort(key=lambda x: {"easy":0,"medium":1,"hard":2}[x["difficulty"]])
    easy_signals = easy_signals[:3]

    js_block = (
        f"window.EXPERT_DIR={json.dumps(expert_direction, ensure_ascii=False)};\n"
        f"window.COIN_CONSENSUS={json.dumps(coin_consensus, ensure_ascii=False)};\n"
        f"window.HOT_MOVES={json.dumps(hot_moves, ensure_ascii=False)};\n"
        f"window.SIM_RETURNS={json.dumps(sim_returns, ensure_ascii=False)};\n"
        f"window.EASY_SIGNALS={json.dumps(easy_signals, ensure_ascii=False)};\n"
        f"window.ALL_STATS={all_stats_js};\n"
        f"window.WALLET_META={json.dumps(load_wallets_meta(), ensure_ascii=False)};\n"
        f"const rd={radar_js};\n"
        f"const SENT={sent_js};\n"
        f"const HIST={hist_js};\n"
        f"const WAR_HIST={war_hist_js};\n"
        + _chart_radar + "\n"
        + f"const weeks={weeks_js},ws={ws_js};\n"
        + _chart_weekly + "\n"
        "function showTab(n,e){var _e=e||window.event;document.querySelectorAll('.section').forEach(el=>el.classList.remove('active'));document.querySelectorAll('.tab').forEach(el=>el.classList.remove('active'));document.getElementById('tab-'+n).classList.add('active');if(_e&&_e.target){var _tab=_e.target.closest('.tab');if(_tab)_tab.classList.add('active');}if(n==='signal')renderSignal();if(n==='sentiment')renderSentiment();if(n==='radar'){if(window._radarChart)window._radarChart.destroy();initRadarChart();}if(n==='styles'){setTimeout(initPlaystyleMap,100);}if(n==='lookup')initLookup();if(n==='guestbook')initGuestbook();if(n==='watchlist')renderWatchlist();if(n==='cards')renderWarAlertBanner();}\n"
    )
    js_block += """
// fetchAISummaryLookup removed — AI summary now uses Python-generated ai_summary exclusively

// GitHub API calls are proxied through Cloudflare Worker.
// No token stored in browser.
var _gbLoaded=false;


// ══ LIKES & WATCHLIST ══════════════════════════════════════════════
const WORKER = 'https://wallet-scout-api.kimsubbae113.workers.dev';
const LIKE_KEY  = addr => 'like_date_' + addr.toLowerCase();
const WATCH_KEY = 'wl_v1';

// ── 좋아요 ─────────────────────────────────────────────────────────
async function loadLikeCounts() {
  try {
    if(location.protocol === 'file:'){
      window._likeCounts = JSON.parse(localStorage.getItem('wallet_scout_like_counts')||'{}');
    } else {
      var r = await fetch(WORKER + '/api/likes');
      if(!r.ok) throw new Error('HTTP '+r.status);
      var data = await r.json();
      window._likeCounts = data || {};
    }
  } catch(e) { window._likeCounts = JSON.parse(localStorage.getItem('wallet_scout_like_counts')||'{}'); }
  // 카드 버튼 초기화
  document.querySelectorAll('.like-btn').forEach(function(btn){
    var addr = btn.dataset.addr.toLowerCase();
    var count = window._likeCounts[addr] || 0;
    var likedToday = localStorage.getItem(LIKE_KEY(addr)) === new Date().toISOString().slice(0,10);
    btn.querySelector('.like-count').textContent = count;
    if(likedToday){
      btn.querySelector('.like-icon').textContent = '❤️';
      btn.style.borderColor = '#f72585';
      btn.style.color = '#f72585';
    } else {
      btn.querySelector('.like-icon').textContent = '🤍';
      btn.style.borderColor = '#2a2a45';
      btn.style.color = '#555';
    }
  });
}

async function toggleLike(btn) {
  var addr = btn.dataset.addr.toLowerCase();
  var today = new Date().toISOString().slice(0,10);
  var likedToday = localStorage.getItem(LIKE_KEY(addr)) === today;
  if(likedToday){
    // 이미 좋아요 — 취소 불가(하루 1회 정책 안내)
    btn.style.transform = 'scale(1.2)';
    setTimeout(function(){ btn.style.transform = 'scale(1)'; }, 200);
    return;
  }
  // 좋아요 즉시 UI 반영
  localStorage.setItem(LIKE_KEY(addr), today);
  var prev = window._likeCounts[addr] || 0;
  window._likeCounts[addr] = prev + 1;
  btn.querySelector('.like-icon').textContent = '❤️';
  btn.querySelector('.like-count').textContent = prev + 1;
  btn.style.borderColor = '#f72585';
  btn.style.color = '#f72585';
  btn.style.transform = 'scale(1.3)';
  setTimeout(function(){ btn.style.transform = 'scale(1)'; }, 250);
  // Worker에 전송
  try {
    localStorage.setItem('wallet_scout_like_counts', JSON.stringify(window._likeCounts||{}));
    if(location.protocol !== 'file:'){
      await fetch(WORKER + '/api/likes', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ address: addr })
      });
    }
  } catch(e) { console.warn('Like sync failed:', e); }
}

// ── 저장 (Watchlist) ───────────────────────────────────────────────
function getWatchlist(){ try{ return JSON.parse(localStorage.getItem(WATCH_KEY)||'[]'); }catch(e){return[];} }
function setWatchlist(list){ localStorage.setItem(WATCH_KEY, JSON.stringify(list)); }

function toggleSave(btn){
  var addr = btn.dataset.addr;
  var list = getWatchlist();
  var idx = list.indexOf(addr);
  if(idx >= 0){
    list.splice(idx, 1);
    btn.querySelector('.save-icon').textContent = '🔖';
    btn.style.borderColor = '#2a2a45';
    btn.style.color = '#555';
    btn.title = 'Add to Watchlist';
  } else {
    list.push(addr);
    btn.querySelector('.save-icon').textContent = '⭐';
    btn.style.borderColor = '#ffbe0b';
    btn.style.color = '#ffbe0b';
    btn.title = 'Remove from Watchlist';
    btn.style.transform = 'scale(1.3)';
    setTimeout(function(){ btn.style.transform = 'scale(1)'; }, 250);
  }
  setWatchlist(list);
  // Watchlist 탭 카운트 업데이트
  updateWatchlistBadge();
}

function initSaveButtons(){
  var list = getWatchlist();
  document.querySelectorAll('.save-btn').forEach(function(btn){
    var addr = btn.dataset.addr;
    if(list.indexOf(addr) >= 0){
      btn.querySelector('.save-icon').textContent = '⭐';
      btn.style.borderColor = '#ffbe0b';
      btn.style.color = '#ffbe0b';
      btn.title = 'Remove from Watchlist';
    } else {
      btn.querySelector('.save-icon').textContent = '🔖';
      btn.style.borderColor = '#2a2a45';
      btn.style.color = '#555';
      btn.title = 'Add to Watchlist';
    }
  });
  updateWatchlistBadge();
}

function updateWatchlistBadge(){
  var cnt = getWatchlist().length;
  document.querySelectorAll('.tab').forEach(function(t){
    if(t.textContent.indexOf('Watchlist') >= 0){
      t.textContent = cnt > 0 ? '⭐ Watchlist (' + cnt + ')' : '⭐ Watchlist';
    }
  });
}

// ── Watchlist 탭 렌더링 ─────────────────────────────────────────────
function renderWatchlist(){
  var root = document.getElementById('watchlist-root');
  if(!root) return;
  var list = getWatchlist();
  if(list.length === 0){
    root.innerHTML = '<div style="text-align:center;padding:60px 0;color:#3a3a55">'
      + '<div style="font-size:40px;margin-bottom:12px">⭐</div>'
      + '<div style="font-size:14px;color:#555">No wallets saved yet.</div>'
      + '<div style="font-size:11px;color:#3a3a55;margin-top:6px">Click 🔖 on any trader card to add to your Watchlist.</div>'
      + '</div>';
    return;
  }
  var statMap = {};
  ALL_STATS.forEach(function(s){ statMap[s.address] = s; });

  var html = '<div style="margin-bottom:16px;display:flex;align-items:center;justify-content:space-between">'
    + '<div style="font-size:12px;font-weight:600;color:#c8d0e7">⭐ My Watchlist <span style="color:#3a3a55;font-size:10px;font-weight:400">(' + list.length + ' wallets · always shows latest data)</span></div>'
    + '<button onclick="clearWatchlist()" style="font-size:10px;padding:3px 10px;border-radius:5px;border:0.5px solid #2a2a45;background:transparent;color:#555;cursor:pointer">Clear All</button>'
    + '</div>';

  html += '<div class="cards-grid">';
  list.forEach(function(addr){
    var s = statMap[addr];
    if(!s){
      html += '<div style="background:#111120;border-radius:10px;padding:16px;border:0.5px solid #1e1e35;color:#3a3a55;font-size:11px">'
        + addr.slice(0,10)+'... · Not in current report<br>'
        + '<span style="font-size:10px">Run --discover to include this wallet.</span>'
        + '<div style="margin-top:8px"><button data-wa="'+ addr +'" onclick="removeFromWatchlist(this.dataset.wa)" style="font-size:9px;padding:2px 7px;border-radius:3px;border:0.5px solid #2a2a45;background:transparent;color:#555;cursor:pointer">Remove</button></div>'
        + '</div>';
    } else {
      // 카드를 클릭하면 모달 열기
      var pnlColor = s.total_pnl >= 0 ? '#00f5d4' : '#f72585';
      var cc = s._color || '#00f5d4';
      var warColor = s.war_score >= 80 ? '#00f5d4' : s.war_score >= 60 ? '#ffbe0b' : '#f72585';
      html += '<div data-wa="'+ addr +'" style="background:#111120;border-radius:10px;padding:16px;border:0.5px solid #1e1e35;cursor:pointer" onclick="openModal(this.dataset.wa)">'
        + '<div style="display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:10px">'
        + '<div>'
        + '<div style="font-size:12px;font-weight:700;color:'+cc+';font-family:Bebas Neue,sans-serif;letter-spacing:1px">' + _esc(s.label) + '</div>'
        + '<div style="font-size:10px;color:'+cc+';margin-top:2px">' + _esc(s.trader_type) + '</div>'
        + '</div>'
        + '<div style="text-align:right">'
        + '<div style="font-size:18px;font-weight:700;font-family:Bebas Neue,sans-serif;color:'+warColor+'">' + s.war_score + '</div>'
        + '<div style="font-size:8px;color:#3a3a55">WAR</div>'
        + '</div>'
        + '</div>'
        + '<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:6px;font-family:DM Mono,monospace">'
        + '<div style="background:#0a0a16;border-radius:6px;padding:6px;text-align:center"><div style="font-size:12px;color:'+pnlColor+'">' + fmtCompact(s.total_pnl) + '</div><div style="font-size:8px;color:#3a3a55">PnL</div></div>'
        + '<div style="background:#0a0a16;border-radius:6px;padding:6px;text-align:center"><div style="font-size:12px;color:#c8d0e7">' + s.win_rate.toFixed(0) + '%</div><div style="font-size:8px;color:#3a3a55">Win Rate</div></div>'
        + '<div style="background:#0a0a16;border-radius:6px;padding:6px;text-align:center"><div style="font-size:12px;color:#c8d0e7">' + s.sharpe.toFixed(1) + '</div><div style="font-size:8px;color:#3a3a55">Sharpe*</div></div>'
        + '</div>'
        + '<div style="margin-top:8px;display:flex;justify-content:space-between;align-items:center">'
        + '<span style="font-size:9px;color:#3a3a55">' + s.first_date + ' ~ ' + s.last_date + '</span>'
        + '<button data-wa="'+ addr +'" onclick="event.stopPropagation();removeFromWatchlist(this.dataset.wa)" style="font-size:9px;padding:2px 7px;border-radius:3px;border:0.5px solid #2a2a45;background:transparent;color:#555;cursor:pointer">Remove</button>'
        + '</div>'
        + '</div>';
    }
  });
  html += '</div>';
  root.innerHTML = html;
}

function removeFromWatchlist(addr){
  var list = getWatchlist().filter(function(a){ return a !== addr; });
  setWatchlist(list);
  initSaveButtons();
  renderWatchlist();
}

function clearWatchlist(){
  if(!confirm('Clear all ' + getWatchlist().length + ' saved wallets?')) return;
  setWatchlist([]);
  initSaveButtons();
  renderWatchlist();
}

// 페이지 로드 시 초기화
(function(){
  try{ renderSignal(); }catch(e){}
  initSaveButtons();
  loadLikeCounts();
  renderWarAlertBanner();
  setTimeout(loadCommentCounts, 300);
  setTimeout(function(){ try{ applyCardFilters(true); }catch(e){} }, 100);
  setTimeout(function(){ try{ renderWarAlertBanner(); }catch(e){} }, 200);
})();

window.addEventListener('DOMContentLoaded', function(){
  setTimeout(function(){ try{ buildTypeFilterBar(); }catch(e){} try{ applyCardFilters(true); }catch(e){} try{ renderWarAlertBanner(); }catch(e){} }, 50);
  setTimeout(function(){ try{ if(window.initWarTrendChart) window.initWarTrendChart(); }catch(e){} }, 250);
});
window.addEventListener('load', function(){
  setTimeout(function(){ try{ buildTypeFilterBar(); }catch(e){} try{ applyCardFilters(true); }catch(e){} try{ renderWarAlertBanner(); }catch(e){} }, 100);
  setTimeout(function(){ try{ if(window.initWarTrendChart) window.initWarTrendChart(); }catch(e){} }, 400);
});

// ── WAR 변화 알림 배너 ───────────────────────────────────────────
function renderWarAlertBanner(){
  var banner = document.getElementById('war-alert-banner');
  if(!Array.isArray(window.ALL_STATS)) return;
  if(!banner || !WAR_HIST || WAR_HIST.length < 2) return;

  // 최신 스냅샷
  var latest = WAR_HIST[WAR_HIST.length-1];
  if(!latest) return;
  var latestTs = new Date(latest.ts.replace(' ','T')).getTime();

  // 24시간 전 기준: 그 시점에서 가장 가까운(최신) 스냅샷 선택
  var cutoff24h = latestTs - 24 * 60 * 60 * 1000;
  var prev = null;
  for(var i = WAR_HIST.length - 2; i >= 0; i--){
    var t = new Date(WAR_HIST[i].ts.replace(' ','T')).getTime();
    if(t <= cutoff24h){ prev = WAR_HIST[i]; break; }
  }
  // 24시간 전 스냅샷이 없으면 가장 오래된 것 사용
  if(!prev) prev = WAR_HIST[0];
  if(!prev || prev === latest) return;

  var latestMap = {}, prevMap = {};
  (latest.top20||[]).forEach(function(t){ latestMap[t.address] = t; });
  (prev.top20||[]).forEach(function(t){ prevMap[t.address] = t; });

  // WAR +5 이상 상승한 지갑
  var risers = [];
  Object.keys(latestMap).forEach(function(addr){
    var cur = latestMap[addr].war;
    var old = prevMap[addr] ? prevMap[addr].war : null;
    if(old !== null && cur - old >= 5){
      risers.push({ label: latestMap[addr].label, addr: addr, cur: cur, old: old, diff: cur-old });
    }
  });
  risers.sort(function(a,b){ return b.diff - a.diff; });

  // 신규 top20 진입 (이전 스냅샷에 없던 주소)
  var newEntries = [];
  Object.keys(latestMap).forEach(function(addr){
    if(!prevMap[addr]){
      var t = latestMap[addr];
      // top20에서 rank 20 이내
      if(t.rank && t.rank <= 20){
        newEntries.push({ label: t.label, addr: addr, war: t.war, rank: t.rank });
      }
    }
  });
  newEntries.sort(function(a,b){ return a.rank - b.rank; });

  // 포지션 급변 지갑: pair별 최대 변화 하나만 선택
  var hotWallets = [];
  ((window.ALL_STATS)||[]).forEach(function(s){
    if(!s.positions && !s.prev_positions) return;
    if(!s.prev_positions || !s.prev_positions.length) return;
    var equity = s.total_equity || 1;
    var threshold = equity * 0.20;
    var prevPosMap = {};
    (s.prev_positions||[]).forEach(function(p){ prevPosMap[p.coin] = p; });
    var curPosMap  = {};
    (s.positions||[]).forEach(function(p){ curPosMap[p.coin] = p; });

    // pair별 변화 계산
    var bestPair = null, bestAbsChange = 0;
    var allCoins = {};
    (s.positions||[]).forEach(function(p){ allCoins[p.coin] = true; });
    (s.prev_positions||[]).forEach(function(p){ allCoins[p.coin] = true; });

    Object.keys(allCoins).forEach(function(coin){
      var cur  = curPosMap[coin];
      var prev = prevPosMap[coin];
      var curNtl  = cur  ? cur.notional  : 0;
      var prevNtl = prev ? prev.notional : 0;
      var curSide  = cur  ? cur.side  : null;
      var prevSide = prev ? prev.side : null;

      var change = 0;      // 잔고 대비 변화 (양수 = 증가, 음수 = 감소)
      var isLong = false;  // 어느 방향 포지션인지
      var lev = cur ? (cur.set_lev || cur.lev || 1) : (prev ? (prev.set_lev || prev.lev || 1) : 1);

      if(cur && !prev){
        // 신규 진입
        change = curNtl;
        isLong = curSide === 'LONG';
      } else if(!cur && prev){
        // 청산 (줄인 것)
        change = -prevNtl;
        isLong = prevSide === 'LONG';
      } else if(cur && prev){
        if(curSide === prevSide){
          change = curNtl - prevNtl;
          isLong = curSide === 'LONG';
        } else {
          // 방향 전환: 더 큰 변화로 표시
          change = curNtl > prevNtl ? curNtl : -prevNtl;
          isLong = curNtl > prevNtl ? curSide === 'LONG' : prevSide === 'LONG';
        }
      }

      var absChange = Math.abs(change);
      if(absChange > bestAbsChange){
        bestAbsChange = absChange;
        bestPair = { coin: coin, change: change, isLong: isLong, lev: lev, absChange: absChange };
      }
    });

    if(bestPair && bestAbsChange >= threshold){
      var meta = (WALLET_META||{})[s.address.toLowerCase()];
      var lbl = (meta && meta.name) ? meta.name : (s.label || s.address.slice(0,8)+'...'+s.address.slice(-4));
      hotWallets.push({ addr: s.address, label: lbl, equity: equity, pair: bestPair });
    }
  });
  hotWallets.sort(function(a,b){ return (b.pair.absChange/b.equity)-(a.pair.absChange/a.equity); });

  if(risers.length === 0 && newEntries.length === 0 && hotWallets.length === 0) return;

  var html = '<div style="background:#0d0d1a;border:0.5px solid #1e1e35;border-radius:10px;padding:12px 16px;display:flex;flex-wrap:wrap;gap:10px;align-items:center">'
    + '<span style="font-size:10px;font-weight:600;color:#4a4a6a;white-space:nowrap">🔥 24H Hot Wallets</span>';

  // 포지션 급변 (최대 4개) — pair명 + 방향 화살표 + 잔고대비%
  // ▲+ = 롱 늘림(파랑), ▼+ = 숏 늘림(빨강), ▲- = 롱 줄임(빨강), ▼- = 숏 줄임(파랑)
  hotWallets.slice(0,4).forEach(function(w){
    var p = w.pair;
    var levRatio = w.equity > 0 ? p.absChange / w.equity : 0;
    var levLabel = levRatio >= 1 ? 'x'+(Math.round(levRatio*10)/10).toFixed(1) : 'x'+(Math.round(levRatio*100)/100).toFixed(2);
    var increased = p.change > 0;
    var sign = increased ? '+' : '-';
    var arrow = p.isLong ? '▲' : '▼';
    // ▲=파랑, ▼=빨강
    var isUp = arrow === '▲';
    var color  = isUp ? '#3a86ff' : '#f72585';
    var border = isUp ? '#3a86ff' : '#f72585';
    var bg     = isUp ? '#001020' : '#1a0010';
    html += '<button data-wa="'+w.addr+'" onclick="openModal(this.dataset.wa)" '
      + 'style="display:flex;align-items:center;gap:4px;font-size:10px;padding:3px 9px;border-radius:5px;'
      + 'border:0.5px solid '+border+';background:'+bg+';color:#c8d0e7;cursor:pointer;font-family:DM Mono,monospace">'
      + '<span style="color:'+color+';font-weight:700">'+p.coin+' '+arrow+sign+levLabel+'</span>'
      + '<span style="color:#888">'+_esc(w.label)+'</span>'
      + '</button>';
  });

  // WAR 상승 (최대 3개) — "WAR ▲+숫자" 형태
  risers.slice(0,3).forEach(function(r){
    html += '<button data-wa="'+r.addr+'" onclick="openModal(this.dataset.wa)" '
      + 'style="display:flex;align-items:center;gap:5px;font-size:10px;padding:3px 9px;border-radius:5px;'
      + 'border:0.5px solid #00f5d4;background:#001a12;color:#c8d0e7;cursor:pointer;font-family:DM Mono,monospace">'
      + '<span style="color:#00f5d4;font-weight:700">WAR ▲+'+r.diff.toFixed(1)+'</span>'
      + '<span>'+_esc(r.label)+'</span>'
      + '<span style="color:#3a3a55">'+r.cur.toFixed(1)+'</span>'
      + '</button>';
  });

  // 신규 Top 20 진입 (최대 2개)
  newEntries.slice(0,2).forEach(function(e){
    html += '<button data-wa="'+e.addr+'" onclick="openModal(this.dataset.wa)" '
      + 'style="display:flex;align-items:center;gap:5px;font-size:10px;padding:3px 9px;border-radius:5px;'
      + 'border:0.5px solid #ffbe0b;background:#1a1400;color:#c8d0e7;cursor:pointer;font-family:DM Mono,monospace">'
      + '<span style="color:#ffbe0b;font-weight:700">New Top 20</span>'
      + '<span>'+_esc(e.label)+'</span>'
      + '<span style="color:#3a3a55">#'+e.rank+'</span>'
      + '</button>';
  });

  html += '</div>';
  banner.innerHTML = html;
}

// ── Comment System ──────────────────────────────────────────────────
const GB_API = 'https://wallet-scout-api.kimsubbae113.workers.dev/api/guestbook';
// 전체 댓글 캐시 (API 중복 호출 방지)
window._gbCache = null;
window._gbCacheTs = 0;
function _gbLocalKey(){ return 'wallet_scout_guestbook_local'; }
function _parseCommentPayload(data){ if(Array.isArray(data)) return data; if(data && Array.isArray(data.items)) return data.items; if(data && Array.isArray(data.comments)) return data.comments; if(data && data.data && Array.isArray(data.data.items)) return data.data.items; if(data && data.data && Array.isArray(data.data.comments)) return data.data.comments; return []; }
function _localCommentsGet(){ try { return JSON.parse(localStorage.getItem(_gbLocalKey())||'[]'); } catch(e){ return []; } }
function _localCommentsSet(items){ try { localStorage.setItem(_gbLocalKey(), JSON.stringify(items||[])); } catch(e){} }

async function _fetchAllComments(){
  var now = Date.now();
  if(window._gbCache && now - window._gbCacheTs < 30000) return window._gbCache;
  if(location.protocol === 'file:'){
    window._gbCache = _localCommentsGet();
    window._gbCacheTs = now;
    return window._gbCache;
  }
  try {
    var r = await fetch(GB_API);
    if(!r.ok) throw new Error('HTTP '+r.status);
    var data = await r.json();
    window._gbCache = _parseCommentPayload(data);
    window._gbCacheTs = now;
    return window._gbCache;
  } catch(e){
    window._gbCache = _localCommentsGet();
    window._gbCacheTs = now;
    return window._gbCache;
  }
}

// 주소로 표시 이름 찾기 (WALLET_META 우선, 없으면 ALL_STATS label)
function _walletLabel(addr){
  if(!addr) return '';
  var k = addr.toLowerCase();
  var meta = (WALLET_META||{})[k];
  if(meta && meta.name) return meta.name;
  var s = ((window.ALL_STATS)||[]).find(function(x){ return x.address && x.address.toLowerCase()===k; });
  return s ? (s.label||addr.slice(0,8)+'...'+addr.slice(-4)) : addr.slice(0,8)+'...'+addr.slice(-4);
}

// 날짜+시분 포맷
function _fmtDate(dateStr){
  var d = new Date(dateStr);
  if(isNaN(d)) return dateStr||'';
  var year = d.getFullYear();
  var mon = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][d.getMonth()];
  var day = String(d.getDate()).padStart(2,'0');
  var hh = String(d.getHours()).padStart(2,'0');
  var mm = String(d.getMinutes()).padStart(2,'0');
  return year+' '+mon+' '+day+' '+hh+':'+mm;
}

// 댓글 1개 렌더링
function _renderComment(item, showWallet){
  var dateStr = _fmtDate(item.date);
  var walletTag = '';
  var _wa = item.wallet_address || item.address || item.wallet;
  if(showWallet && _wa){
    var lbl = _walletLabel(_wa);
    walletTag = '<span data-addr="'+item.wallet_address+'" onclick="openModal(this.dataset.addr)" '
      +'style="cursor:pointer;font-size:9px;padding:1px 7px;border-radius:4px;background:#0a1a2a;'
      +'color:#3a86ff;border:0.5px solid #3a86ff;margin-left:6px;white-space:nowrap" title="Open wallet">'
      +_esc(lbl)+' ↗</span>';
  }
  // 카드 전체 클릭: wallet_address 있으면 openModal
  var cardClick = (showWallet && item.wallet_address)
    ? ' style="background:#111120;border-radius:10px;padding:14px 16px;border:1px solid #1e1e35;cursor:pointer" data-addr="'+item.wallet_address+'" onclick="openModal(this.dataset.addr)"'
    : ' style="background:#111120;border-radius:10px;padding:14px 16px;border:1px solid #1e1e35"';
  return '<div'+cardClick+'>'
    +'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;flex-wrap:wrap;gap:4px">'
    +'<div style="display:flex;align-items:center;flex-wrap:wrap;gap:4px">'
    +'<span style="font-size:12px;font-weight:600;color:#c8d0e7">'+_esc(item.name||'Anonymous')+'</span>'
    +walletTag
    +'</div>'
    +'<span style="font-size:10px;color:#3a3a55;white-space:nowrap">'+dateStr+'</span>'
    +'</div>'
    +'<div style="font-size:12px;color:#888;line-height:1.6">'+_esc(item.msg)+'</div>'
    +'</div>';
}

// ── Guestbook 탭 (전체 댓글) ─────────────────────────────────────
async function initGuestbook(){
  var root=document.getElementById('guestbook-root');
  if(!root) return;
  root.innerHTML=`
    <div style="margin-bottom:20px">
      <div style="font-family:'Bebas Neue',sans-serif;font-size:28px;letter-spacing:2px;color:var(--green);margin-bottom:6px">📝 Guestbook</div>
      <div style="font-size:11px;color:var(--dim);margin-bottom:20px">Leave a message. Be nice 🙂 ${location.protocol==='file:'?'<span style=\"color:#ffbe0b\">(local mode: remote comments unavailable)</span>':''}</div>
      <div style="display:flex;flex-direction:column;gap:8px;margin-bottom:8px">
        <input id="gb-name" type="text" placeholder="Name (optional)" maxlength="30"
          style="background:#0e0e1a;border:1px solid #2a2a45;border-radius:8px;padding:10px 14px;color:#c8d0e7;font-size:13px;outline:none">
        <textarea id="gb-msg" placeholder="Your message..." maxlength="300" rows="3"
          style="background:#0e0e1a;border:1px solid #2a2a45;border-radius:8px;padding:10px 14px;color:#c8d0e7;font-size:13px;outline:none;resize:vertical"></textarea>
        <button onclick="submitGuestbook(null)"
          style="background:var(--green);color:#000;border:none;border-radius:8px;padding:10px 20px;font-weight:700;font-size:13px;cursor:pointer;align-self:flex-start">
          Post
        </button>
      </div>
      <div id="gb-status" style="font-size:11px;color:var(--dim);min-height:16px"></div>
    </div>
    <div id="gb-list" style="display:flex;flex-direction:column;gap:10px">
      <div style="font-size:11px;color:#444">Loading messages...</div>
    </div>
  `;
  loadGuestbook();
}

var _GB_PAGE_SIZE = 20;
var _gbShownCount = 0;

async function loadGuestbook(append){
  try {
    if(!append){ window._gbCache = null; _gbShownCount = 0; }
    var items = await _fetchAllComments();
    var list = document.getElementById('gb-list');
    if(!list) return;
    if(!items.length){ list.innerHTML='<div style="font-size:11px;color:#444">'+(location.protocol==='file:'?'No local messages yet. Open via http://localhost to load shared guestbook.':'No messages yet.')+'</div>'; return; }
    var slice = items.slice(_gbShownCount, _gbShownCount + _GB_PAGE_SIZE);
    _gbShownCount += slice.length;
    var newHTML = slice.map(function(item){ return _renderComment(item, true); }).join('');
    if(append){
      var more = document.getElementById('gb-more-btn');
      if(more) more.remove();
      list.insertAdjacentHTML('beforeend', newHTML);
    } else {
      list.innerHTML = newHTML;
    }
    // 더보기 버튼
    if(_gbShownCount < items.length){
      list.insertAdjacentHTML('beforeend',
        '<button id="gb-more-btn" onclick="loadGuestbook(true)" '
        +'style="width:100%;margin-top:10px;padding:8px;border-radius:8px;border:0.5px solid #2a2a45;'
        +'background:transparent;color:#555;cursor:pointer;font-size:11px;font-family:DM Mono,monospace">'
        +'Load more (' + (items.length - _gbShownCount) + ' remaining)</button>');
    }
  } catch(e){
    var list = document.getElementById('gb-list');
    if(list) list.innerHTML='<div style="font-size:11px;color:#f72585">Failed to load. '+(e&&e.message?e.message:'')+'</div>';
  }
}

// ── 지갑별 댓글 (모달 내) — 전체 캐시에서 클라이언트 필터링
async function loadWalletComments(walletAddr, listEl, countEl, startIdx){
  if(!walletAddr || !listEl) return;
  var WC_PAGE = 10;
  try {
    var all = await _fetchAllComments();
    var lk = walletAddr.toLowerCase();
    var items = all.filter(function(i){ var wa=i.wallet_address||i.address||i.wallet; return wa && wa.toLowerCase()===lk; });
    if(countEl) countEl.textContent = items.length ? ''+items.length : '';
    if(!items.length){ listEl.innerHTML='<div style="font-size:11px;color:#444;padding:8px 0">No comments yet.</div>'; return; }
    var si = startIdx || 0;
    var slice = items.slice(si, si + WC_PAGE);
    var newHTML = slice.map(function(item){ return _renderComment(item, false); }).join('');
    if(si === 0){
      listEl.innerHTML = newHTML;
    } else {
      var more = listEl.querySelector('.wc-more-btn');
      if(more) more.remove();
      listEl.insertAdjacentHTML('beforeend', newHTML);
    }
    var nextIdx = si + slice.length;
    if(nextIdx < items.length){
      var btn = document.createElement('button');
      btn.className = 'wc-more-btn';
      btn.textContent = 'Load more (' + (items.length - nextIdx) + ')';
      btn.style.cssText = 'width:100%;margin-top:8px;padding:6px;border-radius:6px;border:0.5px solid #2a2a45;background:transparent;color:#555;cursor:pointer;font-size:10px;font-family:DM Mono,monospace';
      btn.onclick = function(){ loadWalletComments(walletAddr, listEl, countEl, nextIdx); };
      listEl.appendChild(btn);
    }
  } catch(e){
    listEl.innerHTML='<div style="font-size:11px;color:#f72585">Failed to load. '+(e&&e.message?e.message:'')+'</div>';
  }
}

// ── 댓글 카운트 뱃지 (카드용) ────────────────────────────────────
async function loadCommentCounts(){
  try {
    var items = await _fetchAllComments();
    var counts = {};
    items.forEach(function(item){
      var wa = item.wallet_address || item.address || item.wallet;
      if(wa){
        var k = wa.toLowerCase();
        counts[k] = (counts[k]||0) + 1;
      }
    });
    document.querySelectorAll('[data-address]').forEach(function(card){
      var addr = card.dataset.address;
      if(!addr) return;
      var cnt = counts[addr.toLowerCase()]||0;
      var badge = card.querySelector('.comment-count-badge');
      if(badge) badge.textContent = cnt > 0 ? '💬 '+cnt : '';
    });
    window._commentCounts = counts;
  } catch(e){}
}

// ── 댓글 제출 (wallet_address 옵션) ──────────────────────────────
async function submitGuestbook(walletAddr){
  var nameEl  = document.getElementById(walletAddr ? 'wc-name-'+walletAddr.slice(2,8) : 'gb-name');
  var msgEl   = document.getElementById(walletAddr ? 'wc-msg-'+walletAddr.slice(2,8)  : 'gb-msg');
  var statEl  = document.getElementById(walletAddr ? 'wc-status-'+walletAddr.slice(2,8) : 'gb-status');
  if(!nameEl||!msgEl||!statEl) return;
  var name = (nameEl.value||'Anonymous').trim();
  var msg  = (msgEl.value||'').trim();
  if(!msg){ statEl.innerHTML='<span style="color:#f72585">Please write something.</span>'; return; }
  statEl.innerHTML='<span style="color:var(--dim)">Posting...</span>';
  try {
    var body = {name:name, message:msg, date:new Date().toISOString()};
    if(walletAddr) body.wallet_address = walletAddr.toLowerCase();
    var ok = false;
    if(location.protocol === 'file:'){
      var items = _localCommentsGet();
      items.unshift(body);
      _localCommentsSet(items);
      ok = true;
    } else {
      var r = await fetch(GB_API,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
      if(!r.ok) throw new Error('HTTP '+r.status);
      var data = await r.json();
      ok = !!(data && (data.ok || data.success));
      if(!ok) throw new Error((data && data.error) || 'Post failed');
    }
    if(ok){
      window._gbCache = null;
      msgEl.value=''; nameEl.value='';
      statEl.innerHTML='<span style="color:var(--green)">✓ Posted!</span>';
      if(walletAddr){
        var listEl = document.getElementById('wc-list-'+walletAddr.slice(2,8));
        var cntEl  = document.getElementById('wc-count-'+walletAddr.slice(2,8));
        setTimeout(function(){ loadWalletComments(walletAddr, listEl, cntEl); loadCommentCounts(); }, 200);
      } else {
        setTimeout(function(){ loadGuestbook(false); loadCommentCounts(); }, 200);
      }
    }
  } catch(e){
    statEl.innerHTML='<span style="color:#f72585">Error: '+e.message+'</span>';
  }
}


window._psmInit = false;
function initPlaystyleMap() {
  var container = document.getElementById('playstyle-map');
  if (!container) return;
  if (window._psmInit) return;
  window._psmInit = true;

  var W = container.offsetWidth || 600;
  var H = 400;
  var PAD = { l: 24, r: 24, t: 28, b: 28 };
  var pw = W - PAD.l - PAD.r;
  var ph = H - PAD.t - PAD.b;
  var tip = document.getElementById('psm-tip');

  // Fixed scale: x = big_bet_rate 0~100%, y = sharpe -1~8
  var X_MIN=0, X_MAX=100, Y_MIN=-1, Y_MAX=8;
  function psmNorm(bbr, sh){
    var nx = Math.max(0.03, Math.min(0.97, (bbr-X_MIN)/(X_MAX-X_MIN)));
    var ny = Math.max(0.03, Math.min(0.97, (sh -Y_MIN)/(Y_MAX-Y_MIN)));
    return {nx:nx, ny:ny};
  }

  // grid lines
  [0.25, 0.5, 0.75].forEach(function(f) {
    var hl = document.createElement('div');
    hl.style.cssText = 'position:absolute;top:'+(PAD.t+f*ph)+'px;left:'+PAD.l+'px;right:'+PAD.r+'px;height:0.5px;background:#131328;pointer-events:none;';
    container.appendChild(hl);
    var vl = document.createElement('div');
    vl.style.cssText = 'position:absolute;left:'+(PAD.l+f*pw)+'px;top:'+PAD.t+'px;bottom:'+PAD.b+'px;width:0.5px;background:#131328;pointer-events:none;';
    container.appendChild(vl);
  });

  // center cross
  var cx = document.createElement('div');
  cx.style.cssText = 'position:absolute;left:'+PAD.l+'px;right:'+PAD.r+'px;top:'+(PAD.t+ph/2)+'px;height:0.5px;background:#1e1e35;pointer-events:none;';
  container.appendChild(cx);

  // axis edge labels
  function addLbl(text, css){
    var el = document.createElement('div');
    el.textContent = text;
    el.style.cssText = 'position:absolute;font-size:8px;color:#2a2a45;font-family:DM Mono,monospace;pointer-events:none;'+css;
    container.appendChild(el);
  }
  addLbl('efficient',    'top:4px;left:'+PAD.l+'px');
  addLbl('inefficient',  'bottom:4px;left:'+PAD.l+'px');
  addLbl('passive',      'bottom:4px;left:'+PAD.l+'px');
  addLbl('aggressive',   'bottom:4px;right:'+PAD.r+'px');

  // quadrant labels
  [{t:'passive + efficient',     x:PAD.l+8,      y:PAD.t+8},
   {t:'aggressive + efficient',  x:PAD.l+pw/2+8, y:PAD.t+8},
   {t:'passive + inefficient',   x:PAD.l+8,      y:PAD.t+ph/2+8},
   {t:'aggressive + inefficient',x:PAD.l+pw/2+8, y:PAD.t+ph/2+8}
  ].forEach(function(q){
    var el = document.createElement('div');
    el.textContent = q.t;
    el.style.cssText = 'position:absolute;font-size:8px;color:#1e1e35;font-family:DM Mono,monospace;pointer-events:none;left:'+q.x+'px;top:'+q.y+'px;';
    container.appendChild(el);
  });

  // Archetype anchors
  var archetypes = [
    { n:'Precision', c:'#00f5d4', bbr:15, sh:3.5, d:'win_rate>72 + Sharpe>3 + MDD<25' },
    { n:'Ice Quant', c:'#3a86ff', bbr:10, sh:5.5, d:'Sharpe>5 + MDD<15 + durability>50' },
    { n:'Apex',      c:'#ffbe0b', bbr:68, sh:3.5, d:'big_bet>60 + win_rate>65 + dur>50' },
    { n:'Sniper',    c:'#00f5d4', bbr:25, sh:1.5, d:'profit_factor>3 + win_rate<55' },
    { n:'Hi Roller', c:'#f72585', bbr:58, sh:0.5, d:'MDD>40 + profit_factor>1.5' },
    { n:'Degen',     c:'#f72585', bbr:78, sh:-0.5,d:'MDD>200 or MDD>80+big_bets>10' },
    { n:'Steady',    c:'#9b5de5', bbr:20, sh:2.5, d:'win_rate>65 + total_pnl>0' },
    { n:'All-Round', c:'#9b5de5', bbr:32, sh:2.0, d:'win_rate>60 + durability>55' },
    { n:'Momentum',  c:'#ffbe0b', bbr:38, sh:3.5, d:'Sharpe>3 + total_pnl>0' },
    { n:'Value',     c:'#3a86ff', bbr:10, sh:2.0, d:'profit_factor>2 + total_pnl>0' },
    { n:'Bet Maker', c:'#ffbe0b', bbr:62, sh:1.0, d:'big_bet_count>20 + big_bet_rate>50' },
    { n:'Flash',     c:'#f72585', bbr:48, sh:0.2, d:'durability<35' },
    { n:'Newcomer',  c:'#4a4a6a', bbr:28, sh:1.0, d:'closed_count<100' },
    { n:'Drifter',   c:'#4a4a6a', bbr:22, sh:0.3, d:'fallback' },
  ];

  archetypes.forEach(function(t){
    var _n = psmNorm(t.bbr, t.sh);
    var px = PAD.l + _n.nx * pw;
    var py = PAD.t + (1 - _n.ny) * ph;

    var dot = document.createElement('div');
    dot.style.cssText = 'position:absolute;width:12px;height:12px;border-radius:50%;background:'+t.c+';opacity:0.9;left:'+(px-6)+'px;top:'+(py-6)+'px;z-index:5;cursor:default;';
    dot.addEventListener('mouseenter', function(){
      if(tip){ tip.innerHTML =
        '<div style="font-size:11px;font-weight:600;color:#c8d0e7;margin-bottom:4px">'+t.n+'</div>'
        +'<div style="font-size:10px;color:#888">'+t.d+'</div>';
        tip.style.display='block'; }
    });
    dot.addEventListener('mousemove', function(e){
      if(!tip) return;
      var rect=container.getBoundingClientRect();
      var mx=e.clientX-rect.left, my=e.clientY-rect.top;
      var tipW=220, tipH=80;
      var tx=mx>W/2?mx-tipW-8:mx+14;
      var ty=my+tipH>H?my-tipH:my+14;
      tx=Math.max(4,Math.min(tx,W-tipW-4));
      ty=Math.max(4,Math.min(ty,H-tipH-4));
      tip.style.left=tx+'px'; tip.style.top=ty+'px';
    });
    dot.addEventListener('mouseleave', function(){ if(tip) tip.style.display='none'; });
    container.appendChild(dot);

    var lbl = document.createElement('div');
    lbl.textContent = t.n;
    lbl.style.cssText = 'position:absolute;font-size:9px;color:'+t.c+';left:'+(px+8)+'px;top:'+(py-5)+'px;z-index:5;pointer-events:none;white-space:nowrap;font-family:DM Mono,monospace;opacity:0.85;';
    container.appendChild(lbl);
  });

  // legend
  var legEl = document.getElementById('psm-legend');
  if (legEl) {
    legEl.innerHTML = '';
    [
      { c:'#00f5d4', label:'Precision / systematic' },
      { c:'#ffbe0b', label:'Conviction / momentum' },
      { c:'#f72585', label:'Aggressive / speed' },
      { c:'#3a86ff', label:'Systematic / value' },
      { c:'#9b5de5', label:'Balanced' },
      { c:'#4a4a6a', label:'Unproven' },
    ].forEach(function(g) {
      var item = document.createElement('div');
      item.style.cssText = 'display:flex;align-items:center;gap:5px;font-size:10px;color:#555;font-family:DM Mono,monospace;';
      item.innerHTML = '<div style="width:8px;height:8px;border-radius:50%;background:'+g.c+'"></div>'+g.label;
      legEl.appendChild(item);
    });
  }
}




function _esc(str) {
  if (str === null || str === undefined) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}


// ── card filter / sort ────────────────────────────────────────
window._activeTypeFilter = '';

function buildTypeFilterBar(){
  var typeList = [];
  var typeSet = {};
  (window.ALL_STATS||[]).forEach(function(s){ if(s.trader_type && !typeSet[s.trader_type]){ typeSet[s.trader_type]=1; typeList.push(s.trader_type); } });
  typeList.sort();
  var bar = document.getElementById('type-filter-bar');
  if(!bar) return;
  bar.innerHTML = '';
  // All button
  var allBtn = document.createElement('button');
  allBtn.textContent = 'All';
  allBtn.dataset.t = '';
  allBtn.style.cssText = 'font-size:10px;padding:3px 10px;border-radius:5px;border:0.5px solid #00f5d4;background:#1e1e35;color:#00f5d4;cursor:pointer;font-family:DM Mono,monospace;white-space:nowrap';
  allBtn.onclick = function(){ setTypeFilter(this); };
  bar.appendChild(allBtn);
  // per-type buttons
  typeList.forEach(function(t){
    var btn = document.createElement('button');
    // strip emoji prefix, keep type name only
    var label = t.split(' ').slice(1).join(' ') || t;
    btn.textContent = label;
    btn.dataset.t = t;
    btn.style.cssText = 'font-size:10px;padding:3px 10px;border-radius:5px;border:0.5px solid #2a2a45;background:transparent;color:#666;cursor:pointer;font-family:DM Mono,monospace;white-space:nowrap';
    btn.onclick = function(){ setTypeFilter(this); };
    bar.appendChild(btn);
  });
  applyCardFilters();
}

// run after DOM is ready
if(document.readyState === 'loading'){
  document.addEventListener('DOMContentLoaded', buildTypeFilterBar);
} else {
  buildTypeFilterBar();
}

function setTypeFilter(btn){
  window._activeTypeFilter = btn.dataset.t || '';
  var bar = document.getElementById('type-filter-bar');
  if(bar) bar.querySelectorAll('button').forEach(function(b){
    var active = b === btn;
    b.style.borderColor = active ? '#00f5d4' : '#2a2a45';
    b.style.color       = active ? '#00f5d4' : '#555';
    b.style.background  = active ? '#1e1e35' : 'transparent';
  });
  applyCardFilters();
}

function resetCardFilters(){
  window._activeTypeFilter = '';
  var bar = document.getElementById('type-filter-bar');
  if(bar) bar.querySelectorAll('button').forEach(function(b,i){
    b.style.borderColor = i===0?'#00f5d4':'#2a2a45';
    b.style.color       = i===0?'#00f5d4':'#666';
    b.style.background  = i===0?'#1e1e35':'transparent';
  });
  ['filter-source','filter-conf'].forEach(function(id){
    var el=document.getElementById(id); if(el) el.value='';
  });
  var s=document.getElementById('sort-by'); if(s) s.value='war';
  applyCardFilters();
}

var _CARDS_PER_PAGE = 20;
var _cardsCurrentPage = 1;
var _cardsPairs = [];  // 필터+정렬된 전체 결과 캐시

function applyCardFilters(resetPage){
  if(!Array.isArray(window.ALL_STATS)) return;
  if(resetPage !== false) _cardsCurrentPage = 1;  // 필터/정렬 바뀌면 1페이지로

  var fType  = window._activeTypeFilter||'';
  var fSrc   = (document.getElementById('filter-source')||{}).value||'';
  var fConf  = (document.getElementById('filter-conf')||{}).value||'';
  var sortBy = (document.getElementById('sort-by')||{}).value||'war';

  var grid = document.getElementById('cards-grid-inner');
  if(!grid) return;

  var cards = Array.from(grid.querySelectorAll('.trader-card'));

  var statMap = {};
  (window.ALL_STATS||[]).forEach(function(s){ if(s.address) statMap[s.address.toLowerCase()]=s; });

  var sortKey = {
    war:        function(s){ return -(s.war_score||0); },
    winrate:    function(s){ return -(s.win_rate||0); },
    sharpe:     function(s){ return -(s.sharpe||0); },
    roi:        function(s){ return -(s.roi_pct||0); },
    durability: function(s){ return -(s.durability||0); },
    pnl:        function(s){ return -(s.total_pnl||0); },
    bigbet:     function(s){ return -(s.big_bet_rate||0); },
    equity:     function(s){ return -(s.total_equity||0); },
    follow:     function(s){ return -(s.follow_score||0); },
    likes:      function(s){ return -((window._likeCounts&&window._likeCounts[s.address])||0); },
  }[sortBy] || function(s){ return -(s.war_score||0); };

  // 필터+정렬
  var pairs = cards.map(function(card){
    var addr = (card.dataset.address||'').toLowerCase();
    var s = statMap[addr] || {};
    return { card:card, s:s };
  });
  pairs.sort(function(a,b){ return sortKey(a.s) - sortKey(b.s); });

  // 필터 적용
  var filtered = pairs.filter(function(p){
    var s = p.s;
    if(fType && s.trader_type !== fType)  return false;
    if(fSrc  && s.source      !== fSrc)   return false;
    if(fConf && s.confidence  !== fConf)  return false;
    return true;
  });
  _cardsPairs = filtered;

  // 페이지 계산
  var total     = filtered.length;
  var totalPages = Math.max(1, Math.ceil(total / _CARDS_PER_PAGE));
  if(_cardsCurrentPage > totalPages) _cardsCurrentPage = totalPages;
  var start = (_cardsCurrentPage - 1) * _CARDS_PER_PAGE;
  var end   = start + _CARDS_PER_PAGE;

  // 카드 표시/숨김
  pairs.forEach(function(p){ p.card.style.display = 'none'; });
  filtered.slice(start, end).forEach(function(p){
    p.card.style.display = '';
    grid.appendChild(p.card);
  });

  // 카운트 업데이트
  var cnt = document.getElementById('filter-count');
  if(cnt){
    var showing = Math.min(end, total) - start;
    cnt.textContent = total ? ('Showing ' + (start+1) + '–' + Math.min(end, total) + ' / ' + total) : 'Showing 0 / 0';
    cnt.style.color = total < cards.length ? '#00f5d4' : '#3a3a55';
  }

  // 페이지네이션 렌더
  _renderCardPagination(totalPages);
}

function _renderCardPagination(totalPages){
  var bar = document.getElementById('cards-pagination');
  if(!bar) return;
  if(totalPages <= 1){ bar.innerHTML=''; return; }

  var cur = _cardsCurrentPage;
  var html = '';
  var btnStyle = 'font-size:11px;padding:4px 10px;border-radius:6px;border:0.5px solid #2a2a45;background:transparent;color:#555;cursor:pointer;font-family:DM Mono,monospace';
  var activeStyle = 'font-size:11px;padding:4px 10px;border-radius:6px;border:0.5px solid #00f5d4;background:#001a0f;color:#00f5d4;cursor:pointer;font-family:DM Mono,monospace';

  // ◀ 이전
  html += '<button onclick="_goCardPage('+(cur-1)+')" style="'+btnStyle+'"'+(cur===1?' disabled':'')+'">◀</button>';

  // 페이지 번호 (최대 7개 표시)
  var pages = [];
  if(totalPages <= 7){
    for(var i=1;i<=totalPages;i++) pages.push(i);
  } else {
    pages.push(1);
    if(cur > 3) pages.push('...');
    for(var i=Math.max(2,cur-1); i<=Math.min(totalPages-1,cur+1); i++) pages.push(i);
    if(cur < totalPages-2) pages.push('...');
    pages.push(totalPages);
  }

  pages.forEach(function(p){
    if(p === '...'){
      html += '<span style="color:#3a3a55;padding:0 4px">…</span>';
    } else {
      html += '<button onclick="_goCardPage('+p+')" style="'+(p===cur?activeStyle:btnStyle)+'">'+p+'</button>';
    }
  });

  // ▶ 다음
  html += '<button onclick="_goCardPage('+(cur+1)+')" style="'+btnStyle+'"'+(cur===totalPages?' disabled':'')+'>▶</button>';

  bar.innerHTML = html;
}

function _goCardPage(page){
  var totalPages = Math.max(1, Math.ceil(_cardsPairs.length / _CARDS_PER_PAGE));
  if(page < 1 || page > totalPages) return;
  _cardsCurrentPage = page;
  applyCardFilters(false);  // 페이지만 바꾸고 1페이지 리셋 안 함
  // 카드 섹션 상단으로 스크롤
  var el = document.getElementById('tab-cards');
  if(el) el.scrollIntoView({behavior:'smooth', block:'start'});
}


window._copyAddr=function(a){navigator.clipboard.writeText(a).then(function(){}).catch(function(){});};
async function shareCard(addr, shortAddr, warEst, traderType){
  var btn = document.getElementById('share-btn-'+addr.slice(2,8));
  var hint = document.getElementById('share-hint-'+addr.slice(2,8));
  var card = document.getElementById('lookup-result');
  if(!card||!card.firstElementChild) return;
  if(!card) return;

  if(btn){ btn.textContent='⏳ Capturing...'; btn.disabled=true; }

  try {
    var canvas = await html2canvas(card.firstElementChild||card, {
      backgroundColor: '#0a0a16',
      scale: 2,
      useCORS: true,
      logging: false
    });

    // 이미지 다운로드
    var link = document.createElement('a');
    link.download = 'wallet-scout-'+shortAddr+'.png';
    link.href = canvas.toDataURL('image/png');
    link.click();

    // 트위터 인텐트 열기
    var siteUrl = 'https://wallet-scout-api.kimsubbae113.workers.dev';
    var text = '🔭 '+addr+'\\n'+traderType+' · WAR '+warEst+'\\n\\nCurious what score your wallet gets? Check Hyperliquid Smart Money rankings 👇\\n'+siteUrl;
    var twitterUrl = 'https://twitter.com/intent/tweet?text='+encodeURIComponent(text);
    setTimeout(function(){ window.open(twitterUrl, '_blank'); }, 800);

    if(hint){ hint.style.display='block'; }
    if(btn){
      btn.innerHTML='<svg width="16" height="16" viewBox="0 0 24 24" fill="white"><path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-4.714-6.231-5.401 6.231H2.746l7.73-8.835L1.254 2.25H8.08l4.253 5.622zm-1.161 17.52h1.833L7.084 4.126H5.117z"/></svg> Share on X';
      btn.disabled=false;
    }
  } catch(e) {
    if(btn){ btn.textContent='Share on X'; btn.disabled=false; }
    console.error('Share failed:', e);   } }  function fmtCompact(v){   var sign=v>=0?'+':'-'; var av=Math.abs(v);
  if(av>=1000000) return sign+'$'+(av/1000000).toFixed(1)+'M';
  if(av>=1000) return sign+'$'+Math.round(av/1000)+'K';
  return sign+'$'+Math.round(av);
}
function initLookup(){
  var root=document.getElementById('lookup-root');
  if(!root||root._init) return;
  root._init=true;
  root.innerHTML=`
    <div style="margin-bottom:24px">
      <div style="font-family:'Bebas Neue',sans-serif;font-size:28px;letter-spacing:2px;color:var(--green);margin-bottom:6px">🔍 Lookup</div>
      <div style="font-size:11px;color:var(--dim);margin-bottom:20px">Enter a Hyperliquid address for instant analysis. WAR 40+ wallets are auto-registered.</div>
      <div style="display:flex;gap:10px;margin-bottom:8px">
        <input id="lookup-input" type="text" placeholder="0x..." 
          style="flex:1;background:#0e0e1a;border:1px solid #2a2a45;border-radius:8px;padding:12px 16px;color:#c8d0e7;font-family:'DM Mono',monospace;font-size:13px;outline:none"
          onkeydown="if(event.key==='Enter')doLookup()">
        <button onclick="doLookup()" 
          style="background:var(--green);color:#000;border:none;border-radius:8px;padding:12px 20px;font-weight:700;font-size:13px;cursor:pointer;white-space:nowrap">
          Analyze
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
    document.getElementById('lookup-status').innerHTML='<span style="color:#f72585">Invalid address format (0x...)</span>';
    return;
  }
  var status=document.getElementById('lookup-status');
  var result=document.getElementById('lookup-result');
  status.innerHTML='<span style="color:var(--green)">⏳ Fetching...</span>';
  result.innerHTML='';

  try {
    // Hyperliquid API 직접 호출
    var [chRes, fillsRes, spotRes] = await Promise.all([
      fetch('https://api.hyperliquid.xyz/info', {
        method:'POST', headers:{'Content-Type':'application/json'},
        body:JSON.stringify({type:'clearinghouseState', user:addr})
      }),
      fetch('https://api.hyperliquid.xyz/info', {
        method:'POST', headers:{'Content-Type':'application/json'},
        body:JSON.stringify({type:'userFills', user:addr, aggregateByTime:true})
      }),
      fetch('https://api.hyperliquid.xyz/info', {
        method:'POST', headers:{'Content-Type':'application/json'},
        body:JSON.stringify({type:'spotClearinghouseState', user:addr})
      })
    ]);
    var ch = await chRes.json().catch(()=>({}));
    var fills = await fillsRes.json().catch(()=>[]);
    if(!Array.isArray(fills)) fills = [];
    var spotData = await spotRes.json().catch(()=>({}));
    // spot Portfolio 합산
    var spotHoldings = [];
    var spotEquity = 0;
    ((spotData&&spotData.balances)||[]).forEach(function(b){
      var coin=b.coin, amt=parseFloat(b.total||0);
      if(coin==='USDC'||coin==='USDT') { spotEquity+=amt; }
      else if(amt>0) { spotHoldings.push({coin:coin, amount:amt}); }
    });
    // 카드와 동일한 필터: BTC/ETH/SOL/HYPE≥1개, 나머지≥1000개, 우선순위 정렬, 최대 6개
    function _spotPri(c){ c=c.toUpperCase(); if(['BTC','ETH','SOL','HYPE'].some(function(k){return c.indexOf(k)>=0;})) return 0; if(c.indexOf('USD')>=0) return 1; return 2; }
    spotHoldings = spotHoldings.filter(function(h){
      var c=h.coin.toUpperCase(), a=h.amount;
      if(['BTC','ETH','SOL','HYPE'].some(function(k){return c.indexOf(k)>=0;})) return a>=1;
      return a>=1000;
    }).sort(function(a,b){
      var pa=_spotPri(a.coin), pb=_spotPri(b.coin);
      return pa!==pb ? pa-pb : b.amount-a.amount;
    }).slice(0,6).map(function(h){
      var c=h.coin.toUpperCase();
      var isMajor=['BTC','ETH','SOL','HYPE'].some(function(k){return c.indexOf(k)>=0;});
      var amtStr=isMajor?h.amount.toFixed(2):Math.round(h.amount).toLocaleString();
      return {coin:h.coin, amount:amtStr};
    });

    if(!ch || !ch.marginSummary){
      status.innerHTML='<span style="color:#f72585">Address not found or no data</span>';
      return;
    }

    // 기본 지표 계산 — Python compute_stats와 완전 동일
    var ms = ch.marginSummary||{};
    var _vaultEq = parseFloat(ch.vaultEquity||0);
    var _acctEq  = parseFloat(ms.accountValue||0);
    // Python: vaultEquity > 0 ? vaultEquity : accountValue, 이후 USDC/USDT만 추가
    var _spotUsd = 0;
    ((spotData&&spotData.balances)||[]).forEach(function(b){
      if(b.coin==='USDC'||b.coin==='USDT') _spotUsd += parseFloat(b.total||0);
    });
    var equity = (_vaultEq > 0 ? _vaultEq : _acctEq) + _spotUsd;

    // 첫/마지막 거래 날짜 계산
    var allTimes = (Array.isArray(closed)?closed:[]).filter(function(f){return f&&f.time;}).map(function(f){return f.time;});
    var firstDate='', lastDate='', dataDays=0;
    if(allTimes.length){
      var minT=Math.min.apply(null,allTimes), maxT=Math.max.apply(null,allTimes);
      firstDate=new Date(minT).toISOString().slice(0,10);
      lastDate=new Date(maxT).toISOString().slice(0,10);
      dataDays=Math.round((maxT-minT)/(1000*60*60*24))+1;
    }
    var positions = (Array.isArray(ch.assetPositions)?ch.assetPositions:[])
      .filter(function(ap){return parseFloat((ap&&ap.position||{}).szi||0)!==0;})
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

    // fills 분석 — Python compute_stats와 동일: closedPnl!=0 인 것만 closed로 사용
    var allFills = Array.isArray(fills) ? fills : [];
    var closed = allFills.filter(function(f){ return f.closedPnl && parseFloat(f.closedPnl)!==0; });
    var wins=0, total=closed.length;
    var pnlMap={};
    var realized=0;
    closed.forEach(function(f){
      var pnl=parseFloat(f.closedPnl);
      if(pnl>0) wins++;
      pnlMap[f.coin]=(pnlMap[f.coin]||0)+pnl;
      realized+=pnl;
    });
    var winRate = total>0?Math.round(wins/total*100):0;
    var totalPnl = realized;  // upnl 제외 — Python과 동일
    var roi = equity>0?Math.round(totalPnl/equity*100*10)/10:0;

    // Sharpe — Python compute_stats와 동일: closed fills 일별 PnL
    var dayMap={};
    closed.forEach(function(f){
      var pnl=parseFloat(f.closedPnl);
      var day=f.time?new Date(f.time).toISOString().slice(0,10):'x';
      dayMap[day]=(dayMap[day]||0)+pnl;
    });
    var dayVals=Object.values(dayMap);
    // 최근 30일 스파크라인
    var cutoff14 = new Date(Date.now()-30*24*60*60*1000).toISOString().slice(0,10);
    var sortedDays = Object.keys(dayMap).sort().filter(function(d){return d>=cutoff14;});
    var cumPts = []; var running14 = 0;
    sortedDays.forEach(function(d){ running14+=(dayMap[d]||0); cumPts.push(running14); });
    var lookupSparkSvg = '';
    if(cumPts.length>=2){
      var minV=Math.min.apply(null,cumPts), maxV=Math.max.apply(null,cumPts);
      var rng=maxV-minV||1, W=200, H=36;
      var coords=cumPts.map(function(v,i){ return (i*W/(cumPts.length-1)).toFixed(1)+','+(H-(v-minV)/rng*H).toFixed(1); }).join(' ');
      var col=cumPts[cumPts.length-1]>=cumPts[0]?'#00f5d4':'#f72585';
      var fillPts='0,'+H+' '+coords+' '+W+','+H;
      lookupSparkSvg='<svg viewBox="0 0 '+W+' '+H+'" width="100%" height="36" preserveAspectRatio="none" style="display:block">'        +'<polygon points="'+fillPts+'" fill="'+col+'" fill-opacity="0.15" stroke="none"/>'        +'<polyline points="'+coords+'" fill="none" stroke="'+col+'" stroke-width="1.5" stroke-linejoin="round"/>'        +'</svg>';
    }
    var sharpeEst=0;
    if(dayVals.length>=3){
      var mean=dayVals.reduce(function(a,b){return a+b;},0)/dayVals.length;
      var std=Math.sqrt(dayVals.reduce(function(a,v){return a+(v-mean)*(v-mean);},0)/dayVals.length);
      // Python compute_stats와 동일: mean/std * sqrt(365) 연율화
      sharpeEst=std>0?Math.round(mean/std*Math.sqrt(365)*100)/100:0;
      if(totalPnl<0 && sharpeEst>0) sharpeEst=-Math.abs(sharpeEst);
    }

    // Big Bet Hit Rate
    var bigBets=0, bigBetWins=0;
    closed.forEach(function(f){
      if(!f.closedPnl||!f.sz||!f.px) return;
      var ntl=Math.abs(parseFloat(f.sz))*parseFloat(f.px);
      if(equity>0&&ntl>equity*0.20){ bigBets++; if(parseFloat(f.closedPnl)>0) bigBetWins++; }
    });
    var bigBetHit=bigBets>0?Math.round(bigBetWins/bigBets*100):0;

    // 오각형 레이더 SVG Gen — 카드와 동일한 공식 (WAR 추정보다 먼저 정의)
    var _MIN=10, _NEU=30;
    function _ps(v){ if(v<0) return _MIN; if(v===0) return _NEU; return Math.min(_NEU+(100-_NEU)*Math.log1p(v)/Math.log1p(600000),100); }
    function _rs(r,n){ if(n<3) return _MIN; r=Math.max(-500,Math.min(r,400)); if(r<0) return _MIN; if(r===0) return _NEU; return Math.min(_NEU+(100-_NEU)*Math.log1p(r)/Math.log1p(400),100); }
    function _bs(rate,cnt){ if(cnt===0) return _MIN; if(rate<=50) return _MIN; var above=(rate-50)/50; return Math.min(_MIN+(100-_MIN)*Math.pow(above,0.5),100); }
    function _ss(sp,n){ if(n<3) return _MIN; if(sp<0) return _MIN; if(sp===0) return _NEU; return Math.min(_NEU+(100-_NEU)*Math.log1p(sp)/Math.log1p(16),100); }
    function _wr(wr,n){ if(n<5) return _MIN; if(wr<50) return Math.max(_MIN,wr*0.6); if(wr===50) return _NEU; var above=(wr-50)/50; return Math.max(_NEU,Math.min(_NEU+(100-_NEU)*Math.pow(above,0.5),100)); }

    // WAR 추정 — 카드와 동일한 가중치 공식
    var _wc = {
      Profit:   Math.round(_ps(totalPnl)*0.25*10)/10,
      ROI:      Math.round(_rs(roi,total)*0.25*10)/10,
      BigBet:   Math.round(_bs(bigBetHit,bigBets)*0.15*10)/10,
      Sharpe:   Math.round(_ss(sharpeEst,total)*0.20*10)/10,
      WinRate:  Math.round(_wr(winRate,total)*0.15*10)/10,
    };
    var warEst = Math.min(99, Math.max(0, Math.round(
      _wc.Profit + _wc.ROI + _wc.BigBet + _wc.Sharpe + _wc.WinRate
    )));
    // ── Lookup 디버그 출력 (discover 로그와 비교용) ──
    console.log('[LOOKUP DEBUG]',
      'fills='+allFills.length,
      'closed='+total,
      'vaultEq='+_vaultEq.toFixed(0),
      'acctEq='+_acctEq.toFixed(0),
      'spotUsd='+_spotUsd.toFixed(0),
      'equity='+equity.toFixed(0),
      'realized='+realized.toFixed(0),
      'upnl='+positions.reduce(function(a,p){return a+p.upnl;},0).toFixed(0),
      'total_pnl='+totalPnl.toFixed(0),
      'roi='+roi,
      'big_bet_count='+bigBets,
      'war_components='+JSON.stringify(_wc),
      'war='+warEst
    );
    // Trader Type 추정
    var traderType='🌀 Drifter',traderChar='Still finding a pattern';
    if(totalPnl<0){traderType='💀 Underwater';traderChar='Drowning in losses';}
    else if(total<100){traderType='🌱 Newcomer';traderChar='Not enough data yet';}
    else if(winRate>72&&roi>50){traderType='🦅 Precision Hunter';traderChar='Never misses a shot';}
    else if(winRate>65&&roi>30){traderType='🦁 Apex Predator';traderChar='Top of the food chain';}
    else if(roi>100&&winRate<55){traderType='🎯 Sniper';traderChar='Few shots, big hits';}
    else if(roi>50){traderType='📈 Momentum';traderChar='Riding the momentum';}
    else if(winRate>65){traderType='🎯 Steady Shot';traderChar='Consistently on target';}
    else if(winRate>60){traderType='📊 All-Rounder';traderChar='Balanced all-around';}
    var _radarVals = [
      _ps(totalPnl),          // Profit
      _rs(roi, total),        // ROI
      _bs(bigBetHit, bigBets),// BigBet
      _ss(sharpeEst, total),  // Sharpe
      _wr(winRate, total),    // WinRate
    ];
    var _n=5, _cx=110, _cy=115, _R=75;
    var _lnames=['Profit','ROI','BigBet','Sharpe','Win Rate'];
    // 오각형 배경
    var _bgPoly='', _dataPoly='';
    for(var _i=0;_i<_n;_i++){
      var _a=Math.PI*2*_i/_n-Math.PI/2;
      _bgPoly+=(_cx+_R*Math.cos(_a)).toFixed(1)+','+(_cy+_R*Math.sin(_a)).toFixed(1)+' ';
      _dataPoly+=(_cx+(_radarVals[_i]/100*_R)*Math.cos(_a)).toFixed(1)+','+(_cy+(_radarVals[_i]/100*_R)*Math.sin(_a)).toFixed(1)+' ';
    }
    // 그리드 라인
    var _grid='';
    [0.25,0.5,0.75,1].forEach(function(r){
      var _pts='';
      for(var _i=0;_i<_n;_i++){
        var _a=Math.PI*2*_i/_n-Math.PI/2;
        _pts+=(_cx+_R*r*Math.cos(_a)).toFixed(1)+','+(_cy+_R*r*Math.sin(_a)).toFixed(1)+' ';
      }
      _grid+='<polygon points="'+_pts.trim()+'" fill="none" stroke="#1e1e35" stroke-width="0.8"/>';
    });
    // 축 라인
    var _axes='';
    for(var _i=0;_i<_n;_i++){
      var _a=Math.PI*2*_i/_n-Math.PI/2;
      _axes+='<line x1="'+_cx+'" y1="'+_cy+'" x2="'+(_cx+_R*Math.cos(_a)).toFixed(1)+'" y2="'+(_cy+_R*Math.sin(_a)).toFixed(1)+'" stroke="#2a2a45" stroke-width="0.8"/>';
    }
    // 라벨
    var _lbls='';
    _lnames.forEach(function(nm,_i){
      var _a=Math.PI*2*_i/_n-Math.PI/2;
      var _lx=(_cx+(_R+18)*Math.cos(_a)).toFixed(1), _ly=(_cy+(_R+18)*Math.sin(_a)).toFixed(1);
      var _anc=Math.abs(Math.cos(_a))<0.1?'middle':Math.cos(_a)>0?'start':'end';
      _lbls+='<text x="'+_lx+'" y="'+_ly+'" text-anchor="'+_anc+'" dominant-baseline="middle" font-size="13" fill="'+((_radarVals[_i]||0)>=60?'#00f5d4':(_radarVals[_i]||0)>=40?'#ffbe0b':'#f87171')+'" font-weight="600">'+nm+'</text>';
    });
    var radarSvg='<svg viewBox="-45 -5 280 250" xmlns="http://www.w3.org/2000/svg" style="width:250px;height:230px">'+
      _grid+_axes+
      '<polygon points="'+_bgPoly.trim()+'" fill="none" stroke="#2a2a45" stroke-width="1"/>'+
      '<polygon points="'+_dataPoly.trim()+'" fill="#00f5d422" stroke="#00f5d4" stroke-width="1.5"/>'+
      _lbls+'</svg>';

    // Trader Card 렌더링
    var pnlColor = totalPnl>=0?'#00f5d4':'#f72585';
    var warColor = warEst>=70?'#00f5d4':warEst>=50?'#ffbe0b':'#f72585';
    var stroke = Math.round(warEst/100*2*Math.PI*26);
    var dash = 2*Math.PI*26;

    var topCoins = Object.entries(pnlMap)
      .sort(function(a,b){return Math.abs(b[1])-Math.abs(a[1]);})
      .slice(0,5);

    // 규모 내림차순 정렬 → 최대 규모 10% 미만 제외 → 상위 5개 (카드와 동일)
    var sortedPos = positions.slice().sort(function(a,b){return b.notional-a.notional;});
    var maxNtl = sortedPos.length>0?sortedPos[0].notional:1;
    var filteredPos = sortedPos.filter(function(p){return p.notional>=maxNtl*0.1;}).slice(0,5);
    // 롱숏 바 (트레이더 카드와 동일)
    var longNtl=filteredPos.reduce(function(a,p){return a+(p.side==='LONG'?p.notional:0);},0);
    var shortNtl=filteredPos.reduce(function(a,p){return a+(p.side==='SHORT'?p.notional:0);},0);
    var totalNtl=longNtl+shortNtl;
    var longPct=totalNtl>0?Math.round(longNtl/totalNtl*100):50;
    var netExp=longNtl-shortNtl;
    var netCol=netExp>=0?'#3a86ff':'#f72585';
    var netStr=(netExp>=0?'L':'S')+' $'+Math.round(Math.abs(netExp)).toLocaleString();
    var barHtml=filteredPos.length===0?'':
      '<div style="margin-bottom:6px">'      +'<div style="display:flex;height:4px;border-radius:2px;overflow:hidden;margin-bottom:3px;background:#1e1e35">'      +'<div style="width:'+longPct+'%;background:#3a86ff"></div>'      +'<div style="width:'+(100-longPct)+'%;background:#f72585"></div>'      +'</div>'      +'<div style="display:flex;justify-content:space-between;font-size:9px;font-family:DM Mono,monospace">'      +'<span style="color:#3a86ff">▲'+longPct+'%</span>'      +'<span style="color:'+netCol+';font-weight:600">'+netStr+'</span>'      +'<span style="color:#f72585">▼'+(100-longPct)+'%</span>'      +'</div></div>';
    var posHtml = filteredPos.length===0
      ? '<div style="font-size:9px;color:#3a3a55;font-family:DM Mono,monospace">— No positions</div>'
      : filteredPos.map(function(p){
          var sc=p.side==='LONG'?'#3a86ff':'#f72585';
          var ic=p.side==='LONG'?'▲':'▼';
          var uc=p.upnl>=0?'#00f5d4':'#f87171';
          var lev=p.lev>1?' '+p.lev+'x':'';
          var upnlStr='uPnL '+(p.upnl>=0?'+':'-')+'$'+Math.round(Math.abs(p.upnl)).toLocaleString();
          return '<div style="display:grid;grid-template-columns:80px 1fr 1fr;align-items:center;font-size:10px;font-family:DM Mono,monospace;gap:4px;min-width:0;overflow:hidden">'
            +'<span style="color:'+sc+';white-space:nowrap;overflow:hidden;text-overflow:ellipsis">'+ic+' '+p.coin+lev+'</span>'
            +'<span style="color:#888;font-size:9px;text-align:right;white-space:nowrap">$'+Math.round(p.notional).toLocaleString()+'</span>'
            +'<span style="color:'+uc+';font-size:9px;text-align:right;white-space:nowrap">'+upnlStr+'</span>'
            +'</div>';
        }).join('');

    // spot 보유 태그 추가
    var spotTagHtml = spotHoldings.map(function(h){
      return '<span class="coin-tag" style="color:#ffbe0b;border-color:#ffbe0b">📦'+h.coin+' '+h.amount+'</span>';
    }).join('');

    var coinTagHtml = '';




    var shortAddr = addr.slice(0,6)+'...'+addr.slice(-4);

    // 캐시된 지갑이면 WAR/타입/스탯을 캐시 값으로 교체 (더 정확함)
    // ── 캐시/라이브 분리 원칙 ─────────────────────────────────────
    // 캐시 있음: 성과/분류/WAR 전부 캐시 → 포지션/잔고만 실시간
    // 캐시 없음: JS 추정값 사용, 라벨로 명시
    var _cachedFull = ALL_STATS.find(function(x){ return x.address && x.address.toLowerCase()===addr.toLowerCase(); });
    var _isArchived = !!_cachedFull;
    if(_cachedFull){
      // 성과 지표 전부 캐시로 교체 (혼합 금지)
      warEst    = _cachedFull.war_score || warEst;
      traderType= _cachedFull.trader_type || traderType;
      traderChar= _cachedFull.character || traderChar;
      totalPnl  = _cachedFull.total_pnl  !== undefined ? _cachedFull.total_pnl  : totalPnl;
      winRate   = _cachedFull.win_rate    !== undefined ? _cachedFull.win_rate    : winRate;
      sharpeEst = _cachedFull.sharpe      !== undefined ? _cachedFull.sharpe      : sharpeEst;
      roi       = _cachedFull.roi_pct     !== undefined ? _cachedFull.roi_pct     : roi;
      bigBetHit = _cachedFull.big_bet_rate!== undefined ? _cachedFull.big_bet_rate: bigBetHit;
      // confidence + type_reasons도 캐시
      // (ai_summary는 아래 _box 블록에서 별도 처리)
    }
    var warColor = warEst>=80?'#00f5d4':warEst>=60?'#ffbe0b':'#f72585';
    var stroke   = Math.min(warEst/100*163.4,163.4).toFixed(1);
    var dash     = (163.4 - parseFloat(stroke)).toFixed(1);
    var pnlColor = totalPnl>=0?'#00f5d4':'#f72585';
    // WAR 라벨: 캐시면 정식 점수, 아니면 추정값임을 표시
    var warLabel   = _isArchived ? 'WAR' : '~WAR';
    var warLabelColor = _isArchived ? warColor : '#555';

    var cardHtml = '<div style="background:var(--surface);border:1px solid var(--border);border-radius:16px;padding:24px;position:relative">'
      // 헤더
      +'<div style="display:flex;align-items:flex-start;gap:12px;margin-bottom:16px;border-bottom:1px solid var(--border);padding-bottom:14px">'
        +'<div style="flex:1">'
          +'<div style="font-family:Bebas Neue,sans-serif;font-size:24px;color:var(--text)">'+shortAddr+'</div>'
          +'<div style="font-size:12px;font-weight:600;color:#00f5d4;margin-top:3px">'+traderType+'</div>'
          +'<div style="font-size:10px;color:var(--dim);font-style:italic">≈ '+traderChar+'</div>'
          +'<div style="font-family:DM Mono,monospace;font-size:11px;color:#888;margin-top:4px">$'+Math.round(equity).toLocaleString()+'</div>'
          +(firstDate?'<div style="font-family:DM Mono,monospace;font-size:9px;color:#3a3a55;margin-top:3px">📅 '+firstDate+' ~ '+lastDate+' | '+dataDays+'d</div>':'')
          +'<div style="font-size:10px;margin-top:2px">'+(_isArchived?'<span style="color:#00f5d4">✓ Archived</span> <span style="color:#3a3a55">· Performance from full history</span>':'<span style="color:#555">📊 Quick Analysis</span> <span style="color:#3a3a55">· Stats estimated from recent fills</span>')+'</div>'+(_isArchived&&_cachedFull&&_cachedFull.confidence?'<div style="margin-top:3px"><span style="font-size:9px;padding:1px 6px;border-radius:3px;border:0.5px solid '+(_cachedFull.confidence==='High Confidence'?'#00f5d4':_cachedFull.confidence==='Medium Confidence'?'#ffbe0b':'#4a4a6a')+';color:'+(_cachedFull.confidence==='High Confidence'?'#00f5d4':_cachedFull.confidence==='Medium Confidence'?'#ffbe0b':'#888')+'">'+_cachedFull.confidence+'</span></div>':'')
          +'<div style="font-size:10px;color:#555;margin-top:4px">'+positions.length+' positions · '+total+' trades</div>'
          +'<div style="display:flex;gap:6px;margin-top:6px">'
            +'<button onclick="window._copyAddr(this.dataset.a)" data-a="'+addr+'" style="font-size:10px;padding:2px 8px;border-radius:4px;border:1px solid #2a2a45;background:none;color:#888;cursor:pointer">📋 Copy</button>'
            +'<a href="https://hypurrscan.io/address/'+addr+'" target="_blank" style="font-size:10px;padding:2px 8px;border-radius:4px;border:1px solid #2a2a45;color:#888;text-decoration:none">HypurrScan ↗</a>'
          +'</div>'
        +'</div>'
        +'<div style="position:relative;width:60px;height:60px;flex-shrink:0">'
          +'<svg width="60" height="60"><circle cx="30" cy="30" r="26" fill="none" stroke="#1e1e35" stroke-width="4"/>'
          +'<circle cx="30" cy="30" r="26" fill="none" stroke="'+warColor+'" stroke-width="4" stroke-dasharray="'+stroke+' '+dash+'" stroke-linecap="round" transform="rotate(-90 30 30)"/></svg>'
          +'<div style="position:absolute;top:50%;left:50%;transform:translate(-50%,-60%);font-family:Bebas Neue,sans-serif;font-size:18px;color:'+warColor+'">'+warEst+'</div>'
          +'<div style="position:absolute;top:50%;left:50%;transform:translate(-50%,20%);font-size:8px;color:'+warLabelColor+'">'+warLabel+'</div>'
        +'</div>'
      +'</div>'
      // 스탯
      // 스탯 (수직 레이아웃)
      +'<div style="display:flex;justify-content:center;margin-bottom:12px">'
      +radarSvg
      +'</div>'
      +'<div style="display:grid;grid-template-columns:repeat(5,1fr);gap:6px;margin-bottom:12px">'
        +'<div style="background:#0a0a16;border-radius:8px;padding:8px 6px;text-align:center;min-width:0"><div style="font-family:DM Mono,monospace;font-size:13px;font-weight:500;color:'+pnlColor+';overflow:hidden;text-overflow:ellipsis;white-space:nowrap">'+fmtCompact(totalPnl)+'</div><div style="font-size:9px;color:var(--dim);margin-top:2px">Total PnL</div></div>'
        +'<div style="background:#0a0a16;border-radius:8px;padding:8px 6px;text-align:center;min-width:0"><div style="font-family:DM Mono,monospace;font-size:13px;font-weight:500;color:#c8d0e7">'+winRate+'%</div><div style="font-size:9px;color:var(--dim);margin-top:2px">Win Rate</div></div>'
        +'<div style="background:#0a0a16;border-radius:8px;padding:8px 6px;text-align:center;min-width:0"><div style="font-family:DM Mono,monospace;font-size:13px;font-weight:500;color:'+(sharpeEst>=2?'#00f5d4':sharpeEst>=0?'#ffbe0b':'#f72585')+'">'+sharpeEst+'</div><div style="font-size:9px;color:var(--dim);margin-top:2px">Sharpe*</div></div>'
        +'<div style="background:#0a0a16;border-radius:8px;padding:8px 6px;text-align:center;min-width:0"><div style="font-family:DM Mono,monospace;font-size:13px;font-weight:500;color:'+(roi>=0?'#c8d0e7':'#f72585')+'">'+roi+'%</div><div style="font-size:9px;color:var(--dim);margin-top:2px">ROI</div></div>'
        +'<div style="background:#0a0a16;border-radius:8px;padding:8px 6px;text-align:center;min-width:0"><div style="font-family:DM Mono,monospace;font-size:13px;font-weight:500;color:#c8d0e7">'+bigBetHit+'%</div><div style="font-size:9px;color:var(--dim);margin-top:2px">Big Bet Hit</div></div>'
      +'</div>'
      +'<div style="margin-bottom:4px">'
      +'<div style="font-size:10px;color:#3a3a55;text-transform:uppercase;letter-spacing:.06em;margin-bottom:4px">📈 Recent PnL</div>'
      +lookupSparkSvg
      +'</div>'
      +'<div style="font-size:10px;color:#3a3a55;text-transform:uppercase;letter-spacing:.06em;margin-top:8px;margin-bottom:4px">📍 Current Positions</div>'
      +barHtml
      +posHtml
      // 코인 태그
      +((topCoins.length>0||spotHoldings.length>0)?'<div style="display:flex;flex-wrap:wrap;gap:4px;margin-top:12px">'+coinTagHtml+spotTagHtml+'</div>':'')
      // WAR 주석
      +'<div style="font-size:9px;color:#333;margin-top:12px">* WAR is a quick estimate. For accurate stats run --refresh-all.</div>'
      // 아카이브 추가 버튼
      +(warEst>=50&&equity>=50000
        ?'<div style="margin-top:12px;font-size:10px;color:#555" id="reg-status-'+addr.slice(2,8)+'">⏳ Registering...</div>'
        :'<div style="margin-top:12px;font-size:10px;color:#444">WAR '+warEst+' — Not qualified (WAR 50+ · $50K+ required)</div>')
      // Share 버튼
      +'<div style="margin-top:16px;border-top:1px solid #1e1e35;padding-top:14px">'
      +'<button id="share-btn-'+addr.slice(2,8)+'" data-addr="'+addr+'" data-short="'+shortAddr+'" data-war="'+warEst+'" data-type="'+traderType+'" onclick="var b=this;shareCard(b.dataset.addr,b.dataset.short,b.dataset.war,b.dataset.type)" style="display:flex;align-items:center;gap:8px;background:#000;color:#fff;border:1px solid #333;border-radius:8px;padding:10px 18px;font-size:13px;font-weight:600;cursor:pointer;transition:background .2s">'
      +'<svg width="16" height="16" viewBox="0 0 24 24" fill="white"><path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-4.714-6.231-5.401 6.231H2.746l7.73-8.835L1.254 2.25H8.08l4.253 5.622zm-1.161 17.52h1.833L7.084 4.126H5.117z"/></svg>'
      +'Share on X'
      +'</button>'
      +'<div id="share-hint-'+addr.slice(2,8)+'" style="font-size:9px;color:#555;margin-top:6px;display:none">📥 Image saved — attach it to your tweet!</div>'
      +(function(){
        var wc = (_cachedFull && _cachedFull.war_components) ? _cachedFull.war_components : null;
        if(!wc || Object.keys(wc).length===0){
          var r = (_cachedFull && _cachedFull.radar) || {};
          wc = {
            'Profit':   parseFloat(((r.profit_amt||0)*0.25).toFixed(1)),
            'ROI':      parseFloat(((r.roi||0)*0.25).toFixed(1)),
            'Big Bet':  parseFloat(((r.big_bet||0)*0.15).toFixed(1)),
            'Sharpe':   parseFloat(((r.sharpe||0)*0.20).toFixed(1)),
            'Win Rate': parseFloat(((r.win_rate||0)*0.15).toFixed(1))
          };
        }
        var allZero = Object.values(wc).every(function(v){ return v===0; });
        if(allZero) return '';
        var keys = ['Profit','ROI','Sharpe','Win Rate','Big Bet'];
        var maxMap = {Profit:25, ROI:25, Sharpe:20, 'Win Rate':15, 'Big Bet':15};
        var warColorHdr = warEst>=80?'#00f5d4':warEst>=60?'#ffbe0b':'#f72585';
        var bars = keys.map(function(k){
          var score = wc[k]||0;
          var mx = maxMap[k];
          var pct = Math.min(100, score/mx*100).toFixed(0);
          var bc = score>=mx*0.7?'#00f5d4':score>=mx*0.4?'#ffbe0b':'#f72585';
          return '<div style="margin-bottom:6px">'
            +'<div style="display:flex;justify-content:space-between;font-size:9px;color:#888;font-family:DM Mono,monospace;margin-bottom:2px">'
            +'<span>'+k+'</span><span style="color:#c8d0e7">'+score.toFixed(1)+' / '+mx+'</span></div>'
            +'<div style="height:4px;background:#1e1e35;border-radius:2px;overflow:hidden">'
            +'<div style="height:100%;width:'+pct+'%;background:'+bc+';border-radius:2px"></div>'
            +'</div></div>';
        }).join('');
        return '<div style="margin-top:16px;border-top:1px solid #1e1e35;padding-top:14px">'
          +'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">'
          +'<div style="font-size:11px;font-weight:600;color:#c8d0e7">⚡ WAR Score Breakdown</div>'
          +'<div style="font-size:13px;font-weight:700;font-family:DM Mono,monospace;color:'+warColorHdr+'">'+warEst+'</div></div>'
          +bars+'</div>';
      })()
      +'<div style="margin-top:16px;border-top:1px solid #1e1e35;padding-top:14px">'
      +'<div style="font-size:11px;font-weight:600;color:#c8d0e7;margin-bottom:8px">🤖 AI Analysis</div>'
      +'<div id="ai-lookup-'+addr.slice(2,8)+'" style="font-size:12px;color:#888;line-height:1.7"><span style="color:#3a3a55">⏳ Analyzing...</span></div>'
      +'</div>'
      +'</div>'
      +'</div>';

    result.innerHTML=cardHtml;
    // Use Python ai_summary (cached wallets) or show archive message
    var _cached = ALL_STATS.find(function(x){ return x.address && x.address.toLowerCase()===addr.toLowerCase(); });
    var _box = document.getElementById('ai-lookup-'+addr.slice(2,8));
    if(_box){
      if(_cached && _cached.ai_summary){
        var _conf=_cached.confidence||'';
        var _cc=_conf==='High Confidence'?'#00f5d4':_conf==='Medium Confidence'?'#ffbe0b':'#888';
        var _reasons=(_cached.type_reasons||[]).map(function(r){return '<span style="font-size:10px;font-family:DM Mono,monospace;background:#111120;border:0.5px solid #2a2a45;border-radius:4px;padding:2px 7px;color:#00f5d4;margin-right:4px">\u2713 '+_esc(r)+'</span>';}).join('');
        _box.innerHTML=(_reasons?'<div style="margin-bottom:8px">'+_reasons+'</div>':'')
          +(_conf?'<div style="margin-bottom:6px"><span style="font-size:9px;color:'+_cc+';border:0.5px solid '+_cc+';border-radius:3px;padding:1px 6px;font-family:DM Mono,monospace">'+_esc(_conf)+'</span></div>':'')
          +'<span style="color:#c8d0e7">'+_esc(_cached.ai_summary)+'</span>';
      } else {
        _box.innerHTML='<span style="color:#3a3a55">This wallet is not yet in the archive. Run --discover to add it for full analysis.</span>';
      }
    }
    status.innerHTML='<span style="color:var(--green)">\u2713 Analysis complete</span>';

    // Auto-register if WAR 50+ · $50K+
    if(warEst >= 50 && equity >= 50000){
      autoRegister(addr, warEst);
    }

  } catch(e) {
    status.innerHTML='<span style="color:#f72585">Error: '+e.message+'</span>';
  }
}

async function autoRegister(addr, warEst){
  var statusEl = document.getElementById('reg-status-'+addr.slice(2,8));
  try {
    var r = await fetch('https://wallet-scout-api.kimsubbae113.workers.dev/api/wallet-request',{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({address:addr, war:String(warEst)})
    });
    var data = await r.json();
    if(data.duplicate){
      if(statusEl) statusEl.innerHTML='<span style="color:#555">✓ Already registered</span>';
    } else if(data.ok){
      if(statusEl) statusEl.innerHTML='<span style="color:var(--green)">✓ Registered</span>';
    } else {
      if(statusEl) statusEl.innerHTML='';
    }
  } catch(e){
    if(statusEl) statusEl.innerHTML='';
  }
}

window._addToArchive=async function(addr){
  var btn=document.getElementById('add-btn-'+addr.slice(2,8));
  var status=document.getElementById('lookup-status');
  if(btn){ btn.disabled=true; btn.textContent='⏳ Registering...'; }
  try {
    var r = await fetch('https://wallet-scout-api.kimsubbae113.workers.dev/api/wallet-request',{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({address:addr, war:'50+'})
    });
    var data = await r.json();
    if(data.duplicate){
      if(btn){btn.textContent='✓ Already registered';btn.style.color='#555';}
      if(status) status.innerHTML='<span style="color:#ffbe0b">Already registered.</span>';
    } else if(data.ok){
      if(btn){btn.textContent='✓ Registered';btn.style.background='#1a1a2e';btn.style.color='#555';}
      if(status) status.innerHTML='<span style="color:var(--green)">✓ Registered! Will be added on next update.</span>';
    } else {
      throw new Error(data.error||'Failed');
    }
  } catch(e){
    if(btn){btn.disabled=false;btn.textContent='➕ Add to Traders';}
    // 오류 처리
    if(a){
    } else {
      status.innerHTML='<span style="color:#f72585">Error: '+e.message+'</span>';
    }
  }
}
"""

    js_block += """
// ── WAR Ranking Trend Chart ─────────────────────────────────────────
window._warTrendChart = null;
window._warMode = 'war';
window._warVisible = {};
window._warRangeDays = 7;

window.setWarRange = function(btn) {
  window._warRangeDays = parseInt(btn.dataset.r);
  document.querySelectorAll('.war-range-btn').forEach(function(b){
    var active = b === btn;
    b.style.borderColor = active ? '#00f5d4' : '#2a2a45';
    b.style.color       = active ? '#00f5d4' : '#555';
    b.style.background  = active ? '#1e1e35' : 'transparent';
  });
  window.initWarTrendChart();
};

window.setWarMode = function(mode) {
  window._warMode = mode;
  ['war','rank','pnl','roi','follow'].forEach(function(m){
    var btn = document.getElementById('war-mode-'+m);
    if(!btn) return;
    var active = m === mode;
    btn.style.borderColor = active ? '#00f5d4' : '#2a2a45';
    btn.style.color       = active ? '#00f5d4' : '#555';
    btn.style.background  = active ? '#1e1e35' : 'transparent';
  });
  window.initWarTrendChart();
};

window.initWarTrendChart = function() {
  var ctx = document.getElementById('warTrendChart');
  if(!ctx || !WAR_HIST || WAR_HIST.length < 1) return;

  var now = Date.now();
  var _days = (window._warRangeDays !== undefined) ? window._warRangeDays : 7;
  var cutoff = _days > 0 ? now - _days * 24 * 60 * 60 * 1000 : 0;
  var recent = cutoff > 0 ? WAR_HIST.filter(function(snap){
    var t = new Date(snap.ts.replace(' ','T')).getTime();
    return !isNaN(t) && t >= cutoff;
  }) : WAR_HIST.slice();
  if(recent.length < 2) recent = WAR_HIST.slice();
  if(recent.length < 1) return;

  var mode = window._warMode || 'war';

  // 최신 스냅샷 기준 WAR rank 1~100 주소 수집
  var lastSnap = recent[recent.length - 1];
  var top100addrs = {};
  (lastSnap.top20 || []).forEach(function(t){
    if(t.rank <= 100) top100addrs[t.address.toLowerCase()] = t.label;
  });
  // ALL_STATS에서 WAR rank 순으로 정렬해서 1~100 추가
  if(ALL_STATS && ALL_STATS.length){
    var sorted = ALL_STATS.slice().sort(function(a,b){ return (b.war_score||0)-(a.war_score||0); });
    sorted.slice(0, 100).forEach(function(s){
      if(s.address) top100addrs[s.address.toLowerCase()] = s.label || s.address.slice(0,8)+'...'+s.address.slice(-4);
    });
  }

  var palette = ['#00f5d4','#3a86ff','#f72585','#ffbe0b','#9b5de5','#ff6b6b','#4ecdc4','#45b7d1','#96ceb4','#ffeaa7','#74b9ff','#a29bfe','#fd79a8','#00b894','#e17055'];
  var addresses = Object.keys(top100addrs);

  var datasets = addresses.map(function(addr, idx){
    var points = [];
    recent.forEach(function(snap){
      var snapT = new Date(snap.ts.replace(' ','T')).getTime();
      if(isNaN(snapT)) return;
      // snap.top20에서 해당 주소 찾기
      var found = (snap.top20||[]).find(function(t){ return t.address && t.address.toLowerCase() === addr; });
      // ALL_STATS에서 현재 데이터 (pnl/roi/follow는 여기서)
      var stat = ALL_STATS && ALL_STATS.find(function(s){ return s.address && s.address.toLowerCase() === addr; });
      var val = null;
      if(mode === 'war')         val = found ? found.war    : null;
      else if(mode === 'rank')   val = found ? found.rank   : null;
      // pnl/roi/follow는 스냅샷에 저장된 값 사용 (시간 변화 반영)
      else if(mode === 'pnl')    val = found && found.pnl != null ? Math.max(found.pnl, 1) : null;
      else if(mode === 'roi')    val = found && found.roi != null ? Math.max(found.roi, 0.1) : null;
      else if(mode === 'follow') val = found && found.follow != null ? found.follow : null;
      if(val === null || isNaN(val)) return;
      points.push({ x: snapT, y: val });
    });
    if(points.length === 0) return null;
    return {
      label:           top100addrs[addr],
      address:         addr,
      data:            points,
      borderColor:     palette[idx % palette.length],
      backgroundColor: palette[idx % palette.length] + '15',
      borderWidth: 1, pointRadius: 1, pointHoverRadius: 4,
      tension: 0.3, fill: false, spanGaps: false,
      hidden: window._warVisible[addr] === false,
    };
  }).filter(Boolean);

  var emptyEl = document.getElementById('warTrendEmpty');
  if(datasets.length === 0){
    if(emptyEl) emptyEl.style.display = 'block';
    if(window._warTrendChart){ window._warTrendChart.destroy(); window._warTrendChart = null; }
    return;
  }
  if(emptyEl) emptyEl.style.display = 'none';
  if(window._warTrendChart) window._warTrendChart.destroy();

  // y축 설정
  var yConfig = {};
  if(mode === 'war'){
    var vals=[]; datasets.forEach(function(d){ d.data.forEach(function(p){ if(p&&p.y!=null) vals.push(p.y); }); });
    yConfig = {
      reverse: false,
      min: vals.length ? Math.floor(Math.min.apply(null,vals)/5)*5-5 : 40,
      max: vals.length ? Math.ceil(Math.max.apply(null,vals)/5)*5+2  : 100,
      ticks: { color:'#3a3a55', font:{size:9}, callback: function(v){ return v; } },
      grid: { color:'#1a1a2e' }
    };
  } else if(mode === 'rank'){
    yConfig = {
      reverse: true, min: 1, max: 101,
      ticks: { color:'#3a3a55', font:{size:9}, callback: function(v){ return v<=100 ? '#'+Math.round(v) : ''; } },
      grid: { color:'#1a1a2e' }
    };
  } else if(mode === 'pnl'){
    yConfig = {
      type: 'logarithmic',
      min: 1000,
      ticks: {
        color:'#3a3a55', font:{size:9},
        callback: function(v){
          if(v===1000) return '$1K';
          if(v===5000) return '$5K';
          if(v===10000) return '$10K';
          if(v===50000) return '$50K';
          if(v===100000) return '$100K';
          if(v===500000) return '$500K';
          if(v===1000000) return '$1M';
          return null;
        }
      },
      grid: { color:'#1a1a2e' }
    };
  } else if(mode === 'roi'){
    yConfig = {
      type: 'logarithmic',
      min: 10,
      ticks: {
        color:'#3a3a55', font:{size:9},
        callback: function(v){
          if([10,50,100,500,1000].indexOf(v) >= 0) return v+'%';
          return null;
        }
      },
      grid: { color:'#1a1a2e' }
    };
  } else if(mode === 'follow'){
    var vals=[]; datasets.forEach(function(d){ d.data.forEach(function(p){ if(p&&p.y!=null) vals.push(p.y); }); });
    yConfig = {
      min: 0,
      max: vals.length ? Math.ceil(Math.max.apply(null,vals)/10)*10+5 : 100,
      ticks: { color:'#3a3a55', font:{size:9} },
      grid: { color:'#1a1a2e' }
    };
  }

  window._warTrendChart = new Chart(ctx, {
    type: 'line',
    data: { datasets: datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: 'nearest', intersect: false },
      onClick: function(evt, elements){
        if(!elements || !elements.length) return;
        var ds = window._warTrendChart.data.datasets[elements[0].datasetIndex];
        if(ds && ds.address && typeof openModal === 'function') openModal(ds.address);
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor:'#0d0d1a', borderColor:'#2a2a45', borderWidth:1,
          titleColor:'#c8d0e7', bodyColor:'#888',
          filter: function(item){ return window._hoveredWarIdx === undefined || item.datasetIndex === window._hoveredWarIdx; },
          callbacks: {
            title: function(items){ return items.length ? (items[0].dataset.label||'') : ''; },
            label: function(ctx){
              var v = ctx.parsed.y; if(v===null||isNaN(v)) return '';
              var d = new Date(ctx.parsed.x);
              var time = (d.getMonth()+1)+'/'+d.getDate()+' '+String(d.getHours()).padStart(2,'0')+':'+String(d.getMinutes()).padStart(2,'0');
              var score = mode==='rank'    ? 'Rank #'+Math.round(v)
                        : mode==='pnl'     ? 'PnL $'+Math.round(v).toLocaleString()
                        : mode==='roi'     ? 'ROI '+v.toFixed(1)+'%'
                        : mode==='follow'  ? 'Follow '+v.toFixed(1)
                        : 'WAR '+v.toFixed(1);
              return '  '+score+'  ('+time+')';
            },
            afterLabel: function(ctx){
              var addr = ctx.dataset.address; if(!addr) return '';
              var stat = ALL_STATS && ALL_STATS.find(function(s){ return s.address && s.address.toLowerCase()===addr; });
              var lines = [];
              if(stat && stat.trader_type) lines.push('  '+stat.trader_type);
              lines.push('  Click to open card');
              return lines.join('  |  ');
            }
          }
        }
      },
      scales: {
        x: {
          type: 'time',
          time: { unit:'hour', displayFormats:{hour:'M/d HH:mm', day:'M/d'}, tooltipFormat:'M/d HH:mm' },
          min: cutoff > 0 ? cutoff : undefined, max: now,
          ticks: { color:'#3a3a55', font:{size:9}, maxTicksLimit:7, maxRotation:0 },
          grid: { color:'#1a1a2e' }
        },
        y: yConfig
      }
    }
  });

  // 마우스 근접 강조
  function _distToSeg(px,py,x1,y1,x2,y2){ var dx=x2-x1,dy=y2-y1,l=dx*dx+dy*dy,t=l?Math.max(0,Math.min(1,((px-x1)*dx+(py-y1)*dy)/l)):0; return Math.sqrt(Math.pow(px-x1-t*dx,2)+Math.pow(py-y1-t*dy,2)); }
  function _highlight(mx,my){
    var chart=window._warTrendChart; if(!chart) return;
    var bestIdx=-1,bestDist=20;
    chart.data.datasets.forEach(function(ds,i){
      if(ds.hidden) return;
      var meta=chart.getDatasetMeta(i); if(!meta||!meta.data) return;
      for(var j=1;j<meta.data.length;j++){
        var p0=meta.data[j-1],p1=meta.data[j]; if(!p0||!p1) continue;
        var d=_distToSeg(mx,my,p0.x,p0.y,p1.x,p1.y); if(d<bestDist){bestDist=d;bestIdx=i;}
      }
      meta.data.forEach(function(pt){ if(!pt) return; var d=Math.sqrt(Math.pow(mx-pt.x,2)+Math.pow(my-pt.y,2)); if(d<bestDist){bestDist=d;bestIdx=i;} });
    });
    window._hoveredWarIdx = bestIdx>=0 ? bestIdx : undefined;
    chart.data.datasets.forEach(function(ds,i){
      if(bestIdx===-1){ds.borderWidth=1;ds.pointRadius=1;}
      else if(i===bestIdx){ds.borderWidth=3;ds.pointRadius=3;}
      else{ds.borderWidth=0.4;ds.pointRadius=0;}
    });
    chart.update('none');
  }
  ctx.addEventListener('mousemove',function(e){ var r=ctx.getBoundingClientRect(); _highlight(e.clientX-r.left,e.clientY-r.top); });
  ctx.addEventListener('mouseleave',function(){
    window._hoveredWarIdx=undefined;
    if(window._warTrendChart){ window._warTrendChart.data.datasets.forEach(function(ds){ds.borderWidth=1;ds.pointRadius=1;}); window._warTrendChart.update('none'); }
  });
};
"""

    js_block += """
function renderSignal(){
  var root=document.getElementById('signal-root');
  if(!root)return;
  var ED=window.EXPERT_DIR||{};
  var CC=window.COIN_CONSENSUS||[];
  var HM=window.HOT_MOVES||[];
  var SR=window.SIM_RETURNS||{};
  var ES=window.EASY_SIGNALS||[];

  // ── helpers ──────────────────────────────────────────────────────────
  function pill(dir){
    if(dir==='long')  return '<span style="display:inline-block;padding:2px 10px;border-radius:12px;background:#0a2a1a;color:#00f5d4;border:0.5px solid #00f5d4;font-size:10px;font-weight:700">Long</span>';
    if(dir==='short') return '<span style="display:inline-block;padding:2px 10px;border-radius:12px;background:#2a0a1a;color:#f72585;border:0.5px solid #f72585;font-size:10px;font-weight:700">Short</span>';
    return '<span style="display:inline-block;padding:2px 10px;border-radius:12px;background:#1a1a2a;color:#888;border:0.5px solid #333;font-size:10px;font-weight:700">Neutral</span>';
  }
  function diffBadge(d){
    if(d==='easy') return '<span style="padding:2px 8px;border-radius:8px;background:#0a2a1a;color:#00f5d4;border:0.5px solid #00f5d4;font-size:9px;font-weight:700">Easy</span>';
    if(d==='hard') return '<span style="padding:2px 8px;border-radius:8px;background:#2a0a1a;color:#f72585;border:0.5px solid #f72585;font-size:9px;font-weight:700">Hard</span>';
    return '<span style="padding:2px 8px;border-radius:8px;background:#1a1a0a;color:#ffbe0b;border:0.5px solid #ffbe0b;font-size:9px;font-weight:700">Medium</span>';
  }
  // ── helpers ──────────────────────────────────────────────────────────
  function interpText(d){
    var lp=d.long_pct||50, sp=d.short_pct||50, ch=d.change_24h||0;
    if(sp>55&&ch>10) return 'Top smart money is turning defensive. Beginners should observe rather than chase longs.';
    if(lp>60&&ch>5)  return "Smart money is leaning long again. Check if it's BTC/ETH or altcoin-led before following.";
    if(Math.abs(lp-sp)<10) return 'Mixed signals across smart money. No clear consensus right now — sit tight.';
    if(sp>55) return 'Short bias dominates. Smart money is cautious about upside from here.';
    return 'Smart money leans long with moderate conviction. Follow size matters more than direction.';
  }

  // ── Section 1: Expert Direction Hero ──────────────────────────────
  function buildHero(d, title) {
    if (!d) return '';
    var lp = d.long_pct  || 50;
    var sp = d.short_pct || 50;
    var tc = d.trader_count || 0;
    var ch = d.change_24h || 0;
    var chSign = ch >= 0 ? '+' : '';
    var interp = interpText(d);

    function scaleVal(pct, minV, maxV) {
      var t = Math.min(Math.max((pct - 50) / 20, 0), 1);
      return Math.round(minV + t * (maxV - minV));
    }

    var longFontSize  = scaleVal(lp, 36, 58);
    var shortFontSize = scaleVal(sp, 36, 58);
    var longTriBase   = scaleVal(lp, 22, 40);
    var longTriH      = scaleVal(lp, 28, 50);
    var shortTriBase  = scaleVal(sp, 22, 40);
    var shortTriH     = scaleVal(sp, 28, 50);
    var longLabelSize  = scaleVal(lp, 13, 18);
    var shortLabelSize = scaleVal(sp, 13, 18);

    var longPanel =
      '<div style="flex:' + lp.toFixed(0) + ';background:#dbeafe;display:flex;flex-direction:column;align-items:center;justify-content:center;padding:32px 16px;gap:10px;min-width:0">'
      + '<div style="width:0;height:0;border-left:' + longTriBase + 'px solid transparent;border-right:' + longTriBase + 'px solid transparent;border-bottom:' + longTriH + 'px solid #1e3a8a"></div>'
      + '<div style="font-size:' + longLabelSize + 'px;font-weight:700;color:#1e3a8a;letter-spacing:2px">LONG</div>'
      + '<div style="font-size:' + longFontSize + 'px;font-weight:700;color:#1e3a8a;line-height:1">' + lp.toFixed(0) + '%</div>'
      + '</div>';

    var shortPanel =
      '<div style="flex:' + sp.toFixed(0) + ';background:#fee2e2;display:flex;flex-direction:column;align-items:center;justify-content:center;padding:32px 16px;gap:10px;min-width:0">'
      + '<div style="width:0;height:0;border-left:' + shortTriBase + 'px solid transparent;border-right:' + shortTriBase + 'px solid transparent;border-top:' + shortTriH + 'px solid #7f1d1d"></div>'
      + '<div style="font-size:' + shortLabelSize + 'px;font-weight:700;color:#7f1d1d;letter-spacing:2px">SHORT</div>'
      + '<div style="font-size:' + shortFontSize + 'px;font-weight:700;color:#7f1d1d;line-height:1">' + sp.toFixed(0) + '%</div>'
      + '</div>';

    var panels = lp >= sp ? longPanel + shortPanel : shortPanel + longPanel;

    return '<div style="margin-bottom:8px">'
      + '<div style="font-size:11px;color:#4a4a6a;font-family:DM Mono,monospace;margin-bottom:8px">' + title + ' \xb7 ' + tc + ' experts \xb7 by portfolio size</div>'
      + '<div style="display:flex;border-radius:12px;overflow:hidden;border:1px solid #2a2a3a">'
      + panels
      + '</div>'
      + '</div>';
  }

  var h = '';

  // ── Section 1 ──────────────────────────────────────────────────────
  h += '<div style="margin-bottom:32px">'
    + '<div style="font-family:Bebas Neue,sans-serif;font-size:26px;letter-spacing:2px;color:#c8d0e7;margin-bottom:4px">Smart Money Direction</div>'
    + '<div style="font-size:10px;color:#3a3a55;font-family:DM Mono,monospace;margin-bottom:14px">Are top traders going long or short right now?</div>'
    + buildHero(ED.all, 'Smart Money');

  h += '</div>';

  // ── Section 2: Coin Consensus ──────────────────────────────────────
  h+='<div style="margin-bottom:32px">'
    +'<div style="font-family:Bebas Neue,sans-serif;font-size:26px;letter-spacing:2px;color:#c8d0e7;margin-bottom:4px">Top Coins by Smart Money Bet</div>'
    +'<div style="font-size:10px;color:#3a3a55;font-family:DM Mono,monospace;margin-bottom:14px">Which coins is smart money betting on right now?</div>';
  if(CC.length){
    function ccScale(pct, minV, maxV) {
      var t = Math.min(Math.max((pct - 50) / 30, 0), 1);
      return Math.round(minV + t * (maxV - minV));
    }
    h+='<div style="display:flex;flex-direction:column;gap:10px">';
    CC.forEach(function(c,i){
      var lp=c.long_pct, sp=c.short_pct;
      var lFont=ccScale(lp,16,36); var sFont=ccScale(sp,16,36);
      var lTriB=ccScale(lp,7,18);  var lTriH=ccScale(lp,9,22);
      var sTriB=ccScale(sp,7,18);  var sTriH=ccScale(sp,9,22);
      var lLbl=ccScale(lp,9,13);   var sLbl=ccScale(sp,9,13);
      var longPanel='<div style="flex:'+lp.toFixed(0)+';background:#dbeafe;display:flex;flex-direction:column;align-items:center;justify-content:center;padding:18px 12px;gap:5px;min-width:0">'
        +'<div style="width:0;height:0;border-left:'+lTriB+'px solid transparent;border-right:'+lTriB+'px solid transparent;border-bottom:'+lTriH+'px solid #1e3a8a"></div>'
        +'<div style="font-size:'+lLbl+'px;font-weight:700;color:#1e3a8a;letter-spacing:2px">LONG</div>'
        +'<div style="font-size:'+lFont+'px;font-weight:700;color:#1e3a8a;line-height:1">'+lp.toFixed(0)+'%</div>'
        +'</div>';
      var shortPanel='<div style="flex:'+sp.toFixed(0)+';background:#fee2e2;display:flex;flex-direction:column;align-items:center;justify-content:center;padding:18px 12px;gap:5px;min-width:0">'
        +'<div style="width:0;height:0;border-left:'+sTriB+'px solid transparent;border-right:'+sTriB+'px solid transparent;border-top:'+sTriH+'px solid #7f1d1d"></div>'
        +'<div style="font-size:'+sLbl+'px;font-weight:700;color:#7f1d1d;letter-spacing:2px">SHORT</div>'
        +'<div style="font-size:'+sFont+'px;font-weight:700;color:#7f1d1d;line-height:1">'+sp.toFixed(0)+'%</div>'
        +'</div>';
      h+='<div style="margin-bottom:4px">'
        +'<div style="display:flex;align-items:baseline;gap:8px;margin-bottom:6px">'
          +'<span style="font-family:Bebas Neue,sans-serif;font-size:22px;letter-spacing:2px;color:#c8d0e7">'+(i+1)+'. '+c.coin+'</span>'
          +'<span style="font-size:9px;color:#4a4a6a;font-family:DM Mono,monospace">avg '+Math.round(c.avg_lev*100)+'% of balance per position</span>'
        +'</div>'
        +'<div style="display:flex;border-radius:10px;overflow:hidden;border:1px solid #2a2a3a">'+longPanel+shortPanel+'</div>'
        +'</div>';
    });
    h+='</div>';
  } else {
    h+='<div style="padding:24px;text-align:center;color:#3a3a55;font-size:12px">No active positions data available.</div>';
  }
  h+='</div>';

  // ── Section 3: Hot Moves ───────────────────────────────────────────
  h+='<div style="margin-bottom:32px">'
    +'<div style="font-family:Bebas Neue,sans-serif;font-size:26px;letter-spacing:2px;color:#c8d0e7;margin-bottom:4px">Recent Smart Money Moves</div>'
    +'<div style="font-size:10px;color:#3a3a55;font-family:DM Mono,monospace;margin-bottom:14px">Positions over $100,000 with significant changes</div>';
  if(HM.length){
    h+='<div style="display:flex;flex-direction:column;gap:8px">';
    HM.forEach(function(m){
      var upnlColor=m.upnl>=0?'#00f5d4':'#f87171';
      var upnlSign=m.upnl>=0?'+':'';
      var levX=m.equity>0?(m.notional/m.equity):0;
      var levLabel=levX>=1?'x'+(Math.round(levX*10)/10).toFixed(1):levX>0?'x'+(Math.round(levX*100)/100).toFixed(2):'';
      var isLong=m.action.toLowerCase().indexOf('long')>=0;
      var actionColor=isLong?'#3a86ff':'#f72585';
      var changeBadge=m.change==='new'?'<span style="font-size:9px;background:#1a1a00;color:#ffbe0b;border:0.5px solid #ffbe0b;padding:2px 6px;border-radius:4px;font-family:DM Mono,monospace;font-weight:700">NEW</span>'
        :m.change==='+'?'<span style="font-size:9px;background:#001a10;color:#00f5d4;border:0.5px solid #00f5d4;padding:2px 6px;border-radius:4px;font-family:DM Mono,monospace;font-weight:700">▲ Added</span>'
        :'<span style="font-size:9px;background:#1a0010;color:#f72585;border:0.5px solid #f72585;padding:2px 6px;border-radius:4px;font-family:DM Mono,monospace;font-weight:700">▼ Reduced</span>';
      h+='<div data-addr="'+m.addr+'" onclick="openModal(this.dataset.addr)" style="background:#0e0e1a;border:0.5px solid #1e1e35;border-radius:10px;padding:14px 16px;cursor:pointer">'
        +'<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">'
          +'<div style="display:flex;align-items:center;gap:8px">'
            +'<span style="background:#1a2a3a;color:#3a86ff;font-size:9px;font-weight:700;padding:3px 7px;border-radius:5px;white-space:nowrap;font-family:DM Mono,monospace">WAR '+m.war.toFixed(0)+'</span>'
            +'<span style="font-weight:700;font-size:13px;color:#c8d0e7">'+m.name+'</span>'
          +'</div>'
          +'<span style="font-size:9px;color:#4a4a6a;font-family:DM Mono,monospace">'+m.time_ago+'</span>'
        +'</div>'
        +'<div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px">'
          +'<div style="display:flex;flex-direction:column;gap:6px">'
            +'<div style="display:flex;align-items:center;gap:8px">'
              +'<span style="font-size:14px;font-weight:700;color:'+actionColor+';font-family:DM Mono,monospace">'+m.action+'</span>'
              +changeBadge
            +'</div>'
            +'<div style="display:flex;gap:10px;align-items:baseline">'
              +'<span style="font-size:11px;color:#c8d0e7;font-family:DM Mono,monospace">$'+m.notional.toLocaleString()+'</span>'
              +(levLabel?'<span style="font-size:16px;font-weight:700;color:#ffbe0b;font-family:Bebas Neue,sans-serif;letter-spacing:1px">'+levLabel+'</span><span style="font-size:9px;color:#4a4a6a;font-family:DM Mono,monospace">leverage</span>':'')
            +'</div>'
          +'</div>'
          +'<div style="text-align:right">'
            +'<div style="font-size:9px;color:#4a4a6a;font-family:DM Mono,monospace;margin-bottom:2px">Unrealized PnL</div>'
            +'<div style="font-size:14px;font-weight:700;color:'+upnlColor+';font-family:DM Mono,monospace">'+upnlSign+'$'+Math.abs(m.upnl).toLocaleString()+'</div>'
          +'</div>'
        +'</div>'
      +'</div>';
    });
    h+='</div>';
  } else {
    h+='<div style="padding:24px;text-align:center;color:#3a3a55;font-size:12px">No significant moves detected in the last 24 hours.</div>';
  }
  h+='</div>';

  // ── Section 4: Simulator ───────────────────────────────────────────
  var _simGroup='war80',_simPeriod='7d';
  h+='<div style="margin-bottom:32px">'
    +'<div style="font-family:Bebas Neue,sans-serif;font-size:26px;letter-spacing:2px;color:#c8d0e7;margin-bottom:4px">If You Had Followed...</div>'
    +'<div style="font-size:10px;color:#3a3a55;font-family:DM Mono,monospace;margin-bottom:14px">Hypothetical returns based on smart money directional bias + BTC price movement</div>'
    +'<div style="background:#0e0e1a;border:0.5px solid #1e1e35;border-radius:10px;padding:20px">'
      +'<div style="display:flex;flex-wrap:wrap;gap:12px;margin-bottom:16px;align-items:center">'
        +'<div>'
          +'<div style="font-size:9px;color:#4a4a6a;margin-bottom:6px;font-family:DM Mono,monospace">Capital ($)</div>'
          +'<input id="sim-capital" type="number" value="1000" min="100" oninput="updateSimulator()" style="width:110px;padding:6px 10px;border-radius:6px;border:0.5px solid #2a2a45;background:#111120;color:#c8d0e7;font-family:DM Mono,monospace;font-size:12px">'
        +'</div>'
        +'<div>'
          +'<div style="font-size:9px;color:#4a4a6a;margin-bottom:6px;font-family:DM Mono,monospace">Period</div>'
          +'<div style="display:flex;gap:4px">'
            +['1d','7d','30d','90d'].map(p=>'<button data-p="'+p+'" onclick="setSimPeriod(this.dataset.p)" id="sper-'+p+'" style="padding:5px 10px;border-radius:6px;border:0.5px solid #2a2a45;background:'+(p==='7d'?'#1a2a3a':'transparent')+';color:'+(p==='7d'?'#3a86ff':'#555')+';font-size:10px;cursor:pointer;font-family:DM Mono,monospace">'+p+'</button>').join('')
          +'</div>'
        +'</div>'
        +'<div>'
          +'<div style="font-size:9px;color:#4a4a6a;margin-bottom:6px;font-family:DM Mono,monospace">Group</div>'
          +'<div style="display:flex;gap:4px">'
            +'<button data-g="war80" onclick="setSimGroup(this.dataset.g)" id="sgr-war80" style="padding:5px 10px;border-radius:6px;border:0.5px solid #3a86ff;background:#1a2a3a;color:#3a86ff;font-size:10px;cursor:pointer;font-family:DM Mono,monospace">80+</button>'
            +'<button data-g="war70" onclick="setSimGroup(this.dataset.g)" id="sgr-war70" style="padding:5px 10px;border-radius:6px;border:0.5px solid #2a2a45;background:transparent;color:#555;font-size:10px;cursor:pointer;font-family:DM Mono,monospace">70~80</button>'
            +'<button data-g="war60" onclick="setSimGroup(this.dataset.g)" id="sgr-war60" style="padding:5px 10px;border-radius:6px;border:0.5px solid #2a2a45;background:transparent;color:#555;font-size:10px;cursor:pointer;font-family:DM Mono,monospace">60~70</button>'
            +'<button data-g="war50" onclick="setSimGroup(this.dataset.g)" id="sgr-war50" style="padding:5px 10px;border-radius:6px;border:0.5px solid #2a2a45;background:transparent;color:#555;font-size:10px;cursor:pointer;font-family:DM Mono,monospace">50~60</button>'
            +'<button data-g="follow" onclick="setSimGroup(this.dataset.g)" id="sgr-follow" style="padding:5px 10px;border-radius:6px;border:0.5px solid #2a2a45;background:transparent;color:#555;font-size:10px;cursor:pointer;font-family:DM Mono,monospace">Followable</button>'
          +'</div>'
        +'</div>'
      +'</div>'
      +'<div id="sim-result" style="display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:10px;margin-bottom:14px"></div>'
      +'<div style="font-size:9px;color:#3a3a55;font-family:DM Mono,monospace;line-height:1.5">Estimated only. Does not account for real fills, fees, or slippage. Past performance does not guarantee future results.</div>'
    +'</div>'
  +'</div>';
  window._simCurrentGroup='war80';window._simCurrentPeriod='7d';

  // ── Section 5: Easy Signals ────────────────────────────────────────
  h+='<div style="margin-bottom:32px">'
    +'<div style="font-family:Bebas Neue,sans-serif;font-size:26px;letter-spacing:2px;color:#c8d0e7;margin-bottom:4px">Easy Signals Today</div>'
    +'<div style="font-size:10px;color:#3a3a55;font-family:DM Mono,monospace;margin-bottom:14px">Expert signals ranked by beginner-friendliness</div>'
    +'<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));gap:12px">';
  if(ES.length){
    ES.forEach(function(e){
      var borderColor=e.difficulty==='easy'?'#00f5d4':e.difficulty==='hard'?'#f72585':'#1e1e35';
      var riskColor=e.risk_level==='low'?'#00f5d4':e.risk_level==='high'?'#f72585':'#ffbe0b';
      h+='<div style="background:#0e0e1a;border:0.5px solid '+borderColor+';border-radius:10px;padding:16px">'
        +'<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px">'
          +'<span style="font-size:18px;font-weight:700;color:#c8d0e7;font-family:Bebas Neue,sans-serif;letter-spacing:1px">'+e.coin+'</span>'
          +pill(e.direction)
        +'</div>'
        +'<div style="margin-bottom:10px">'+diffBadge(e.difficulty)+'</div>'
        +'<div style="display:flex;flex-direction:column;gap:5px;margin-bottom:10px">'
          +'<div style="display:flex;justify-content:space-between;font-size:10px;font-family:DM Mono,monospace">'
            +'<span style="color:#4a4a6a">Consensus</span>'
            +'<span style="color:#c8d0e7">'+e.consensus_pct.toFixed(0)+'%</span>'
          +'</div>'
          +'<div style="display:flex;justify-content:space-between;font-size:10px;font-family:DM Mono,monospace">'
            +'<span style="color:#4a4a6a">Avg Position Size</span>'
            +'<span style="color:#c8d0e7">'+(e.avg_leverage>=1?'x'+(Math.round(e.avg_leverage*10)/10).toFixed(1):'x'+(Math.round(e.avg_leverage*100)/100).toFixed(2))+'</span>'
          +'</div>'
          +'<div style="display:flex;justify-content:space-between;font-size:10px;font-family:DM Mono,monospace">'
            +'<span style="color:#4a4a6a">Risk</span>'
            +'<span style="color:'+riskColor+'">'+e.risk_level.charAt(0).toUpperCase()+e.risk_level.slice(1)+'</span>'
          +'</div>'
        +'</div>'
        +'<div style="font-size:10px;color:#888;line-height:1.5;border-top:0.5px solid #1e1e35;padding-top:8px">'+e.note+'</div>'
      +'</div>';
    });
  } else {
    h+='<div style="padding:24px;text-align:center;color:#3a3a55;font-size:12px;grid-column:1/-1">Not enough data to generate easy signals.</div>';
  }
  h+='</div></div>';

  root.innerHTML=h;
  updateSimulator();
}

window._simCurrentGroup='war80';window._simCurrentPeriod='7d';
function setSimPeriod(p){
  window._simCurrentPeriod=p;
  ['1d','7d','30d','90d'].forEach(function(x){
    var btn=document.getElementById('sper-'+x);
    if(!btn)return;
    btn.style.background=x===p?'#1a2a3a':'transparent';
    btn.style.color=x===p?'#3a86ff':'#555';
    btn.style.borderColor=x===p?'#3a86ff':'#2a2a45';
  });
  updateSimulator();
}
function setSimGroup(g){
  window._simCurrentGroup=g;
  ['war80','war70','war60','war50','follow'].forEach(function(x){
    var btn=document.getElementById('sgr-'+x);
    if(!btn)return;
    btn.style.background=x===g?'#1a2a3a':'transparent';
    btn.style.color=x===g?'#3a86ff':'#555';
    btn.style.borderColor=x===g?'#3a86ff':'#2a2a45';
  });
  updateSimulator();
}
function updateSimulator(){
  var capInput=document.getElementById('sim-capital');
  var resDiv=document.getElementById('sim-result');
  if(!capInput||!resDiv)return;
  var cap=parseFloat(capInput.value)||1000;
  var SR=window.SIM_RETURNS||{};
  var group=window._simCurrentGroup||'war80';
  var period=window._simCurrentPeriod||'7d';
  var gd=SR[group]||{};
  var pct=gd[period]||0;
  var profit=cap*pct/100;
  var profitColor=profit>=0?'#00f5d4':'#f72585';
  var profitSign=profit>=0?'+':'';
  var groupLabel=group==='war80'?'WAR 80+':group==='war70'?'WAR 70~80':group==='war60'?'WAR 60~70':group==='war50'?'WAR 50~60':'Followable (FS 80+)';
  resDiv.innerHTML=
    '<div style="background:#111120;border:0.5px solid #1e1e35;border-radius:8px;padding:14px;text-align:center">'
      +'<div style="font-size:9px;color:#4a4a6a;font-family:DM Mono,monospace;margin-bottom:4px">Starting Capital</div>'
      +'<div style="font-size:20px;font-weight:700;color:#c8d0e7;font-family:DM Mono,monospace">$'+cap.toLocaleString()+'</div>'
    +'</div>'
    +'<div style="background:#111120;border:0.5px solid '+profitColor+'44;border-radius:8px;padding:14px;text-align:center">'
      +'<div style="font-size:9px;color:#4a4a6a;font-family:DM Mono,monospace;margin-bottom:4px">Est. Profit ('+period+')</div>'
      +'<div style="font-size:20px;font-weight:700;color:'+profitColor+';font-family:DM Mono,monospace">'+profitSign+'$'+Math.abs(profit).toFixed(2)+'</div>'
      +'<div style="font-size:9px;color:#4a4a6a;font-family:DM Mono,monospace;margin-top:2px">'+profitSign+pct.toFixed(2)+'% · '+groupLabel+'</div>'
    +'</div>'
    +'<div style="background:#111120;border:0.5px solid #1e1e35;border-radius:8px;padding:14px;text-align:center">'
      +'<div style="font-size:9px;color:#4a4a6a;font-family:DM Mono,monospace;margin-bottom:4px">Data Period</div>'
      +'<div style="font-size:20px;font-weight:700;color:#c8d0e7;font-family:DM Mono,monospace">'+period+'</div>'
      +'<div style="font-size:9px;color:#4a4a6a;font-family:DM Mono,monospace;margin-top:2px">BTC-proxy estimate</div>'
    +'</div>';
}
"""

    js_block += """function renderSentiment(){
  const root=document.getElementById('sent-root');
  const BL='#3a86ff', RD='#f72585';
  if(!root) return;
  if(!SENT) SENT = {all:null, coins:[], bands:[], types:[], equities:[]};

  // ── 전체 게이지 바 ─────────────────────────────────────────────
  const a=SENT.all;
  let h='';
  if(a){
    const _maxRaw=Math.max(a.long_pct,a.short_pct,1);
    const _maxVal=Math.ceil(_maxRaw/100)*100;
    const lW=Math.min(a.long_pct/_maxVal*100,100).toFixed(2);
    const sW=Math.min(a.short_pct/_maxVal*100,100).toFixed(2);
    h+=`<div id="bcard-all" onclick="switchBubbles('all')" style="cursor:pointer;margin-bottom:20px;padding:12px;border-radius:8px;border:1px solid ${BL};background:#1a1a2e">
      <div style="font-size:11px;color:#888;margin-bottom:10px">Smart Money ${a.traders} · Portfolio-weighted exposure (All)</div>
      <div style="display:flex;flex-direction:column;gap:8px;margin-bottom:6px">
        <div style="display:flex;align-items:center;gap:10px">
          <span style="font-size:12px;color:${BL};font-weight:600;width:44px;flex-shrink:0;display:flex;align-items:center;gap:2px;line-height:1"><span style="font-size:10px">▲</span><span>Long</span></span>
          <div style="flex:1;position:relative;height:20px;background:#1e1e35;border-radius:4px">
            <div style="width:${lW}%;height:100%;background:${BL};border-radius:4px 0 0 4px;position:relative">
              <span style="position:absolute;right:6px;top:50%;transform:translateY(-50%);font-size:11px;font-weight:600;color:#c8e8ff;white-space:nowrap">${a.long_pct}%</span>
            </div>
            ${Array.from({length:Math.max(0,_maxVal/100-1)},(_,i)=>(i+1)*100).map(v=>'<div style="position:absolute;top:0;left:'+(v/_maxVal*100).toFixed(1)+'%;width:1px;height:100%;background:#3a3a55"></div>').join("")}
          </div>
        </div>
        <div style="display:flex;align-items:center;gap:10px">
          <span style="font-size:12px;color:${RD};font-weight:600;width:44px;flex-shrink:0;display:flex;align-items:center;gap:2px;line-height:1"><span style="font-size:10px">▼</span><span>Short</span></span>
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

  // ── 3Section 구조: WAR / Trader Type / Portfolio 규모 ─────────────────
  // 각 Section 클릭 시 해당 Section 아래에 Bubble맵 인라인 표시
  h+='<div id="sent-sections">';

  // ─ WAR Section ─
  h+='<div class="sent-section" id="ssec-war">';
  h+='<div class="sent-sec-header" onclick="toggleSection(&quot;war&quot;)" style="cursor:pointer;display:flex;align-items:center;justify-content:space-between;padding:10px 14px;background:#111120;border-radius:8px;margin-bottom:8px;user-select:none">';
  h+='<span style="font-size:12px;font-weight:600;color:#c8d0e7">📊 By WAR Band</span>';
  h+='<span id="sarrow-war" style="font-size:10px;color:#4a4a6a">▼</span>';
  h+='</div>';
  h+='<div id="sbody-war" class="sent-sec-body">';
  h+='<div class="sent-war-grid" style="display:grid;gap:8px;margin-bottom:8px">';
  SENT.bands.forEach(function(b){
    var r=b.result;
    var cntLabel=b.count+''+(r&&r.traders!==b.count?' / Positions '+r.traders+'':'');
    var inner='';
    if(r){
      var _bmaxRaw=Math.max(r.long_pct,r.short_pct,1);
      var _bmaxVal=Math.ceil(_bmaxRaw/100)*100;
      var bLW=Math.min(r.long_pct/_bmaxVal*100,100).toFixed(2);
      var bSW=Math.min(r.short_pct/_bmaxVal*100,100).toFixed(2);
      var dividers='';
      for(var di=1;di<_bmaxVal/100;di++) dividers+='<div style="position:absolute;top:0;left:'+(di*100/_bmaxVal*100).toFixed(1)+'%;width:1px;height:100%;background:#3a3a55"></div>';
      inner='<div style="display:flex;flex-direction:column;gap:3px">'
        +'<div style="display:flex;align-items:center;gap:4px"><span style="font-size:9px;color:'+BL+';width:20px;flex-shrink:0">▲L</span>'
        +'<div style="flex:1;position:relative;height:7px;background:#2a2a45;border-radius:3px;overflow:hidden"><div style="width:'+bLW+'%;height:100%;background:'+BL+'"></div>'+dividers+'</div>'
        +'<span style="font-size:9px;color:'+BL+';width:36px;text-align:right">'+r.long_pct+'%</span></div>'
        +'<div style="display:flex;align-items:center;gap:4px"><span style="font-size:9px;color:'+RD+';width:20px;flex-shrink:0">▼S</span>'
        +'<div style="flex:1;position:relative;height:7px;background:#2a2a45;border-radius:3px;overflow:hidden"><div style="width:'+bSW+'%;height:100%;background:'+RD+'"></div>'+dividers+'</div>'
        +'<span style="font-size:9px;color:'+RD+';width:36px;text-align:right">'+r.short_pct+'%</span></div>'
        +'</div>';
    } else { inner='<div style="font-size:10px;color:#333">Data N/A</div>'; }
    h+='<div id="bcard-'+b.label+'" data-bkey="'+b.label+'" onclick="switchBubbles(this.dataset.bkey)" style="background:#1a1a2e;border-radius:8px;padding:10px;cursor:pointer;border:0.5px solid #2a2a45">'
      +'<div style="font-size:10px;color:#888;margin-bottom:4px">WAR '+b.label+' <span style="color:#555">('+cntLabel+')</span></div>'+inner+'</div>';
  });
  h+='</div>';
  h+='<div id="bubble-war" class="inline-bubble" style="display:none"></div>';
  h+='</div></div>';

  // ─ Trader Type Section ─
  h+='<div class="sent-section" id="ssec-type" style="margin-top:12px">';
  h+='<div class="sent-sec-header" onclick="toggleSection(&quot;type&quot;)" style="cursor:pointer;display:flex;align-items:center;justify-content:space-between;padding:10px 14px;background:#111120;border-radius:8px;margin-bottom:8px;user-select:none">';
  h+='<span style="font-size:12px;font-weight:600;color:#c8d0e7">🎭 By Trader Type</span>';
  h+='<span id="sarrow-type" style="font-size:10px;color:#4a4a6a">▶</span>';
  h+='</div>';
  h+='<div id="sbody-type" class="sent-sec-body" style="display:none">';
  if(SENT.types&&SENT.types.length){
    h+='<div class="sent-type-grid" style="display:grid;gap:6px;margin-bottom:8px">';
    SENT.types.forEach(function(t){
      if(t.count===0) return;
      var r=t.result, key=t.label;
      var cntLabel=r?(t.count+' / Positions '+r.traders+' · WAR '+t.avg_war):(t.count+' · WAR '+t.avg_war);
      var inner='';
      if(r){
        var _tmaxRaw=Math.max(r.long_pct,r.short_pct,1);
        var _tmaxVal=Math.ceil(_tmaxRaw/100)*100;
        var bLW=Math.min(r.long_pct/_tmaxVal*100,100).toFixed(2);
        var bSW=Math.min(r.short_pct/_tmaxVal*100,100).toFixed(2);
        var dividers='';
        for(var di=1;di<_tmaxVal/100;di++) dividers+='<div style="position:absolute;top:0;left:'+(di*100/_tmaxVal*100).toFixed(1)+'%;width:1px;height:100%;background:#3a3a55"></div>';
        inner='<div style="display:flex;flex-direction:column;gap:3px">'
          +'<div style="display:flex;align-items:center;gap:4px"><span style="font-size:8px;color:'+BL+';width:16px;flex-shrink:0">▲L</span>'
          +'<div style="flex:1;position:relative;height:5px;background:#2a2a45;border-radius:3px;overflow:hidden"><div style="width:'+bLW+'%;height:100%;background:'+BL+'"></div>'+dividers+'</div>'
          +'<span style="font-size:8px;color:'+BL+';width:32px;text-align:right">'+r.long_pct+'%</span></div>'
          +'<div style="display:flex;align-items:center;gap:4px"><span style="font-size:8px;color:'+RD+';width:16px;flex-shrink:0">▼S</span>'
          +'<div style="flex:1;position:relative;height:5px;background:#2a2a45;border-radius:3px;overflow:hidden"><div style="width:'+bSW+'%;height:100%;background:'+RD+'"></div>'+dividers+'</div>'
          +'<span style="font-size:8px;color:'+RD+';width:32px;text-align:right">'+r.short_pct+'%</span></div>'
          +'</div>';
      } else { inner='<div style="font-size:9px;color:#333">Positions N/A</div>'; }
      h+='<div id="bcard-'+key+'" data-bkey="'+key+'" onclick="switchBubbles(this.dataset.bkey)" style="background:#1a1a2e;border-radius:8px;padding:10px;cursor:pointer;border:0.5px solid #2a2a45">'
        +'<div style="font-size:10px;color:#c8d0e7;margin-bottom:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">'+key+'</div>'
        +'<div style="font-size:9px;color:#555;margin-bottom:5px">'+cntLabel+'</div>'+inner+'</div>';
    });
    h+='</div>';
  }
  h+='<div id="bubble-type" class="inline-bubble" style="display:none"></div>';
  h+='</div></div>';

  // ─ Portfolio 규모 Section ─
  h+='<div class="sent-section" id="ssec-equity" style="margin-top:12px">';
  h+='<div class="sent-sec-header" onclick="toggleSection(&quot;equity&quot;)" style="cursor:pointer;display:flex;align-items:center;justify-content:space-between;padding:10px 14px;background:#111120;border-radius:8px;margin-bottom:8px;user-select:none">';
  h+='<span style="font-size:12px;font-weight:600;color:#c8d0e7">💰 By Portfolio Size</span>';
  h+='<span id="sarrow-equity" style="font-size:10px;color:#4a4a6a">▶</span>';
  h+='</div>';
  h+='<div id="sbody-equity" class="sent-sec-body" style="display:none">';
  if(SENT.equities&&SENT.equities.length){
    h+='<div class="sent-type-grid" style="display:grid;gap:6px;margin-bottom:8px">';
    SENT.equities.forEach(function(t){
      if(t.count===0) return;
      var r=t.result, key=t.label;
      var cntLabel=r?(t.count+' / Positions '+r.traders+' · WAR '+t.avg_war):(t.count+' · WAR '+t.avg_war);
      var inner='';
      if(r){
        var _emaxRaw=Math.max(r.long_pct,r.short_pct,1);
        var _emaxVal=Math.ceil(_emaxRaw/100)*100;
        var eLW=Math.min(r.long_pct/_emaxVal*100,100).toFixed(2);
        var eSW=Math.min(r.short_pct/_emaxVal*100,100).toFixed(2);
        var dividers='';
        for(var di=1;di<_emaxVal/100;di++) dividers+='<div style="position:absolute;top:0;left:'+(di*100/_emaxVal*100).toFixed(1)+'%;width:1px;height:100%;background:#3a3a55"></div>';
        inner='<div style="display:flex;flex-direction:column;gap:3px">'
          +'<div style="display:flex;align-items:center;gap:4px"><span style="font-size:8px;color:'+BL+';width:16px;flex-shrink:0">▲L</span>'
          +'<div style="flex:1;position:relative;height:5px;background:#2a2a45;border-radius:3px;overflow:hidden"><div style="width:'+eLW+'%;height:100%;background:'+BL+'"></div>'+dividers+'</div>'
          +'<span style="font-size:8px;color:'+BL+';width:32px;text-align:right">'+r.long_pct+'%</span></div>'
          +'<div style="display:flex;align-items:center;gap:4px"><span style="font-size:8px;color:'+RD+';width:16px;flex-shrink:0">▼S</span>'
          +'<div style="flex:1;position:relative;height:5px;background:#2a2a45;border-radius:3px;overflow:hidden"><div style="width:'+eSW+'%;height:100%;background:'+RD+'"></div>'+dividers+'</div>'
          +'<span style="font-size:8px;color:'+RD+';width:32px;text-align:right">'+r.short_pct+'%</span></div>'
          +'</div>';
      } else { inner='<div style="font-size:9px;color:#333">Positions N/A</div>'; }
      h+='<div id="bcard-'+key+'" data-bkey="'+key+'" onclick="switchBubbles(this.dataset.bkey)" style="background:#1a1a2e;border-radius:8px;padding:10px;cursor:pointer;border:0.5px solid #2a2a45">'
        +'<div style="font-size:10px;color:#ffbe0b;margin-bottom:2px;font-weight:600">'+key+'</div>'
        +'<div style="font-size:9px;color:#555;margin-bottom:5px">'+cntLabel+'</div>'+inner+'</div>';
    });
    h+='</div>';
  }
  h+='<div id="bubble-equity" class="inline-bubble" style="display:none"></div>';
  h+='</div></div>';

  h+='</div>'; // close sent-sections

  // ── 히스토리 차트 ────────────────────────────────────────────
  if(HIST && HIST.length >= 2){
    h+='<div style="margin-top:24px;background:#111120;border-radius:10px;padding:16px">';
    h+='<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:4px">';
    h+='<div style="font-size:12px;font-weight:600;color:#c8d0e7">📈 Sentiment History</div>';
    h+='<div id="hist-group-label" style="font-size:10px;color:#555">All Smart Money</div>';
    h+='</div>';
    h+='<div style="font-size:10px;color:#444;margin-bottom:10px">Smart Money long/short exposure + BTC price (right axis) · Click a card to compare · Solid=All Dashed=Group</div>';
    h+='<div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:10px">';
    h+='<span style="font-size:10px;color:#3a86ff">━ Long %</span>';
    h+='<span style="font-size:10px;color:#f72585">━ Short %</span>';
    h+='<span id="hist-legend-long" style="font-size:10px;color:#3a86ff;display:none">╌ Group Long %</span>';
    h+='<span id="hist-legend-short" style="font-size:10px;color:#f72585;display:none">╌ Group Short %</span>';
    h+='<span id="hist-legend-btc" style="font-size:10px;color:#f7931a">― BTC Price (right axis)</span>';
    h+='</div>';
    h+='<div style="position:relative;width:100%;height:220px;overflow:hidden"><canvas id="histChart"></canvas></div>';
    h+='</div>';
  }

  // ── WAR 랭킹 트렌드 차트 ──────────────────────────────────────
  if(WAR_HIST && WAR_HIST.length >= 2){
    h+='<div style="margin-top:24px;background:#111120;border-radius:10px;padding:16px">';
    h+='<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:4px">';
    h+='<div style="font-size:12px;font-weight:600;color:#c8d0e7">🏆 WAR Ranking Trend</div>';
    h+='<div style="display:flex;gap:6px;flex-wrap:wrap" id="war-btn-bar">';
    h+='<button id="war-mode-war" data-m="war" style="font-size:9px;padding:3px 8px;border-radius:4px;border:0.5px solid #00f5d4;background:#1e1e35;color:#00f5d4;cursor:pointer">WAR Score</button>';
    h+='<button id="war-mode-rank" data-m="rank" style="font-size:9px;padding:3px 8px;border-radius:4px;border:0.5px solid #2a2a45;background:transparent;color:#555;cursor:pointer">Ranking</button>';
    h+='<button id="war-mode-pnl" data-m="pnl" style="font-size:9px;padding:3px 8px;border-radius:4px;border:0.5px solid #2a2a45;background:transparent;color:#555;cursor:pointer">PnL</button>';
    h+='<button id="war-mode-roi" data-m="roi" style="font-size:9px;padding:3px 8px;border-radius:4px;border:0.5px solid #2a2a45;background:transparent;color:#555;cursor:pointer">ROI</button>';
    h+='<button id="war-mode-follow" data-m="follow" style="font-size:9px;padding:3px 8px;border-radius:4px;border:0.5px solid #2a2a45;background:transparent;color:#555;cursor:pointer">Follow</button>';
    h+='<div style="width:1px;background:#2a2a45;margin:0 2px"></div>';
    h+='<button class="war-range-btn" data-r="1" style="font-size:9px;padding:3px 8px;border-radius:4px;border:0.5px solid #2a2a45;background:transparent;color:#555;cursor:pointer">24H</button>';
    h+='<button class="war-range-btn" data-r="7" style="font-size:9px;padding:3px 8px;border-radius:4px;border:0.5px solid #00f5d4;background:#1e1e35;color:#00f5d4;cursor:pointer">7D</button>';
    h+='<button class="war-range-btn" data-r="30" style="font-size:9px;padding:3px 8px;border-radius:4px;border:0.5px solid #2a2a45;background:transparent;color:#555;cursor:pointer">30D</button>';
    h+='<button class="war-range-btn" data-r="0" style="font-size:9px;padding:3px 8px;border-radius:4px;border:0.5px solid #2a2a45;background:transparent;color:#555;cursor:pointer">All</button>';
    h+='</div></div>';
    h+='<div style="font-size:10px;color:#444;margin-bottom:8px">WAR score or ranking change for top 100 wallets · hover to inspect · click to open card</div>';
    // war-legend removed
    h+='<div style="position:relative;width:100%;height:480px;overflow:hidden"><canvas id="warTrendChart"></canvas><div id="warTrendEmpty" style="display:none;position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);font-size:11px;color:#3a3a55;text-align:center;pointer-events:none">Not enough snapshots yet.<br>Run discover to collect more data.</div></div>';
    h+='</div>';
  }

  root.innerHTML=h;

  // WAR 버튼 이벤트 바인딩 (innerHTML 이후에 해야 동작함)
  var warBar = document.getElementById('war-btn-bar');
  if(warBar){
    warBar.addEventListener('click', function(e){
      var btn = e.target.closest('button');
      if(!btn) return;
      if(btn.dataset.m){
        // WAR Score / Ranking 모드 전환
        window._warMode = btn.dataset.m;
        warBar.querySelectorAll('[data-m]').forEach(function(b){
          var a = b===btn;
          b.style.borderColor = a?'#00f5d4':'#2a2a45';
          b.style.color       = a?'#00f5d4':'#555';
          b.style.background  = a?'#1e1e35':'transparent';
        });
        window.initWarTrendChart();
      } else if(btn.dataset.r !== undefined){
        // Range 전환
        window._warRangeDays = parseInt(btn.dataset.r);
        warBar.querySelectorAll('.war-range-btn').forEach(function(b){
          var a = b===btn;
          b.style.borderColor = a?'#00f5d4':'#2a2a45';
          b.style.color       = a?'#00f5d4':'#555';
          b.style.background  = a?'#1e1e35':'transparent';
        });
        window.initWarTrendChart();
      }
    });
  }

  // 히스토리 차트 Init 및 그룹 업데이트 함수
  var _histChart=null;
  window.updateHistChart=function(groupLabel, groupLong, groupShort){
    var ctx=document.getElementById('histChart');
    if(!ctx||!HIST||HIST.length<2) return;
    // 1주일 이내 Data만
    var oneWeekAgo=Date.now()-7*24*60*60*1000;
    var filteredHIST=HIST.filter(function(d){return new Date(d.ts.replace(' ','T')).getTime()>=oneWeekAgo;});
    window._filteredHIST=filteredHIST;
    if(filteredHIST.length<1) filteredHIST=HIST.slice(-20); // fallback: last 20
    var labels=filteredHIST.map(function(d){return new Date(d.ts.replace(' ','T')).getTime();});
    var datasets=[
      {label:'Long %', data:filteredHIST.map(function(d,i){return {x:labels[i],y:d.all?d.all.long_pct:null};}),
       borderColor:'#3a86ff',borderWidth:2,tension:0.3,fill:false,pointRadius:2,pointHoverRadius:4},
      {label:'Short %', data:filteredHIST.map(function(d,i){return {x:labels[i],y:d.all?d.all.short_pct:null};}),
       borderColor:'#f72585',borderWidth:2,tension:0.3,fill:false,pointRadius:2,pointHoverRadius:4},
    ];
    // BTC 가격 (오른쪽 y축)
    var hasBtc = filteredHIST.some(function(d){ return typeof d.btc_price === 'number' && d.btc_price > 0; });
    if(hasBtc){
      datasets.push({
        label:'BTC Price',
        data: filteredHIST.map(function(d,i){ var p=d.btc_price; return {x:labels[i], y:(p&&p>0)?p:null}; }),
        borderColor:'#f7931a',
        backgroundColor:'#f7931a18',
        borderWidth:1.5,
        tension:0.3,
        fill:false,
        pointRadius:0,
        pointHoverRadius:3,
        yAxisID:'yBtc',
        borderDash:[3,3],
        spanGaps: true,  // 데이터 없는 구간 연결
      });
    }
    if(groupLong){
      datasets.push({label:groupLabel+' Long%', data:groupLong,
        borderColor:'#3a86ff',borderWidth:1.5,tension:0.3,fill:false,
        pointRadius:2,borderDash:[5,3],backgroundColor:'transparent'});
      datasets.push({label:groupLabel+' Short%', data:groupShort,
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
            callbacks:{
              title:function(items){
                if(!items.length) return '';
                var d=new Date(items[0].parsed.x);
                var mo=String(d.getMonth()+1).padStart(2,'0');
                var day=String(d.getDate()).padStart(2,'0');
                var h=String(d.getHours()).padStart(2,'0');
                var mn=String(d.getMinutes()).padStart(2,'0');
                return mo+'/'+day+' '+h+':'+mn;
              },
              label:function(ctx){
                if(ctx.dataset.yAxisID==='yBtc') return 'BTC: $'+Math.round(ctx.parsed.y).toLocaleString();
                return ctx.dataset.label+': '+ctx.parsed.y+'%';
              }
            }
          }
        },
        scales:{
          x:{
            type:'linear',
            min:Date.now()-7*24*60*60*1000,
            max:Date.now(),
            ticks:{
              color:'#3a3a55',font:{size:9},maxTicksLimit:7,maxRotation:0,
              callback:function(val){
                var d=new Date(val);
                var m=String(d.getMonth()+1).padStart(2,'0');
                var day=String(d.getDate()).padStart(2,'0');
                var h=String(d.getHours()).padStart(2,'0');
                var mn=String(d.getMinutes()).padStart(2,'0');
                return m+'/'+day+' '+h+':'+mn;
              }
            },
            grid:{color:'#1a1a2e'}
          },
          y:{
            ticks:{color:'#3a3a55',font:{size:9},callback:function(v){return v+'%';}},
            grid:{color:'#1a1a2e'},
            min:0,
            title:{display:true,text:'Long/Short %',color:'#3a3a55',font:{size:9}}
          },
          yBtc:{
            display: hasBtc,
            position:'right',
            ticks:{
              color:'#f7931a',
              font:{size:9},
              stepSize: 500,
              callback:function(v){
                if(v % 500 !== 0) return '';
                return '$'+Math.round(v).toLocaleString();
              }
            },
            grid:{display:false},
            title:{display:hasBtc,text:'BTC Price',color:'#f7931a',font:{size:9}}
          }
        }
      }
    });
    // 범례 표시
    var ll=document.getElementById('hist-legend-long');
    var ls=document.getElementById('hist-legend-short');
    var lb=document.getElementById('hist-legend-btc');
    var gl=document.getElementById('hist-group-label');
    if(ll) ll.style.display=groupLong?'':'none';
    if(ls) ls.style.display=groupShort?'':'none';
    if(lb) lb.style.display = hasBtc ? 'inline' : 'none';
    if(gl) gl.textContent=groupLabel;
  };

  if(HIST && HIST.length >= 2){
    setTimeout(function(){ window.updateHistChart('All Smart Money',null,null); }, 200);
  }

  // ── WAR 랭킹 트렌드 차트 Init ─────────────────────────────────
  if(WAR_HIST && WAR_HIST.length >= 2){
    setTimeout(function(){ window.initWarTrendChart(); }, 150);
  }

  // ── Section 토글 ──────────────────────────────────────────────────
  window._activeSection='war'; // default: WAR section
  window.toggleSection=function(sec){
    if(window._activeSection===sec){
      // 같은 Section 클릭: 접기
      document.getElementById('sbody-'+sec).style.display='none';
      document.getElementById('sarrow-'+sec).textContent='▶';
      document.getElementById('bubble-'+sec).style.display='none';
      window._activeSection=null;
    } else {
      // 기존 Section 닫기
      if(window._activeSection){
        document.getElementById('sbody-'+window._activeSection).style.display='none';
        document.getElementById('sarrow-'+window._activeSection).textContent='▶';
        document.getElementById('bubble-'+window._activeSection).style.display='none';
      }
      // 새 Section col기
      document.getElementById('sbody-'+sec).style.display='block';
      document.getElementById('sarrow-'+sec).textContent='▼';
      window._activeSection=sec;
    }
  }

  // ── Bubble맵 Init ──────────────────────────────────────────────
  const MAX_R=64, MIN_R=10;

  // ── 글로벌 최대값: 절대 Positions 달러 규모 기준으로 Bubble 크기 고정 ──
  const _allBubbleData = [
    ...SENT.coins,
    ...Object.values(SENT.band_bubbles||{}).flat(),
    ...Object.values(SENT.type_bubbles||{}).flat(),
    ...Object.values(SENT.equity_bubbles||{}).flat(),
  ];
  // Section별 독립 max (war / type / equity)
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

  // Bubble state (Section별로 분리)
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

    // state Init
    _bubbleState[secId]={};
    allCoins.forEach(coin=>{
      _bubbleState[secId][coin]={
        x:fixedPos[coin].x%(W-120)+60, y:fixedPos[coin].y,
        vx:fixedPos[coin].vx, vy:fixedPos[coin].vy,
        drifting:fixedPos[coin].drifting, driftTimer:fixedPos[coin].driftTimer,
        curOuter:0, curInner:0, tgtOuter:0, tgtInner:0, tgtLong:0, tgtShort:0,
      };
    });

    // SVG 요소 Gen
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
          +'<span style="color:'+BL+'">▲ Long '+fN(lNtl)+'</span>  <span style="color:'+RD+'">▼ Short '+fN(sNtl)+'</span><br>'
          +'<span style="color:#888;font-size:10px">vs Portfolio: '+s.tgtLong.toFixed(1)+'% / '+s.tgtShort.toFixed(1)+'%</span>';
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
    // Card 테두리 Init
    document.querySelectorAll('[id^="bcard-"]').forEach(el=>el.style.border='0.5px solid #2a2a45');
    var allCard=document.getElementById('bcard-all');
    if(allCard) allCard.style.border='1px solid #2a2a45';
    var sel=document.getElementById('bcard-'+key);
    if(sel) sel.style.border='1px solid '+BL;
    if(key==='all'&&allCard) allCard.style.border='2px solid '+BL;

    // 어느 Section 키인지 판별
    var secId=null;
    if(key==='all') secId='war';
    else if(SENT.band_bubbles&&SENT.band_bubbles[key]) secId='war';
    else if(SENT.type_bubbles&&SENT.type_bubbles[key]) secId='type';
    else if(SENT.equity_bubbles&&SENT.equity_bubbles[key]) secId='equity';
    if(!secId) return;

    // 해당 Section col기
    if(window._activeSection!==secId){
      window.toggleSection(secId);
    }

    // Bubble Data 세팅
    var coins;
    if(key==='all') coins=SENT.coins;
    else if(SENT.band_bubbles&&SENT.band_bubbles[key]) coins=SENT.band_bubbles[key];
    else if(SENT.type_bubbles&&SENT.type_bubbles[key]) coins=SENT.type_bubbles[key];
    else if(SENT.equity_bubbles&&SENT.equity_bubbles[key]) coins=SENT.equity_bubbles[key];
    else coins=[];

    // Bubble Init 및 표시
    var bubWrap=document.getElementById('bubble-'+secId);
    if(bubWrap){
      bubWrap.style.display='block';
      window.initBubble(secId);
      window.setTargetsFor(secId,coins);
      if(!_animIds[secId]) window.tickBubble(secId);
    }

    // 히스토리 차트 연동
    if(window.updateHistChart && HIST && HIST.length >= 2){
      var gLong=null, gShort=null, gLabel='All Smart Money';
      if(key==='all'){
        gLabel='All Smart Money';
      } else if(SENT.band_bubbles&&SENT.band_bubbles[key]){
        gLabel='WAR '+key;
        gLong =(window._filteredHIST||HIST).map(function(d,i){var b=d.bands&&d.bands.find(function(x){return x.label===key;});return b?b.long_pct:null;});
        gShort=(window._filteredHIST||HIST).map(function(d){var b=d.bands&&d.bands.find(function(x){return x.label===key;});return b?b.short_pct:null;});
      } else if(SENT.type_bubbles&&SENT.type_bubbles[key]){
        gLabel=key;
        gLong =(window._filteredHIST||HIST).map(function(d,i){var t=d.types&&d.types.find(function(x){return x.label===key;});return t?t.long_pct:null;});
        gShort=(window._filteredHIST||HIST).map(function(d){var t=d.types&&d.types.find(function(x){return x.label===key;});return t?t.short_pct:null;});
      } else if(SENT.equity_bubbles&&SENT.equity_bubbles[key]){
        gLabel=key;
        gLong =(window._filteredHIST||HIST).map(function(d,i){var e=d.equities&&d.equities.find(function(x){return x.label===key;});return e?e.long_pct:null;});
        gShort=(window._filteredHIST||HIST).map(function(d){var e=d.equities&&d.equities.find(function(x){return x.label===key;});return e?e.short_pct:null;});
      }
      window.updateHistChart(gLabel, gLong, gShort);
    }
  };

  // 기본: WAR Section col고 전체 Bubble 표시
  setTimeout(()=>{
    var bw=document.getElementById('bubble-war');
    if(bw){ bw.style.display='block'; initBubble('war'); setTargetsFor('war',SENT.coins); tickBubble('war'); }
    document.getElementById('bcard-all')&&(document.getElementById('bcard-all').style.border='2px solid '+BL);
  },100);
}
"""
    html = html.replace("%%SCRIPT%%", js_block)
    html = html.replace("%%MODAL%%", modal_block)
    html = html.replace("%%ALL_STATS%%", all_stats_js)
    # wallets_meta를 JS에 주입
    _wm = load_wallets_meta()
    html = html.replace("%%WALLET_META%%", json.dumps(_wm, ensure_ascii=False))
    return html


# ══ SEASON PICKS ═══════════════════════════════════════════════════════
def print_season_picks(archive: ArchiveManager, n=20):
    picks = archive.top_war_stats(n=n)
    console.print(f"\n[bold yellow]★ 시즌 출전 추천 TOP {n}[/bold yellow]  "
                  f"[dim](조건: ${MIN_EQUITY:,}+ · WAR 정렬)[/dim]\n")

    tbl = Table(show_header=True, header_style="bold dim", border_style="dim")
    for col, w in [("RANK",5),("LABEL",18),("TYPE",20),("WAR",6),
                   ("EQUITY",14),("WIN%",6),("SHARPE",7),("First seen",12)]:
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

    # WAR 50 미만 자동 정리
    _pruned = archive.prune_low_war(min_war=50.0, min_equity=MIN_EQUITY)
    if _pruned:
        console.print(f"  [dim]WAR 50 미만 {_pruned}개 아카이브에서 제거[/dim]")

    # --prune-war: remove wallets below WAR threshold
    if getattr(args, "prune_war", None) is not None:
        before = len(archive.data)
        archive.prune_low_war(min_war=args.prune_war, min_equity=MIN_EQUITY)
        after = len(archive.data)
        if before != after:
            console.print(f"[yellow]--prune-war: removed {before-after} wallets below WAR {args.prune_war}[/yellow]")

    # --mark-vault: manually tag addresses as vault source
    if getattr(args, "mark_vault", None):
        for addr in args.mark_vault:
            key = addr.strip().lower()
            if key in archive.data:
                stats = archive.data[key].get("stats", {})
                stats["source"] = "vault"
                archive.data[key]["stats"] = stats
                console.print(f"[green]--mark-vault: {addr[:12]}... tagged as vault[/green]")
            else:
                console.print(f"[yellow]--mark-vault: {addr[:12]}... not in archive — skipped[/yellow]")
        archive.save()

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
        _excl_for_disc = ExcludedManager()
        candidates = await disc.discover(archive, target=args.discover_n, excluded=_excl_for_disc)
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
            console.print("[dim]갱신 필요한 지갑 N/A[/dim]")
    elif addresses:
        await process_addresses(addresses, labels, sources, archive, force=args.force_refresh)
    elif not args.season:
        console.print("[dim]수집할 지갑 N/A. --file/--discover/--refresh-stale/--refresh-all 사용[/dim]")

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
        console.print(f"\n[bold cyan]🔍 Wallet Lookup 및 캐시 저장: {len(lookup_addrs)}개[/bold cyan]")
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
        console.print(f"  [green]✓ 완료 — --report 로 리포트 재Gen하세요[/green]")

    # --report: HTML 리포트 Gen
    if args.report:
        # vault 주소 캐시 보정 (vaultSummaries API)
        # vault_discovery.json으로 label 보정
        try:
            _vd_path = Path("vault_discovery.json")
            if _vd_path.exists():
                _vd = json.loads(_vd_path.read_text(encoding="utf-8"))
                _name_map = {v["vault_addr"].lower(): v["name"] for v in _vd.get("direct_vaults", []) if v.get("name")}
                _lpatched = 0
                for addr, entry in archive.data.items():
                    vname = _name_map.get(addr.lower())
                    if vname:
                        old_label = entry.get("stats", {}).get("label", "")
                        # 주소 형태(0x...)이면 vault 이름으로 교체
                        if old_label.startswith("0x") or old_label == "":
                            entry["stats"]["label"] = vname
                            entry["stats"]["source"] = "vault"
                            entry["stats"]["is_vault"] = True
                            _lpatched += 1
                if _lpatched:
                    archive.save()
                    console.print(f"  [dim]vault 이름 보정: {_lpatched}개[/dim]")
        except Exception as _le:
            console.print(f"  [dim]vault 이름 보정 실패: {_le}[/dim]")

        # vault 보정: vaultSummaries API + 캐시 내 is_vault 필드
        _patched = 0
        try:
            import httpx as _hx
            _r = _hx.get("https://stats-data.hyperliquid.xyz/Mainnet/vaults", timeout=15)
            if _r.status_code == 200:
                _vaults = _r.json() if isinstance(_r.json(), list) else []
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
                _pos_traders = [s for s in _snap_stats if s.get("positions") and s.get("war_score",0)>=50]
                _all_eq    = sum(_eq(s) for s in _pos_traders if sum(p["notional"] for p in s["positions"])>0)
                _all_long  = sum(sum(p["notional"] for p in s.get("positions",[]) if p["side"]=="LONG") for s in _pos_traders)
                _all_short = sum(sum(p["notional"] for p in s.get("positions",[]) if p["side"]=="SHORT") for s in _pos_traders)
                _snap = {
                    "ts": _dt.now().strftime("%Y-%m-%dT%H:%M:%S"),
                    "all": {
                        "long_pct":  round(_all_long/_all_eq*100, 1) if _all_eq>0 else 0,
                        "short_pct": round(_all_short/_all_eq*100, 1) if _all_eq>0 else 0,
                        "traders":   len(_pos_traders),
                    },
                    "btc_price": 0,
                    "bands": [],
                    "types": [],
                }
                # BTC 현재 가격 조회 (allMids는 POST)
                try:
                    import httpx as _hx2
                    _btc_r = _hx2.post(
                        "https://api.hyperliquid.xyz/info",
                        json={"type": "allMids"},
                        timeout=5
                    )
                    _mids = _btc_r.json() if _btc_r.status_code==200 else {}
                    _snap["btc_price"] = float(_mids.get("BTC", 0))
                except Exception:
                    _snap["btc_price"] = 0
                # By WAR Band (total_equity 기준)
                WAR_B = [(50,60,"50-60"),(60,70,"60-70"),(70,80,"70-80"),(80,999,"80+")]
                for lo, hi, lbl in WAR_B:
                    grp = [s for s in _pos_traders if lo <= s.get("war_score",0) < hi]
                    geq = sum(_eq(s) for s in grp if sum(p["notional"] for p in s["positions"])>0) or 1
                    ln  = sum(sum(p["notional"] for p in s.get("positions",[]) if p["side"]=="LONG") for s in grp)
                    sn  = sum(sum(p["notional"] for p in s.get("positions",[]) if p["side"]=="SHORT") for s in grp)
                    _snap["bands"].append({"label": lbl, "long_pct": round(ln/geq*100,1), "short_pct": round(sn/geq*100,1)})
                # Type별 (total_equity 기준)
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
                # By Portfolio Size
                EQ_BANDS = [(50000,100000,"$50K~100K"),
                            (100000,500000,"$100K~500K"),(500000,1000000,"$500K~1M"),
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

        # WAR 히스토리 스냅샷 저장 (WAR 순수 top 100)
        try:
            _war_hist_path = Path(WAR_HIST_FILE)
            _war_hist = json.loads(_war_hist_path.read_text(encoding="utf-8")) if _war_hist_path.exists() else []
            _all_qs = archive.qualified_stats()
            # WAR 기준 정렬 → 순수 top 100만 저장 (Follow 혼합 없음)
            _war_sorted = sorted(_all_qs, key=lambda x: x.get("war_score", 0), reverse=True)
            _top100 = _war_sorted[:100]
            if _top100:
                _war_snap = {
                    "ts": _dt.now().strftime("%Y-%m-%dT%H:%M:%S"),
                    "top20": [
                        {
                            "address": s["address"],
                            "label":   s.get("label", s["address"][:8]),
                            "war":     round(s.get("war_score", 0), 1),
                            "rank":    i + 1,
                            "type":    s.get("trader_type", ""),
                            "pnl":     round(s.get("total_pnl", 0), 0),
                            "roi":     round(s.get("roi_pct", 0), 2),
                            "follow":  round(s.get("follow_score", 0), 1),
                        }
                        for i, s in enumerate(_top100)
                    ]
                }
                _war_hist.append(_war_snap)
                if len(_war_hist) > 200:
                    _war_hist = _war_hist[-200:]
                _war_hist_path.write_text(json.dumps(_war_hist, ensure_ascii=False, indent=2), encoding="utf-8")
                console.print(f"  [dim]WAR 히스토리 저장: {len(_war_hist)}개 스냅샷[/dim]")
        except Exception as _we:
            console.print(f"  [dim]WAR 히스토리 저장 실패: {_we}[/dim]")

        console.print("\n[bold magenta]▶ HTML report generating...[/bold magenta]")
        report_stats = archive.qualified_stats()  # WAR 50+ · $10k+
        tournament = run_tournament(report_stats)
        html = generate_html(report_stats, tournament, archive, hist_path=Path(HIST_FILE), war_hist_path=Path(WAR_HIST_FILE))
        ts_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        out = f"scouting_report_{ts_str}.html"
        with open(out, "w", encoding="utf-8") as f:
            f.write(html)
        console.print(f"\n[bold green]✓ Saved: {out}[/bold green]  ({len(report_stats)})")


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
    parser.add_argument("--mark-vault", nargs="+", metavar="ADDR", help="Manually tag addresses as vault source")
    parser.add_argument("--prune-war", type=float, default=None, help="Remove wallets below this WAR score from archive")
    parser.add_argument("--report", "-r", action="store_true", help="HTML 리포트 Gen")
    args = parser.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    import sys
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    main()
