# -*- coding: utf-8 -*-
"""ExcludedManager, ArchiveManager — 지갑 데이터 영속성"""
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from rich.console import Console

from ..config import ARCHIVE_FILE, EXCLUDED_FILE, CACHE_TTL, MIN_EQUITY, SCHEMA_VERSION

BACKOFF_DAYS = {1: 3, 2: 7}
BACKOFF_MAX_DAYS = 7
BACKOFF_PERMANENT_THRESHOLD = 3  # 이 횟수 이상 시 영구 스킵
from ..utils import console

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
        permanent = entry.get("permanent", False) or cnt >= BACKOFF_PERMANENT_THRESHOLD
        reason_str = "WAR 미달" if reason == "war" else "Equity 부족"
        if permanent:
            return f"{reason_str} (WAR {war:.1f}) · 제외 {cnt}회 · 🚫 영구 스킵"
        return f"{reason_str} (WAR {war:.1f}) · 제외 {cnt}회 · 다음체크 {nc}"

    def record_exclusion(self, address: str, war: float, reason: str = "war"):
        """WAR 50 미만 또는 equity 부족으로 제외될 때 호출 — backoff 갱신"""
        key = address.lower()
        now = datetime.now(tz=timezone.utc)
        cnt = self.data.get(key, {}).get("exclude_count", 0) + 1
        if cnt >= BACKOFF_PERMANENT_THRESHOLD:
            # 영구 스킵: next_check_at을 100년 후로 설정
            next_check = (now + timedelta(days=36500)).isoformat()
            permanent = True
        else:
            days = BACKOFF_DAYS.get(cnt, BACKOFF_MAX_DAYS)
            next_check = (now + timedelta(days=days)).isoformat()
            permanent = False
        self.data[key] = {
            "exclude_count":    cnt,
            "last_excluded_at": now.isoformat(),
            "next_check_at":    next_check,
            "last_war":         round(war, 1),
            "reason":           reason,   # "war" | "equity"
            "permanent":        permanent,
        }

    def clear(self, address: str):
        """WAR 50 이상으로 통과 시 backoff 초기화"""
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
    def __init__(self, archive_file=ARCHIVE_FILE, path=None):
        self.path = Path(path or archive_file)
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
            # prev_positions 갱신: 24시간 이상 지난 경우만 현재→prev로 이동
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

    def prune_low_war(self, min_war=50.0, min_equity=50_000):
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
