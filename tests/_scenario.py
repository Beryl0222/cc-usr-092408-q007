"""测试共享：以固定时钟构造一条典型试点场景。

所有业务时间都显式给出，保证命令 -> 事件是确定性的，
两套独立账本执行同一脚本应产生逐字节相同的事件与状态指纹。
"""
from __future__ import annotations

import os
import tempfile
from pathlib import Path

from src.service import LedgerService
from src.store import EventStore


def at(day: int, hour: int, minute: int = 0, tz: str = "+08:00") -> str:
    return f"2026-09-{day:02d}T{hour:02d}:{minute:02d}:00{tz}"


def pilot_spec(**overrides) -> dict:
    spec = {
        "pilot_id": "p1",
        "title": "环湖夜跑照明试点",
        "cluster_ids": ["c-run"],
        "zones": ["A", "B"],
        "control_zones": ["C"],
        "conflict_tags": ["night_lighting", "quiet_rest"],
        "window": {"start": at(20, 18), "end": "2026-10-20T22:00:00+08:00"},
        "metrics": {"照度达标率": {"target": 0.9, "tolerance": 0.05}},
        "safety_limits": {"眩光值": {"max": 50}},
        "bias_threshold": 0.7,
        "required_sample_size": 2,
        "review_within_hours": 48,
        "budget_total": 3000,
        "slots": 6,
        "pool_id": "pool-1",
        "approver": "王主任",
    }
    spec.update(overrides)
    return spec


def new_service() -> tuple[LedgerService, Path]:
    tmp = Path(tempfile.mkdtemp())
    service = LedgerService(EventStore(tmp / "journal.jsonl"), clock=lambda: at(30, 12))
    return service, tmp


def seed_world(service: LedgerService) -> None:
    """资源池 + 三类冲突诉求 + 建议性聚类。"""
    service.fund_pool("pool-1", 10000, 20, at=at(1, 9))
    groups = ["夜跑者"] * 3 + ["亲子家庭"] * 2 + ["周边居民"] * 2
    for i, group in enumerate(groups):
        service.log_need(f"need-{i}", f"{group}的去标识诉求摘要{i}", group,
                         theme_tags=["tag"], at=at(1, 10 + i // 3))
    service.propose_cluster(
        "c-run", ["need-0", "need-1", "need-2"], "夜跑照明",
        rationale="高频夜间照明诉求", confidence=0.82, at=at(2, 9)
    )


def approved_pilot(service: LedgerService, **overrides) -> dict:
    seed_world(service)
    return service.approve_pilot(pilot_spec(**overrides), at=at(2, 15))
