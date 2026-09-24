"""领域事件定义与确定性标识。

事件是账本的唯一事实来源：一旦追加即不可改写，更正只能再追加后继事件。
event_id 由事件内容规范化哈希得到，保证同一事件流重复回放时标识完全一致。
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

# ---- 事件类型 ----
NEED_LOGGED = "NEED_LOGGED"
CLUSTER_PROPOSED = "CLUSTER_PROPOSED"
POOL_FUNDED = "POOL_FUNDED"
PILOT_APPROVED = "PILOT_APPROVED"
PILOT_REJECTED = "PILOT_REJECTED"
OBSERVATION_RECORDED = "OBSERVATION_RECORDED"
FEEDBACK_RECORDED = "FEEDBACK_RECORDED"
RESOURCE_USED = "RESOURCE_USED"
REVIEW_SCHEDULED = "REVIEW_SCHEDULED"
REVIEW_HELD = "REVIEW_HELD"
SAFETY_STOP_TRIGGERED = "SAFETY_STOP_TRIGGERED"
PILOT_PAUSED = "PILOT_PAUSED"
PILOT_RESUMED = "PILOT_RESUMED"
NOTIFICATION_SENT = "NOTIFICATION_SENT"
PILOT_DECIDED = "PILOT_DECIDED"
PILOT_ROLLED_BACK = "PILOT_ROLLED_BACK"
BATCH_IMPORTED = "BATCH_IMPORTED"
BATCH_QUARANTINED = "BATCH_QUARANTINED"
MAINTENANCE_COMPLETED = "MAINTENANCE_COMPLETED"

# ---- 聚合类型 ----
AG_COMMUNITY_NEED = "community_need"
AG_NEED_CLUSTER = "need_cluster"
AG_RESOURCE_POOL = "resource_pool"
AG_PILOT = "pilot_measure"
AG_IMPORT_BATCH = "import_batch"
AG_OBSERVATION_WINDOW = "observation_window"
AG_MAINTENANCE_CASE = "maintenance_case"


class DomainError(ValueError):
    """业务规则被违反；调用方应修正命令后重试，账本不会留下半条记录。"""


@dataclass(frozen=True)
class Event:
    event_type: str
    aggregate_type: str
    aggregate_id: str
    occurred_at: str
    payload: dict[str, Any]
    summary: str
    version: int
    event_id: str
    seq: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "aggregate_type": self.aggregate_type,
            "aggregate_id": self.aggregate_id,
            "occurred_at": self.occurred_at,
            "version": self.version,
            "seq": self.seq,
            "summary": self.summary,
            "payload": self.payload,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "Event":
        return cls(
            event_id=raw["event_id"],
            event_type=raw["event_type"],
            aggregate_type=raw["aggregate_type"],
            aggregate_id=raw["aggregate_id"],
            occurred_at=raw["occurred_at"],
            version=raw["version"],
            seq=raw.get("seq", 0),
            summary=raw["summary"],
            payload=raw.get("payload", {}),
        )


def parse_at(value: str) -> datetime:
    """解析业务时间，要求带时区，避免夏令时/时区歧义。"""
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        raise DomainError(f"时间必须带时区偏移：{value}")
    return dt


def deterministic_event_id(
    aggregate_type: str,
    aggregate_id: str,
    version: int,
    event_type: str,
    occurred_at: str,
    payload: dict[str, Any],
    summary: str,
) -> str:
    basis = [aggregate_type, aggregate_id, version, event_type, occurred_at, payload, summary]
    blob = json.dumps(basis, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:24]


def make_event(
    event_type: str,
    aggregate_type: str,
    aggregate_id: str,
    occurred_at: str,
    payload: dict[str, Any],
    summary: str,
    version: int,
) -> Event:
    parse_at(occurred_at)
    eid = deterministic_event_id(
        aggregate_type, aggregate_id, version, event_type, occurred_at, payload, summary
    )
    return Event(event_type, aggregate_type, aggregate_id, occurred_at, payload, summary, version, eid)
