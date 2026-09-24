"""可逆试点决策账（事件溯源内核，纯标准库）。

核心原则
- 只追加、不原地改写：任何业务状态都是事件流的确定性折叠结果。
- 原始诉求只保存去标识摘要与来源群体；聚类结果标记为“建议”而非事实。
- 试点审批时冻结区域、时间窗、成功指标、停止条件、批准人；后续命令不得改写。
- 环境观测与居民反馈按发生时间进入决策账；后来数据不能重写当时决策，
  扩大/延长/终止以决策时已入流数据（证据快照）为依据，可逐条解释。
- 到达安全停止线立即暂停受影响区域；普通指标偏离进入复核。
- 回滚（终止）保留已发生的资源使用与通知历史；名额与预算原子释放。
- 同一观测重传（幂等键）不重复计数；事件流重复回放得到一致状态。

例外（DomainError）与批量隔离（rejected_records）分离：
- 单条记录级别的问题（解析失败、字段缺失、重复键、业务冲突）进入
  批量导入的 rejected_records 隔离区，其余好记录照常提交；
- 整体不合法或状态冲突（如冻结后改写、全局预算超支）抛 DomainError。
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

# 公园本地时区（+08:00）
LOCAL_TZ = timezone(timedelta(hours=8))

# ---------------------------------------------------------------------------
# 事件类型
# ---------------------------------------------------------------------------

NEED_LOGGED = "NEED_LOGGED"                    # 去标识诉求登记
NEEDS_CLUSTERED = "NEEDS_CLUSTERED"            # 建议性聚类（advisory=True）
PILOT_APPROVED = "PILOT_APPROVED"              # 试点冻结：区域/时间窗/指标/停止条件/批准人
RESOURCES_RESERVED = "RESOURCES_RESERVED"      # 名额、预算原子预留
RESOURCES_ADJUSTED = "RESOURCES_ADJUSTED"      # 扩大时原子追加名额/预算/区域
RESOURCE_USAGE_LOGGED = "RESOURCE_USAGE_LOGGED"  # 已发生的资源使用（回滚后仍保留）
OBSERVATION_RECORDED = "OBSERVATION_RECORDED"  # 环境观测 / 居民反馈（按发生时间）
SAFETY_HALT_TRIGGERED = "SAFETY_HALT_TRIGGERED"  # 触及安全停止线：立即暂停受影响区域
METRIC_DEVIATION_FLAGGED = "METRIC_DEVIATION_FLAGGED"  # 普通指标偏离：进入复核
REVIEW_SCHEDULED = "REVIEW_SCHEDULED"
REVIEW_RESOLVED = "REVIEW_RESOLVED"
PILOT_DECIDED = "PILOT_DECIDED"                # 扩大/延长/终止（回滚），附证据快照
RESOURCES_RELEASED = "RESOURCES_RELEASED"      # 名额、预算原子释放（回滚/拒绝后）
NOTIFICATION_SENT = "NOTIFICATION_SENT"        # 已发出的通知（回滚后仍保留）

DECISION_EXPAND = "expand"
DECISION_EXTEND = "extend"
DECISION_TERMINATE = "terminate"
DECISIONS = (DECISION_EXPAND, DECISION_EXTEND, DECISION_TERMINATE)

OBSERVATION = "observation"
FEEDBACK = "feedback"


class DomainError(Exception):
    """整体不可提交的领域冲突。"""


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------

def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_ts(value: str) -> datetime:
    """解析 ISO8601；无时区者按公园本地时区（+08:00）处理。"""
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=LOCAL_TZ)
    return dt


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def content_event_id(event_type: str, aggregate_id: str, version: int, payload: dict) -> str:
    """内容寻址：同类型/同聚合/同版本/同内容必然得到同一事件 ID（确定性）。"""
    body = json.dumps(
        {"t": event_type, "a": aggregate_id, "v": version, "p": payload},
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(body.encode("utf-8")).hexdigest()[:24]


# ---------------------------------------------------------------------------
# 事件存储：仅追加 JSONL，批量原子落盘，可重启续用
# ---------------------------------------------------------------------------

@dataclass
class EventStore:
    path: str

    def append(self, events: list[dict]) -> None:
        """整批原子写入：既有内容 + 新批次先写临时文件，再一次性 rename 替换。

        进程在任何时刻被杀死，正式文件要么是旧版本、要么是完整新版本，
        不会出现只写入半批的状态。
        """
        if not events:
            return
        directory = os.path.dirname(os.path.abspath(self.path)) or "."
        os.makedirs(directory, exist_ok=True)
        existing = b""
        if os.path.exists(self.path):
            with open(self.path, "rb") as old:
                existing = old.read()
        payload = "".join(
            json.dumps(ev, ensure_ascii=False, sort_keys=True) + "\n" for ev in events
        ).encode("utf-8")
        fd, tmp = tempfile.mkstemp(prefix=".events-", suffix=".tmp", dir=directory)
        try:
            with os.fdopen(fd, "wb") as fh:
                fh.write(existing)
                fh.write(payload)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self.path)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)

    def load(self) -> list[dict]:
        """读取全部事件。

        末尾若存在写坏/截断的半行（例如进程在落盘中途被杀死），
        跳过该坏尾行而不是让整账不可读；正常提交的批次永远以完整行为单位。
        """
        if not os.path.exists(self.path):
            return []
        events: list[dict] = []
        with open(self.path, encoding="utf-8") as fh:
            for line_no, raw in enumerate(fh, 1):
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    events.append(json.loads(raw))
                except json.JSONDecodeError:
                    # 仅容忍末尾截断；中间坏行说明文件被外部破坏，必须显式失败。
                    if line_no == _count_lines(self.path):
                        continue
                    raise DomainError(f"事件流第 {line_no} 行损坏，终止加载以避免错误结论")
        return events


def _count_lines(path: str) -> int:
    with open(path, "rb") as fh:
        return sum(1 for _ in fh)


# ---------------------------------------------------------------------------
# 状态折叠（纯函数）：同一事件流 => 同一状态
# ---------------------------------------------------------------------------

@dataclass
class PilotView:
    pilot_id: str
    cluster_id: str
    zone: str
    started_at: datetime
    ended_at: datetime
    success_metrics: dict
    safety_limits: dict
    approver: str
    status: str = "running"            # running / safety_halted / review / terminated / expanded / extended
    reservation: dict | None = None
    released: dict | None = None
    expansion: dict | None = None
    usages: list[dict] = field(default_factory=list)
    decisions: list[dict] = field(default_factory=list)
    notifications: list[dict] = field(default_factory=list)
    halted_at: datetime | None = None
    halt_reason: str | None = None
    review_from_status: str | None = None


@dataclass
class LedgerState:
    needs: dict[str, dict] = field(default_factory=dict)            # need_id -> 去标识记录
    clusters: dict[str, dict] = field(default_factory=dict)         # cluster_id -> 建议
    pilots: dict[str, PilotView] = field(default_factory=dict)
    reviews: dict[str, dict] = field(default_factory=dict)
    # 幂等表：ideq_key -> (pilot_id, 事件序号)，重传不重复计数
    observations_seen: dict[str, tuple[str, int]] = field(default_factory=dict)
    observations: list[dict] = field(default_factory=list)
    resource_log: list[dict] = field(default_factory=list)          # 全部预留/调整/释放（回滚后仍保留）
    usages: list[dict] = field(default_factory=list)                # 全部已发生资源使用（回滚后仍保留）
    notifications: list[dict] = field(default_factory=list)
    seq: int = 0
    versions: dict[str, int] = field(default_factory=dict)          # aggregate_id -> 版本
    aggregate_types: dict[str, str] = field(default_factory=dict)

    # -- 资源与冲突的派生视图 --
    def active_reservations(self) -> dict[str, dict]:
        """当前生效的预留（释放后不再生效，但 resource_log 保留痕迹）。"""
        active: dict[str, dict] = {}
        for entry in self.resource_log:
            if entry["event_type"] == RESOURCES_RESERVED:
                active[entry["pilot_id"]] = dict(entry)
            elif entry["event_type"] == RESOURCES_ADJUSTED:
                cur = active[entry["pilot_id"]]
                cur["quota"] = cur.get("quota", 0) + entry.get("add_quota", 0)
                cur["budget"] = cur.get("budget", 0) + entry.get("add_budget", 0)
                cur["zones"] = list(dict.fromkeys(cur.get("zones", []) + entry.get("add_zones", [])))
            else:  # RESOURCES_RELEASED
                active.pop(entry["pilot_id"], None)
        return active

    def total_reserved_budget(self) -> float:
        return sum(r["budget"] for r in self.active_reservations().values())

    def zones_taken(self) -> dict[str, str]:
        """受占用区域 -> pilot_id。

        对照区域不能同时参加冲突试点：运行中、安全暂停、复核中的试点都
        继续占用对照名额；只有终止（回滚）释放后区域才可被其他试点使用。
        """
        taken: dict[str, str] = {}
        for pid, res in self.active_reservations().items():
            for z in res["zones"]:
                taken[z] = pid
        return taken


def fold(events: list[dict]) -> LedgerState:
    state = LedgerState()
    for ev in events:
        apply_one(state, ev)
    return state


def apply_one(state: LedgerState, ev: dict) -> None:
    state.seq += 1
    etype = ev["event_type"]
    aid = ev["aggregate_id"]
    state.aggregate_types.setdefault(aid, ev.get("aggregate_type", ""))
    state.versions[aid] = ev["version"]
    p = ev.get("payload", {})

    if etype == NEED_LOGGED:
        state.needs[aid] = {
            "need_id": aid,
            "summary": p["summary"],
            "source_group": p["source_group"],
            "category": p.get("category"),
            "occurred_at": parse_ts(ev["occurred_at"]),
            "dedup_key": p.get("dedup_key"),
        }

    elif etype == NEEDS_CLUSTERED:
        state.clusters[aid] = {
            "cluster_id": aid,
            "need_ids": list(p["need_ids"]),
            "label": p["label"],
            "advisory": True,  # 聚类是建议而非事实
            "method": p.get("method", "manual"),
        }

    elif etype == PILOT_APPROVED:
        state.pilots[aid] = PilotView(
            pilot_id=aid,
            cluster_id=p["cluster_id"],
            zone=p["zone"],
            started_at=parse_ts(p["window"]["start"]),
            ended_at=parse_ts(p["window"]["end"]),
            success_metrics=dict(p["success_metrics"]),
            safety_limits=dict(p["safety_limits"]),
            approver=p["approver"],
        )

    elif etype == RESOURCES_RESERVED:
        state.resource_log.append({"event_type": etype, "pilot_id": aid, **p})

    elif etype == RESOURCES_ADJUSTED:
        state.resource_log.append({"event_type": etype, "pilot_id": aid, **p})
        state.pilots[aid].expansion = {
            "add_quota": p.get("add_quota", 0),
            "add_budget": p.get("add_budget", 0),
            "add_zones": p.get("add_zones", []),
        }

    elif etype == RESOURCES_RELEASED:
        state.resource_log.append({"event_type": etype, "pilot_id": aid, **p})
        if aid in state.pilots:
            state.pilots[aid].released = dict(p)

    elif etype == RESOURCE_USAGE_LOGGED:
        entry = {"pilot_id": aid, "at": ev["occurred_at"], **p}
        state.usages.append(entry)
        state.pilots[aid].usages.append(entry)

    elif etype == OBSERVATION_RECORDED:
        entry = {
            "pilot_id": aid,
            "idempotency_key": p["idempotency_key"],
            "kind": p["kind"],
            "metric": p.get("metric"),
            "value": p.get("value"),
            "source_group": p.get("source_group"),
            "occurred_at": parse_ts(p["occurred_at"]),
            "ingested_at": parse_ts(ev["occurred_at"]),
            "seq": state.seq,
        }
        state.observations.append(entry)
        state.observations_seen[p["idempotency_key"]] = (aid, state.seq)

    elif etype == SAFETY_HALT_TRIGGERED:
        pilot = state.pilots[aid]
        pilot.status = "safety_halted"
        pilot.halted_at = parse_ts(ev["occurred_at"])
        pilot.halt_reason = p["reason"]

    elif etype == METRIC_DEVIATION_FLAGGED:
        review_id = p["review_id"]
        existing = state.reviews.get(review_id)
        if existing and existing.get("status") == "scheduled":
            existing.update({
                "pilot_id": aid,
                "metric": p["metric"],
                "observed": p["observed"],
                "expected": p["expected"],
                "status": "open",
                "created_at": parse_ts(ev["occurred_at"]),
                "resolution": None,
            })
        else:
            state.reviews[review_id] = {
                "review_id": review_id,
                "pilot_id": aid,
                "metric": p["metric"],
                "observed": p["observed"],
                "expected": p["expected"],
                "status": "open",
                "created_at": parse_ts(ev["occurred_at"]),
                "resolution": None,
            }
        if state.pilots[aid].status in ("running", "expanded", "extended"):
            pilot = state.pilots[aid]
            pilot.review_from_status = pilot.status
            pilot.status = "review"

    elif etype == REVIEW_RESOLVED:
        rv = state.reviews[p["review_id"]]
        rv["status"] = "resolved"
        rv["resolution"] = p["resolution"]
        pilot = state.pilots[aid]
        if pilot.status == "review":
            still_open = any(
                r.get("pilot_id") == aid and r.get("status") == "open"
                for r in state.reviews.values())
            if not still_open:
                pilot.status = pilot.review_from_status or "running"
                pilot.review_from_status = None

    elif etype == REVIEW_SCHEDULED:
        # 到期评审排期；重启后由 due_reviews() 重新发现，不丢评审。
        state.reviews.setdefault(p["review_id"], {
            "review_id": p["review_id"], "pilot_id": aid,
            "status": "scheduled", "due_at": p["due_at"],
            "reason": p.get("reason", "到期评审"),
        })

    elif etype == PILOT_DECIDED:
        pilot = state.pilots[aid]
        record = {
            "decision": p["decision"],
            "decided_at": parse_ts(ev["occurred_at"]),
            "approver": p["approver"],
            "reason": p.get("reason", ""),
            "evidence": p["evidence"],  # 证据快照：决策时引用了哪些数据
        }
        if p["decision"] == DECISION_EXTEND and p.get("new_window_end"):
            # 延长是新的决策事件；原窗口仍保存在 PILOT_APPROVED 中，不可改写。
            record["new_window_end"] = p["new_window_end"]
            pilot.ended_at = parse_ts(p["new_window_end"])
        pilot.decisions.append(record)
        if p["decision"] == DECISION_TERMINATE:
            pilot.status = "terminated"
        elif p["decision"] == DECISION_EXPAND:
            pilot.status = "expanded"
        elif p["decision"] == DECISION_EXTEND:
            pilot.status = "extended"

    elif etype == NOTIFICATION_SENT:
        entry = {"pilot_id": aid, "at": ev["occurred_at"], **p}
        state.notifications.append(entry)
        state.pilots[aid].notifications.append(entry)

    else:
        raise DomainError(f"未知事件类型：{etype}")
