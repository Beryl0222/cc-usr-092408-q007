"""命令服务：把居民诉求、观测、审批与复核命令转成只追加事件。

设计要点
- 事件构造与业务校验纯函数化（build_*），Service 负责加载事件流、折叠状态、
  构造事件、原子追加、再折叠。重启 = 重新加载同一文件；无其他易失状态。
- 批量导入逐条 try/except：坏记录进隔离区，其余好记录在同一批次原子提交。
- 观测按业务发生时间（occurred_at）进入；版本/序号在同一命令批次内连续分配，
  保证「同一事件流重复回放结果一致」。
"""
from __future__ import annotations

import hashlib
import re
from collections import Counter
from dataclasses import dataclass, field

from .ledger import (
    DECISION_EXPAND,
    DECISION_EXTEND,
    DECISION_TERMINATE,
    FEEDBACK,
    METRIC_DEVIATION_FLAGGED,
    NEED_LOGGED,
    NEEDS_CLUSTERED,
    NOTIFICATION_SENT,
    OBSERVATION,
    OBSERVATION_RECORDED,
    PILOT_APPROVED,
    PILOT_DECIDED,
    RESOURCE_USAGE_LOGGED,
    RESOURCES_ADJUSTED,
    RESOURCES_RESERVED,
    RESOURCES_RELEASED,
    REVIEW_RESOLVED,
    REVIEW_SCHEDULED,
    SAFETY_HALT_TRIGGERED,
    DomainError,
    EventStore,
    content_event_id,
    fold,
    iso,
    parse_ts,
    utcnow,
)

AGGREGATE_TYPES = {
    NEED_LOGGED: "community_need",
    NEEDS_CLUSTERED: "need_cluster",
    PILOT_APPROVED: "pilot_measure",
    RESOURCES_RESERVED: "pilot_measure",
    RESOURCES_ADJUSTED: "pilot_measure",
    RESOURCES_RELEASED: "pilot_measure",
    RESOURCE_USAGE_LOGGED: "pilot_measure",
    OBSERVATION_RECORDED: "observation_window",
    SAFETY_HALT_TRIGGERED: "pilot_measure",
    METRIC_DEVIATION_FLAGGED: "pilot_measure",
    REVIEW_SCHEDULED: "pilot_measure",
    REVIEW_RESOLVED: "pilot_measure",
    PILOT_DECIDED: "pilot_measure",
    NOTIFICATION_SENT: "pilot_measure",
}

# 不进入存储的个人标识：手机号、证件号、邮箱、@账号、姓名提示词
_PII_PATTERNS = [
    (re.compile(r"1[3-9]\d{9}"), "[手机号]"),
    (re.compile(r"\d{17}[\dXx]"), "[证件号]"),
    (re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+"), "[邮箱]"),
    (re.compile(r"@[\w一-龥_-]{2,}"), "@[账号]"),
    (re.compile(r"(我是|我叫|姓名[:：\s]?|联系电话[:：\s]?)[一-龥]{2,4}"), ""),
]


def deidentify(text: str) -> str:
    """去标识：剥离个人标识，只保留诉求摘要文本。"""
    if not isinstance(text, str) or not text.strip():
        raise DomainError("诉求文本为空")
    cleaned = text.strip()
    for pattern, repl in _PII_PATTERNS:
        cleaned = pattern.sub(repl, cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if not cleaned:
        raise DomainError("去标识后无有效内容")
    return cleaned


def _hash_id(prefix: str, *parts: str) -> str:
    digest = hashlib.sha256("".join(parts).encode("utf-8")).hexdigest()[:16]
    return f"{prefix}-{digest}"


# ---------------------------------------------------------------------------
# 事件构造（纯函数）
# ---------------------------------------------------------------------------

def make_event(state, event_type: str, aggregate_id: str, payload: dict,
               occurred_at: str | None = None) -> dict:
    """基于当前折叠状态分配版本并构造事件（不修改状态）。"""
    version = state.versions.get(aggregate_id, 0) + 1
    ev = {
        "event_id": content_event_id(event_type, aggregate_id, version, payload),
        "event_type": event_type,
        "aggregate_type": AGGREGATE_TYPES[event_type],
        "aggregate_id": aggregate_id,
        "occurred_at": occurred_at or iso(utcnow()),
        "version": version,
        "summary": _event_summary(event_type, aggregate_id, payload),
        "payload": payload,
    }
    return ev


def _event_summary(event_type: str, aggregate_id: str, payload: dict) -> str:
    table = {
        NEED_LOGGED: "登记去标识居民诉求",
        NEEDS_CLUSTERED: "生成建议性诉求聚类",
        PILOT_APPROVED: "审批并冻结试点",
        RESOURCES_RESERVED: "原子预留名额与预算",
        RESOURCES_ADJUSTED: "扩大试点并追加资源",
        RESOURCES_RELEASED: "原子释放名额与预算",
        RESOURCE_USAGE_LOGGED: "登记已发生资源使用",
        OBSERVATION_RECORDED: "记录环境观测或居民反馈",
        SAFETY_HALT_TRIGGERED: "触及安全停止线，立即暂停",
        METRIC_DEVIATION_FLAGGED: "普通指标偏离，进入复核",
        REVIEW_SCHEDULED: "安排到期评审",
        REVIEW_RESOLVED: "完成复核",
        PILOT_DECIDED: "记录扩大/延长/终止决策与证据",
        NOTIFICATION_SENT: "记录已发出的通知",
    }
    return f"{table[event_type]}（{aggregate_id}）"


# ---------------------------------------------------------------------------
# 服务
# ---------------------------------------------------------------------------

@dataclass
class ImportResult:
    accepted: list[dict] = field(default_factory=list)   # 生成的事件
    rejected: list[dict] = field(default_factory=list)   # 隔离的坏记录


@dataclass
class IngestionResult:
    recorded: list[dict] = field(default_factory=list)
    duplicate_keys: list[str] = field(default_factory=list)
    safety_halt: dict | None = None
    deviations: list[dict] = field(default_factory=list)


class LedgerService:
    def __init__(self, path: str, budget_cap: float = 100_000.0,
                 global_quota_cap: int = 1_000, clock=None):
        self.store = EventStore(path)
        self.budget_cap = float(budget_cap)
        self.global_quota_cap = int(global_quota_cap)
        # 可注入时钟（无参返回 aware datetime）；默认 UTC 当前时间。
        self.clock = clock or utcnow
        self.state = fold(self.store.load())

    def _ev(self, state, event_type: str, aggregate_id: str, payload: dict,
            occurred_at: str | None = None) -> dict:
        """事件构造收口于此，未显式给时间时使用可注入时钟（保证回放可复现）。"""
        return make_event(state, event_type, aggregate_id, payload,
                          occurred_at or iso(self.clock()))

    # -- 内部持久化 --
    def _commit(self, events: list[dict]) -> list[dict]:
        if not events:
            return []
        self.store.append(events)
        self.state = fold(self.store.load())
        return events

    # ------------------------------------------------------------------
    # 诉求登记：只保存去标识摘要 + 来源群体
    # ------------------------------------------------------------------
    def log_need(self, raw_text: str, source_group: str, category: str | None = None,
                 occurred_at: str | None = None, dedup_key: str | None = None) -> dict:
        summary = deidentify(raw_text)
        if not source_group or not isinstance(source_group, str):
            raise DomainError("来源群体必填")
        key = dedup_key or hashlib.sha256(summary.encode("utf-8")).hexdigest()[:16]
        need_id = _hash_id("need", source_group, key)
        if need_id in self.state.needs:
            raise DomainError(f"重复诉求已隔离：{need_id}")
        payload = {
            "summary": summary,
            "source_group": source_group,
            "category": category,
            "dedup_key": key,
        }
        ev = self._ev(self.state, NEED_LOGGED, need_id, payload, occurred_at)
        self._commit([ev])
        return ev

    def import_needs(self, records: list[dict]) -> ImportResult:
        """批量导入：逐条隔离坏记录（含重复、含个人信息无法清洗等），好记录整批提交。

        每条记录形如 {"text", "source_group", "category"?, "occurred_at"?}。
        """
        staged: list[dict] = []
        result = ImportResult()
        # 在暂存视图上校验，避免同批内重复键也漏网
        preview = self.state
        for idx, rec in enumerate(records):
            try:
                if not isinstance(rec, dict):
                    raise DomainError("记录不是对象")
                summary = deidentify(rec.get("text", ""))
                source_group = rec.get("source_group")
                if not source_group:
                    raise DomainError("缺少来源群体")
                key = rec.get("dedup_key") or hashlib.sha256(
                    summary.encode("utf-8")).hexdigest()[:16]
                need_id = _hash_id("need", source_group, key)
                if need_id in preview.needs or any(
                        e["aggregate_id"] == need_id for e in staged):
                    raise DomainError("重复诉求")
                payload = {
                    "summary": summary,
                    "source_group": source_group,
                    "category": rec.get("category"),
                    "dedup_key": key,
                }
                ev = self._ev(preview, NEED_LOGGED, need_id, payload,
                                rec.get("occurred_at"))
                staged.append(ev)
            except DomainError as exc:
                result.rejected.append({"index": idx, "record": rec, "reason": str(exc)})
            except (AttributeError, TypeError) as exc:
                result.rejected.append({"index": idx, "record": rec, "reason": f"字段类型错误：{exc}"})
        # 好记录一次性原子提交（坏记录不影响其余记录）
        committed = self._commit(staged)
        result.accepted = committed
        return result

    # ------------------------------------------------------------------
    # 聚类：结果是建议（advisory），不是事实
    # ------------------------------------------------------------------
    def cluster_needs(self, need_ids: list[str], label: str,
                      method: str = "keyword") -> dict:
        if not need_ids:
            raise DomainError("聚类至少包含一条诉求")
        missing = [n for n in need_ids if n not in self.state.needs]
        if missing:
            raise DomainError(f"引用了不存在的诉求：{missing[:3]}")
        cluster_id = _hash_id("cluster", label, "|".join(sorted(need_ids)))
        if cluster_id in self.state.clusters:
            raise DomainError("相同输入的建议性聚类已存在")
        payload = {
            "need_ids": list(need_ids),
            "label": label,
            "method": method,
            "advisory": True,
        }
        ev = self._ev(self.state, NEEDS_CLUSTERED, cluster_id, payload)
        self._commit([ev])
        return ev

    # ------------------------------------------------------------------
    # 试点审批：冻结区域、时间窗、指标、停止条件、批准人 + 原子预留资源
    # ------------------------------------------------------------------
    def approve_pilot(self, pilot_id: str, cluster_id: str, zone: str,
                      window: dict, success_metrics: dict, safety_limits: dict,
                      approver: str, quota: int, budget: float,
                      control_zones: list[str] | None = None) -> list[dict]:
        if pilot_id in self.state.pilots:
            raise DomainError("试点已存在；冻结后不得重新审批或改写")
        if cluster_id not in self.state.clusters:
            raise DomainError("试点必须基于一个建议性聚类")
        start = parse_ts(window["start"])
        end = parse_ts(window["end"])
        if end <= start:
            raise DomainError("时间窗结束必须晚于开始")
        if not success_metrics:
            raise DomainError("成功指标在冻结时必须明确")
        if not safety_limits:
            raise DomainError("安全停止条件在冻结时必须明确")
        if not approver:
            raise DomainError("批准人必填")
        if quota <= 0 or budget < 0:
            raise DomainError("名额必须为正，预算不可为负")

        zones = [zone] + list(control_zones or [])
        # 对照区域不能同时参加冲突试点：运行/暂停/复核中的试点都仍占用区域。
        taken = self.state.zones_taken()
        clash = [z for z in zones if z in taken]
        if clash:
            raise DomainError(f"区域已被未结束试点占用（对照冲突）：{clash}")
        # 名额与预算原子预留：任一超限则整笔拒绝，不产生半个预留。
        self._check_caps(quota, budget, zones)

        approved_payload = {
            "cluster_id": cluster_id,
            "zone": zone,
            "control_zones": list(control_zones or []),
            "window": {"start": iso(start), "end": iso(end)},
            "success_metrics": success_metrics,
            "safety_limits": safety_limits,
            "approver": approver,
            "frozen": True,
        }
        reserve_payload = {
            "quota": quota,
            "budget": budget,
            "zones": zones,
        }
        events = [
            self._ev(self.state, PILOT_APPROVED, pilot_id, approved_payload),
        ]
        # 第二条事件基于第一条后的版本分配
        mid = fold(self.store.load() + events)
        events.append(self._ev(mid, RESOURCES_RESERVED, pilot_id, reserve_payload))
        return self._commit(events)

    def _check_caps(self, add_quota: int, add_budget: float,
                    new_zones: list[str] | None = None, pilot_id: str | None = None) -> None:
        active = self.state.active_reservations()
        # 扩大时试点自身已占名额/预算仍计入总额；仅区域冲突判断排除自身。
        used_quota = sum(r.get("quota", 0) for r in active.values())
        used_budget = sum(r.get("budget", 0.0) for r in active.values())
        if used_quota + add_quota > self.global_quota_cap:
            raise DomainError(
                f"名额原子预留失败：{used_quota}+{add_quota} 超过总名额 {self.global_quota_cap}")
        if used_budget + add_budget > self.budget_cap:
            raise DomainError(
                f"预算原子预留失败：{used_budget}+{add_budget} 超过总预算 {self.budget_cap}")
        if new_zones:
            taken = self.state.zones_taken()
            clash = [z for z in new_zones if z in taken and taken[z] != pilot_id]
            if clash:
                raise DomainError(f"区域已被未结束试点占用（对照冲突）：{clash}")

    # ------------------------------------------------------------------
    # 观测/反馈：幂等 + 按发生时间 + 自动评估安全线/普通偏离
    # ------------------------------------------------------------------
    def record_observation(self, pilot_id: str, kind: str, metric: str, value: float,
                           idempotency_key: str, occurred_at: str,
                           source_group: str | None = None) -> IngestionResult:
        return self.ingest_observations(pilot_id, [{
            "kind": kind, "metric": metric, "value": value,
            "idempotency_key": idempotency_key, "occurred_at": occurred_at,
            "source_group": source_group,
        }])

    def ingest_observations(self, pilot_id: str, records: list[dict]) -> IngestionResult:
        if pilot_id not in self.state.pilots:
            raise DomainError("试点不存在")
        pilot = self.state.pilots[pilot_id]
        result = IngestionResult()

        # 1) 过滤重传（同一幂等键不重复计数），并按业务发生时间排序。
        unique: list[dict] = []
        batch_keys: set[str] = set()
        for rec in records:
            key = rec.get("idempotency_key")
            if not key:
                raise DomainError("观测缺少幂等键，无法防重")
            if key in self.state.observations_seen or key in batch_keys:
                result.duplicate_keys.append(key)
                continue
            if rec.get("kind") not in (OBSERVATION, FEEDBACK):
                raise DomainError("kind 只能是 observation 或 feedback")
            ts = parse_ts(rec["occurred_at"])  # 格式错误整体拒绝
            batch_keys.add(key)
            unique.append({**rec, "_ts": ts})
        unique.sort(key=lambda r: r["_ts"])

        events: list[dict] = []
        preview = fold(self.store.load())  # 在暂存流上推进，保证版本/评估正确
        for rec in unique:
            payload = {
                "kind": rec["kind"],
                "metric": rec["metric"],
                "value": rec["value"],
                "source_group": rec.get("source_group"),
                "occurred_at": iso(rec["_ts"]),
                "idempotency_key": rec["idempotency_key"],
            }
            events.append(self._ev(preview, OBSERVATION_RECORDED, pilot_id, payload,
                                     iso(rec["_ts"])))
            preview = fold(self.store.load() + events)

            # 2) 自动评估：安全停止线优先，立即暂停受影响区域；否则普通偏离进复核。
            assessment = self._assess(preview, pilot_id, rec)
            if assessment == "safety":
                halt_payload = {
                    "reason": self._safety_reason(pilot, rec),
                    "metric": rec["metric"],
                    "observed": rec["value"],
                    "limit": pilot.safety_limits.get(rec["metric"]),
                    "trigger_observation": rec["idempotency_key"],
                    "zones": self._pilot_zones(preview, pilot_id),
                }
                halt_ev = self._ev(preview, SAFETY_HALT_TRIGGERED, pilot_id, halt_payload)
                events.append(halt_ev)
                preview = fold(self.store.load() + events)
                note = self._ev(
                    preview, NOTIFICATION_SENT, pilot_id,
                    {"channel": "ops", "message": f"区域立即暂停：{halt_payload['reason']}"},
                    iso(rec["_ts"]))
                events.append(note)
                preview = fold(self.store.load() + events)
                result.safety_halt = halt_ev
            elif assessment == "deviation":
                review_id = _hash_id("review", pilot_id, rec["idempotency_key"])
                flag_payload = {
                    "review_id": review_id,
                    "metric": rec["metric"],
                    "observed": rec["value"],
                    "expected": pilot.success_metrics.get(rec["metric"]),
                    "trigger_observation": rec["idempotency_key"],
                }
                flag_ev = self._ev(preview, METRIC_DEVIATION_FLAGGED, pilot_id,
                                     flag_payload, iso(rec["_ts"]))
                events.append(flag_ev)
                preview = fold(self.store.load() + events)
                result.deviations.append(flag_ev)

        committed = self._commit(events)
        result.recorded = [e for e in committed if e["event_type"] == OBSERVATION_RECORDED]
        return result

    def _pilot_zones(self, state, pilot_id: str) -> list[str]:
        res = state.active_reservations().get(pilot_id)
        return list(res["zones"]) if res else [state.pilots[pilot_id].zone]

    def _safety_reason(self, pilot, rec) -> str:
        return f"{rec['metric']}={rec['value']} 触及安全停止线 {pilot.safety_limits.get(rec['metric'])}"

    def _assess(self, state, pilot_id: str, rec: dict) -> str | None:
        """返回 'safety' / 'deviation' / None。安全线优先于普通偏离。"""
        pilot = state.pilots[pilot_id]
        if pilot.status in ("terminated", "safety_halted"):
            return None
        metric, value = rec["metric"], rec["value"]
        limit = pilot.safety_limits.get(metric)
        if limit is not None and _breaches(value, limit):
            return "safety"
        target = pilot.success_metrics.get(metric)
        if target is not None and _deviates(value, target):
            return "deviation"
        return None

    # ------------------------------------------------------------------
    # 复核
    # ------------------------------------------------------------------
    def resolve_review(self, review_id: str, resolution: str, approver: str) -> dict:
        rv = self.state.reviews.get(review_id)
        if not rv or rv.get("status") not in ("open", "scheduled"):
            raise DomainError("复核不存在或已处理")
        pilot_id = rv["pilot_id"]
        payload = {"review_id": review_id, "resolution": resolution, "approver": approver}
        ev = self._ev(self.state, REVIEW_RESOLVED, pilot_id, payload)
        self._commit([ev])
        return ev

    def schedule_review(self, pilot_id: str, due_at: str, reason: str = "到期评审") -> dict:
        if pilot_id not in self.state.pilots:
            raise DomainError("试点不存在")
        if self.state.pilots[pilot_id].status == "terminated":
            raise DomainError("已终止试点不再安排评审")
        review_id = _hash_id("sched-review", pilot_id, due_at)
        payload = {"review_id": review_id, "due_at": iso(parse_ts(due_at)), "reason": reason}
        ev = self._ev(self.state, REVIEW_SCHEDULED, pilot_id, payload)
        self._commit([ev])
        return ev

    def due_reviews(self, now: str | None = None) -> list[dict]:
        """重启后继续到期评审：完全由事件流重放得出，无独立定时器状态。"""
        moment = parse_ts(now) if now else utcnow()
        due = []
        for rv in self.state.reviews.values():
            due_at = rv.get("due_at")
            pilot = self.state.pilots.get(rv.get("pilot_id", ""))
            if (due_at and rv.get("status") == "scheduled" and parse_ts(due_at) <= moment
                    and pilot and pilot.status != "terminated"):
                due.append(rv)
        return due

    # ------------------------------------------------------------------
    # 已发生资源使用与通知（回滚后仍保留）
    # ------------------------------------------------------------------
    def log_resource_usage(self, pilot_id: str, amount: float, kind: str,
                           occurred_at: str | None = None) -> dict:
        if pilot_id not in self.state.pilots:
            raise DomainError("试点不存在")
        if amount < 0:
            raise DomainError("使用量不可为负")
        payload = {"kind": kind, "amount": amount}
        ev = self._ev(self.state, RESOURCE_USAGE_LOGGED, pilot_id, payload, occurred_at)
        self._commit([ev])
        return ev

    def send_notification(self, pilot_id: str, channel: str, message: str,
                          occurred_at: str | None = None) -> dict:
        if pilot_id not in self.state.pilots:
            raise DomainError("试点不存在")
        payload = {"channel": channel, "message": message}
        ev = self._ev(self.state, NOTIFICATION_SENT, pilot_id, payload, occurred_at)
        self._commit([ev])
        return ev

    # ------------------------------------------------------------------
    # 扩大 / 延长 / 终止（回滚）——证据快照使决策可解释
    # ------------------------------------------------------------------
    def decide(self, pilot_id: str, decision: str, approver: str, reason: str = "",
               add_quota: int = 0, add_budget: float = 0.0, add_zones: list[str] | None = None,
               new_window_end: str | None = None,
               now: str | None = None) -> list[dict]:
        if decision not in (DECISION_EXPAND, DECISION_EXTEND, DECISION_TERMINATE):
            raise DomainError("决策只能是 expand/extend/terminate")
        pilot = self.state.pilots.get(pilot_id)
        if not pilot:
            raise DomainError("试点不存在")
        if pilot.status == "terminated":
            raise DomainError("已终止试点不可再做决策")
        if pilot.status == "safety_halted" and decision != DECISION_TERMINATE:
            raise DomainError("安全暂停期间只能终止（回滚），不得扩大或延长")
        if pilot.status == "review" and decision != DECISION_TERMINATE:
            raise DomainError("存在未结复核（普通指标偏离），复核完成前不得扩大或延长")
        if not approver:
            raise DomainError("批准人必填")
        moment = parse_ts(now) if now else utcnow()

        events: list[dict] = []
        preview = fold(self.store.load())

        if decision == DECISION_EXPAND:
            add_zones = add_zones or []
            if add_quota <= 0 and add_budget <= 0 and not add_zones:
                raise DomainError("扩大必须追加名额、预算或区域")
            # 资源原子追加 + 对照冲突检查；任一失败整笔不提交。
            self._check_caps(add_quota, add_budget, add_zones, pilot_id=pilot_id)
            adjust_payload = {
                "add_quota": add_quota,
                "add_budget": add_budget,
                "add_zones": add_zones,
            }
            events.append(self._ev(preview, RESOURCES_ADJUSTED, pilot_id, adjust_payload))
            preview = fold(self.store.load() + events)

        elif decision == DECISION_EXTEND:
            if not new_window_end:
                raise DomainError("延长必须给出新的结束时间")
            if parse_ts(new_window_end) <= pilot.ended_at:
                raise DomainError("延长后的结束时间必须晚于当前冻结结束时间")

        # 证据快照：只引用决策时刻之前（含）已入流的数据。后来数据无法重写本决策。
        evidence = self._evidence_snapshot(pilot_id, moment)
        decide_payload = {
            "decision": decision,
            "approver": approver,
            "reason": reason,
            "evidence": evidence,
        }
        if decision == DECISION_EXTEND:
            decide_payload["new_window_end"] = iso(parse_ts(new_window_end))
        events.append(self._ev(preview, PILOT_DECIDED, pilot_id, decide_payload, iso(moment)))
        preview = fold(self.store.load() + events)

        if decision == DECISION_TERMINATE:
            # 回滚：原子释放名额与预算；已发生使用与通知不删除。
            active = preview.active_reservations().get(pilot_id)
            if active:
                release_payload = {
                    "quota": active.get("quota", 0),
                    "budget": active.get("budget", 0),
                    "zones": list(active.get("zones", [])),
                    "reason": "试点终止，原子释放",
                }
                events.append(self._ev(preview, RESOURCES_RELEASED, pilot_id,
                                         release_payload, iso(moment)))
                preview = fold(self.store.load() + events)
            note = self._ev(
                preview, NOTIFICATION_SENT, pilot_id,
                {"channel": "public", "message": f"试点终止（回滚）：{reason or '见决策记录'}"},
                iso(moment))
            events.append(note)
            preview = fold(self.store.load() + events)

        return self._commit(events)

    def _evidence_snapshot(self, pilot_id: str, moment) -> dict:
        """汇总决策时点可用的数据，供事后解释「由哪些数据支持」。"""
        obs = [o for o in self.state.observations
               if o["pilot_id"] == pilot_id and o["occurred_at"] <= moment]
        metric_rows = Counter()
        values: dict[str, list[float]] = {}
        for o in obs:
            if o.get("metric"):
                metric_rows[o["metric"]] += 1
                values.setdefault(o["metric"], []).append(o["value"])
        open_reviews = [r["review_id"] for r in self.state.reviews.values()
                        if r.get("pilot_id") == pilot_id and r.get("status") == "open"]
        return {
            "as_of": iso(moment),
            "observation_count": len(obs),
            "feedback_count": sum(1 for o in obs if o["kind"] == FEEDBACK),
            "metrics": {
                m: {"count": metric_rows[m],
                    "latest": values[m][-1],
                    "min": min(values[m]),
                    "max": max(values[m])}
                for m in values
            },
            "open_reviews": open_reviews,
            "safety_halted": self.state.pilots[pilot_id].status == "safety_halted",
            "observation_keys": [o["idempotency_key"] for o in obs],
        }

    def explain_decision(self, pilot_id: str, index: int = -1) -> dict:
        """返回某次扩大/延长/终止及其证据，便于向管理者解释。"""
        pilot = self.state.pilots.get(pilot_id)
        if not pilot or not pilot.decisions:
            raise DomainError("该试点暂无决策记录")
        record = pilot.decisions[index]
        return {
            "pilot_id": pilot_id,
            "status": pilot.status,
            "frozen": {
                "zone": pilot.zone,
                "window": {"start": iso(pilot.started_at), "end": iso(pilot.ended_at)},
                "success_metrics": pilot.success_metrics,
                "safety_limits": pilot.safety_limits,
                "approver": pilot.approver,
            },
            "decision": record,
            "resource_usage_kept": [u for u in self.state.usages if u["pilot_id"] == pilot_id],
            "notifications_kept": pilot.notifications,
        }


def _breaches(value: float, limit) -> bool:
    """安全线表达：{"max": x} 或 {"min": x}。"""
    if isinstance(limit, dict):
        if "max" in limit and value > limit["max"]:
            return True
        if "min" in limit and value < limit["min"]:
            return True
        return False
    return value > limit  # 简写：裸数字视为上限


def _deviates(value: float, target) -> bool:
    """成功指标表达：{"min": x} / {"max": x} / {"target": x, "tolerance": 比例}。"""
    if isinstance(target, dict):
        if "min" in target and value < target["min"]:
            return True
        if "max" in target and value > target["max"]:
            return True
        if "target" in target:
            tol = target.get("tolerance", 0.1)
            if abs(value - target["target"]) > abs(target["target"]) * tol:
                return True
        return False
    return False