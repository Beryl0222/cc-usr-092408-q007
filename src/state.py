"""事件流 -> 当前状态的纯函数折叠。

fold(events) 不做任何 I/O，也不依赖当前墙钟：给定同一序列的事件，
必定得到完全相同的状态。这是"同一事件流重复回放结论一致"的落点。
后来数据只会在折叠结果上叠加，永不修改早期事件本身。

资源记账模型（预算与名额同构）：
- 资源池持有 total / reserved / used 三个标量；
- 试点持有按冻结版本分层的预留余量 reserve_tiers；
- 已发生使用按 FIFO 消耗各层余量，used 永不冲销；
- 终止原子释放全部剩余层；回滚扩大只移除最新层、回滚终止按快照重建各层。
折叠层是唯一记账处，服务层不自行推算资源数字。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .events import (
    BATCH_IMPORTED,
    BATCH_QUARANTINED,
    CLUSTER_PROPOSED,
    FEEDBACK_RECORDED,
    NEED_LOGGED,
    NOTIFICATION_SENT,
    OBSERVATION_RECORDED,
    PILOT_APPROVED,
    PILOT_DECIDED,
    PILOT_PAUSED,
    PILOT_RESUMED,
    PILOT_REJECTED,
    PILOT_ROLLED_BACK,
    POOL_FUNDED,
    RESOURCE_USED,
    REVIEW_HELD,
    REVIEW_SCHEDULED,
    SAFETY_STOP_TRIGGERED,
    Event,
    parse_at,
)


@dataclass
class Pilot:
    pilot_id: str
    title: str = ""
    approver: str = ""
    cluster_ids: list[str] = field(default_factory=list)
    zones: list[str] = field(default_factory=list)
    control_zones: list[str] = field(default_factory=list)
    conflict_tags: list[str] = field(default_factory=list)
    window_start: str = ""
    window_end: str = ""
    metrics: dict[str, dict[str, float]] = field(default_factory=dict)
    safety_limits: dict[str, float] = field(default_factory=dict)
    bias_threshold: float = 1.0
    required_sample_size: int = 1
    review_within_hours: int = 48
    pool_id: str = ""
    used_budget: float = 0.0
    used_slots: int = 0
    # [{spec_version, budget, slots}]：按冻结版本分层的未使用预留余量。
    reserve_tiers: list[dict[str, Any]] = field(default_factory=list)
    spec_version: int = 1
    status: str = "approved"  # approved | paused | terminated
    paused_zones: set[str] = field(default_factory=set)
    active_stops: dict[str, dict[str, Any]] = field(default_factory=dict)
    observations: dict[str, dict[str, Any]] = field(default_factory=dict)
    feedback: dict[str, dict[str, Any]] = field(default_factory=dict)
    reviews: dict[str, dict[str, Any]] = field(default_factory=dict)
    decisions: list[dict[str, Any]] = field(default_factory=list)
    rollbacks: list[dict[str, Any]] = field(default_factory=list)
    history: list[dict[str, Any]] = field(default_factory=list)

    @property
    def reserved_budget(self) -> float:
        return sum(t["budget"] for t in self.reserve_tiers)

    @property
    def reserved_slots(self) -> int:
        return sum(t["slots"] for t in self.reserve_tiers)

    # ---- 派生指标：只统计冻结时间窗内、未标记异常且区域未被安全暂停的读数 ----
    def counted_observations(self) -> list[dict[str, Any]]:
        from .events import parse_at

        start = parse_at(self.window_start) if self.window_start else None
        end = parse_at(self.window_end) if self.window_end else None
        selected: list[dict[str, Any]] = []
        for obs in self.observations.values():
            if obs["anomaly"] or set(obs["zones"]) & self.paused_zones:
                continue
            when = parse_at(obs["observed_at"])
            if start is not None and when < start:
                continue
            if end is not None and when > end:
                continue
            selected.append(obs)
        return sorted(selected, key=lambda o: o["observed_at"])

    def metric_aggregates(self) -> dict[str, dict[str, float]]:
        agg: dict[str, list[float]] = {}
        for obs in self.counted_observations():
            for reading in obs["readings"]:
                if reading["kind"] != "metric" or reading.get("anomaly"):
                    continue
                agg.setdefault(reading["metric"], []).append(float(reading["value"]))
        return {
            metric: {
                "count": float(len(values)),
                "latest": values[-1],
                "mean": sum(values) / len(values),
                "min": min(values),
                "max": max(values),
            }
            for metric, values in agg.items()
        }

    def feedback_group_share(self) -> dict[str, float]:
        total = len(self.feedback)
        if total == 0:
            return {}
        counts: dict[str, int] = {}
        for item in self.feedback.values():
            counts[item["source_group"]] = counts.get(item["source_group"], 0) + 1
        return {group: count / total for group, count in counts.items()}


@dataclass
class State:
    needs: dict[str, dict[str, Any]] = field(default_factory=dict)
    clusters: dict[str, dict[str, Any]] = field(default_factory=dict)
    pools: dict[str, dict[str, Any]] = field(default_factory=dict)
    pilots: dict[str, Pilot] = field(default_factory=dict)
    notifications: list[dict[str, Any]] = field(default_factory=list)
    batches: dict[str, dict[str, Any]] = field(default_factory=dict)
    rejected_pilots: dict[str, dict[str, Any]] = field(default_factory=dict)

    # ---- 查询 ----
    def pool_available(self, pool_id: str) -> tuple[float, int]:
        pool = self.pools[pool_id]
        budget_available = pool["budget_total"] - pool["budget_reserved"] - pool["budget_used"]
        slots_available = pool["slots_total"] - pool["slots_reserved"] - pool["slots_used"]
        return budget_available, slots_available

    def zone_conflicts(
        self,
        zones: list[str],
        control_zones: list[str],
        tags: list[str],
        exclude_pilot_id: str | None = None,
    ) -> list[str]:
        """返回当前不能同时参加冲突试点的占用说明。"""
        conflicts: list[str] = []
        wanted = set(zones) | set(control_zones)
        for pilot in self.pilots.values():
            if pilot.pilot_id == exclude_pilot_id or pilot.status == "terminated":
                continue
            held = set(pilot.zones) | set(pilot.control_zones)
            overlap = wanted & held
            if not overlap:
                continue
            shared_tags = set(tags) & set(pilot.conflict_tags)
            if shared_tags:
                conflicts.append(
                    f"区域 {sorted(overlap)} 已被同属冲突类 {sorted(shared_tags)} 的试点 {pilot.pilot_id} 占用"
                )
            # 对照区域在任何在跑试点期内都不得再入组（即使诉求不同）。
            control_overlap = wanted & set(pilot.control_zones)
            if control_overlap:
                conflicts.append(
                    f"区域 {sorted(control_overlap)} 是试点 {pilot.pilot_id} 的对照区域，试点期内不得入组"
                )
        return conflicts

    def due_reviews(self, now: str) -> list[tuple[str, str]]:
        now_dt = parse_at(now)
        due: list[tuple[str, str]] = []
        for pilot_id, pilot in self.pilots.items():
            for review_id, review in pilot.reviews.items():
                if review["status"] == "scheduled" and parse_at(review["due_at"]) <= now_dt:
                    due.append((pilot_id, review_id))
        due.sort(key=lambda pair: (pair[1], pair[0]))
        return due

    def explain_decision(self, pilot_id: str, decision_id: str) -> dict[str, Any]:
        pilot = self.pilots[pilot_id]
        decision = next((d for d in pilot.decisions if d["decision_id"] == decision_id), None)
        if decision is None:
            raise KeyError(f"试点 {pilot_id} 没有决策 {decision_id}")
        return {
            "pilot_id": pilot_id,
            "decision": decision,
            "frozen_spec_before": _last_spec(pilot.history, decision["prior_spec_version"]),
            "rollback": next((r for r in pilot.rollbacks if r["decision_id"] == decision_id), None),
        }


def _last_spec(history: list[dict[str, Any]], spec_version: int) -> dict[str, Any] | None:
    return next(
        (h for h in reversed(history) if h.get("kind") == "spec" and h["spec_version"] == spec_version),
        None,
    )


def fold(events: list[Event]) -> State:
    state = State()
    # 已持久化的事件按全局 seq 回放；服务层用于预判的暂存事件 seq=0，
    # 保持其在输入流中的相对顺序（它们本就排在历史之后）。
    if events and all(e.seq for e in events):
        ordered = sorted(events, key=lambda e: e.seq)
    else:
        ordered = list(events)
    for event in ordered:
        _apply(state, event)
    return state


def state_digest(state: State) -> str:
    """状态的确定性指纹：只依赖规范化后的领域状态，与内存对象地址无关。

    同一事件流重复回放（或换进程重载后回放）必须得到相同指纹，
    用于核对"决策与资源状态一致"这一硬约束。
    """
    import hashlib
    import json

    def pilot_view(pilot: Pilot) -> dict[str, Any]:
        return {
            "pilot_id": pilot.pilot_id,
            "status": pilot.status,
            "spec_version": pilot.spec_version,
            "zones": pilot.zones,
            "control_zones": pilot.control_zones,
            "window": [pilot.window_start, pilot.window_end],
            "reserve_tiers": pilot.reserve_tiers,
            "used": [pilot.used_budget, pilot.used_slots],
            "paused_zones": sorted(pilot.paused_zones),
            "observations": sorted(pilot.observations.keys()),
            "feedback": sorted(pilot.feedback.keys()),
            "reviews": sorted(
                (rid, r["status"]) for rid, r in pilot.reviews.items()
            ),
            "stops": sorted(
                (sid, s["resolved"]) for sid, s in pilot.active_stops.items()
            ),
            "decisions": [
                [d["decision_id"], d["decision"], d.get("released")]
                for d in pilot.decisions
            ],
            "rollbacks": [
                [r["rollback_id"], r["decision_id"], r["released"]]
                for r in pilot.rollbacks
            ],
        }

    view = {
        "needs": sorted(state.needs.keys()),
        "clusters": sorted(
            (cid, c["proposed_theme"], c["advisory"]) for cid, c in state.clusters.items()
        ),
        "pools": [
            [pid, pool["budget_total"], pool["budget_reserved"], pool["budget_used"],
             pool["slots_total"], pool["slots_reserved"], pool["slots_used"]]
            for pid, pool in sorted(state.pools.items())
        ],
        "pilots": [pilot_view(state.pilots[pid]) for pid in sorted(state.pilots)],
        "notifications": [n["subject"] + "|" + n["recipient_scope"] for n in state.notifications],
        "batches": sorted(
            (bid, b.get("accepted"), b.get("rejected"), b.get("ignored"),
             len(b.get("quarantined", [])))
            for bid, b in state.batches.items()
        ),
    }
    blob = json.dumps(view, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def fold_digest(events: list[Event]) -> str:
    return state_digest(fold(events))


def _consume_tiers(tiers: list[dict[str, Any]], budget: float, slots: int) -> None:
    """FIFO 消耗预留余量：先耗尽最早冻结的层。"""
    need_b, need_s = budget, slots
    for tier in tiers:
        if need_b > 0:
            take = min(tier["budget"], need_b)
            tier["budget"] -= take
            need_b -= take
        if need_s > 0:
            take = min(tier["slots"], need_s)
            tier["slots"] -= take
            need_s -= take
        if need_b <= 0 and need_s <= 0:
            break


def _apply(state: State, event: Event) -> None:
    p = event.payload
    t = event.event_type

    if t == NEED_LOGGED:
        # 仅保存去标识摘要与来源群体；原始可识别信息不进入账本。
        state.needs[p["need_id"]] = {
            "need_id": p["need_id"],
            "summary": p["summary"],
            "source_group": p["source_group"],
            "theme_tags": p.get("theme_tags", []),
            "recorded_at": event.occurred_at,
        }

    elif t == CLUSTER_PROPOSED:
        # 聚类是"建议"：advisory 恒为真，不回写诉求，也不充当审批事实。
        state.clusters[p["cluster_id"]] = {
            "cluster_id": p["cluster_id"],
            "need_ids": list(p["need_ids"]),
            "proposed_theme": p["proposed_theme"],
            "rationale": p.get("rationale", ""),
            "confidence": p.get("confidence"),
            "advisory": True,
            "proposed_at": event.occurred_at,
        }

    elif t == POOL_FUNDED:
        state.pools[p["pool_id"]] = {
            "pool_id": p["pool_id"],
            "budget_total": float(p["budget"]),
            "slots_total": int(p["slots"]),
            "budget_reserved": 0.0,
            "slots_reserved": 0,
            "budget_used": 0.0,
            "slots_used": 0,
            "currency": p.get("currency", "CNY"),
        }

    elif t == PILOT_APPROVED:
        window = p["window"]
        reserved_budget = float(p.get("budget_total", 0.0))
        reserved_slots = int(p.get("slots", 0))
        pilot = Pilot(
            pilot_id=p["pilot_id"],
            title=p.get("title", p["pilot_id"]),
            approver=p["approver"],
            cluster_ids=list(p.get("cluster_ids", [])),
            zones=list(p["zones"]),
            control_zones=list(p.get("control_zones", [])),
            conflict_tags=list(p.get("conflict_tags", [])),
            window_start=window["start"],
            window_end=window["end"],
            metrics={k: dict(v) for k, v in p.get("metrics", {}).items()},
            safety_limits={k: float(v["max"]) for k, v in p.get("safety_limits", {}).items()},
            bias_threshold=float(p.get("bias_threshold", 1.0)),
            required_sample_size=int(p.get("required_sample_size", 1)),
            review_within_hours=int(p.get("review_within_hours", 48)),
            pool_id=p.get("pool_id", ""),
            reserve_tiers=[{"spec_version": 1, "budget": reserved_budget, "slots": reserved_slots}],
        )
        pilot.history.append(_spec_snapshot(event, pilot))
        state.pilots[pilot.pilot_id] = pilot
        if pilot.pool_id:
            pool = state.pools[pilot.pool_id]
            pool["budget_reserved"] += reserved_budget
            pool["slots_reserved"] += reserved_slots

    elif t == PILOT_REJECTED:
        state.rejected_pilots[p["pilot_id"]] = {
            "pilot_id": p["pilot_id"],
            "reasons": p.get("reasons", []),
            "rejected_at": event.occurred_at,
        }

    elif t == OBSERVATION_RECORDED:
        pilot = state.pilots[p["pilot_id"]]
        oid = p["observation_id"]
        # 重传在服务层已拦截；折叠层同样以自然标识去重，双保险不重复计数。
        if oid in pilot.observations:
            return
        readings = [dict(r) for r in p.get("readings", [])]
        anomaly = bool(p.get("anomaly")) or any(r.get("anomaly") for r in readings)
        pilot.observations[oid] = {
            "observation_id": oid,
            "observed_at": p["observed_at"],
            "recorded_seq": event.seq,
            "zones": list(p.get("zones", pilot.zones)),
            "readings": readings,
            "anomaly": anomaly,
        }

    elif t == FEEDBACK_RECORDED:
        pilot = state.pilots[p["pilot_id"]]
        fid = p["feedback_id"]
        if fid in pilot.feedback:
            return
        pilot.feedback[fid] = {
            "feedback_id": fid,
            "source_group": p["source_group"],
            "rating": p.get("rating"),
            "summary": p.get("summary", ""),
            "observed_at": p.get("observed_at", event.occurred_at),
            "recorded_seq": event.seq,
        }

    elif t == RESOURCE_USED:
        pool = state.pools[p["pool_id"]]
        db = float(p.get("budget_delta", 0.0))
        ds = int(p.get("slots_delta", 0))
        # 已发生使用永久保留；预留按 FIFO 转入已用。
        pool["budget_used"] += db
        pool["slots_used"] += ds
        pool["budget_reserved"] = max(0.0, pool["budget_reserved"] - db)
        pool["slots_reserved"] = max(0, pool["slots_reserved"] - ds)
        pilot = state.pilots[p["pilot_id"]]
        _consume_tiers(pilot.reserve_tiers, db, ds)
        pilot.used_budget += db
        pilot.used_slots += ds

    elif t == REVIEW_SCHEDULED:
        pilot = state.pilots[p["pilot_id"]]
        pilot.reviews[p["review_id"]] = {
            "review_id": p["review_id"],
            "status": "scheduled",
            "due_at": p["due_at"],
            "reasons": list(p.get("reasons", [])),
            "metric": p.get("metric"),
            "trigger_observation_id": p.get("trigger_observation_id"),
            "metric_snapshot": p.get("metric_snapshot"),
            "scheduled_at": event.occurred_at,
        }

    elif t == REVIEW_HELD:
        pilot = state.pilots[p["pilot_id"]]
        review = pilot.reviews[p["review_id"]]
        review.update(
            status="held",
            held_at=p["held_at"],
            finding=p.get("finding", ""),
            recommendation=p["recommendation"],
            evidence=p.get("evidence", {}),
        )

    elif t == SAFETY_STOP_TRIGGERED:
        pilot = state.pilots[p["pilot_id"]]
        pilot.active_stops[p["stop_id"]] = {
            "stop_id": p["stop_id"],
            "indicator": p["indicator"],
            "value": p["value"],
            "limit": p["limit"],
            "zones": list(p["zones"]),
            "observation_id": p.get("observation_id"),
            "triggered_at": event.occurred_at,
            "resolved": False,
        }

    elif t == PILOT_PAUSED:
        pilot = state.pilots[p["pilot_id"]]
        pilot.status = "paused"
        pilot.paused_zones.update(p["zones"])
        pilot.history.append(
            {"spec_version": pilot.spec_version, "kind": "paused", "zones": list(p["zones"]),
             "reason": p.get("reason", ""), "at": event.occurred_at}
        )

    elif t == PILOT_RESUMED:
        pilot = state.pilots[p["pilot_id"]]
        for zone in p["zones"]:
            pilot.paused_zones.discard(zone)
        stop = pilot.active_stops.get(p.get("ref_stop_id"))
        if stop is not None and not (set(stop["zones"]) & pilot.paused_zones):
            stop["resolved"] = True
        if not pilot.paused_zones and pilot.status == "paused":
            pilot.status = "approved"
        pilot.history.append(
            {"spec_version": pilot.spec_version, "kind": "resumed", "zones": list(p["zones"]),
             "at": event.occurred_at, "approver": p.get("approver", "")}
        )

    elif t == NOTIFICATION_SENT:
        # 通知只追加；回滚不删除已发出的通知。
        state.notifications.append(
            {
                "channel": p["channel"],
                "recipient_scope": p["recipient_scope"],
                "subject": p["subject"],
                "body": p.get("body", ""),
                "ref_event_type": p.get("ref_event_type"),
                "sent_at": event.occurred_at,
                "seq": event.seq,
            }
        )

    elif t == PILOT_DECIDED:
        pilot = state.pilots[p["pilot_id"]]
        new_spec = p.get("new_spec") or {}
        # 封存"决策前最后一张冻结快照"：终止不升版本号，事后按版本号回查
        # 会误取到终止后写入的快照，因此必须在任何变更前捕获；状态字段取
        # 实时值（暂停不会产生新的冻结快照）。
        pre_spec_snapshot = _last_spec(pilot.history, p["prior_spec_version"])
        if pre_spec_snapshot is not None:
            pre_spec = dict(pre_spec_snapshot)
            pre_spec["status"] = pilot.status
            pre_spec["paused_zones"] = sorted(pilot.paused_zones)
        else:
            pre_spec = None
        released = None
        released_tiers = None
        if p["decision"] == "TERMINATE":
            # 终止与资源释放在同一事件内原子发生：释放全部剩余预留，已用不回收。
            # 同时封存当时完整分层台账；资源使用可能发生在两次冻结之间，
            # 只有封存分层，回滚终止才能原样补回，而不是误用初始冻结额度。
            released = {"budget": pilot.reserved_budget, "slots": pilot.reserved_slots}
            released_tiers = [dict(tier) for tier in pilot.reserve_tiers]
            if pilot.pool_id:
                pool = state.pools[pilot.pool_id]
                pool["budget_reserved"] = max(0.0, pool["budget_reserved"] - released["budget"])
                pool["slots_reserved"] = max(0, pool["slots_reserved"] - released["slots"])
            pilot.reserve_tiers = []
            pilot.status = "terminated"
        elif p["decision"] == "EXPAND":
            added_budget = float(new_spec.get("budget_total", 0.0))
            added_slots = int(new_spec.get("slots", 0))
            pilot.spec_version += 1
            pilot.reserve_tiers.append(
                {"spec_version": pilot.spec_version, "budget": added_budget, "slots": added_slots}
            )
            for zone in new_spec.get("zones", []):
                if zone not in pilot.zones:
                    pilot.zones.append(zone)
            if pilot.pool_id and (added_budget or added_slots):
                pool = state.pools[pilot.pool_id]
                pool["budget_reserved"] += added_budget
                pool["slots_reserved"] += added_slots
        elif p["decision"] == "EXTEND":
            pilot.spec_version += 1
            if "window_end" in new_spec:
                pilot.window_end = new_spec["window_end"]
            if "window_start" in new_spec:
                pilot.window_start = new_spec["window_start"]
        pilot.decisions.append(
            {
                "decision_id": p["decision_id"],
                "decision": p["decision"],
                "approver": p["approver"],
                "prior_spec_version": p["prior_spec_version"],
                "pre_spec": pre_spec,
                "new_spec": new_spec,
                "evidence": p.get("evidence", {}),
                "released": released,
                "released_tiers": released_tiers,
                "decided_at": event.occurred_at,
            }
        )
        # 终止不是新的冻结版本：写 termination 类条目，避免污染按版本号回查的冻结快照。
        if p["decision"] == "TERMINATE":
            pilot.history.append(
                {"spec_version": pilot.spec_version, "kind": "termination",
                 "decision_id": p["decision_id"], "at": event.occurred_at,
                 "released": released}
            )
        else:
            pilot.history.append(_spec_snapshot(event, pilot))

    elif t == PILOT_ROLLED_BACK:
        pilot = state.pilots[p["pilot_id"]]
        restored_version = p["restored_spec_version"]
        decision = next(d for d in pilot.decisions if d["decision_id"] == p["decision_id"])
        target_spec = decision.get("pre_spec") or _last_spec(pilot.history, restored_version)
        released = {"budget": 0.0, "slots": 0}

        if decision["decision"] == "TERMINATE":
            # 回滚终止：按终止时封存的分层台账原样重建预留并补回资源池，
            # 已发生使用（终止前用掉的部分）仍留在 used，不回补到预留。
            pilot.reserve_tiers = [dict(tier) for tier in decision.get("released_tiers", [])]
            released = {"budget": -pilot.reserved_budget, "slots": -pilot.reserved_slots}
            if pilot.pool_id:
                pool = state.pools[pilot.pool_id]
                pool["budget_reserved"] += pilot.reserved_budget
                pool["slots_reserved"] += pilot.reserved_slots
            pilot.status = target_spec["status"]
        elif decision["decision"] == "EXPAND":
            # 回滚扩大：只移除该决策新增的最新层（FIFO 下它最后被消耗），
            # 层内余量即原子释放量；老层的已消耗由 used 保留，不回补。
            tier = next((t for t in pilot.reserve_tiers if t["spec_version"] == pilot.spec_version), None)
            if tier is not None:
                released = {"budget": tier["budget"], "slots": tier["slots"]}
                pilot.reserve_tiers = [t for t in pilot.reserve_tiers if t is not tier]
                if pilot.pool_id:
                    pool = state.pools[pilot.pool_id]
                    pool["budget_reserved"] = max(0.0, pool["budget_reserved"] - released["budget"])
                    pool["slots_reserved"] = max(0, pool["slots_reserved"] - released["slots"])

        pilot.spec_version = restored_version
        if target_spec is not None:
            pilot.zones = list(target_spec["zones"])
            pilot.window_start = target_spec["window"]["start"]
            pilot.window_end = target_spec["window"]["end"]
        # 被回滚掉的新增区域若正处于暂停，随快照一并移除；若已无暂停区域则恢复在跑。
        pilot.paused_zones &= set(pilot.zones)
        if decision["decision"] != "TERMINATE" and not pilot.paused_zones:
            pilot.status = "approved"
        pilot.rollbacks.append(
            {
                "rollback_id": p["rollback_id"],
                "decision_id": p["decision_id"],
                "restored_spec_version": restored_version,
                "released": released,
                "reason": p.get("reason", ""),
                "rolled_back_at": event.occurred_at,
                "retained_used_budget": pilot.used_budget,
                "retained_used_slots": pilot.used_slots,
            }
        )
        pilot.history.append(
            {"spec_version": pilot.spec_version, "kind": "rolled_back",
             "decision_id": p["decision_id"], "at": event.occurred_at}
        )

    elif t in (BATCH_IMPORTED, BATCH_QUARANTINED):
        batch = state.batches.setdefault(
            p["batch_id"], {"batch_id": p["batch_id"], "quarantined": []}
        )
        if t == BATCH_IMPORTED:
            batch.update(
                source=p.get("source", ""),
                accepted=p.get("accepted", 0),
                rejected=p.get("rejected", 0),
                ignored=p.get("ignored", 0),
                imported_at=event.occurred_at,
            )
        else:
            batch["quarantined"].extend(p.get("records", []))


def _spec_snapshot(event: Event, pilot: Pilot) -> dict[str, Any]:
    return {
        "kind": "spec",
        "spec_version": pilot.spec_version,
        "at": event.occurred_at,
        "zones": list(pilot.zones),
        "control_zones": list(pilot.control_zones),
        "window": {"start": pilot.window_start, "end": pilot.window_end},
        "reserve_tiers": [dict(tier) for tier in pilot.reserve_tiers],
        "reserved_budget": pilot.reserved_budget,
        "reserved_slots": pilot.reserved_slots,
        "used_budget": pilot.used_budget,
        "used_slots": pilot.used_slots,
        "status": pilot.status,
        "paused_zones": sorted(pilot.paused_zones),
    }
