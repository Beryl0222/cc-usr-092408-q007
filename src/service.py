"""可逆决策账服务门面。

所有命令的结果都落成事件：规则判断只决定"追加哪些事件"，一旦追加，
重放事件流即可复原同样的决策与资源状态，不依赖再次运行判断逻辑。
每个命令在一个 append_all 原子批次内提交，规则不满足时整批拒绝，
账本不会留下"占了名额却没批试点"之类的半成品。
"""
from __future__ import annotations

import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from .events import (
    AG_COMMUNITY_NEED,
    AG_IMPORT_BATCH,
    AG_NEED_CLUSTER,
    AG_PILOT,
    AG_RESOURCE_POOL,
    BATCH_IMPORTED,
    BATCH_QUARANTINED,
    CLUSTER_PROPOSED,
    DomainError,
    FEEDBACK_RECORDED,
    NEED_LOGGED,
    NOTIFICATION_SENT,
    OBSERVATION_RECORDED,
    PILOT_APPROVED,
    PILOT_DECIDED,
    PILOT_PAUSED,
    PILOT_REJECTED,
    PILOT_RESUMED,
    PILOT_ROLLED_BACK,
    POOL_FUNDED,
    RESOURCE_USED,
    REVIEW_HELD,
    REVIEW_SCHEDULED,
    SAFETY_STOP_TRIGGERED,
    Event,
    make_event,
    parse_at,
)
from .state import Pilot, State, fold
from .store import EventStore

# 明显的可识别信息模式；命中则要求调用方先去标识，原始信息绝不入入账本。
_PII_PATTERNS = (
    ("手机号", re.compile(r"(?<!\d)1[3-9]\d{9}(?!\d)")),
    ("电子邮箱", re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")),
    ("身份证号", re.compile(r"(?<!\d)\d{17}[\dXx](?!\d)")),
)

DECISIONS = ("EXPAND", "EXTEND", "TERMINATE")


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def screen_deidentified(text: str) -> list[str]:
    """返回文本中疑似可识别信息的说明；空列表表示通过去标识检查。"""
    hits = [label for label, pattern in _PII_PATTERNS if pattern.search(text or "")]
    return [f"摘要疑似包含{label}，请去标识后再登记" for label in hits]


class LedgerService:
    def __init__(self, store: EventStore, clock: Callable[[], str] = _utcnow):
        self.store = store
        self._clock = clock
        self._state: State | None = None

    # ---- 内部工具 ----
    def _when(self, at: str | None) -> str:
        value = at or self._clock()
        parse_at(value)
        return value

    @property
    def state(self) -> State:
        if self._state is None:
            self._state = fold(self.store.events)
        return self._state

    def refresh(self) -> State:
        """进程重启或外部写入后重新折叠；到期评审由此跨进程继续。"""
        self.store.load()
        self._state = fold(self.store.events)
        return self._state

    def _commit(self, events: list[Event]) -> list[Event]:
        stored = self.store.append_all(events)
        # 下次访问 state 时从已含新事件的存储重新折叠，避免暂存事件被重复 apply。
        self._state = None
        return stored

    def _allocator(self, base: dict[str, int] | None = None) -> dict[str, int]:
        versions = base or {}
        for agg_id in {e.aggregate_id for e in self.store.events}:
            versions.setdefault(agg_id, self.store.version_of(agg_id))
        return versions

    def _emit(
        self,
        out: list[Event],
        alloc: dict[str, int],
        event_type: str,
        aggregate_type: str,
        aggregate_id: str,
        at: str,
        payload: dict[str, Any],
        summary: str,
    ) -> Event:
        version = alloc.get(aggregate_id, 0) + 1
        alloc[aggregate_id] = version
        event = make_event(event_type, aggregate_type, aggregate_id, at, payload, summary, version)
        out.append(event)
        return event

    # ---- 诉求登记：只存去标识摘要与来源群体 ----
    def log_need(
        self,
        need_id: str,
        summary: str,
        source_group: str,
        theme_tags: list[str] | None = None,
        at: str | None = None,
    ) -> Event:
        at = self._when(at)
        problems = screen_deidentified(summary)
        if problems:
            raise DomainError("；".join(problems))
        if need_id in self.state.needs:
            raise DomainError(f"诉求 {need_id} 已存在，更正请追加后继记录而非改写")
        event = make_event(
            NEED_LOGGED,
            AG_COMMUNITY_NEED,
            need_id,
            at,
            {
                "need_id": need_id,
                "summary": summary,
                "source_group": source_group,
                "theme_tags": theme_tags or [],
            },
            f"登记来自{source_group}的去标识诉求",
            self.store.version_of(need_id) + 1,
        )
        return self._commit([event])[0]

    # ---- 聚类只是建议，不回写诉求、不自动推动审批 ----
    def propose_cluster(
        self,
        cluster_id: str,
        need_ids: list[str],
        proposed_theme: str,
        rationale: str = "",
        confidence: float = 0.5,
        at: str | None = None,
    ) -> Event:
        at = self._when(at)
        missing = [n for n in need_ids if n not in self.state.needs]
        if missing:
            raise DomainError(f"聚类引用了不存在的诉求：{missing}")
        if not 0.0 <= confidence <= 1.0:
            raise DomainError("confidence 必须在 0 到 1 之间")
        if cluster_id in self.state.clusters:
            raise DomainError(f"聚类建议 {cluster_id} 已存在，应提出新版本建议而非覆盖")
        event = make_event(
            CLUSTER_PROPOSED,
            AG_NEED_CLUSTER,
            cluster_id,
            at,
            {
                "cluster_id": cluster_id,
                "need_ids": list(need_ids),
                "proposed_theme": proposed_theme,
                "rationale": rationale,
                "confidence": confidence,
                "advisory": True,
            },
            f"提出关于「{proposed_theme}」的聚类建议（仅供参考，置信度 {confidence}）",
            self.store.version_of(cluster_id) + 1,
        )
        return self._commit([event])[0]

    # ---- 资源池 ----
    def fund_pool(
        self, pool_id: str, budget: float, slots: int, currency: str = "CNY", at: str | None = None
    ) -> Event:
        at = self._when(at)
        if budget < 0 or slots < 0:
            raise DomainError("资源池额度不能为负")
        if pool_id in self.state.pools:
            raise DomainError("资源池已建立，追加预算请走专门命令")
        event = make_event(
            POOL_FUNDED,
            AG_RESOURCE_POOL,
            pool_id,
            at,
            {"pool_id": pool_id, "budget": budget, "slots": slots, "currency": currency},
            f"建立资源池 {pool_id}：预算 {budget}、名额 {slots}",
            self.store.version_of(pool_id) + 1,
        )
        return self._commit([event])[0]

    # ---- 试点审批：冻结区域/时间窗/指标/停止线/批准人，并原子占用资源 ----
    def approve_pilot(self, spec: dict[str, Any], at: str | None = None) -> dict[str, Any]:
        at = self._when(at)
        pilot_id = spec["pilot_id"]
        reasons: list[str] = []
        if pilot_id in self.state.pilots or pilot_id in self.state.rejected_pilots:
            raise DomainError(f"试点 {pilot_id} 已有审批记录")
        zones = spec.get("zones", [])
        control_zones = spec.get("control_zones", [])
        if not zones:
            reasons.append("必须冻结至少一个目标区域")
        if set(zones) & set(control_zones):
            reasons.append("同一区域不能既是干预区域又是对照区域")
        window = spec.get("window") or {}
        if not window.get("start") or not window.get("end"):
            reasons.append("必须冻结开始与结束时间窗")
        elif parse_at(window["end"]) <= parse_at(window["start"]):
            reasons.append("时间窗结束必须晚于开始")
        if not spec.get("metrics"):
            reasons.append("必须冻结至少一个成功指标")
        if not spec.get("safety_limits"):
            reasons.append("必须冻结安全停止线")
        if not spec.get("approver"):
            reasons.append("必须登记批准人")
        unknown_clusters = [c for c in spec.get("cluster_ids", []) if c not in self.state.clusters]
        if unknown_clusters:
            reasons.append(f"引用的聚类建议不存在：{unknown_clusters}")

        pool_id = spec.get("pool_id", "")
        budget_total = float(spec.get("budget_total", 0.0))
        slots = int(spec.get("slots", 0))
        if pool_id:
            if pool_id not in self.state.pools:
                reasons.append(f"资源池 {pool_id} 不存在")
            else:
                avail_b, avail_s = self.state.pool_available(pool_id)
                if budget_total > avail_b:
                    reasons.append(f"预算不足：需要 {budget_total}，可用 {avail_b}")
                if slots > avail_s:
                    reasons.append(f"名额不足：需要 {slots}，可用 {avail_s}")
        reasons.extend(self.state.zone_conflicts(zones, control_zones, spec.get("conflict_tags", [])))

        alloc = self._allocator()
        events: list[Event] = []
        if reasons:
            # 驳回本身留痕，但不占用任何资源。
            self._emit(
                events, alloc, PILOT_REJECTED, AG_PILOT, pilot_id, at,
                {"pilot_id": pilot_id, "reasons": reasons},
                f"试点 {pilot_id} 审批驳回：{len(reasons)} 项问题",
            )
            self._commit(events)
            return {"approved": False, "pilot_id": pilot_id, "reasons": reasons}

        payload = {
            "pilot_id": pilot_id,
            "title": spec.get("title", pilot_id),
            "cluster_ids": list(spec.get("cluster_ids", [])),
            "zones": list(zones),
            "control_zones": list(control_zones),
            "conflict_tags": list(spec.get("conflict_tags", [])),
            "window": {"start": window["start"], "end": window["end"]},
            "metrics": spec["metrics"],
            "safety_limits": spec["safety_limits"],
            "bias_threshold": float(spec.get("bias_threshold", 0.8)),
            "required_sample_size": int(spec.get("required_sample_size", 1)),
            "review_within_hours": int(spec.get("review_within_hours", 48)),
            "budget_total": budget_total,
            "slots": slots,
            "pool_id": pool_id,
            "approver": spec["approver"],
        }
        self._emit(
            events, alloc, PILOT_APPROVED, AG_PILOT, pilot_id, at, payload,
            f"批准试点「{payload['title']}」，冻结 {len(zones)} 个区域与时间窗，批准人 {spec['approver']}",
        )
        self._commit(events)
        return {"approved": True, "pilot_id": pilot_id}

    # ---- 观测入账：异常环境标记隔离、安全线立即暂停、普通偏离进入复核 ----
    def record_observation(
        self,
        pilot_id: str,
        observation_id: str,
        observed_at: str,
        readings: list[dict[str, Any]],
        zones: list[str] | None = None,
        anomaly: bool = False,
        at: str | None = None,
    ) -> dict[str, Any]:
        at = self._when(at)
        parse_at(observed_at)
        pilot = self._require_pilot(pilot_id)
        if observation_id in pilot.observations:
            return {"recorded": False, "ignored": "duplicate_observation", "observation_id": observation_id}
        events: list[Event] = []
        alloc = self._allocator()
        payload = {
            "pilot_id": pilot_id,
            "observation_id": observation_id,
            "observed_at": observed_at,
            "zones": zones or list(pilot.zones),
            "readings": readings,
            "anomaly": anomaly,
        }
        self._emit(
            events, alloc, OBSERVATION_RECORDED, AG_PILOT, pilot_id, at, payload,
            f"记录观测 {observation_id}（{'异常环境条件，隔离评估' if anomaly else '常规条件'}）",
        )
        events.extend(self._observation_effects(alloc, pilot_id, payload, at, events))
        self._commit(events)
        return {"recorded": True, "observation_id": observation_id,
                "generated": [e.event_type for e in events[1:]]}

    def _observation_effects(
        self,
        alloc: dict[str, int],
        pilot_id: str,
        obs_payload: dict[str, Any],
        at: str,
        staged: list[Event],
    ) -> list[Event]:
        """在"本批次此前暂存事件均已入账"的假想状态上判定安全线/指标偏离。

        实时命令与批量导入共用这段逻辑，保证两条入口判定一致；同一批次内
        先发生的暂停/复核会反映到后续观测的判定中，不重复触发。
        """
        scratch = fold(self.store.events + staged)
        pilot = scratch.pilots[pilot_id]
        out: list[Event] = []
        if obs_payload.get("anomaly"):
            return out  # 异常环境条件：记录留档，但不据此触发任何结论
        for reading in obs_payload["readings"]:
            if reading.get("anomaly"):
                continue
            # 同一条观测内前面的读数若已触发暂停，后续读数判定应看到该暂停。
            if out:
                scratch = fold(self.store.events + staged + out)
                pilot = scratch.pilots[pilot_id]
            metric = reading["metric"]
            value = float(reading["value"])
            zones = list(reading.get("zones") or obs_payload.get("zones") or pilot.zones)
            if reading["kind"] == "safety" and metric in pilot.safety_limits:
                stop_id = f"{pilot.pilot_id}-stop-{metric}-{obs_payload['observation_id']}"
                if value > pilot.safety_limits[metric] and stop_id not in pilot.active_stops:
                    self._safety_stop(
                        out, alloc, pilot, metric, value,
                        pilot.safety_limits[metric], zones,
                        obs_payload["observation_id"], at,
                    )
            elif reading["kind"] == "metric" and metric in pilot.metrics:
                spec = pilot.metrics[metric]
                target, tolerance = float(spec["target"]), float(spec["tolerance"])
                if not (target - tolerance <= value <= target + tolerance):
                    self._schedule_review(
                        out, alloc, pilot, metric, obs_payload, at,
                        reason="metric_deviation",
                        detail={"metric": metric, "value": value, "target": target,
                                "tolerance": tolerance},
                    )
        return out

    def _safety_stop(self, out, alloc, pilot: Pilot, indicator: str,
                     value: float, limit: float, zones: list[str], observation_id: str, at: str) -> None:
        stop_id = f"{pilot.pilot_id}-stop-{indicator}-{observation_id}"
        fresh_zones = [z for z in zones if z not in pilot.paused_zones]
        self._emit(
            out, alloc, SAFETY_STOP_TRIGGERED, AG_PILOT, pilot.pilot_id, at,
            {"pilot_id": pilot.pilot_id, "stop_id": stop_id, "indicator": indicator,
             "value": value, "limit": limit, "zones": zones, "observation_id": observation_id},
            f"安全停止线越线：{indicator}={value} > {limit}，涉及区域 {zones}",
        )
        if fresh_zones:
            self._emit(
                out, alloc, PILOT_PAUSED, AG_PILOT, pilot.pilot_id, at,
                {"pilot_id": pilot.pilot_id, "zones": fresh_zones,
                 "reason": f"安全停止 {stop_id}：{indicator} 越线", "ref_stop_id": stop_id},
                f"立即暂停受影响区域 {fresh_zones}",
            )
            self._emit(
                out, alloc, NOTIFICATION_SENT, AG_PILOT, pilot.pilot_id, at,
                {"channel": "park_notice", "recipient_scope": f"zones:{','.join(fresh_zones)}",
                 "subject": f"区域照明试点安全暂停通知",
                 "body": f"因 {indicator} 超过安全停止线，区域 {fresh_zones} 已立即暂停，复核后另行通知。",
                 "ref_event_type": SAFETY_STOP_TRIGGERED},
                "向受影响区域群体发出安全暂停通知（去标识、按范围发送）",
            )

    def _schedule_review(self, out, alloc, pilot: Pilot, metric: str,
                         obs_payload: dict[str, Any], at: str, reason: str, detail: dict) -> None:
        open_reviews = [r for r in pilot.reviews.values() if r["status"] == "scheduled"]
        if reason == "metric_deviation" and any(r.get("metric") == metric for r in open_reviews):
            # 同一指标已有待办复核：不再就后续读数重复立案，避免重复反馈刷爆复核队列。
            return
        if reason == "sampling_bias" and any("sampling_bias" in r["reasons"] for r in open_reviews):
            return
        review_id = f"{pilot.pilot_id}-rev-{reason}-{obs_payload['observation_id']}"
        due = (parse_at(at) + timedelta(hours=pilot.review_within_hours)).isoformat()
        self._emit(
            out, alloc, REVIEW_SCHEDULED, AG_PILOT, pilot.pilot_id, at,
            {"pilot_id": pilot.pilot_id, "review_id": review_id, "due_at": due,
             "reasons": [reason], "trigger_observation_id": obs_payload["observation_id"],
             "metric_snapshot": detail, "metric": metric},
            f"普通指标偏离进入复核（{reason}: {metric}），须于 {due} 前完成",
        )

    # ---- 居民反馈：去标识、重传幂等、采样偏差复核 ----
    def record_feedback(
        self,
        pilot_id: str,
        feedback_id: str,
        source_group: str,
        observed_at: str,
        rating: int | None = None,
        summary: str = "",
        at: str | None = None,
    ) -> dict[str, Any]:
        at = self._when(at)
        parse_at(observed_at)
        problems = screen_deidentified(summary)
        if problems:
            raise DomainError("；".join(problems))
        pilot = self._require_pilot(pilot_id)
        if feedback_id in pilot.feedback:
            return {"recorded": False, "ignored": "duplicate_feedback", "feedback_id": feedback_id}
        events: list[Event] = []
        alloc = self._allocator()
        self._emit(
            events, alloc, FEEDBACK_RECORDED, AG_PILOT, pilot_id, at,
            {"pilot_id": pilot_id, "feedback_id": feedback_id, "source_group": source_group,
             "rating": rating, "summary": summary, "observed_at": observed_at},
            f"记录来自{source_group}的反馈 {feedback_id}",
        )
        scratch = fold(self.store.events + events)
        spilot = scratch.pilots[pilot_id]
        already_open = any(
            "sampling_bias" in r["reasons"] and r["status"] == "scheduled"
            for r in spilot.reviews.values()
        )
        share = spilot.feedback_group_share()
        dominant = [(g, s) for g, s in share.items() if s > spilot.bias_threshold]
        if dominant and len(spilot.feedback) >= spilot.required_sample_size and not already_open:
            group, frac = max(dominant, key=lambda x: x[1])
            self._emit(
                events, alloc, REVIEW_SCHEDULED, AG_PILOT, pilot_id, at,
                {"pilot_id": pilot_id,
                 "review_id": f"{pilot_id}-rev-sampling_bias-{feedback_id}",
                 "due_at": (parse_at(at) + timedelta(hours=spilot.review_within_hours)).isoformat(),
                 "reasons": ["sampling_bias"], "trigger_observation_id": None,
                 "metric_snapshot": {"metric": "feedback_source_share", "group": group, "share": frac}},
                f"采样偏差复核：{group} 反馈占比 {frac:.0%} 超过阈值 {spilot.bias_threshold:.0%}",
            )
        self._commit(events)
        return {"recorded": True, "feedback_id": feedback_id,
                "generated": [e.event_type for e in events[1:]]}

    # ---- 到期评审：重启后仍可继续；举行时冻结当时证据 ----
    def due_reviews(self, now: str | None = None) -> list[dict[str, str]]:
        now = self._when(now)
        return [
            {"pilot_id": pid, "review_id": rid, "due_at": self.state.pilots[pid].reviews[rid]["due_at"]}
            for pid, rid in self.state.due_reviews(now)
        ]

    def hold_review(
        self,
        pilot_id: str,
        review_id: str,
        finding: str,
        recommendation: str,
        held_at: str | None = None,
    ) -> Event:
        held_at = self._when(held_at)
        pilot = self._require_pilot(pilot_id)
        if review_id not in pilot.reviews:
            raise DomainError(f"复核 {review_id} 不存在")
        if pilot.reviews[review_id]["status"] != "scheduled":
            raise DomainError("复核已举行，结论更正应追加后继记录")
        if recommendation not in ("CONTINUE", "ESCALATE", "RECOMMEND_TERMINATE"):
            raise DomainError("recommendation 必须是 CONTINUE/ESCALATE/RECOMMEND_TERMINATE")
        evidence = self.evidence_snapshot(pilot_id, held_at)
        event = make_event(
            REVIEW_HELD, AG_PILOT, pilot_id, held_at,
            {"pilot_id": pilot_id, "review_id": review_id, "held_at": held_at,
             "finding": finding, "recommendation": recommendation, "evidence": evidence},
            f"举行复核 {review_id}，结论 {recommendation}",
            self.store.version_of(pilot_id) + 1,
        )
        return self._commit([event])[0]

    # ---- 安全暂停后的恢复（必须点名停止线与批准人） ----
    def resume_pilot(self, pilot_id: str, zones: list[str], ref_stop_id: str,
                     approver: str, at: str | None = None) -> list[Event]:
        at = self._when(at)
        pilot = self._require_pilot(pilot_id)
        if ref_stop_id not in pilot.active_stops:
            raise DomainError(f"安全停止 {ref_stop_id} 不存在")
        if not pilot.active_stops[ref_stop_id]["zones"]:
            pass
        missing = [z for z in zones if z not in pilot.paused_zones]
        if missing:
            raise DomainError(f"区域当前并未暂停：{missing}")
        events: list[Event] = []
        alloc = self._allocator()
        self._emit(
            events, alloc, PILOT_RESUMED, AG_PILOT, pilot_id, at,
            {"pilot_id": pilot_id, "zones": zones, "ref_stop_id": ref_stop_id, "approver": approver},
            f"经 {approver} 批准，恢复区域 {zones}",
        )
        self._emit(
            events, alloc, NOTIFICATION_SENT, AG_PILOT, pilot_id, at,
            {"channel": "park_notice", "recipient_scope": f"zones:{','.join(zones)}",
             "subject": "区域试点恢复通知",
             "body": f"安全停止 {ref_stop_id} 已解除，区域 {zones} 恢复试点。",
             "ref_event_type": PILOT_RESUMED},
            "向受影响区域群体发出恢复通知",
        )
        return self._commit(events)

    # ---- 已发生资源使用：回滚后也保留 ----
    def record_resource_use(self, pilot_id: str, budget_delta: float, slots_delta: int,
                            reason: str, at: str | None = None) -> Event:
        at = self._when(at)
        pilot = self._require_pilot(pilot_id)
        if not pilot.pool_id:
            raise DomainError("该试点未关联资源池")
        pool = self.state.pools[pilot.pool_id]
        # 只能消耗本试点自己冻结的预留余量，不能动用共享池里其他试点的预留。
        if budget_delta > pilot.reserved_budget + 1e-9:
            raise DomainError(
                f"预算使用量 {budget_delta} 超过本试点剩余预留 {pilot.reserved_budget}"
            )
        if slots_delta > pilot.reserved_slots:
            raise DomainError(
                f"名额使用量 {slots_delta} 超过本试点剩余预留 {pilot.reserved_slots}"
            )
        event = make_event(
            RESOURCE_USED, AG_RESOURCE_POOL, pilot.pool_id, at,
            {"pool_id": pilot.pool_id, "pilot_id": pilot_id,
             "budget_delta": budget_delta, "slots_delta": slots_delta, "reason": reason},
            f"登记已发生资源使用：预算 {budget_delta}、名额 {slots_delta}（{reason}）",
            self.store.version_of(pilot.pool_id) + 1,
        )
        return self._commit([event])[0]

    # ---- 扩大 / 延长 / 终止：全部带证据快照，可解释、可回滚 ----
    def decide(
        self,
        pilot_id: str,
        decision: str,
        approver: str,
        decision_id: str,
        at: str | None = None,
        extend_window_end: str | None = None,
        extra_zones: list[str] | None = None,
        extra_budget: float = 0.0,
        extra_slots: int = 0,
    ) -> dict[str, Any]:
        at = self._when(at)
        pilot = self._require_pilot(pilot_id)
        if decision not in DECISIONS:
            raise DomainError(f"decision 必须是 {DECISIONS} 之一")
        if any(d["decision_id"] == decision_id for d in pilot.decisions):
            raise DomainError(f"决策号 {decision_id} 已使用")
        evidence = self.evidence_snapshot(pilot_id, at)

        if decision != "TERMINATE":
            blocks: list[str] = []
            if pilot.status == "paused":
                blocks.append("试点处于安全暂停状态，不得扩大或延长")
            open_due = [r for r in pilot.reviews.values() if r["status"] == "scheduled"]
            if open_due:
                blocks.append(f"尚有 {len(open_due)} 个复核未闭合，不得扩大或延长")
            if len(evidence["observation_ids"]) < pilot.required_sample_size:
                blocks.append(
                    f"有效样本 {len(evidence['observation_ids'])} 少于冻结要求 {pilot.required_sample_size}，证据不足"
                )
            if decision == "EXTEND":
                if not extend_window_end or parse_at(extend_window_end) <= parse_at(pilot.window_end):
                    blocks.append("延长后的时间窗结束必须晚于当前冻结结束时间")
            if decision == "EXPAND":
                if not extra_zones:
                    blocks.append("扩大必须给出新增区域")
                conflicts = self.state.zone_conflicts(
                    extra_zones or [], [], pilot.conflict_tags, exclude_pilot_id=pilot_id
                )
                blocks.extend(conflicts)
            if pilot.pool_id and (extra_budget or extra_slots):
                avail_b, avail_s = self.state.pool_available(pilot.pool_id)
                if extra_budget > avail_b:
                    blocks.append(f"预算不足：需要追加 {extra_budget}，可用 {avail_b}")
                if extra_slots > avail_s:
                    blocks.append(f"名额不足：需要追加 {extra_slots}，可用 {avail_s}")
            if blocks:
                raise DomainError("决策被阻止：" + "；".join(blocks))

        new_spec: dict[str, Any] = {}
        if decision == "EXTEND":
            new_spec["window_end"] = extend_window_end
        if decision == "EXPAND":
            new_spec["zones"] = list(extra_zones or [])
            new_spec["budget_total"] = float(extra_budget)
            new_spec["slots"] = int(extra_slots)

        events: list[Event] = []
        alloc = self._allocator()
        decide_payload = {
            "pilot_id": pilot_id,
            "decision_id": decision_id,
            "decision": decision,
            "approver": approver,
            "prior_spec_version": pilot.spec_version,
            "new_spec": new_spec,
            "evidence": evidence,
        }
        # 资源释放在 PILOT_DECIDED 内由折叠层原子完成，不再产生独立释放事件：
        # 决策与释放要么同批可见，要么都不可见，杜绝"终止了但名额没还"。
        self._emit(
            events, alloc, PILOT_DECIDED, AG_PILOT, pilot_id, at, decide_payload,
            {
                "EXPAND": f"决定扩大试点，新增区域 {new_spec.get('zones', [])}",
                "EXTEND": f"决定延长试点至 {new_spec.get('window_end')}",
                "TERMINATE": "决定终止试点并原子释放剩余预留资源",
            }[decision]
            + f"（决策号 {decision_id}，批准人 {approver}）",
        )
        self._emit(
            events, alloc, NOTIFICATION_SENT, AG_PILOT, pilot_id, at,
            {"channel": "park_notice",
             "recipient_scope": f"zones:{','.join(pilot.zones + pilot.control_zones)}",
             "subject": f"试点决策通知：{decision}",
             "body": f"试点 {pilot_id} 的决策为 {decision}，决策号 {decision_id}。",
             "ref_event_type": PILOT_DECIDED},
            f"发出 {decision} 决策通知",
        )
        self._commit(events)
        return {"decision_id": decision_id, "decision": decision, "committed": len(events),
                "evidence": evidence}

    def rollback_decision(self, pilot_id: str, decision_id: str, reason: str,
                          approver: str | None = None, at: str | None = None) -> dict[str, Any]:
        """回滚最近一次未回滚的扩大/延长/终止；已发生资源使用与已发通知保留。"""
        at = self._when(at)
        pilot = self._require_pilot(pilot_id)
        target = next((d for d in pilot.decisions if d["decision_id"] == decision_id), None)
        if target is None:
            raise DomainError(f"决策 {decision_id} 不存在")
        rolled_back = {r["decision_id"] for r in pilot.rollbacks}
        if decision_id in rolled_back:
            raise DomainError("该决策已回滚")
        active_decisions = [d for d in pilot.decisions if d["decision_id"] not in rolled_back]
        if not active_decisions or active_decisions[-1]["decision_id"] != decision_id:
            raise DomainError("只能回滚最近一次尚未回滚的决策")

        rollback_id = f"{decision_id}-rollback"
        events: list[Event] = []
        alloc = self._allocator()
        # 释放/补回的具体资源量不在这里给出：折叠层依据冻结快照与分层台账唯一计算，
        # 保证实时路径与回放路径得到同一数字。
        self._emit(
            events, alloc, PILOT_ROLLED_BACK, AG_PILOT, pilot_id, at,
            {"pilot_id": pilot_id, "rollback_id": rollback_id, "decision_id": decision_id,
             "restored_spec_version": target["prior_spec_version"],
             "reason": reason, "approver": approver or ""},
            f"回滚决策 {decision_id}，恢复到冻结版本 v{target['prior_spec_version']}；"
            "已发生资源使用与已发通知保留",
        )
        self._emit(
            events, alloc, NOTIFICATION_SENT, AG_PILOT, pilot_id, at,
            {"channel": "park_notice",
             "recipient_scope": f"zones:{','.join(pilot.zones + pilot.control_zones)}",
             "subject": f"试点决策回滚通知：{decision_id}",
             "body": f"决策 {decision_id} 已回滚：{reason}。已发生的资源使用与既有通知仍然有效。",
             "ref_event_type": PILOT_ROLLED_BACK},
            "发出回滚通知",
        )
        self._commit(events)
        # 资源数字以折叠层为准，从最新状态读回实际释放/补回与保留的已用量。
        actual = next(r for r in self.state.pilots[pilot_id].rollbacks if r["rollback_id"] == rollback_id)
        return {"rollback_id": rollback_id, "released": actual["released"],
                "retained_used_budget": actual["retained_used_budget"],
                "retained_used_slots": actual["retained_used_slots"]}

    # ---- 证据快照：解释一次扩大/延长/终止由哪些数据支持 ----
    def evidence_snapshot(self, pilot_id: str, at: str) -> dict[str, Any]:
        pilot = self._require_pilot(pilot_id)
        as_of = parse_at(at)
        win_start = parse_at(pilot.window_start) if pilot.window_start else None
        win_end = parse_at(pilot.window_end) if pilot.window_end else None
        counted, excluded_anomaly = [], []
        for obs in sorted(pilot.observations.values(), key=lambda o: o["observed_at"]):
            when = parse_at(obs["observed_at"])
            if when > as_of:
                continue  # 后来数据不得支持当时决策
            if (
                obs["anomaly"]
                or set(obs["zones"]) & pilot.paused_zones
                or (win_start is not None and when < win_start)
                or (win_end is not None and when > win_end)
            ):
                excluded_anomaly.append(obs["observation_id"])
            else:
                counted.append(obs)
        aggregates: dict[str, list[float]] = defaultdict(list)
        for obs in counted:
            for reading in obs["readings"]:
                if reading["kind"] == "metric" and not reading.get("anomaly"):
                    aggregates[reading["metric"]].append(float(reading["value"]))
        metric_aggregates = {
            metric: {"count": len(values), "latest": values[-1], "mean": sum(values) / len(values),
                     "min": min(values), "max": max(values)}
            for metric, values in aggregates.items()
        }
        feedback_ids = sorted(
            (f["feedback_id"] for f in pilot.feedback.values()
             if parse_at(f["observed_at"]) <= as_of),
        )
        feedback_in_scope = [
            f for f in pilot.feedback.values() if parse_at(f["observed_at"]) <= as_of
        ]
        total_fb = len(feedback_in_scope)
        group_counts: dict[str, int] = {}
        for f in feedback_in_scope:
            group_counts[f["source_group"]] = group_counts.get(f["source_group"], 0) + 1
        group_share = {g: c / total_fb for g, c in group_counts.items()} if total_fb else {}
        return {
            "as_of": at,
            "frozen_spec_version": pilot.spec_version,
            "observation_ids": [o["observation_id"] for o in counted],
            "excluded_anomaly_ids": excluded_anomaly,
            "feedback_ids": feedback_ids,
            "feedback_group_share": group_share,
            "review_ids": [
                rid for rid, r in pilot.reviews.items()
                if r["status"] == "held" and parse_at(r["held_at"]) <= as_of
            ],
            "open_review_ids": [
                rid for rid, r in pilot.reviews.items()
                if r["status"] == "scheduled" and parse_at(r["scheduled_at"]) <= as_of
            ],
            "stop_ids": [
                sid for sid, s in pilot.active_stops.items()
                if parse_at(s["triggered_at"]) <= as_of
            ],
            "active_stop_ids": [
                sid for sid, s in pilot.active_stops.items()
                if not s["resolved"] and parse_at(s["triggered_at"]) <= as_of
            ],
            "cluster_ids": list(pilot.cluster_ids),
            "cluster_advisory": True,
            "metric_aggregates": metric_aggregates,
        }

    def explain_decision(self, pilot_id: str, decision_id: str) -> dict[str, Any]:
        return self.state.explain_decision(pilot_id, decision_id)

    def pilot_timeline(self, pilot_id: str) -> list[dict[str, Any]]:
        """按业务发生时间排列观测与反馈，供解释使用。"""
        pilot = self._require_pilot(pilot_id)
        items = [
            {"kind": "observation", "at": o["observed_at"], "id": oid, "anomaly": o["anomaly"]}
            for oid, o in pilot.observations.items()
        ] + [
            {"kind": "feedback", "at": f["observed_at"], "id": fid, "group": f["source_group"]}
            for fid, f in pilot.feedback.items()
        ]
        return sorted(items, key=lambda x: (x["at"], x["kind"], x["id"]))

    # ---- 批量导入：好记录入账、坏记录隔离、重传去重，整体原子提交 ----
    def import_batch(self, batch_id: str, records: list[dict[str, Any]],
                     source: str = "", at: str | None = None) -> dict[str, Any]:
        at = self._when(at)
        if batch_id in self.state.batches:
            return {"batch_id": batch_id, "ignored": "duplicate_batch"}
        alloc = self._allocator()
        events: list[Event] = []
        quarantined: list[dict[str, Any]] = []
        accepted = ignored = 0

        known_needs = set(self.state.needs)
        known_obs = {oid for p in self.state.pilots.values() for oid in p.observations}
        seen_in_batch: set[str] = set()

        for index, raw in enumerate(records):
            kind = raw.get("kind", "need")
            reasons = self._validate_raw(kind, raw)
            natural_id = raw.get("need_id") or raw.get("observation_id")
            if natural_id in seen_in_batch:
                reasons.append("批次内自然标识重复")
            if not reasons:
                already = natural_id in known_needs if kind == "need" else natural_id in known_obs
                if already:
                    ignored += 1
                    seen_in_batch.add(natural_id)
                    continue
            if reasons:
                quarantined.append({
                    "index": index,
                    "reasons": reasons,
                    "raw": {k: v for k, v in raw.items() if k not in ("summary",)},
                })
                continue

            seen_in_batch.add(natural_id)
            if kind == "need":
                self._emit(
                    events, alloc, NEED_LOGGED, AG_COMMUNITY_NEED, natural_id, at,
                    {"need_id": natural_id, "summary": raw["summary"],
                     "source_group": raw["source_group"], "theme_tags": raw.get("theme_tags", [])},
                    f"批量导入登记诉求 {natural_id}",
                )
                known_needs.add(natural_id)
                accepted += 1
            elif kind == "observation":
                pilot_id = raw["pilot_id"]
                payload = {
                    "pilot_id": pilot_id,
                    "observation_id": natural_id,
                    "observed_at": raw["observed_at"],
                    "zones": raw.get("zones") or self.state.pilots[pilot_id].zones,
                    "readings": raw["readings"],
                    "anomaly": bool(raw.get("anomaly", False)),
                }
                self._emit(
                    events, alloc, OBSERVATION_RECORDED, AG_PILOT, pilot_id, at, payload,
                    f"批量导入观测 {natural_id}",
                )
                events.extend(
                    self._observation_effects(alloc, pilot_id, payload, at, events)
                )
                known_obs.add(natural_id)
                accepted += 1

        if quarantined:
            self._emit(
                events, alloc, BATCH_QUARANTINED, AG_IMPORT_BATCH, batch_id, at,
                {"batch_id": batch_id, "records": quarantined},
                f"批次 {batch_id} 隔离 {len(quarantined)} 条坏记录",
            )
        self._emit(
            events, alloc, BATCH_IMPORTED, AG_IMPORT_BATCH, batch_id, at,
            {"batch_id": batch_id, "source": source,
             "accepted": accepted, "rejected": len(quarantined), "ignored": ignored},
            f"批次 {batch_id} 导入完成：入账 {accepted}、隔离 {len(quarantined)}、去重 {ignored}",
        )
        self._commit(events)
        return {"batch_id": batch_id, "accepted": accepted,
                "rejected": len(quarantined), "ignored": ignored,
                "quarantined": quarantined}

    def _validate_raw(self, kind: str, raw: dict[str, Any]) -> list[str]:
        reasons: list[str] = []
        if kind == "need":
            for field_name in ("need_id", "summary", "source_group"):
                if not raw.get(field_name):
                    reasons.append(f"缺少字段：{field_name}")
            if raw.get("summary"):
                reasons.extend(screen_deidentified(raw["summary"]))
        elif kind == "observation":
            for field_name in ("pilot_id", "observation_id", "observed_at", "readings"):
                if not raw.get(field_name):
                    reasons.append(f"缺少字段：{field_name}")
            if raw.get("pilot_id") and raw["pilot_id"] not in self.state.pilots:
                reasons.append("观测指向不存在的试点")
            if raw.get("readings"):
                for reading in raw["readings"]:
                    if reading.get("kind") not in ("metric", "safety"):
                        reasons.append("读数 kind 必须是 metric 或 safety")
                    if "metric" not in reading or "value" not in reading:
                        reasons.append("读数缺少 metric/value")
        else:
            reasons.append(f"不支持的记录类型：{kind}")
        return reasons

    def _require_pilot(self, pilot_id: str) -> Pilot:
        if pilot_id not in self.state.pilots:
            raise DomainError(f"试点 {pilot_id} 不存在")
        return self.state.pilots[pilot_id]
