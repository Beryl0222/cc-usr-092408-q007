"""可逆试点决策账：领域行为测试。"""
import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from dataclasses import asdict, is_dataclass

from src.ledger import (
    DomainError,
    EventStore,
    fold,
    iso,
    parse_ts,
)
from src.service import LedgerService, deidentify


FIXED_NOW = "2026-09-21T09:00:00+08:00"


def primitives(obj):
    if is_dataclass(obj):
        return {k: primitives(v) for k, v in asdict(obj).items()}
    if isinstance(obj, dict):
        return {k: primitives(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [primitives(v) for v in obj]
    if isinstance(obj, datetime):
        return iso(obj)
    return obj


def make_service(tmpdir: str, name: str = "events.jsonl", clock=FIXED_NOW, **kw) -> LedgerService:
    def fixed_clock():
        return parse_ts(clock) if isinstance(clock, str) else clock()
    return LedgerService(os.path.join(tmpdir, name), clock=fixed_clock, **kw)


def seed_need(svc: LedgerService, text="希望增加夜跑照明", group="夜跑居民群",
              category="lighting", at="2026-09-20T19:00:00+08:00") -> str:
    svc.log_need(text, group, category=category, occurred_at=at)
    return list(svc.state.needs)[0]


def seed_pilot(svc: LedgerService, pilot_id="pilot-1", zone="东湖环线",
               controls=("西湖环线",), quota=10, budget=200.0,
               cluster_label="夜间照明需求簇",
               need_text="希望增加夜跑照明", group="夜跑居民群") -> str:
    need_id = seed_need(svc, text=need_text, group=group)
    svc.cluster_needs([need_id], cluster_label)
    cluster_id = list(svc.state.clusters)[0]
    svc.approve_pilot(
        pilot_id, cluster_id, zone,
        {"start": "2026-09-21T18:00:00+08:00", "end": "2026-10-21T22:00:00+08:00"},
        {"satisfaction": {"min": 4.0}, "noise_db": {"max": 55}},
        {"noise_db": {"max": 75}, "crowd_density": {"max": 100}},
        "王主任", quota=quota, budget=budget, control_zones=list(controls))
    return pilot_id


class DeidentifyTest(unittest.TestCase):
    def test_strips_personal_identifiers(self):
        text = "我电话13812345678，邮箱a@b.com，@张三 请联系，希望加灯"
        out = deidentify(text)
        self.assertNotIn("13812345678", out)
        self.assertNotIn("a@b.com", out)
        self.assertNotIn("张三", out)
        self.assertIn("希望加灯", out)

    def test_need_stores_only_summary_and_group(self):
        with tempfile.TemporaryDirectory() as d:
            svc = make_service(d)
            ev = svc.log_need("我电话13812345678 需要照明", "夜跑群",
                              occurred_at="2026-09-20T19:00:00+08:00")
            payload = ev["payload"]
            self.assertEqual(set(payload), {"summary", "source_group", "category", "dedup_key"})
            self.assertNotIn("13812345678", json.dumps(payload, ensure_ascii=False))
            self.assertEqual(payload["source_group"], "夜跑群")

    def test_batch_import_isolates_bad_records(self):
        with tempfile.TemporaryDirectory() as d:
            svc = make_service(d)
            rows = [
                {"text": "夜跑照明", "source_group": "夜跑群",
                 "occurred_at": "2026-09-20T19:00:00+08:00"},
                {"text": "坏：无来源", "source_group": ""},
                "不是对象",
                {"text": "儿童软垫", "source_group": "家长群"},
                {"text": "夜跑照明", "source_group": "夜跑群"},  # 与首条重复
            ]
            result = svc.import_needs(rows)
            self.assertEqual(len(result.accepted), 2)
            self.assertEqual([r["index"] for r in result.rejected], [1, 2, 4])
            # 好记录已原子落盘
            self.assertEqual(len(EventStore(svc.store.path).load()), 2)


class ClusterTest(unittest.TestCase):
    def test_cluster_is_advisory(self):
        with tempfile.TemporaryDirectory() as d:
            svc = make_service(d)
            nid = seed_need(svc)
            ev = svc.cluster_needs([nid], "照明簇")
            self.assertTrue(ev["payload"]["advisory"])
            self.assertTrue(svc.state.clusters[ev["aggregate_id"]]["advisory"])

    def test_cluster_rejects_unknown_need(self):
        with tempfile.TemporaryDirectory() as d:
            svc = make_service(d)
            with self.assertRaises(DomainError):
                svc.cluster_needs(["need-nope"], "空簇")


class FreezeTest(unittest.TestCase):
    def test_pilot_frozen_fields_and_no_reapproval(self):
        with tempfile.TemporaryDirectory() as d:
            svc = make_service(d)
            pid = seed_pilot(svc)
            with self.assertRaises(DomainError):
                # 冻结后不得改写：再次审批同一试点
                seed_pilot(svc, pilot_id=pid)
            pilot = svc.state.pilots[pid]
            self.assertEqual(pilot.approver, "王主任")
            self.assertEqual(pilot.zone, "东湖环线")
            self.assertEqual(pilot.success_metrics["noise_db"], {"max": 55})

    def test_window_must_be_valid(self):
        with tempfile.TemporaryDirectory() as d:
            svc = make_service(d)
            nid = seed_need(svc)
            svc.cluster_needs([nid], "簇")
            cid = list(svc.state.clusters)[0]
            with self.assertRaises(DomainError):
                svc.approve_pilot(
                    "p2", cid, "Z", {"start": "2026-10-21T18:00:00+08:00",
                                     "end": "2026-10-01T18:00:00+08:00"},
                    {"m": {"min": 1}}, {"m": {"max": 2}}, "A", 1, 1.0)


class SafetyAndReviewTest(unittest.TestCase):
    def test_safety_limit_halts_immediately_and_notifies(self):
        with tempfile.TemporaryDirectory() as d:
            svc = make_service(d)
            pid = seed_pilot(svc)
            res = svc.record_observation(
                pid, "observation", "noise_db", 82.0, "obs-noise-1",
                "2026-09-22T21:00:00+08:00")
            self.assertIsNotNone(res.safety_halt)
            pilot = svc.state.pilots[pid]
            self.assertEqual(pilot.status, "safety_halted")
            # 受影响区域（实验区+对照区）都进入暂停范围
            self.assertEqual(set(res.safety_halt["payload"]["zones"]),
                             {"东湖环线", "西湖环线"})
            # 立即产生运维通知
            self.assertTrue(any(n["channel"] == "ops" for n in pilot.notifications))

    def test_safety_halt_not_retriggered(self):
        with tempfile.TemporaryDirectory() as d:
            svc = make_service(d)
            pid = seed_pilot(svc)
            svc.record_observation(pid, "observation", "noise_db", 82.0, "o1",
                                   "2026-09-22T21:00:00+08:00")
            before = len(svc.store.load())
            svc.record_observation(pid, "observation", "noise_db", 83.0, "o2",
                                   "2026-09-22T21:05:00+08:00")
            after = len(svc.store.load())
            self.assertEqual(after - before, 1)  # 只有观测，无重复暂停/通知

    def test_normal_deviation_enters_review_then_resumes(self):
        with tempfile.TemporaryDirectory() as d:
            svc = make_service(d)
            pid = seed_pilot(svc)
            res = svc.record_observation(
                pid, "feedback", "satisfaction", 3.2, "fb-1",
                "2026-09-22T20:00:00+08:00", "夜跑群")
            self.assertEqual(len(res.deviations), 1)
            self.assertEqual(svc.state.pilots[pid].status, "review")
            rid = res.deviations[0]["payload"]["review_id"]
            svc.resolve_review(rid, "短期波动，继续", "王主任")
            self.assertEqual(svc.state.pilots[pid].status, "running")

    def test_review_blocks_expand(self):
        with tempfile.TemporaryDirectory() as d:
            svc = make_service(d)
            pid = seed_pilot(svc)
            svc.record_observation(pid, "feedback", "satisfaction", 3.2, "fb-1",
                                   "2026-09-22T20:00:00+08:00")
            with self.assertRaises(DomainError):
                svc.decide(pid, "expand", "王主任", "想扩", add_quota=1)

    def test_halted_pilot_cannot_expand_or_extend(self):
        with tempfile.TemporaryDirectory() as d:
            svc = make_service(d)
            pid = seed_pilot(svc)
            svc.record_observation(pid, "observation", "noise_db", 82.0, "o1",
                                   "2026-09-22T21:00:00+08:00")
            with self.assertRaises(DomainError):
                svc.decide(pid, "expand", "王主任", "x", add_quota=1)
            with self.assertRaises(DomainError):
                svc.decide(pid, "extend", "王主任", "x",
                           new_window_end="2026-11-01T22:00:00+08:00")


class ResourceTest(unittest.TestCase):
    def test_control_zone_conflict_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            svc = make_service(d, budget_cap=1000, global_quota_cap=50)
            seed_pilot(svc, pilot_id="pilot-a", zone="东湖环线",
                       controls=("西湖环线",), need_text="希望增加夜跑照明", group="夜跑居民群")
            with self.assertRaises(DomainError):
                seed_pilot(svc, pilot_id="pilot-b", zone="长椅区",
                           controls=("西湖环线",), budget=50.0,
                           need_text="希望保持长椅区安静", group="休憩居民群")

    def test_budget_cap_failure_is_atomic(self):
        with tempfile.TemporaryDirectory() as d:
            svc = make_service(d, budget_cap=300, global_quota_cap=50)
            seed_pilot(svc, pilot_id="pilot-a", budget=200.0)
            with self.assertRaises(DomainError):
                seed_pilot(svc, pilot_id="pilot-b", zone="南山坡", controls=(),
                           budget=200.0, need_text="儿童活动区需要软垫", group="家长群")
            # 不存在半笔预留；试点也不应被创建
            self.assertNotIn("pilot-b", svc.state.pilots)
            self.assertEqual(svc.state.total_reserved_budget(), 200.0)

    def test_expand_cap_failure_changes_nothing(self):
        with tempfile.TemporaryDirectory() as d:
            svc = make_service(d, budget_cap=250, global_quota_cap=15)
            pid = seed_pilot(svc, budget=200.0)
            with self.assertRaises(DomainError):
                svc.decide(pid, "expand", "王主任", "追加过多", add_budget=100.0)
            self.assertEqual(svc.state.total_reserved_budget(), 200.0)
            self.assertEqual(svc.state.pilots[pid].status, "running")

    def test_terminate_releases_atomically_and_keeps_history(self):
        with tempfile.TemporaryDirectory() as d:
            svc = make_service(d, budget_cap=1000, global_quota_cap=50)
            pid = seed_pilot(svc)
            svc.log_resource_usage(pid, 60.0, "灯具租赁",
                                   "2026-09-23T12:00:00+08:00")
            svc.send_notification(pid, "public", "试点开始通知",
                                  "2026-09-21T19:00:00+08:00")
            svc.decide(pid, "terminate", "王主任", "噪声触及安全线",
                       now="2026-09-24T09:00:00+08:00")
            # 名额与预算已释放、区域可复用
            self.assertEqual(svc.state.total_reserved_budget(), 0.0)
            self.assertEqual(svc.state.zones_taken(), {})
            # 已发生使用与通知保留
            self.assertEqual(len(svc.state.usages), 1)
            self.assertGreaterEqual(len(svc.state.notifications), 2)
            # 释放后区域可参加新试点
            seed_pilot(svc, pilot_id="pilot-c", zone="西湖环线", controls=(),
                       budget=30.0, need_text="希望在西湖环线增设安静阅读角",
                       group="阅读群", cluster_label="安静阅读簇")

    def test_extend_preserves_original_window_in_decision_history(self):
        with tempfile.TemporaryDirectory() as d:
            svc = make_service(d)
            pid = seed_pilot(svc)
            svc.decide(pid, "extend", "王主任", "需求稳定",
                       new_window_end="2026-11-21T22:00:00+08:00",
                       now="2026-10-20T09:00:00+08:00")
            self.assertEqual(svc.state.pilots[pid].ended_at,
                             parse_ts("2026-11-21T22:00:00+08:00"))
            rec = svc.state.pilots[pid].decisions[0]
            self.assertIn("new_window_end", rec)
            with self.assertRaises(DomainError):
                svc.decide(pid, "extend", "王主任", "不能缩短",
                           new_window_end="2026-10-22T22:00:00+08:00")


class IdempotencyAndOrderingTest(unittest.TestCase):
    def test_retransmitted_observation_counted_once(self):
        with tempfile.TemporaryDirectory() as d:
            svc = make_service(d)
            pid = seed_pilot(svc)
            args = (pid, "observation", "noise_db", 50.0, "obs-x",
                    "2026-09-22T20:00:00+08:00")
            svc.record_observation(*args)
            again = svc.record_observation(*args)
            self.assertEqual(again.duplicate_keys, ["obs-x"])
            self.assertEqual(len(svc.state.observations), 1)

    def test_batch_ordered_by_occurrence_time(self):
        with tempfile.TemporaryDirectory() as d:
            svc = make_service(d)
            pid = seed_pilot(svc)
            res = svc.ingest_observations(pid, [
                {"kind": "observation", "metric": "noise_db", "value": 51,
                 "idempotency_key": "late", "occurred_at": "2026-09-23T21:00:00+08:00"},
                {"kind": "observation", "metric": "noise_db", "value": 49,
                 "idempotency_key": "early", "occurred_at": "2026-09-22T21:00:00+08:00"},
                {"kind": "observation", "metric": "noise_db", "value": 49,
                 "idempotency_key": "early", "occurred_at": "2026-09-22T21:00:00+08:00"},
            ])
            self.assertEqual([o["payload"]["idempotency_key"] for o in res.recorded],
                             ["early", "late"])
            self.assertEqual(res.duplicate_keys, ["early"])

    def test_missing_idempotency_key_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            svc = make_service(d)
            pid = seed_pilot(svc)
            with self.assertRaises(DomainError):
                svc.ingest_observations(pid, [
                    {"kind": "observation", "metric": "noise_db", "value": 1,
                     "occurred_at": "2026-09-22T21:00:00+08:00"}])


class ReplayAndRestartTest(unittest.TestCase):
    def test_same_command_stream_byte_identical_event_log(self):
        def run_script(target_dir: str) -> bytes:
            svc = make_service(target_dir, clock=lambda: parse_ts(FIXED_NOW))
            pid = seed_pilot(svc)
            svc.record_observation(pid, "feedback", "satisfaction", 4.6, "fb-1",
                                   "2026-09-22T20:00:00+08:00")
            svc.log_resource_usage(pid, 30.0, "电费",
                                   "2026-09-23T12:00:00+08:00")
            svc.decide(pid, "expand", "王主任", "满意度高",
                       add_quota=3, add_budget=50, add_zones=["北湖环线"],
                       now="2026-09-25T09:00:00+08:00")
            with open(svc.store.path, "rb") as fh:
                return fh.read()

        with tempfile.TemporaryDirectory() as d1, tempfile.TemporaryDirectory() as d2:
            blob1 = run_script(d1)
            blob2 = run_script(d2)
            self.assertEqual(blob1, blob2)

    def test_reopen_yields_identical_state(self):
        with tempfile.TemporaryDirectory() as d:
            svc = make_service(d)
            pid = seed_pilot(svc)
            svc.record_observation(pid, "observation", "noise_db", 82.0, "o1",
                                   "2026-09-22T21:00:00+08:00")
            svc.decide(pid, "terminate", "王主任", "安全线",
                       now="2026-09-23T09:00:00+08:00")
            before = primitives(svc.state)
            # 模拟重启：新实例从同一事件流重建
            svc2 = make_service(d)
            after = primitives(svc2.state)
            self.assertEqual(before, after)
            self.assertEqual(svc2.state.total_reserved_budget(), 0.0)

    def test_fold_is_pure_and_deterministic(self):
        with tempfile.TemporaryDirectory() as d:
            svc = make_service(d)
            seed_pilot(svc)
            events = svc.store.load()
            self.assertEqual(primitives(fold(events)), primitives(fold(list(events))))

    def test_due_reviews_survive_restart(self):
        with tempfile.TemporaryDirectory() as d:
            svc = make_service(d)
            pid = seed_pilot(svc)
            svc.schedule_review(pid, "2026-10-01T09:00:00+08:00", "满月到期评审")
            svc2 = make_service(d)
            self.assertEqual(svc2.due_reviews("2026-09-30T00:00:00+08:00"), [])
            due = svc2.due_reviews("2026-10-02T00:00:00+08:00")
            self.assertEqual(len(due), 1)
            self.assertEqual(due[0]["pilot_id"], pid)
            # 已终止试点的到期评审不再出现
            svc2.decide(pid, "terminate", "王主任", "提前终止",
                        now="2026-09-29T09:00:00+08:00")
            self.assertEqual(svc2.due_reviews("2026-10-02T00:00:00+08:00"), [])

    def test_tail_truncation_tolerated_middle_corruption_raises(self):
        with tempfile.TemporaryDirectory() as d:
            svc = make_service(d)
            seed_pilot(svc)
            path = svc.store.path
            with open(path, encoding="utf-8") as fh:
                good = fh.read().splitlines(keepends=True)
            # 末尾半行（写入中途被杀）
            with open(path, "w", encoding="utf-8") as fh:
                fh.write("".join(good) + '{"event_id": "broken", "event_type":')
            self.assertEqual(len(EventStore(path).load()), len(good))
            # 中间坏行必须显式失败，避免错误结论
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(good[0] + "BROKEN_LINE\n" + "".join(good[1:]))
            with self.assertRaises(DomainError):
                EventStore(path).load()


class EvidenceTest(unittest.TestCase):
    def test_evidence_snapshot_explains_decision(self):
        with tempfile.TemporaryDirectory() as d:
            svc = make_service(d)
            pid = seed_pilot(svc)
            svc.record_observation(pid, "feedback", "satisfaction", 4.7, "fb-1",
                                   "2026-09-22T20:00:00+08:00", "夜跑群")
            svc.decide(pid, "expand", "王主任", "满意度高于目标，需求集中",
                       add_quota=2, now="2026-09-25T09:00:00+08:00")
            explanation = svc.explain_decision(pid)
            ev = explanation["decision"]["evidence"]
            self.assertEqual(ev["observation_count"], 1)
            self.assertEqual(ev["observation_keys"], ["fb-1"])
            self.assertEqual(ev["metrics"]["satisfaction"]["latest"], 4.7)
            self.assertEqual(explanation["frozen"]["approver"], "王主任")

    def test_later_data_cannot_rewrite_past_decision(self):
        with tempfile.TemporaryDirectory() as d:
            svc = make_service(d)
            pid = seed_pilot(svc)
            svc.record_observation(pid, "feedback", "satisfaction", 4.7, "fb-1",
                                   "2026-09-22T20:00:00+08:00")
            svc.decide(pid, "expand", "王主任", "早期数据支持扩大",
                       add_quota=2, now="2026-09-25T09:00:00+08:00")
            # 决策之后到达的数据（即使发生时间较早地补传，计数但不改写旧决策）
            svc.record_observation(pid, "feedback", "satisfaction", 2.1, "fb-late",
                                   "2026-09-26T20:00:00+08:00")
            first = svc.explain_decision(pid, index=0)["decision"]
            self.assertEqual(first["evidence"]["observation_keys"], ["fb-1"])
            # 重放同一事件流后，第一次决策的证据仍然不变
            svc2 = make_service(d)
            replayed = svc2.explain_decision(pid, index=0)["decision"]
            self.assertEqual(replayed, first)


if __name__ == "__main__":
    unittest.main()
