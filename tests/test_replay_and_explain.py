"""同一事件流重复回放 -> 一致的决策与资源状态；决策可解释、不被后来数据重写。"""
import json
import unittest

from src.service import LedgerService
from src.state import fold, state_digest
from src.store import EventStore
from tests._scenario import approved_pilot, at, new_service


def run_full_script(service: LedgerService) -> None:
    """覆盖诉求/建议/审批/观测/反馈/复核/安全线/资源/决策/回滚/批量的完整脚本。"""
    approved_pilot(service)
    # 常规观测 + 偏离复核
    service.record_observation(
        "p1", "o1", at(20, 19),
        [{"kind": "metric", "metric": "照度达标率", "value": 0.72}], at=at(20, 19),
    )
    # 异常环境（暴雨）观测，不影响评估
    service.record_observation(
        "p1", "o-storm", at(20, 22),
        [{"kind": "safety", "metric": "眩光值", "value": 99, "zones": ["A"]}],
        anomaly=True, at=at(20, 22),
    )
    # 混合反馈
    for i, group in enumerate(["夜跑者", "亲子家庭"]):
        service.record_feedback(
            "p1", f"fb-{i}", group, at(20, 20 + i), summary="总体可行",
            at=at(20, 20 + i),
        )
    review_id = next(iter(service.state.pilots["p1"].reviews))
    service.hold_review("p1", review_id, "调试期波动", "CONTINUE", held_at=at(22, 10))
    # 复核闭合后的达标观测
    service.record_observation(
        "p1", "o2", at(22, 11),
        [{"kind": "metric", "metric": "照度达标率", "value": 0.92}], at=at(22, 11),
    )
    service.record_resource_use("p1", 800, 2, "灯具租赁", at=at(22, 12))
    # 扩大
    service.decide("p1", "EXPAND", "王主任", "dec-expand", at=at(22, 13),
                   extra_zones=["D"], extra_budget=500, extra_slots=2)
    # 安全越线暂停新区域
    service.record_observation(
        "p1", "o-stop", at(23, 20),
        [{"kind": "safety", "metric": "眩光值", "value": 80, "zones": ["D"]}],
        at=at(23, 20),
    )
    stop_id = next(iter(service.state.pilots["p1"].active_stops))
    service.resume_pilot("p1", ["D"], stop_id, "王主任", at=at(24, 9))
    # 回滚扩大：保留已用资源与全部通知
    service.rollback_decision("p1", "dec-expand", "新区域诉求不足", "王主任", at=at(24, 10))
    # 终止再回滚终止
    service.decide("p1", "TERMINATE", "王主任", "dec-term", at=at(24, 14))
    service.rollback_decision("p1", "dec-term", "诉求仍待观察", "王主任", at=at(24, 16))
    # 批量导入：1 好 1 坏
    service.import_batch(
        "batch-final",
        [
            {"kind": "need", "need_id": "late-1", "summary": "后续诉求", "source_group": "周边居民"},
            {"kind": "need", "need_id": "late-bad"},
        ],
        at=at(25, 9),
    )


class ReplayDeterminismTest(unittest.TestCase):
    def test_two_independent_journals_running_same_script_are_identical(self) -> None:
        svc_a, tmp_a = new_service()
        svc_b, tmp_b = new_service()
        run_full_script(svc_a)
        run_full_script(svc_b)

        bytes_a = (tmp_a / "journal.jsonl").read_bytes()
        bytes_b = (tmp_b / "journal.jsonl").read_bytes()
        # 事件内容（含确定性 event_id、业务时间、payload、summary）逐字节一致；
        # seq 不写入 event_id，但其顺序也一致。
        self.assertEqual(bytes_a, bytes_b)
        self.assertEqual(state_digest(svc_a.state), state_digest(svc_b.state))

    def test_reload_and_refold_from_disk_matches_live_state(self) -> None:
        service, tmp = new_service()
        run_full_script(service)
        live_digest = state_digest(service.state)

        reopened = LedgerService(EventStore(tmp / "journal.jsonl"))
        reopened.refresh()
        self.assertEqual(state_digest(reopened.state), live_digest)

        # 直接从磁盘事件再折叠一次，结果仍然相同。
        store = EventStore(tmp / "journal.jsonl")
        store.load()
        self.assertEqual(state_digest(fold(store.events)), live_digest)

    def test_replaying_events_in_reverse_then_sorted_matches(self) -> None:
        # 折叠对已持久化事件按 seq 排序：即便输入顺序被打乱，结果不变。
        service, _ = new_service()
        run_full_script(service)
        events = service.store.events
        digest_forward = state_digest(fold(events))
        digest_reversed = state_digest(fold(list(reversed(events))))
        self.assertEqual(digest_forward, digest_reversed)

    def test_event_ids_are_stable_across_replay(self) -> None:
        service, tmp = new_service()
        run_full_script(service)
        ids_first = [e.event_id for e in service.store.events]
        store = EventStore(tmp / "journal.jsonl")
        store.load()
        ids_second = [e.event_id for e in store.events]
        self.assertEqual(ids_first, ids_second)


class ExplainabilityTest(unittest.TestCase):
    def test_expand_decision_lists_supporting_data_and_excludes_anomaly(self) -> None:
        service, _ = new_service()
        run_full_script(service)
        explanation = service.explain_decision("p1", "dec-expand")
        decision = explanation["decision"]
        evidence = decision["evidence"]
        # 扩大时有效观测为 o1（偏离但已复核闭合）与 o2；异常 o-storm 被排除。
        self.assertEqual(evidence["observation_ids"], ["o1", "o2"])
        self.assertEqual(evidence["excluded_anomaly_ids"], ["o-storm"])
        self.assertEqual(evidence["feedback_ids"], ["fb-0", "fb-1"])
        self.assertEqual(evidence["review_ids"], [next(
            rid for rid in service.state.pilots["p1"].reviews
            if rid.endswith("o1")
        )])
        # 聚类只是参考。
        self.assertTrue(evidence["cluster_advisory"])
        # 决策前冻结版本是 v1，区域仅 A/B。
        self.assertEqual(decision["prior_spec_version"], 1)
        self.assertEqual(explanation["frozen_spec_before"]["zones"], ["A", "B"])
        # 决策后来被回滚，解释中能看到回滚事实与保留的已用量。
        self.assertIsNotNone(explanation["rollback"])
        self.assertEqual(explanation["rollback"]["retained_used_budget"], 800.0)

    def test_later_data_does_not_rewrite_past_decision_evidence(self) -> None:
        service, _ = new_service()
        run_full_script(service)
        # 决策在 09-22 13:00；之后追加大量观测，证据快照不得变化。
        for i in range(5):
            service.record_observation(
                "p1", f"late-o-{i}", at(26, 9 + i),
                [{"kind": "metric", "metric": "照度达标率", "value": 0.5}],
                at=at(26, 9 + i),
            )
        evidence = service.explain_decision("p1", "dec-expand")["decision"]["evidence"]
        self.assertEqual(evidence["observation_ids"], ["o1", "o2"])
        self.assertEqual(evidence["as_of"], at(22, 13))

    def test_decide_requires_sample_size_and_blocks_premature_expansion(self) -> None:
        service, _ = new_service()
        approved_pilot(service)
        from src.events import DomainError
        # 零有效观测：样本不足，扩大被阻止。
        with self.assertRaises(DomainError):
            service.decide("p1", "EXPAND", "王主任", "premature", at=at(21, 9),
                           extra_zones=["D"], extra_budget=100, extra_slots=1)

    def test_timeline_orders_observations_and_feedback_by_occurrence_time(self) -> None:
        service, _ = new_service()
        run_full_script(service)
        timeline = service.pilot_timeline("p1")
        times = [item["at"] for item in timeline]
        self.assertEqual(times, sorted(times))
        self.assertIn("o-storm", [item["id"] for item in timeline])


class EventIntegrityTest(unittest.TestCase):
    def test_store_rejects_version_gap_and_duplicate_id(self) -> None:
        service, _ = new_service()
        run_full_script(service)
        from src.events import DomainError, make_event, AG_PILOT, OBSERVATION_RECORDED
        # 制造版本断裂事件并尝试直接追加 -> 拒绝。
        bad = make_event(
            OBSERVATION_RECORDED, AG_PILOT, "p1", at(27, 9),
            {"pilot_id": "p1", "observation_id": "x", "observed_at": at(27, 9),
             "readings": [], "zones": ["A"]},
            "断裂版本", version=service.store.version_of("p1") + 5,
        )
        with self.assertRaises(DomainError):
            service.store.append(bad)

    def test_loading_tampered_journal_raises(self) -> None:
        service, tmp = new_service()
        run_full_script(service)
        path = tmp / "journal.jsonl"
        lines = path.read_text(encoding="utf-8").splitlines()
        tampered = dict(json.loads(lines[5]))
        tampered["summary"] = "被篡改的摘要"
        lines[5] = json.dumps(tampered, ensure_ascii=False)
        path.write_text("\n".join(lines), encoding="utf-8")
        store = EventStore(path)
        from src.events import DomainError
        with self.assertRaises(DomainError):
            store.load()  # event_id 与内容不符会在版本/重复校验或折叠中暴露问题


if __name__ == "__main__":
    unittest.main()
