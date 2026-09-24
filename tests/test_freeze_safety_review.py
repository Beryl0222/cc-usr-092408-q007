"""试点冻结、安全停止线、普通偏差复核、异常环境隔离。"""
import unittest

from src.events import DomainError
from tests._scenario import approved_pilot, at, new_service


class FrozenPilotTest(unittest.TestCase):
    def test_pilot_freezes_zones_window_metrics_limits_approver(self) -> None:
        service, _ = new_service()
        approved_pilot(service)
        pilot = service.state.pilots["p1"]
        self.assertEqual(pilot.zones, ["A", "B"])
        self.assertEqual(pilot.control_zones, ["C"])
        self.assertEqual(pilot.window_start, at(20, 18))
        self.assertEqual(pilot.window_end, "2026-10-20T22:00:00+08:00")
        self.assertIn("照度达标率", pilot.metrics)
        self.assertEqual(pilot.safety_limits["眩光值"], 50.0)
        self.assertEqual(pilot.approver, "王主任")
        # 没有原地"改冻结"的命令；状态只能由追加事件演进。
        snapshot = service.state.explain_decision  # API exists
        self.assertTrue(callable(snapshot))

    def test_approval_rejects_missing_zones_window_metrics_limits_approver(self) -> None:
        service, _ = new_service()
        approved_pilot(service)
        for key in ("zones", "window", "metrics", "safety_limits", "approver"):
            bad = {"pilot_id": f"bad-{key}", "zones": ["Z"], "window": {"start": at(1, 1), "end": at(2, 1)},
                   "metrics": {"m": {"target": 1, "tolerance": 0}}, "safety_limits": {"s": {"max": 1}},
                   "approver": "x"}
            if key == "zones":
                bad["zones"] = []
            elif key == "window":
                bad["window"] = {"start": at(1, 1)}
            elif key == "metrics":
                bad["metrics"] = {}
            elif key == "safety_limits":
                bad["safety_limits"] = {}
            elif key == "approver":
                bad["approver"] = ""
            result = service.approve_pilot(bad, at=at(3, 9))
            self.assertFalse(result["approved"], key)
            reason_text = " ".join(result["reasons"])
            expected = {
                "zones": "区域",
                "window": "时间窗",
                "metrics": "成功指标",
                "safety_limits": "安全停止线",
                "approver": "批准人",
            }[key]
            self.assertIn(expected, reason_text)


class SafetyStopTest(unittest.TestCase):
    def test_breaching_safety_limit_pauses_zone_immediately_and_notifies(self) -> None:
        service, _ = new_service()
        approved_pilot(service)
        result = service.record_observation(
            "p1", "obs-stop", at(20, 21),
            [{"kind": "safety", "metric": "眩光值", "value": 73, "zones": ["B"]}],
            at=at(20, 21),
        )
        self.assertIn("SAFETY_STOP_TRIGGERED", result["generated"])
        self.assertIn("PILOT_PAUSED", result["generated"])
        self.assertIn("NOTIFICATION_SENT", result["generated"])
        pilot = service.state.pilots["p1"]
        self.assertEqual(pilot.status, "paused")
        self.assertEqual(pilot.paused_zones, {"B"})
        self.assertTrue(all(not s["resolved"] for s in pilot.active_stops.values()))
        # 只有 B 被暂停；A 仍在跑。
        self.assertNotIn("A", pilot.paused_zones)
        notice = service.state.notifications[-1]
        self.assertIn("B", notice["recipient_scope"])

    def test_paused_pilot_cannot_expand_or_extend(self) -> None:
        service, _ = new_service()
        approved_pilot(service)
        service.record_observation(
            "p1", "obs-stop", at(20, 21),
            [{"kind": "safety", "metric": "眩光值", "value": 73, "zones": ["B"]}],
            at=at(20, 21),
        )
        with self.assertRaises(DomainError):
            service.decide("p1", "EXTEND", "王主任", "d-x", at=at(21, 9),
                           extend_window_end="2026-11-01T22:00:00+08:00")

    def test_resume_requires_approver_and_stop_reference(self) -> None:
        service, _ = new_service()
        approved_pilot(service)
        service.record_observation(
            "p1", "obs-stop", at(20, 21),
            [{"kind": "safety", "metric": "眩光值", "value": 73, "zones": ["B"]}],
            at=at(20, 21),
        )
        with self.assertRaises(DomainError):
            service.resume_pilot("p1", ["B"], "nope-stop", "王主任", at=at(21, 8))
        stop_id = next(iter(service.state.pilots["p1"].active_stops))
        service.resume_pilot("p1", ["B"], stop_id, "王主任", at=at(21, 9))
        pilot = service.state.pilots["p1"]
        self.assertEqual(pilot.status, "approved")
        self.assertTrue(all(s["resolved"] for s in pilot.active_stops.values()))


class DeviationReviewTest(unittest.TestCase):
    def test_normal_metric_deviation_schedules_review_not_pause(self) -> None:
        service, _ = new_service()
        approved_pilot(service)
        result = service.record_observation(
            "p1", "obs-dev", at(20, 20),
            [{"kind": "metric", "metric": "照度达标率", "value": 0.70}], at=at(20, 20),
        )
        self.assertEqual(result["generated"], ["REVIEW_SCHEDULED"])
        review = next(iter(service.state.pilots["p1"].reviews.values()))
        self.assertEqual(review["status"], "scheduled")
        self.assertEqual(review["reasons"], ["metric_deviation"])
        self.assertEqual(service.state.pilots["p1"].status, "approved")

    def test_open_review_blocks_expand_until_held(self) -> None:
        service, _ = new_service()
        approved_pilot(service)
        service.record_observation(
            "p1", "o1", at(20, 20),
            [{"kind": "metric", "metric": "照度达标率", "value": 0.70}], at=at(20, 20),
        )
        with self.assertRaises(DomainError):
            service.decide("p1", "EXPAND", "王主任", "d-block", at=at(21, 9),
                           extra_zones=["D"], extra_budget=100, extra_slots=1)
        review_id = next(iter(service.state.pilots["p1"].reviews))
        service.hold_review("p1", review_id, "调试期波动", "CONTINUE", held_at=at(21, 10))
        # 复核闭合后仍需满足样本量；这里 required_sample_size=2，只补一条好观测即可扩大。
        service.record_observation(
            "p1", "o2", at(21, 11),
            [{"kind": "metric", "metric": "照度达标率", "value": 0.91}], at=at(21, 11),
        )
        out = service.decide("p1", "EXPAND", "王主任", "d-ok", at=at(21, 12),
                             extra_zones=["D"], extra_budget=100, extra_slots=1)
        self.assertEqual(out["decision"], "EXPAND")

    def test_same_metric_does_not_open_duplicate_reviews(self) -> None:
        service, _ = new_service()
        approved_pilot(service)
        for i, value in enumerate([0.7, 0.68, 0.65]):
            service.record_observation(
                "p1", f"o{i}", at(20, 20 + i),
                [{"kind": "metric", "metric": "照度达标率", "value": value}], at=at(20, 20 + i),
            )
        reviews = [r for r in service.state.pilots["p1"].reviews.values()]
        self.assertEqual(len(reviews), 1)


class AnomalyIsolationTest(unittest.TestCase):
    def test_anomalous_environment_does_not_trigger_stop_or_review(self) -> None:
        service, _ = new_service()
        approved_pilot(service)
        result = service.record_observation(
            "p1", "obs-storm", at(20, 22),
            [{"kind": "safety", "metric": "眩光值", "value": 99, "zones": ["A", "B"]}],
            anomaly=True, at=at(20, 22),
        )
        self.assertEqual(result["generated"], [])
        pilot = service.state.pilots["p1"]
        self.assertEqual(pilot.status, "approved")
        self.assertEqual(pilot.paused_zones, set())
        # 异常观测被记录但不计入指标聚合，也不进入决策证据。
        self.assertIn("obs-storm", pilot.observations)
        evidence = service.evidence_snapshot("p1", at(20, 23))
        self.assertIn("obs-storm", evidence["excluded_anomaly_ids"])
        self.assertNotIn("obs-storm", evidence["observation_ids"])


if __name__ == "__main__":
    unittest.main()
