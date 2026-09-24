"""资源占用原子释放、冲突区域互斥、回滚保留已发生使用与通知。"""
import unittest

from src.events import DomainError
from tests._scenario import approved_pilot, at, new_service, pilot_spec


class ReservationTest(unittest.TestCase):
    def test_approval_reserves_budget_and_slots_atomically(self) -> None:
        service, _ = new_service()
        approved_pilot(service)
        pool = service.state.pools["pool-1"]
        self.assertEqual(pool["budget_reserved"], 3000.0)
        self.assertEqual(pool["slots_reserved"], 6)
        available_b, available_s = service.state.pool_available("pool-1")
        self.assertEqual(available_b, 7000.0)
        self.assertEqual(available_s, 14)

    def test_insufficient_budget_is_rejected_and_reserves_nothing(self) -> None:
        service, _ = new_service()
        approved_pilot(service)
        result = service.approve_pilot(
            pilot_spec(pilot_id="p2", budget_total=9000, slots=20,
                       zones=["X"], control_zones=[]),
            at=at(3, 9),
        )
        self.assertFalse(result["approved"])
        self.assertTrue(any("预算不足" in r for r in result["reasons"]))
        # 驳回不留任何资源占用。
        pool = service.state.pools["pool-1"]
        self.assertEqual(pool["budget_reserved"], 3000.0)
        self.assertEqual(pool["slots_reserved"], 6)

    def test_resource_use_cannot_exceed_reservation(self) -> None:
        service, _ = new_service()
        approved_pilot(service)
        with self.assertRaises(DomainError):
            service.record_resource_use("p1", 5000, 1, "超额", at=at(21, 9))
        with self.assertRaises(DomainError):
            service.record_resource_use("p1", 100, 99, "超额名额", at=at(21, 9))


class ZoneConflictTest(unittest.TestCase):
    def test_conflicting_pilot_cannot_share_zone(self) -> None:
        service, _ = new_service()
        approved_pilot(service)
        result = service.approve_pilot(
            pilot_spec(pilot_id="p2", title="安静休憩试点",
                       zones=["A"], control_zones=[],
                       conflict_tags=["quiet_rest"], budget_total=100, slots=1),
            at=at(3, 9),
        )
        self.assertFalse(result["approved"])
        self.assertTrue(any("冲突类" in r and "A" in r for r in result["reasons"]))

    def test_control_zone_cannot_join_any_running_pilot(self) -> None:
        service, _ = new_service()
        approved_pilot(service)
        # 即使诉求标签不同，对照区域 C 在试点期内也不得入组。
        result = service.approve_pilot(
            pilot_spec(pilot_id="p2", title="儿童活动",
                       zones=["C"], control_zones=[],
                       conflict_tags=["children_activity"], budget_total=100, slots=1),
            at=at(3, 9),
        )
        self.assertFalse(result["approved"])
        self.assertTrue(any("对照区域" in r for r in result["reasons"]))

    def test_zone_released_after_terminate_allows_new_pilot(self) -> None:
        service, _ = new_service()
        approved_pilot(service)
        service.record_observation(
            "p1", "o1", at(20, 19),
            [{"kind": "metric", "metric": "照度达标率", "value": 0.9}], at=at(20, 19),
        )
        service.decide("p1", "TERMINATE", "王主任", "term-1", at=at(21, 9))
        result = service.approve_pilot(
            pilot_spec(pilot_id="p2", title="儿童活动试点",
                       zones=["A", "C"], control_zones=["D"],
                       conflict_tags=["children_activity"], budget_total=100, slots=1),
            at=at(22, 9),
        )
        self.assertTrue(result["approved"])


class RollbackRetentionTest(unittest.TestCase):
    def _expanded_with_use(self, service):
        approved_pilot(service)
        service.record_observation(
            "p1", "o1", at(20, 19),
            [{"kind": "metric", "metric": "照度达标率", "value": 0.9}], at=at(20, 19),
        )
        service.record_observation(
            "p1", "o2", at(20, 20),
            [{"kind": "metric", "metric": "照度达标率", "value": 0.92}], at=at(20, 20),
        )
        service.record_resource_use("p1", 1000, 2, "灯具租赁", at=at(21, 8))
        service.decide("p1", "EXPAND", "王主任", "dec-expand", at=at(21, 10),
                       extra_zones=["D"], extra_budget=500, extra_slots=2)
        service.record_resource_use("p1", 300, 1, "新区域线缆", at=at(21, 12))

    def test_terminate_atomically_releases_remaining_reservation(self) -> None:
        service, _ = new_service()
        self._expanded_with_use(service)
        service.decide("p1", "TERMINATE", "王主任", "dec-term", at=at(21, 14))
        pool = service.state.pools["pool-1"]
        self.assertEqual(pool["budget_reserved"], 0.0)
        self.assertEqual(pool["slots_reserved"], 0)
        # 已发生使用不回收。
        self.assertEqual(pool["budget_used"], 1300.0)
        self.assertEqual(pool["slots_used"], 3)

    def test_rollback_expand_releases_only_untouched_new_tier(self) -> None:
        service, _ = new_service()
        self._expanded_with_use(service)
        notices_before = len(service.state.notifications)
        result = service.rollback_decision(
            "p1", "dec-expand", "新区域夜间噪声投诉集中", "王主任", at=at(21, 16)
        )
        # FIFO 下新层尚未被消耗，回滚扩大原子释放整个新层 500/2；
        # 老层已用部分（含第二笔使用）全部保留为 used。
        self.assertEqual(result["released"], {"budget": 500.0, "slots": 2})
        pool = service.state.pools["pool-1"]
        self.assertEqual(pool["budget_used"], 1300.0)
        self.assertEqual(pool["slots_used"], 3)
        self.assertEqual(pool["budget_reserved"], 1700.0)
        self.assertEqual(pool["slots_reserved"], 3)
        pilot = service.state.pilots["p1"]
        self.assertEqual(pilot.zones, ["A", "B"])
        self.assertEqual(pilot.spec_version, 1)
        # 已发通知不删除，且回滚本身再发一条。
        self.assertGreater(len(service.state.notifications), notices_before)

    def test_rollback_terminate_restores_reservation_but_keeps_used(self) -> None:
        service, _ = new_service()
        self._expanded_with_use(service)
        service.decide("p1", "TERMINATE", "王主任", "dec-term", at=at(21, 14))
        result = service.rollback_decision(
            "p1", "dec-term", "诉求仍待验证", "王主任", at=at(21, 18)
        )
        # 回滚终止把终止时的剩余预留（2200/5）原样补回；已用 1300/3 保留。
        self.assertEqual(result["released"], {"budget": -2200.0, "slots": -5})
        pool = service.state.pools["pool-1"]
        self.assertEqual(pool["budget_reserved"], 2200.0)
        self.assertEqual(pool["slots_reserved"], 5)
        self.assertEqual(pool["budget_used"], 1300.0)
        pilot = service.state.pilots["p1"]
        self.assertEqual(pilot.status, "approved")
        # 扩大未被回滚，仍停留在 v2 与区域 D。
        self.assertEqual(pilot.spec_version, 2)
        self.assertEqual(pilot.zones, ["A", "B", "D"])

    def test_cannot_rollback_already_rolled_back_or_non_latest(self) -> None:
        service, _ = new_service()
        self._expanded_with_use(service)
        service.rollback_decision("p1", "dec-expand", "理由", "王主任", at=at(21, 16))
        with self.assertRaises(DomainError):
            service.rollback_decision("p1", "dec-expand", "再次回滚", "王主任", at=at(21, 17))


if __name__ == "__main__":
    unittest.main()
