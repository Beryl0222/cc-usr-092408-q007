"""隐私（只存去标识摘要+来源群体）与"聚类是建议不是事实"。"""
import unittest

from src.events import DomainError
from src.state import fold
from tests._scenario import approved_pilot, new_service


class PrivacyTest(unittest.TestCase):
    def test_need_keeps_only_deidentified_summary_and_group(self) -> None:
        service, _ = new_service()
        event = service.log_need(
            "need-x", "希望延长夜间照明，无需登记个人信息", "夜跑者",
            theme_tags=["night_lighting"], at="2026-09-01T10:00:00+08:00",
        )
        need = service.state.needs["need-x"]
        self.assertEqual(set(need), {"need_id", "summary", "source_group", "theme_tags", "recorded_at"})
        self.assertEqual(need["source_group"], "夜跑者")
        # 事件里不应出现任何除摘要/群体外的个人字段。
        self.assertNotIn("name", event.payload)
        self.assertNotIn("contact", event.payload)

    def test_phone_email_idcard_are_rejected(self) -> None:
        service, _ = new_service()
        for text in ("联系我 13800138000", "邮箱 a@b.com", "证件 11010119900307123X"):
            with self.assertRaises(DomainError):
                service.log_need(f"need-{text[:4]}", text, "夜跑者")

    def test_feedback_also_screened(self) -> None:
        service, _ = new_service()
        approved_pilot(service)
        with self.assertRaises(DomainError):
            service.record_feedback(
                "p1", "f-bad", "夜跑者", "2026-09-20T19:00:00+08:00",
                summary="回电 13912345678",
            )


class AdvisoryClusterTest(unittest.TestCase):
    def test_cluster_is_flagged_advisory_and_does_not_rewrite_needs(self) -> None:
        service, _ = new_service()
        approved_pilot(service)  # 内部已建立 c-run
        cluster = service.state.clusters["c-run"]
        self.assertTrue(cluster["advisory"])
        # 聚类不回写诉求：诉求记录上没有 cluster 归属字段。
        need = service.state.needs["need-0"]
        self.assertNotIn("cluster_id", need)

    def test_cluster_cannot_reference_unknown_need(self) -> None:
        service, _ = new_service()
        service.fund_pool("pool-1", 1, 1, at="2026-09-01T09:00:00+08:00")
        with self.assertRaises(DomainError):
            service.propose_cluster("c-x", ["ghost-need"], "主题")

    def test_re_proposing_same_cluster_is_rejected_not_overwrite(self) -> None:
        service, _ = new_service()
        approved_pilot(service)
        with self.assertRaises(DomainError):
            service.propose_cluster("c-run", ["need-0"], "另一个主题", confidence=0.2)

    def test_approval_records_cluster_as_reference_only(self) -> None:
        service, _ = new_service()
        approved_pilot(service)
        pilot = service.state.pilots["p1"]
        self.assertEqual(pilot.cluster_ids, ["c-run"])
        # 决策证据中聚类被显式标注为 advisory。
        service.record_observation(
            "p1", "o1", "2026-09-20T19:00:00+08:00",
            [{"kind": "metric", "metric": "照度达标率", "value": 0.9}],
            at="2026-09-20T19:00:00+08:00",
        )
        evidence = service.evidence_snapshot("p1", "2026-09-20T20:00:00+08:00")
        self.assertTrue(evidence["cluster_advisory"])
        self.assertIn("c-run", evidence["cluster_ids"])
        # 纯函数折叠不依赖 I/O：直接对事件流折叠结果一致。
        again = fold(service.store.events)
        self.assertEqual(again.clusters["c-run"]["advisory"], True)


if __name__ == "__main__":
    unittest.main()
