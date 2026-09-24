"""批量导入坏记录隔离、观测重传幂等、采样偏差复核、重启后到期评审续跑。"""
import unittest

from src.service import LedgerService
from src.store import EventStore
from tests._scenario import approved_pilot, at, new_service


class BatchImportTest(unittest.TestCase):
    def test_good_records_import_bad_records_quarantined(self) -> None:
        service, tmp = new_service()
        approved_pilot(service)
        records = [
            {"kind": "need", "need_id": "bn-1", "summary": "夜跑照明诉求", "source_group": "夜跑者"},
            {"kind": "need", "need_id": "bn-2", "source_group": "夜跑者"},  # 缺摘要
            {"kind": "need", "need_id": "bn-3", "summary": "联系 13800138000", "source_group": "x"},  # PII
            {"kind": "observation", "pilot_id": "p1", "observation_id": "bo-1",
             "observed_at": at(20, 19),
             "readings": [{"kind": "metric", "metric": "照度达标率", "value": 0.91}]},
            {"kind": "observation", "pilot_id": "ghost", "observation_id": "bo-2",
             "observed_at": at(20, 19), "readings": []},  # 不存在的试点
            {"kind": "survey", "id": "weird"},  # 不支持的类型
        ]
        result = service.import_batch("batch-1", records, source="网格员上报", at=at(19, 12))
        self.assertEqual(result["accepted"], 2)
        self.assertEqual(result["rejected"], 4)
        self.assertIn("bn-1", service.state.needs)
        self.assertNotIn("bn-2", service.state.needs)
        self.assertIn("bo-1", service.state.pilots["p1"].observations)
        batch = service.state.batches["batch-1"]
        self.assertEqual({q["index"] for q in batch["quarantined"]}, {1, 2, 4, 5})
        # 隔离记录中不回灌可能的个人摘要。
        for q in batch["quarantined"]:
            self.assertNotIn("summary", q["raw"])

    def test_retransmitted_observation_is_not_double_counted(self) -> None:
        service, _ = new_service()
        approved_pilot(service)
        reading = [{"kind": "metric", "metric": "照度达标率", "value": 0.91}]
        first = service.record_observation("p1", "dup-obs", at(20, 19), reading, at=at(20, 19))
        second = service.record_observation("p1", "dup-obs", at(20, 19), reading, at=at(20, 22))
        self.assertTrue(first["recorded"])
        self.assertFalse(second["recorded"])
        self.assertEqual(second["ignored"], "duplicate_observation")
        pilot = service.state.pilots["p1"]
        self.assertEqual(len(pilot.observations), 1)
        self.assertEqual(pilot.metric_aggregates()["照度达标率"]["count"], 1.0)

    def test_batch_retransmission_dedups_and_batch_id_is_idempotent(self) -> None:
        service, _ = new_service()
        approved_pilot(service)
        records = [
            {"kind": "need", "need_id": "rx-1", "summary": "诉求一", "source_group": "夜跑者"},
            {"kind": "need", "need_id": "rx-1", "summary": "诉求一重复", "source_group": "夜跑者"},
        ]
        result = service.import_batch("batch-x", records, at=at(19, 12))
        self.assertEqual(result["accepted"], 1)
        self.assertEqual(result["rejected"], 1)  # 批次内自然标识重复 -> 隔离
        # 整个批次用同 batch_id 重传：整体忽略。
        again = service.import_batch("batch-x", records, at=at(19, 13))
        self.assertEqual(again["ignored"], "duplicate_batch")
        self.assertEqual(len(service.state.needs), 7 + 1)  # seed 7 条 + 1 条新诉求

    def test_batch_with_safety_breach_still_pauses_atomically(self) -> None:
        service, _ = new_service()
        approved_pilot(service)
        records = [
            {"kind": "observation", "pilot_id": "p1", "observation_id": "safe-1",
             "observed_at": at(20, 21),
             "readings": [{"kind": "safety", "metric": "眩光值", "value": 88, "zones": ["A"]}]},
            {"kind": "observation", "pilot_id": "p1", "observation_id": "safe-bad",
             "observed_at": at(20, 21)},  # 缺 readings，隔离
        ]
        result = service.import_batch("batch-stop", records, at=at(20, 21, 30))
        self.assertEqual(result["accepted"], 1)
        self.assertEqual(result["rejected"], 1)
        self.assertEqual(service.state.pilots["p1"].status, "paused")
        self.assertEqual(service.state.pilots["p1"].paused_zones, {"A"})


class SamplingBiasTest(unittest.TestCase):
    def test_dominant_single_group_feedback_opens_bias_review(self) -> None:
        service, _ = new_service()
        approved_pilot(service)
        # bias_threshold=0.7：同一群体反馈占比超 70% 且达到最小样本量即立案（仅一次）。
        generated = []
        for i in range(4):
            result = service.record_feedback(
                "p1", f"fb-{i}", "夜跑者", at(20, 19, i * 5),
                rating=5, summary="希望更亮", at=at(20, 19, i * 5),
            )
            generated.extend(result.get("generated", []))
        self.assertEqual(generated.count("REVIEW_SCHEDULED"), 1)
        bias_reviews = [
            r for r in service.state.pilots["p1"].reviews.values()
            if "sampling_bias" in r["reasons"]
        ]
        self.assertEqual(len(bias_reviews), 1)

    def test_mixed_groups_stay_below_threshold(self) -> None:
        service, _ = new_service()
        approved_pilot(service)
        groups = ["夜跑者", "亲子家庭", "夜跑者", "周边居民"]
        for i, group in enumerate(groups):
            service.record_feedback(
                "p1", f"fb-{i}", group, at(20, 19, i * 5), summary="正常建议",
                at=at(20, 19, i * 5),
            )
        bias_reviews = [
            r for r in service.state.pilots["p1"].reviews.values()
            if "sampling_bias" in r["reasons"]
        ]
        self.assertEqual(bias_reviews, [])

    def test_feedback_retransmit_is_idempotent(self) -> None:
        service, _ = new_service()
        approved_pilot(service)
        service.record_feedback("p1", "fb-1", "夜跑者", at(20, 19), at=at(20, 19))
        again = service.record_feedback("p1", "fb-1", "夜跑者", at(20, 19), at=at(20, 20))
        self.assertFalse(again["recorded"])
        self.assertEqual(len(service.state.pilots["p1"].feedback), 1)


class RestartReviewTest(unittest.TestCase):
    def test_due_review_survives_restart_and_can_be_held(self) -> None:
        service, tmp = new_service()
        approved_pilot(service)
        service.record_observation(
            "p1", "o-dev", at(20, 20),
            [{"kind": "metric", "metric": "照度达标率", "value": 0.66}], at=at(20, 20),
        )
        review_id = next(iter(service.state.pilots["p1"].reviews))

        # 全新进程：重新加载文件并折叠，到期评审继续可见。
        reopened = LedgerService(EventStore(tmp / "journal.jsonl"))
        reopened.refresh()
        self.assertEqual(reopened.due_reviews(at(20, 21)), [])  # 还没到期
        due = reopened.due_reviews(at(23, 20))
        self.assertEqual(due, [{"pilot_id": "p1", "review_id": review_id,
                                "due_at": reopened.state.pilots["p1"].reviews[review_id]["due_at"]}])
        reopened.hold_review("p1", review_id, "短期波动，已恢复", "CONTINUE", held_at=at(23, 21))
        self.assertEqual(reopened.state.pilots["p1"].reviews[review_id]["status"], "held")
        # 已举行的复核不再出现在到期队列。
        self.assertEqual(reopened.due_reviews(at(30, 1)), [])


if __name__ == "__main__":
    unittest.main()
