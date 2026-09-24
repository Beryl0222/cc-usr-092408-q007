"""命令行：回放事件账并输出确定性指纹、试点状态与决策依据。

用法：
    python3 -m src.replay path/to/journal.jsonl
    python3 -m src.replay path/to/journal.jsonl --pilot p1 --explain dec-1

重复回放同一事件文件，digest 必须逐字节一致；换进程/重启后亦同。
"""
from __future__ import annotations

import argparse
import json
import sys

from .state import fold, state_digest
from .store import EventStore


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="回放公园试点决策账")
    parser.add_argument("journal", help="JSONL 事件账文件")
    parser.add_argument("--pilot", help="只输出指定试点的状态与时间线")
    parser.add_argument("--explain", metavar="DECISION_ID", help="解释指定决策由哪些数据支持")
    parser.add_argument("--json", action="store_true", help="以 JSON 输出")
    args = parser.parse_args(argv)

    store = EventStore(args.journal)
    store.load()
    state = fold(store.events)
    digest = state_digest(state)

    report: dict[str, object] = {
        "journal": args.journal,
        "event_count": len(store.events),
        "digest": digest,
    }

    if args.pilot:
        if args.pilot not in state.pilots:
            print(f"试点不存在：{args.pilot}", file=sys.stderr)
            return 2
        pilot = state.pilots[args.pilot]
        if args.explain:
            report["explanation"] = state.explain_decision(args.pilot, args.explain)
        else:
            pool = state.pools.get(pilot.pool_id) if pilot.pool_id else None
            report["pilot"] = {
                "pilot_id": pilot.pilot_id,
                "title": pilot.title,
                "status": pilot.status,
                "spec_version": pilot.spec_version,
                "approver": pilot.approver,
                "zones": pilot.zones,
                "control_zones": pilot.control_zones,
                "window": {"start": pilot.window_start, "end": pilot.window_end},
                "paused_zones": sorted(pilot.paused_zones),
                "reserved": {"budget": pilot.reserved_budget, "slots": pilot.reserved_slots},
                "used": {"budget": pilot.used_budget, "slots": pilot.used_slots},
                "pool": pool,
                "metric_aggregates": pilot.metric_aggregates(),
                "feedback_group_share": pilot.feedback_group_share(),
                "open_reviews": [
                    rid for rid, r in pilot.reviews.items() if r["status"] == "scheduled"
                ],
                "decisions": [
                    {"decision_id": d["decision_id"], "decision": d["decision"],
                     "approver": d["approver"], "decided_at": d["decided_at"],
                     "observation_ids": d["evidence"].get("observation_ids"),
                     "excluded_anomaly_ids": d["evidence"].get("excluded_anomaly_ids"),
                     "feedback_ids": d["evidence"].get("feedback_ids"),
                     "review_ids": d["evidence"].get("review_ids")}
                    for d in pilot.decisions
                ],
                "rollbacks": pilot.rollbacks,
            }
    else:
        report["pools"] = state.pools
        report["pilots"] = sorted(
            (pid, p.status, p.spec_version) for pid, p in state.pilots.items()
        )
        report["notifications_sent"] = len(state.notifications)

    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    else:
        print(f"事件数 {report['event_count']}  回放指纹 {digest}")
        if args.pilot and not args.explain:
            pilot_report = report["pilot"]
            print(f"\n试点 {pilot_report['pilot_id']}「{pilot_report['title']}」"
                  f" 状态={pilot_report['status']} 冻结版本=v{pilot_report['spec_version']}")
            print(f"窗口 {pilot_report['window']['start']} ~ {pilot_report['window']['end']}")
            print(f"区域 {pilot_report['zones']} 对照 {pilot_report['control_zones']}")
            print(f"预留 {pilot_report['reserved']} 已发生使用 {pilot_report['used']}")
            print(f"指标聚合 {json.dumps(pilot_report['metric_aggregates'], ensure_ascii=False)}")
            print(f"反馈群体占比 {pilot_report['feedback_group_share']}")
            print("决策：")
            for d in pilot_report["decisions"]:
                print(f"  - {d['decision']} {d['decision_id']}（{d['approver']}）"
                      f" 证据观测 {d['observation_ids']} 异常排除 {d['excluded_anomaly_ids']}")
        elif args.pilot and args.explain:
            print(json.dumps(report["explanation"], ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
