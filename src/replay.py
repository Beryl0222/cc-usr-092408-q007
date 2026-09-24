"""只读回放工具：从事件流重建决策账并解释试点决策。

用法：
    python3 -m src.replay <events.jsonl>                # 列出试点与状态
    python3 -m src.replay <events.jsonl> <pilot_id>     # 解释该试点每次决策的证据

不写入、不改写任何事件；同一文件多次执行输出一致。
"""
from __future__ import annotations

import json
import sys

from .ledger import EventStore, DomainError, fold


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(__doc__)
        return 2
    path = argv[1]
    try:
        state = fold(EventStore(path).load())
    except DomainError as exc:
        print(f"事件流损坏，拒绝回放：{exc}", file=sys.stderr)
        return 1

    if len(argv) == 2:
        for pid, pilot in state.pilots.items():
            print(f"{pid}\t状态={pilot.status}\t区域={pilot.zone}\t"
                  f"窗口={pilot.started_at.isoformat()}~{pilot.ended_at.isoformat()}\t"
                  f"决策次数={len(pilot.decisions)}")
        print(f"预留预算合计={state.total_reserved_budget()}\t"
              f"占用区域={sorted(state.zones_taken())}\t"
              f"观测总数={len(state.observations)}")
        return 0

    pilot_id = argv[2]
    pilot = state.pilots.get(pilot_id)
    if not pilot:
        print(f"试点不存在：{pilot_id}", file=sys.stderr)
        return 1

    frozen = {
        "zone": pilot.zone,
        "window": {"start": pilot.started_at.isoformat(),
                   "end": pilot.ended_at.isoformat()},
        "success_metrics": pilot.success_metrics,
        "safety_limits": pilot.safety_limits,
        "approver": pilot.approver,
    }
    print("== 冻结基线 ==")
    print(json.dumps(frozen, ensure_ascii=False, indent=2))
    for i, dec in enumerate(pilot.decisions):
        print(f"== 决策 {i}: {dec['decision']}（{dec['decided_at'].isoformat()}，"
              f"批准人 {dec['approver']}）==")
        print(f"理由：{dec['reason']}")
        print("证据快照：")
        print(json.dumps(dec["evidence"], ensure_ascii=False, indent=2))
    print("== 回滚后仍保留 ==")
    print(f"资源使用记录：{len(pilot.usages)} 条；通知记录：{len(pilot.notifications)} 条")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
