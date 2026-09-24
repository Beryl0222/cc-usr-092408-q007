# 公园需求可逆试点决策账

面向"夜跑照明 / 儿童活动 / 安静休憩"等相互冲突的居民诉求，提供一套**事件溯源、可逆、可解释**的试点决策账。目标是防止采样偏差、异常环境条件和重复反馈让小范围试点结论被过早推广。

## 设计原则（对应管理诉求）

| 管理诉求 | 落地方式 |
| --- | --- |
| 原始诉求只保存去标识摘要与来源群体 | `NEED_LOGGED` 仅含 `summary/source_group/theme_tags`；登记与导入时用正则拦截手机号、邮箱、身份证号 |
| 聚类结果是建议而非事实 | `CLUSTER_PROPOSED` 恒带 `advisory: true`，不回写诉求，审批只把它当参考；决策证据里也显式标注 `cluster_advisory` |
| 试点冻结五要素与批准人 | `PILOT_APPROVED` 冻结目标/对照区域、时间窗、成功指标及容差、安全停止线、批准人；没有"原地改冻结"的命令 |
| 观测与反馈按发生时间进入，后来数据不重写当时决策 | 事件仅追加；观测/反馈带 `observed_at`；决策证据快照以决策时刻为界，后续数据只追加、不改快照 |
| 达到安全停止线立即暂停受影响区域 | 安全读数越线在同一原子批次产生 `SAFETY_STOP_TRIGGERED + PILOT_PAUSED + NOTIFICATION_SENT` |
| 普通指标偏离进入复核 | 容差外读数产生 `REVIEW_SCHEDULED`（带到期时间），未闭合的复核阻止扩大/延长 |
| 回滚保留已发生资源使用与通知 | `used_*` 与通知只增不冲销；回滚只调整未使用预留 |
| 对照区域不能同时参加冲突试点 | 冲突标签相同的在跑试点不得共用区域；对照区域在试点期内对任何新试点关闭 |
| 名额预算占用原子释放 | 审批/扩大与占用、终止与释放都在一个提交批次；分层预留 + FIFO 消耗 |
| 批量导入隔离坏记录 | 好记录入账、坏记录进 `BATCH_QUARANTINED`（不回灌敏感摘要），批次整体原子提交 |
| 同一观测重传不重复计数 | 以 `observation_id/feedback_id` 为幂等键，服务层与折叠层双重去重 |
| 重启后继续到期评审 | 复核到期时间持久化；新进程 `refresh()` 重载折叠后 `due_reviews()` 继续可见 |
| 能解释一次扩大/延长/终止由哪些数据支持 | 每个 `PILOT_DECIDED` 带证据快照：有效观测、被排除的异常观测、反馈、复核、停止线、聚类参考、指标聚合 |
| 同一事件流重复回放结论一致 | `event_id` 为内容规范化哈希；`fold(events)` 是纯函数；提供状态指纹 `state_digest` |

## 目录

- `contracts/domain.schema.json`：事件类型、聚合类型与各事件 payload 约定。
- `src/events.py`：事件定义、时区时间解析、确定性 `event_id`（内容 SHA-256 前缀）。
- `src/store.py`：JSONL 仅追加存储；整批原子写（临时文件 + fsync + rename）、版本单调校验、内容哈希防篡改。
- `src/state.py`：纯函数 `fold`、试点/资源池状态、分层预留台账、确定性状态指纹。
- `src/service.py`：`LedgerService` 命令门面（登记诉求、建议聚类、审批、观测、反馈、复核、暂停/恢复、资源使用、决策、回滚、批量导入）。
- `src/replay.py`：回放/审计 CLI。
- `tests/`：45 个行为测试，覆盖上述全部约束。

## 资源记账模型

资源池有 `total / reserved / used`；试点持有按冻结版本分层的未使用预留 `reserve_tiers`：

- 审批：创建第 1 层预留；扩大：新增一层；延长：只改窗口不占资源。
- 已发生使用 `RESOURCE_USED` 按 **FIFO** 从最早层消耗，`used` 永不冲销。
- 终止：在 `PILOT_DECIDED` 内原子清空全部剩余层并释放回池（同时封存分层台账，供回滚终止原样补回）。
- 回滚扩大：移除最新层并释放其**未使用**余量；老层已用部分保留。
- 回滚终止：按封存台账重建预留并补回池；终止前已用部分仍留在 `used`。

## 典型流程

```python
from src.store import EventStore
from src.service import LedgerService

svc = LedgerService(EventStore("journal.jsonl"))
svc.fund_pool("pool-1", budget=10000, slots=20)
svc.log_need("need-0", "希望延长环湖步道夜灯", "夜跑者")
svc.propose_cluster("c-run", ["need-0"], "夜跑照明", confidence=0.82)
svc.approve_pilot({
    "pilot_id": "p1", "zones": ["A", "B"], "control_zones": ["C"],
    "conflict_tags": ["night_lighting", "quiet_rest"],
    "window": {"start": "2026-09-20T18:00:00+08:00", "end": "2026-10-20T22:00:00+08:00"},
    "metrics": {"照度达标率": {"target": 0.9, "tolerance": 0.05}},
    "safety_limits": {"眩光值": {"max": 50}},
    "bias_threshold": 0.7, "required_sample_size": 2,
    "budget_total": 3000, "slots": 6, "pool_id": "pool-1", "approver": "王主任",
})
# 观测：越安全线 -> 立即暂停；普通偏离 -> 复核；anomaly=True 的异常环境观测只留档不评估
svc.record_observation("p1", "obs-1", "2026-09-20T21:00:00+08:00",
                       [{"kind": "safety", "metric": "眩光值", "value": 73, "zones": ["B"]}])
```

决策必须带证据，且样本不足 / 有未闭合复核 / 处于安全暂停时会被阻止：

```python
svc.decide("p1", "EXPAND", "王主任", "dec-1",
           extra_zones=["D"], extra_budget=500, extra_slots=2)
svc.rollback_decision("p1", "dec-1", "新区域诉求不足", "王主任")  # 已用资源与通知保留
print(svc.explain_decision("p1", "dec-1"))
```

## 回放一致性（审计）

```bash
python3 -m src.replay journal.jsonl                 # 输出事件数与状态指纹
python3 -m src.replay journal.jsonl --pilot p1      # 试点状态、指标聚合、决策证据
python3 -m src.replay journal.jsonl --pilot p1 --explain dec-1 --json
```

同一事件文件重复回放（或换进程重启后回放），**指纹逐字节一致**。任何事件字段被改写都会因 `event_id` 内容哈希失配而在加载时被拒绝。

## 本地检查

```bash
python3 -m unittest discover -s tests
python3 -m compileall -q src tests
```

均可在单个 Linux 容器内用 Python 3.11+ 标准库执行，无需外部服务。
