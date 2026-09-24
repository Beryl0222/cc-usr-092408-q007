# 公园需求可逆试点决策账

处理夜跑照明、儿童活动、安静休憩等相互冲突的居民诉求时，本服务提供一套
**可逆决策账**：诉求去标识登记、建议性聚类、冻结式试点审批、按发生时间入流的
观测/反馈、安全停止线立即暂停、普通偏离进入复核、扩大/延长/终止均可逐条解释，
且同一事件流重复回放得到一致的决策与资源状态。

## 不变量（业务规则如何落地）

| 规则 | 实现方式 |
| --- | --- |
| 原始诉求只保存去标识摘要与来源群体 | `deidentify()` 剥离手机号/证件号/邮箱/@账号/姓名提示；`NEED_LOGGED.payload` 只含 `summary/source_group/category/dedup_key` |
| 聚类结果是建议而非事实 | `NEEDS_CLUSTERED.payload.advisory` 恒为 `true`，折叠态同样标记 |
| 试点冻结目标区域、时间窗、成功指标、停止条件、批准人 | `PILOT_APPROVED` 一次性写入，同一 pilot 重复审批直接拒绝；时间窗不可原地改写，延长产生新的决策事件 |
| 观测按发生时间进入，后来数据不重写当时决策 | 入库按 `occurred_at` 排序；每次决策保存**证据快照**（决策时点之前的观测键集合与统计），新数据追加但不影响历史快照 |
| 触及安全停止线立即暂停受影响区域 | 入库即评估，命中安全线产生 `SAFETY_HALT_TRIGGERED`（实验区+对照区全部列入暂停范围）与运维通知；暂停期只能终止 |
| 普通指标偏离进入复核 | 产生 `METRIC_DEVIATION_FLAGGED`，试点转 `review`；复核结清前不得扩大/延长，结清后恢复原状态 |
| 回滚保留已发生资源使用与通知 | `RESOURCE_USAGE_LOGGED` 与 `NOTIFICATION_SENT` 永不删除；终止只追加 `RESOURCES_RELEASED` |
| 对照区域不能同时参加冲突试点 | 运行中/暂停/复核中的试点都继续占用区域（`zones_taken()`），终止释放后才可复用 |
| 名额与预算原子释放/预留 | 先校验总额与区域冲突，再把同一命令的全部事件**一次性原子追加**；任一超限整笔拒绝，无半笔预留 |
| 批量导入隔离坏记录 | 逐条校验，坏记录进 `rejected`（含索引与原因），好记录整批提交 |
| 同一观测重传不重复计数 | 必传 `idempotency_key`，全局去重，重传进入 `duplicate_keys` |
| 重启后继续到期评审 | 评审排期是事件；重启即重放，`due_reviews(now)` 纯查询重新得出 |
| 决策可解释 | `explain_decision()` / `python3 -m src.replay` 展示冻结基线、决策理由、证据快照与保留历史 |
| 重复回放一致性 | 事件 ID 由内容寻址（类型+聚合+版本+负载的哈希）；状态是事件流的纯折叠；时钟可注入，相同命令流生成字节级相同的事件日志 |

## 存储

- 单个 JSONL 文件（默认 `events.jsonl`），仅追加；每批命令先写临时文件再
  `rename` 原子替换，崩溃不会出现半批。
- 末尾截断的半行可容忍（视为未落盘）；**中间**坏行直接拒绝加载，避免错误结论。
- 无外部服务、无数据库，Python 3.11+ 标准库即可运行。

## 代码结构

- `src/ledger.py`：事件类型、JSONL 原子存储、纯函数折叠器 `fold`。
- `src/service.py`：命令服务（登记/聚类/审批/观测/复核/决策/资源）与去标识、安全线评估。
- `src/replay.py`：只读回放与决策解释 CLI。
- `src/validator.py`：事件信封基础校验。
- `contracts/domain.schema.json`：事件契约。
- `tests/test_ledger.py`：28 项行为测试，覆盖上述全部不变量。

## 本地检查

```bash
python3 -m unittest discover -s tests
python3 -m compileall -q src tests
```

## 典型流程

```python
svc = LedgerService("events.jsonl", budget_cap=1000, global_quota_cap=50)

svc.import_needs([...])                       # 坏记录隔离，好记录去标识入库
svc.cluster_needs(need_ids, "夜间照明需求簇")  # advisory=True
svc.approve_pilot("pilot-1", cluster_id, "东湖环线",
    window={"start": ..., "end": ...},
    success_metrics={"satisfaction": {"min": 4.0}},
    safety_limits={"noise_db": {"max": 75}},
    approver="王主任", quota=10, budget=200.0,
    control_zones=["西湖环线"])               # 冻结 + 原子预留

svc.record_observation("pilot-1", "observation", "noise_db", 82.0,
                       "sensor-20260922-2100", "2026-09-22T21:00:00+08:00")
# -> 立即 safety_halt；随后只能 terminate（原子释放，使用与通知保留）

svc.explain_decision("pilot-1")               # 由哪些数据支持，逐条可查
```

```bash
python3 -m src.replay events.jsonl pilot-1
```
