"""仅追加事件存储。

- JSONL 单文件，每行一个事件；append 后 fsync，进程重启可完整重载。
- 每个聚合的 version 必须严格 +1；全局 seq 只增，决定回放顺序。
- 同一 event_id（内容哈希）重复提交视为命令重试：跳过而不是再计数。
- append_all 是一个提交单元：要么全部入账，要么全部拒绝（调用方收 DomainError）。
"""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

from .events import DomainError, Event, deterministic_event_id


class EventStore:
    def __init__(self, path: str | os.PathLike[str]):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._events: list[Event] = []
        self._versions: dict[str, int] = {}
        self._ids: dict[str, int] = {}  # event_id -> seq
        self._loaded = False

    # ---- 持久化 ----
    def load(self) -> None:
        self._events.clear()
        self._versions.clear()
        self._ids.clear()
        if not self.path.exists():
            self._loaded = True
            return
        seq = 0
        for lineno, line in enumerate(self.path.read_text(encoding="utf-8").splitlines(), 1):
            if not line.strip():
                continue
            try:
                raw = json.loads(line)
                event = Event.from_dict(raw)
            except (json.JSONDecodeError, KeyError, TypeError) as exc:
                raise DomainError(f"事件文件第 {lineno} 行损坏：{exc}") from exc
            seq += 1
            if event.seq and event.seq != seq:
                raise DomainError(f"第 {lineno} 行全局序号断裂：记录 {event.seq}，期望 {seq}")
            expected = self._versions.get(event.aggregate_id, 0) + 1
            if event.version != expected:
                raise DomainError(
                    f"聚合 {event.aggregate_id} 版本断裂：第 {lineno} 行 v{event.version}，期望 v{expected}"
                )
            if event.event_id in self._ids:
                raise DomainError(f"第 {lineno} 行 event_id 重复：{event.event_id}")
            # 完整性：event_id 必须等于内容规范化哈希，任何字段被篡改都会失配。
            expected_id = deterministic_event_id(
                event.aggregate_type, event.aggregate_id, event.version, event.event_type,
                event.occurred_at, event.payload, event.summary,
            )
            if event.event_id != expected_id:
                raise DomainError(
                    f"第 {lineno} 行事件内容与 event_id 不符（疑似被改写或损坏）：{event.event_id}"
                )
            event = Event(**{**event.to_dict(), "seq": seq})
            self._events.append(event)
            self._versions[event.aggregate_id] = event.version
            self._ids[event.event_id] = seq
        self._loaded = True

    def _require_loaded(self) -> None:
        if not self._loaded:
            self.load()

    def _append_writes(self, new_events: list[Event]) -> None:
        # 先在临时文件写完并 fsync，再原子改名，保证重启不会读到半截批次。
        fd, tmp_name = tempfile.mkstemp(prefix=".journal-", dir=self.path.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                for event in self._events:
                    fh.write(json.dumps(event.to_dict(), ensure_ascii=False) + "\n")
                for event in new_events:
                    fh.write(json.dumps(event.to_dict(), ensure_ascii=False) + "\n")
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_name, self.path)
            dir_fd = os.open(self.path.parent, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        finally:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)

    # ---- 写入 ----
    def append_all(self, candidates: list[Event]) -> list[Event]:
        """提交一批事件。内容哈希相同的重传被跳过；版本冲突整批拒绝。"""
        self._require_loaded()
        if not candidates:
            return []
        staged: list[Event] = []
        next_seq = len(self._events)
        work_versions = dict(self._versions)
        work_ids = dict(self._ids)
        for event in candidates:
            if event.event_id in work_ids:
                # 确定性重放/命令重试：同一事实已入账，跳过且不分配新 seq。
                continue
            expected = work_versions.get(event.aggregate_id, 0) + 1
            if event.version != expected:
                raise DomainError(
                    f"聚合 {event.aggregate_id} 版本冲突：提交 v{event.version}，期望 v{expected}"
                )
            next_seq += 1
            stored = Event(**{**event.to_dict(), "seq": next_seq})
            staged.append(stored)
            work_versions[event.aggregate_id] = event.version
            work_ids[event.event_id] = next_seq
        if staged:
            self._append_writes(staged)
            self._events.extend(staged)
            self._versions = work_versions
            self._ids = work_ids
        return staged

    def append(self, event: Event) -> Event | None:
        stored = self.append_all([event])
        return stored[0] if stored else None

    # ---- 读取 ----
    @property
    def events(self) -> list[Event]:
        self._require_loaded()
        return list(self._events)

    def events_for(self, aggregate_id: str) -> list[Event]:
        self._require_loaded()
        return [e for e in self._events if e.aggregate_id == aggregate_id]

    def version_of(self, aggregate_id: str) -> int:
        self._require_loaded()
        return self._versions.get(aggregate_id, 0)
