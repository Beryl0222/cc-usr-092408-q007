"""校验领域事件信封的基础字段。"""

REQUIRED = ("event_id", "event_type", "aggregate_type", "aggregate_id",
            "occurred_at", "version", "summary", "payload")


def validate_event(record: dict) -> list[str]:
    errors = [f"缺少字段：{name}" for name in REQUIRED if name not in record]
    if "version" in record and (not isinstance(record["version"], int)
                                or isinstance(record["version"], bool)
                                or record["version"] < 1):
        errors.append("version 必须是正整数")
    if "payload" in record and not isinstance(record["payload"], dict):
        errors.append("payload 必须是对象")
    return errors
