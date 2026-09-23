# 公园需求试点账

本仓库保存该服务的领域资料与最小事件约定，供业务团队在统一语义上继续建设。

## 已有内容

- `contracts/domain.schema.json`：领域事件的基础字段、聚合类型和事件名称。
- `data/sample.json`：可用于联调的中文样例记录。
- `src/`：只负责校验基础事件信封，不包含业务流程。
- `tests/`：验证样例与基础约定保持一致。

## 领域边界

当前资料围绕居民需求、可逆试点和环境变量整理。事件一旦被接收，其标识、发生时间和版本不应被原地改写；业务更正应产生后继记录。涉及个人、机构或商业敏感信息时，调用方只读取完成职责所必需的字段。

## 本地检查

```bash
python3 -m unittest discover -s tests
```

## 测试与构建

测试命令：

```bash
python3 -m unittest discover -s tests
```

编译或构建命令：

```bash
python3 -m compileall -q src tests
```

上述命令均可在单个 Linux 应用容器内执行，不需要另行启动外部服务。
