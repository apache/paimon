# Sort Compact 在线测试手册（AI Agent 版）

> **目标读者**：AI Agent  
> **用途**：在 Shopee 内网环境验证当前分支（`fix_sort_compact` / PR #7595）的 sort compact 行为：sort compact 应产出 `CommitKind.COMPACT`，而非 `OVERWRITE`；并发 append 数据不应被误删。  
> **关联设计文档**：[PR-7595-sort-compact-commit-plan.md](./PR-7595-sort-compact-commit-plan.md)

---

## 1. 测试目标与通过标准

### 1.1 功能变更摘要

| 引擎 | 支持范围 | 预期变更 |
|------|----------|----------|
| Flink `SortCompactAction` | bucket-unaware append、hash-dynamic bucket | 最新 snapshot 的 `commitKind` 为 `COMPACT`（不是 `OVERWRITE`） |
| Spark `CompactProcedure` sort compact | **仅** bucket-unaware append | 同上 |

### 1.2 通用通过标准

| 检查项 | 通过条件 | 证据来源 |
|--------|----------|----------|
| Snapshot 类型 | compaction 后新增 snapshot 的 `commitKind == "COMPACT"` | `snapshot/snapshot-N` JSON |
| 数据量 | `totalRecordCount` 与 compaction 前基线一致（允许 dynamic bucket upsert 表在 rollback 后重新累计，但单次 compact 前后应一致） | snapshot JSON 或 `SELECT count(*)` |
| 并发 append（bucket-unaware） | compaction 窗口内存在 `APPEND` snapshot；compact 完成后这些 append 写入的数据文件仍可见 | snapshot 时间线 + manifest 文件集合 |
| Spark dynamic bucket sort compact | **不在本 PR 支持范围**；若执行应得到明确不支持错误 | Livy 任务日志 |

### 1.3 失败判据

- compaction 后最新 snapshot 的 `commitKind` 为 `OVERWRITE`
- `totalRecordCount` 无故减少（并发 append 数据丢失）
- compaction 任务失败或使用了错误分支 jar

---

## 2. 前置条件

### 2.1 认证与访问

```bash
# Flink Platform token
source /Users/hbg/Documents/docs_for_ai/tokens.sh

# 内网主机访问（禁止 plain ssh）
DI01=10.169.72.160
```

| 资源 | 访问方式 |
|------|----------|
| di01 / HDFS R2 | `smc toc $DI01 "<command>"` |
| Flink Platform API | `fm-token: $FM_TOKEN`，host: `https://flink.idata.shopeemobile.com` |
| Flink 控制台 | `https://datasuite.shopee.io/flink/operation/application?...` |

### 2.2 推荐 Skills

- `shopee-flink-platform` — 启停 Flink 任务、查 instance 状态
- `shopee-internal-env` — di01、HDFS、dev_hudi 环境
- `paimon` — 表 location、snapshot 排查
- `spark-hive-query` — di01 本地 spark-sql 元数据查询（轻量查询）
- `prod-test` — 测试报告格式参考

### 2.3 JDK 要求

使用 **JDK 8** 打包（Paimon 默认运行时）。

---

## 3. 测试资源清单

### 3.1 测试表

| 表名 | 类型 | HDFS Location | 说明 |
|------|------|---------------|------|
| `dev_hudi.paimon_append_bucket_unaware_test01` | bucket-unaware append (`bucket=-1`) | `hdfs://R2/projects/hudi/hive/dev_hudi/paimon_append_bucket_unaware_test01` | 初始数据已写好；当前 snapshot 均为 APPEND |
| `dev_hudi.paimon_upsert_dynamic_bucket_test01` | hash-dynamic bucket upsert | `hdfs://R2/projects/hudi/hive/dev_hudi/paimon_upsert_dynamic_bucket_test01` | 初始数据已写好；测试前需 rollback 到最近 APPEND snapshot |

两张表 schema 均含 `bigint11` 字段，sort compact 的 `order_by` 使用 `bigint11`。

### 3.2 Flink 任务

| 用途 | appId | 类型 | 控制台链接 |
|------|-------|------|------------|
| bucket-unaware 写入 | 1415797 | stream | https://datasuite.shopee.io/flink/operation/application?operationType=stream&appId=1415797&project_code=hudi |
| dynamic bucket 写入 | 1415820 | stream | https://datasuite.shopee.io/flink/operation/application?operationType=stream&appId=1415820&project_code=hudi |
| sort compact（Flink SQL） | 1415798 | batch | https://datasuite.shopee.io/flink/operation/application?operationType=batch&appId=1415798&project_code=hudi |

**Flink compaction 任务关键配置**（release `V20260706-100328725`）：

- `sqlJobMode`: `RAW`
- `refJars`: `paimon-flink-1.20-custom`（自定义依赖，需上传新 jar 版本）
- `flinkVersion`: `shopee-1.20`
- SQL 模板：

```sql
CREATE CATALOG paimon WITH (
    'type' = 'paimon',
    'metastore' = 'hive'
);

CALL paimon.sys.compact(
  `table` => '<TARGET_TABLE>',
  order_strategy => 'zorder',
  options => 'manifest.format=orc',
  order_by => 'bigint11'
);
```

> **Agent 注意**：每次跑不同表前，必须确认 release 中 `` `table` => `` 指向正确表名，或创建新 release。

### 3.3 Jar 路径

| Jar | 本地构建产物 | HDFS 上传目标 |
|-----|-------------|---------------|
| Spark | `paimon-spark/paimon-spark-3.2/target/paimon-spark-3.2_2.12-1.5-SNAPSHOT.jar` | `hdfs://R2/projects/hudi/hdfs/dev/bingeng.huang/jars/` |
| Hive connector | `paimon-hive/paimon-hive-connector-2.3/target/paimon-hive-connector-2.3-1.5-SNAPSHOT.jar` | 同上 |
| Flink | `paimon-flink/paimon-flink-1.20/target/paimon-flink-1.20-1.5-SNAPSHOT.jar` | Flink Platform 自定义依赖 `paimon-flink-1.20-custom` |

---

## 4. 测试前：打包与部署

### 4.1 打包

在仓库根目录执行：

```bash
cd /Users/hbg/code/paimon/paimon-github/paimon

env JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-1.8.jdk/Contents/Home \
  mvn clean package -DskipTests \
  -pl 'paimon-spark/paimon-spark-3.2,paimon-hive/paimon-hive-connector-2.3,paimon-flink/paimon-flink-1.20' \
  -am -Drat.skip=true
```

### 4.2 验证 jar 生成时间

```bash
ls -la \
  paimon-spark/paimon-spark-3.2/target/paimon-spark-3.2_2.12-1.5-SNAPSHOT.jar \
  paimon-hive/paimon-hive-connector-2.3/target/paimon-hive-connector-2.3-1.5-SNAPSHOT.jar \
  paimon-flink/paimon-flink-1.20/target/paimon-flink-1.20-1.5-SNAPSHOT.jar
```

**通过标准**：三个 jar 的修改时间均晚于本次 `mvn package` 开始时间。

### 4.3 上传 Spark / Hive jar 到 HDFS

```bash
DI01=10.169.72.160
JAR_DIR=/Users/hbg/code/paimon/paimon-github/paimon
HDFS_JAR_DIR=hdfs://R2/projects/hudi/hdfs/dev/bingeng.huang/jars

# 先在 di01 上删除旧 jar（可选，建议覆盖上传）
smc toc $DI01 "hdfs dfs -put -f $HDFS_JAR_DIR/paimon-spark-3.2_2.12-1.5-SNAPSHOT.jar $HDFS_JAR_DIR/paimon-spark-3.2_2.12-1.5-SNAPSHOT.jar.bak 2>/dev/null; true"

# 从本地上传到 di01 再 put 到 HDFS（若 smc 支持 scp，也可用 smc-remote skill）
# 简化路径：在 di01 上直接用 hdfs dfs -put -f 覆盖（需先把 jar 传到 di01）
```

**推荐做法（Agent 可执行）**：在 di01 本地编译产物同步：

```bash
# 1. 将本地 jar scp 到 di01（通过 smc-remote / smc toc 文件传输流程）
# 2. 在 di01 执行覆盖上传：
smc toc $DI01 "
  hdfs dfs -put -f /tmp/paimon-spark-3.2_2.12-1.5-SNAPSHOT.jar \
    hdfs://R2/projects/hudi/hdfs/dev/bingeng.huang/jars/paimon-spark-3.2_2.12-1.5-SNAPSHOT.jar && \
  hdfs dfs -put -f /tmp/paimon-hive-connector-2.3-1.5-SNAPSHOT.jar \
    hdfs://R2/projects/hudi/hdfs/dev/bingeng.huang/jars/paimon-hive-connector-2.3-1.5-SNAPSHOT.jar && \
  hdfs dfs -ls hdfs://R2/projects/hudi/hdfs/dev/bingeng.huang/jars/paimon-*-1.5-SNAPSHOT.jar
"
```

**通过标准**：HDFS 上两个 jar 的时间戳为本次上传时间。

### 4.4 上传 Flink jar 到平台

通过 Flink Platform API 上传为新版本自定义依赖（`paimon-flink-1.20-custom`）：

```bash
source /Users/hbg/Documents/docs_for_ai/tokens.sh

# 方式 A：HDFS 源（先把 jar 放到 HDFS）
curl -X POST "https://flink.idata.shopeemobile.com/api/jars/version" \
  -H "fm-token: $FM_TOKEN" \
  -F "jarType=DATA_STREAM_LIB_JAR" \
  -F "sourceType=HDFS" \
  -F "project=hudi" \
  -F "jarName=paimon-flink-1.20-custom" \
  -F "hdfsPath=hdfs://R2/projects/hudi/hdfs/dev/bingeng.huang/jars/paimon-flink-1.20-1.5-SNAPSHOT.jar" \
  -F "isNewVersion=true" \
  -F "applicationId=1415798" \
  -F "regions=SG"

# 轮询上传状态
# GET /api/jars/{id}/status 直到 Success
```

然后在 compaction 任务 release 中引用最新版本的 `paimon-flink-1.20-custom`。

---

## 5. 通用验证命令

以下命令均在 di01 上通过 `smc toc` 执行。

### 5.1 读取最新 snapshot

```bash
TABLE_LOC="hdfs://R2/projects/hudi/hive/dev_hudi/<TABLE_NAME>"

smc toc 10.169.72.160 "
  LATEST=\$(hdfs dfs -cat \$TABLE_LOC/snapshot/LATEST)
  echo \"LATEST snapshot id: \$LATEST\"
  hdfs dfs -cat \$TABLE_LOC/snapshot/snapshot-\$LATEST
"
```

关注字段：

- `commitKind`：期望 `COMPACT` 或 `APPEND`
- `totalRecordCount`：数据总量
- `timeMillis`：提交时间

### 5.2 列出最近 N 个 snapshot 的 commitKind

```bash
smc toc 10.169.72.160 '
TABLE_LOC="hdfs://R2/projects/hudi/hive/dev_hudi/paimon_append_bucket_unaware_test01"
LATEST=$(hdfs dfs -cat $TABLE_LOC/snapshot/LATEST)
START=$((LATEST - 10))
for i in $(seq $START $LATEST); do
  echo -n "snapshot-$i: "
  hdfs dfs -cat $TABLE_LOC/snapshot/snapshot-$i 2>/dev/null \
    | python -c "import sys,json; d=json.load(sys.stdin); print(d[\"commitKind\"], d[\"totalRecordCount\"])" \
    2>/dev/null || echo "N/A"
done
'
```

### 5.3 记录测试基线（每次测试前必做）

Agent 必须在 compaction **开始前**记录：

```text
baseline_snapshot_id: <N>
baseline_commit_kind: <APPEND|...>
baseline_total_record_count: <count>
baseline_time: <ISO8601>
```

compaction **结束后**记录：

```text
result_snapshot_id: <N+1 or higher>
result_commit_kind: 必须为 COMPACT
result_total_record_count: 必须 == baseline（bucket-unaware 场景）
```

### 5.4 行数交叉验证（可选）

```bash
smc toc 10.169.72.160 "spark-sql --master local[4] --conf spark.sql.catalogImplementation=hive \
  -e \"select count(*) from dev_hudi.<TABLE_NAME>\""
```

> 大表可能较慢；优先以 snapshot `totalRecordCount` 为准。

---

## 6. Spark Sort Compact：Livy 提交命令（di01）

### 6.1 环境参数（已验证）

| 参数 | 值 | 说明 |
|------|-----|------|
| 主机 | `10.169.72.160` (di01) | SG live HMS |
| Livy 工具 | `/opt/livy-command-tool-0.8.0-incubating-sdi-063-bin/bin/spark-sql` | 优先用最新版（063/062） |
| YARN 队列 | `dev` | **不要用** `infra@non-live`（di01 livy 会报 unknown queue） |
| Paimon jar 配置 | `spark.livy.paimon.jar.v3.2` | 逗号分隔 hive-connector + spark jar |

### 6.2 推荐：SQL 文件 + `-f`（避免引号转义问题）

**Step 1：在 di01 上准备 SQL 文件**

```bash
smc toc 10.169.72.160 'cat > /tmp/sort_compact_bucket_unaware.sql <<'"'"'EOF'"'"'
CREATE CATALOG paimon WITH (
    '"'"'type'"'"' = '"'"'paimon'"'"',
    '"'"'metastore'"'"' = '"'"'hive'"'"'
);

CALL paimon.sys.compact(
  table => '"'"'dev_hudi.paimon_append_bucket_unaware_test01'"'"',
  order_strategy => '"'"'zorder'"'"',
  options => '"'"'manifest.format=orc'"'"',
  order_by => '"'"'bigint11'"'"'
);
EOF'
```

**Step 2：提交 Livy 任务**

```bash
smc toc 10.169.72.160 "
/opt/livy-command-tool-0.8.0-incubating-sdi-063-bin/bin/spark-sql \
  --name sort_compact_bucket_unaware_spark \
  --queue dev \
  --conf spark.sql.catalogImplementation=hive \
  --conf 'spark.livy.paimon.jar.v3.2=hdfs://R2/projects/hudi/hdfs/dev/bingeng.huang/jars/paimon-hive-connector-2.3-1.5-SNAPSHOT.jar,hdfs://R2/projects/hudi/hdfs/dev/bingeng.huang/jars/paimon-spark-3.2_2.12-1.5-SNAPSHOT.jar' \
  -f /tmp/sort_compact_bucket_unaware.sql
"
```

**Step 3：确认任务完成**

- 命令退出码为 0
- 日志中出现 `100%` 进度条
- 无 `Exception` / `Error` 关键字
- 记录 Keyhole tracking URL（日志中的 `application_...`）

### 6.3 单行 `-e` 备选（仅适合简单探测）

```bash
smc toc 10.169.72.160 "
/opt/livy-command-tool-0.8.0-incubating-sdi-063-bin/bin/spark-sql \
  --name sort_compact_probe \
  --queue dev \
  --conf spark.sql.catalogImplementation=hive \
  --conf 'spark.livy.paimon.jar.v3.2=hdfs://R2/projects/hudi/hdfs/dev/bingeng.huang/jars/paimon-hive-connector-2.3-1.5-SNAPSHOT.jar,hdfs://R2/projects/hudi/hdfs/dev/bingeng.huang/jars/paimon-spark-3.2_2.12-1.5-SNAPSHOT.jar' \
  -e \"CREATE CATALOG paimon WITH ('type'='paimon','metastore'='hive'); CALL paimon.sys.compact(table => 'dev_hudi.paimon_append_bucket_unaware_test01', order_strategy => 'zorder', options => 'manifest.format=orc', order_by => 'bigint11');\"
"
```

### 6.4 Dynamic bucket 表的 Spark sort compact

**本 PR 不支持**。Spark `CompactProcedure` 对 sort compact 仅支持 bucket-unaware append 表。对 `paimon_upsert_dynamic_bucket_test01` 执行 sort compact 预期报错：

```text
only support unaware-bucket append-only table yet.
```

若用户流程仍要求覆盖此场景，应标记为 **预期失败（unsupported）**，不作为回归失败。

---

## 7. Flink 任务操作

### 7.1 启停任务（API）

```bash
source /Users/hbg/Documents/docs_for_ai/tokens.sh

# 查看最新 instance
curl -s "https://flink.idata.shopeemobile.com/api/applications/<appId>/instances/latest" \
  -H "fm-token: $FM_TOKEN"

# 启动新 instance（需 releaseId）
curl -X POST "https://flink.idata.shopeemobile.com/api/instances" \
  -H "fm-token: $FM_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "applicationId": <appId>,
    "releaseId": <releaseId>,
    "clusterName": "yarn-ytl0",
    "queueName": "infra@non-live"
  }'

# 停止 instance
curl -X PATCH "https://flink.idata.shopeemobile.com/api/instances/<instanceId>/stop?graceful=false" \
  -H "fm-token: $FM_TOKEN"
```

### 7.2 写入任务（并发 append）

- bucket-unaware 写入：appId **1415797**，datagen 100 rows/s 持续写入
- dynamic bucket 写入：appId **1415820**

**并发验证要点（bucket-unaware）**：

1. 先启动写入任务，确认 LATEST snapshot 持续增长且 `commitKind=APPEND`
2. 在写入运行期间启动 compaction（1415798）
3. compaction 运行期间轮询 snapshot（每 10–30s）：

```bash
# 在 compaction 运行期间循环执行
smc toc 10.169.72.160 '
  L=$(hdfs dfs -cat hdfs://R2/projects/hudi/hive/dev_hudi/paimon_append_bucket_unaware_test01/snapshot/LATEST)
  hdfs dfs -cat hdfs://R2/projects/hudi/hive/dev_hudi/paimon_append_bucket_unaware_test01/snapshot/snapshot-$L \
    | python -c "import sys,json; d=json.load(sys.stdin); print(d[\"id\"], d[\"commitKind\"], d[\"timeMillis\"])"
'
```

4. 若 compaction 完成前出现新的 `APPEND` snapshot → 证明 **3.1 并发写入成立**
5. compaction 完成后暂停写入，验证 **3.2** 和 **3.3**

### 7.3 Flink sort compact

1. 确认 app **1415798** release 中 `table` 参数正确
2. 确认 `refJars` 指向刚上传的 `paimon-flink-1.20-custom` 最新版本
3. 提交 batch instance 并等待 `actualState=FINISHED`（或控制台显示成功）
4. 按第 5 节验证 snapshot

---

## 8. 测试用例

### 用例 A：bucket-unaware + Flink sort compact（含并发 append）

| 步骤 | 操作 | 验证点 |
|------|------|--------|
| A0 | 记录基线 snapshot（第 5.3 节） | 有 baseline |
| A1 | 启动写入任务 appId=1415797 | instance `RUNNING`；LATEST 持续增长 |
| A2 | 启动 compaction appId=1415798（table=bucket-unaware 表） | batch 任务提交成功 |
| A3 | compaction 运行中轮询 snapshot | 出现 `APPEND` snapshot（并发写入） |
| A4 | compaction 完成后停止写入任务 | instance `STOPPED` |
| A5 | 检查 snapshot | **3.1** 有并发 APPEND；**3.2** `totalRecordCount` 不变；**3.3** 最新为 `COMPACT` |

### 用例 B：bucket-unaware + Spark sort compact（含并发 append）

与用例 A 相同，但：

- A2 替换为：通过 **第 6 节 Livy** 提交 Spark compact
- 写入任务仍为 appId=1415797
- Spark compact 运行较慢（YARN + Livy），需等待 Keyhole 任务 100% 完成

### 用例 C：dynamic bucket + Flink sort compact

| 步骤 | 操作 | 验证点 |
|------|------|--------|
| C0 | **Rollback 到最近 APPEND snapshot**（见 8.1） | LATEST 的 `commitKind=APPEND` |
| C0 | 记录 rollback 后基线 `totalRecordCount` | 有 baseline |
| C1 | 确认 compaction release 中 table=`dev_hudi.paimon_upsert_dynamic_bucket_test01` | release SQL 正确 |
| C2 | 运行 Flink compaction appId=1415798 | batch 成功 |
| C3 | 检查 snapshot | **3.1** `totalRecordCount` 不变；**3.2** 最新为 `COMPACT` |

#### 8.1 Rollback 到最近 APPEND snapshot

**查找最近 APPEND snapshot**：

```bash
smc toc 10.169.72.160 '
TABLE_LOC="hdfs://R2/projects/hudi/hive/dev_hudi/paimon_upsert_dynamic_bucket_test01"
LATEST=$(hdfs dfs -cat $TABLE_LOC/snapshot/LATEST)
TARGET=""
for i in $(seq $LATEST -1 1); do
  KIND=$(hdfs dfs -cat $TABLE_LOC/snapshot/snapshot-$i 2>/dev/null \
    | python -c "import sys,json; print(json.load(sys.stdin)[\"commitKind\"])" 2>/dev/null)
  if [ "$KIND" = "APPEND" ]; then TARGET=$i; break; fi
done
echo "Rollback target APPEND snapshot: $TARGET"
'
```

**执行 rollback（Flink SQL 或 Livy）**：

```sql
CREATE CATALOG paimon WITH ('type' = 'paimon', 'metastore' = 'hive');

-- 方式 1：Flink procedure（推荐，不删除后续 snapshot）
CALL paimon.sys.rollback_to_as_latest(
  `table` => 'dev_hudi.paimon_upsert_dynamic_bucket_test01',
  snapshot_id => <TARGET_SNAPSHOT_ID>
);

-- 方式 2：Spark procedure
-- CALL paimon.sys.rollback(table => 'dev_hudi.paimon_upsert_dynamic_bucket_test01', version => '<TARGET_SNAPSHOT_ID>');
```

Rollback 可通过临时 Flink batch 任务或 Livy 执行上述 SQL。

### 用例 D：dynamic bucket + Spark sort compact

**预期：不支持（跳过或记为 expected-fail）**。若强制执行，日志应含 `only support unaware-bucket append-only table yet.`

---

## 9. 并发 append 数据未丢失的验证方法

bucket-unaware 场景的核心回归点（对应单测 `testSortCompactConcurrentAppendBeforeCommit`）：

1. **compaction 前**：记录 snapshot `S_before` 及当时活跃 data file 列表（可通过 manifest 或文件数估算）
2. **compaction 期间**：确认有新的 `APPEND` snapshot `S_append`（`S_append > S_before`）
3. **compaction 后**：最新 snapshot 为 `COMPACT`
4. **数据完整性**：
   - `totalRecordCount`（compact 后）>= baseline（并发 append 后可能略增，但 compact 本身不应减少总量）
   - 更严格：compact 后 snapshot 的 manifest 仍包含 `S_append` 阶段新增的文件（需对比 manifest entry 或执行 `select count(*)` 与 baseline 对比）

简化判定（推荐 Agent 使用）：

```text
PASS if:
  - exists APPEND snapshot with id in (baseline_id, compact_snapshot_id) during compaction window
  - compact snapshot commitKind == COMPACT
  - compact snapshot totalRecordCount >= baseline_total_record_count
  - select count(*) after compact >= baseline count (optional)
```

---

## 10. Agent 测试报告模板

每次测试运行结束，Agent 应输出以下结构化报告（可保存为 `sort-compact-test-report-<timestamp>.md`）：

```markdown
# Sort Compact 测试报告

- **分支/提交**: fix_sort_compact @ <commit_sha>
- **执行时间**: <start> ~ <end>
- **Jar 版本**: spark/hive/flink jar HDFS 时间戳

## 结果总览

| 用例 | 引擎 | 表 | 结果 | 备注 |
|------|------|-----|------|------|
| A | Flink | bucket-unaware | PASS/FAIL | |
| B | Spark/Livy | bucket-unaware | PASS/FAIL | |
| C | Flink | dynamic bucket | PASS/FAIL | |
| D | Spark/Livy | dynamic bucket | SKIP/EXPECTED-FAIL | |

## 用例详情（每个 FAIL 必填）

### 用例 <X>

- **任务链接**: <Flink app URL 或 Keyhole YARN URL>
- **Baseline**: snapshot_id=, commitKind=, totalRecordCount=
- **Result**: snapshot_id=, commitKind=, totalRecordCount=
- **并发 append 证据**: snapshot ids / 时间线
- **判定**: 期望 vs 实际
- **日志/证据路径**: <hdfs path / log snippet>
```

---

## 11. 原手册缺漏与本文补充说明

以下是针对 AI Agent 执行时发现的原手册缺漏，本文已补充：

| 缺漏 | 补充内容 |
|------|----------|
| Livy 命令不完整 | 补充 di01 工具路径、**queue=dev**、`-f` SQL 文件方式、`spark.livy.paimon.jar.v3.2` 配置 |
| 缺少 `CREATE CATALOG paimon` | Spark/Livy 必须先创建 paimon catalog 再 `CALL paimon.sys.compact` |
| Jar 产物路径不明确 | 补充三个 target jar 完整路径及 HDFS 目标 |
| Flink jar 上传步骤模糊 | 补充 `POST /api/jars/version` 流程 |
| 无基线记录要求 | 增加 compaction 前后 snapshot 基线采集 |
| 无具体 snapshot 检查命令 | 增加第 5 节可复制的 hdfs/python 命令 |
| dynamic bucket rollback 步骤模糊 | 增加查找最近 APPEND snapshot + `rollback_to_as_latest` SQL |
| Flink compaction 需改表名 | 明确 release 中 `` `table` => `` 必须按用例切换 |
| 并发 append 判定标准模糊 | 增加轮询 snapshot 与 PASS 条件（第 9 节） |
| **Spark dynamic bucket sort compact 不应测为 PASS** | PR 范围仅 bucket-unaware；用例 D 标为 SKIP/EXPECTED-FAIL |
| 无认证/技能引用 | 增加 tokens.sh、smc toc、相关 skills |
| 无 Agent 报告格式 | 增加第 10 节模板 |
| 未说明写入任务队列/集群 | Flink 写入/compact 使用 `yarn-ytl0` + `infra@non-live`（与线上一致） |
| JDK 版本 | 明确要求 JDK 8 打包 |

---

## 12. 快速检查清单（Agent 跑测前）

- [ ] 当前分支代码已打包，三个 jar 时间戳正确
- [ ] Spark/Hive jar 已上传到 HDFS `bingeng.huang/jars/`
- [ ] Flink jar 已上传为新版本 `paimon-flink-1.20-custom`，compaction 任务已引用
- [ ] 已知各 Flink appId 与表名对应关系
- [ ] dynamic bucket 用例已 rollback 到 APPEND snapshot
- [ ] compaction 前已记录 baseline snapshot
- [ ] Livy 使用 `--queue dev`（非 `infra@non-live`）
