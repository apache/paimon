<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Cache Policy 参数与路由说明

Cache policy 控制 Paimon 文件操作访问缓存端点还是 origin 端点。Java 和 PyPaimon
在通用 `CachePolicyFileIO` 包装层中执行相同的路由规则，底层 FileIO 负责实际读写。
它独立于客户端本地 block cache，也不改变表本身的存储和读取语义。

REST Catalog 通过 table token 下发凭证、端点和 `io-cache.*` 策略。
`RESTTokenFileIO` 合并配置并创建包装层；策略刷新随 token 刷新生效。
也可以显式调用 `CachePolicyFileIO.create`，传入相同的配置。
普通 `FileIO.get` 调用不会自动装配远端 cache policy。

## 有效参数

下表中的默认值指新的 cache policy 层，Java 与 PyPaimon 一致。

| 参数 | 类型 | 默认值 | 含义 |
| --- | --- | --- | --- |
| `fs.oss.endpoint` | 字符串 | 无，由下发配置或底层 FileIO 配置提供 | 缓存操作使用的端点。包装层始终保留这个值作为 cache endpoint。 |
| `io-cache.origin.endpoint` | 字符串 | 未设置时保留 `fs.oss.endpoint` | 不走缓存的操作使用的 origin endpoint。非空时，仅覆盖 origin delegate 的 `fs.oss.endpoint`。 |
| `io-cache.policy` | 字符串 | 未设置，没有缓存操作 | 启用哪些操作类型，可组合 `meta`、`read`、`write`；`none` 关闭全部缓存操作。 |
| `io-cache.whitelist` | 字符串 | `*` | 允许缓存的文件类型，多个类型用逗号分隔。 |
| `io-cache.whitelist-path` | 字符串 | `*` | 允许缓存的 URI path 子串，多个子串用逗号分隔。类型与路径白名单必须同时命中。 |

协议不再包含 VPC validate 步骤。`io-cache.vpc-validated` 和
`io-cache.ignore-vpc-validate` 均已移除，不参与策略判断。
也没有独立的 `io-cache.endpoint` 参数；缓存端点统一使用 `fs.oss.endpoint`。
新的 policy 层不需要 `io-cache.enabled` 客户端开关。

### 端点规则

cache 与 origin 使用原始文件路径和相同的对象存储命名空间，仅访问端点不同。
两个端点使用下发的同一组凭证。

- 命中缓存策略：使用 `fs.oss.endpoint`。
- 未命中、可变文件或固定走 origin 的操作：使用 `io-cache.origin.endpoint`。
- 没有策略或策略为 `none`：直接返回 origin FileIO，origin 配置仍生效。
- origin 参数缺失、为空或仅含空白：保留原来的 `fs.oss.endpoint`，不自动推导真实 OSS 端点。
  需要绕过缓存访问真实 OSS 时，服务端应明确下发 origin 参数。

REST token 中已有的 `fs.oss.endpoint` 优先于客户端端点设置。
客户端 `dlf.oss-endpoint` 仅在服务端未提供 `fs.oss.endpoint` 时作为兼容回退。
Java 同时把选定端点写入 delegate 的 options 和复制后的 Hadoop configuration，
保证 Hadoop 配置回退与原生 reader 取得相同的端点；不会修改原始配置对象。

### policy 标签

| 标签 | 控制的操作 | 示例 |
| --- | --- | --- |
| `read` | 文件内容读取 | `newInputStream`、Python `new_input_stream`、Arrow 文件读取 |
| `write` | 普通文件内容写入 | `newOutputStream`、Python 普通格式写入、Arrow 文件写入 |
| `meta` | 文件属性查询 | `getFileStatus`、`exists`、`getFileSize`、`isDir`；Python 对应接口及批量属性查询 |
| `none` | 关闭全部缓存操作，优先于其他标签 | `none,read` 仍全部走 origin |

例如，`read` 只允许读取内容走缓存；`meta,read` 允许属性查询和内容读取；
`meta,read,write` 允许三类操作。即使标签已开启，仍需满足文件类型、路径和不可变性条件。

`meta` 标签表示**操作类型**，不等同于文件类型白名单中的 `meta`。
例如 `io-cache.policy=meta` 且 `io-cache.whitelist=data` 时，
数据文件的属性查询可以走缓存，但数据内容读取仍走 origin。

参数使用小写标签和逗号分隔的规范写法。当前实现沿用标签子串包含判断，
并未引入新的策略表达式或大小写转换。未设置策略或策略为空时，所有操作都走 origin。

## 文件类型与路径白名单

| 文件类型值 | 覆盖范围与示例 |
| --- | --- |
| `meta` | snapshot、schema、manifest、manifest list、manifest sidecar、statistics 等元数据文件。可变元数据仍会被不可变性规则排除。 |
| `global-index` | 表级全局索引，例如 `btree-global-index-<id>.index`。 |
| `bucket-index` | bucket 级索引，例如 `index-<id>-0`。 |
| `file-index` | 数据文件附属索引，例如 `data-<id>-0.orc.index`。 |
| `data` | 数据文件，以及未匹配其他分类规则的文件。PyPaimon 同时包含其 `STRUCTURED_DATA` 分类。 |
| `*` | 所有文件类型，仍受不可变性和路径白名单约束。 |

PyPaimon 另支持 `structured-data`，只匹配其 Parquet、ORC、Avro、ROW、Mosaic
结构化数据分类。Java 没有单独的该分类；需要跨 Java/Python 使用相同配置时采用 `data`。

白名单规则如下：

- 同一白名单内部为 OR，不同白名单之间为 AND。
  `io-cache.whitelist=data,file-index` 表示两类文件都允许；
  它们还必须命中 `io-cache.whitelist-path` 才能走缓存。
- 路径匹配的是 URI 的 path 部分，不匹配 scheme、bucket authority、query 或 fragment。
  例如 `oss://bucket/db/table/bucket-0/data.orc` 使用 `/db/table/bucket-0/data.orc` 匹配。
- 路径采用区分大小写的子串匹配，不是 glob 或正则；`bucket-0,bucket-1` 表示任一子串命中。
  单独的 `*` 才表示所有路径，`bucket-*` 是普通字面子串。
- 文件类型和路径条目会去掉两侧空白；显式空串或只有逗号、空白时，没有匹配项。
  未知文件类型会被忽略并记录告警。例如 `manifest` 不是类型值，应使用 `meta`。

新 policy 层在未显式设置路径白名单时默认允许全部路径。
Java 的共享配置常量仍保留旧 JindoCache 的 `bucket,manifest,index` 默认值，
新的 `CachePolicy` 解析逻辑会区分“未设置”与“显式设置”，按本节的 `*` 默认值执行。

## 路由条件与固定回退

一次操作只有同时满足以下条件才会访问缓存端点：

```text
policy 未包含 none
AND 当前操作的 meta/read/write 标签已开启
AND 文件不是可变文件
AND 命中文件类型白名单
AND 命中 URI path 白名单
```

可变文件始终绕过缓存，即使两个白名单都是 `*`。当前规则包含 `LATEST`、`EARLIEST`、
`_SUCCESS` 及以 `_SUCCESS` 结尾的文件、`consumer-*`、`service-*`、`tag-*`、
Paimon 生成的临时文件，以及会被覆盖的 Iceberg 兼容元数据：`version-hint.text`、
`retire-pending`、`*.metadata.json`。

以下操作始终使用 origin，以保留目录、命名空间和提交语义：

| 操作 | 行为 |
| --- | --- |
| 目录列表、递归列表 | 走 origin，目录可能同时包含多种文件类型。 |
| `delete`、`rename`、`mkdirs` | 走 origin。 |
| 原子写 `tryToWriteAtomic` 及 Python 对应接口 | 直接转发 origin 的实现。 |
| Java `newTwoPhaseOutputStream`、multipart 提交和清理 | 走 origin，上传与提交使用同一类端点；提交时沿现有 REST token 链路取得有效凭证。 |
| Blob presign、Java archive/restore/unarchive | 走 origin，保留底层签名和存储管理能力。 |

cache delegate 在第一次命中缓存操作时惰性创建，并对该操作的文件路径执行一次
`exists` 连通性探测（文件尚不存在是有效结果）。这是为了在 Jindo 等后端延迟到
输出流关闭时才建立网络连接的情况下，仍能在开始写入前发现不可达端点。

初始化、探测或打开文件/查询元数据遇到可识别的连接错误时，该操作改用 origin，
并输出一次 `WARN CACHE_POLICY_FALLBACK` 日志，包含操作、路径、两端 endpoint 和
异常类型，并完整打印异常堆栈及原因链。此后同一个 FileIO 实例持续屏蔽 cache endpoint，
所有操作使用 origin，
序列化也保留这一状态；新建 FileIO 实例才会重新尝试缓存。并发中已经开始的请求可能
仍在使用 cache delegate，delegate 保留至 FileIO 关闭时统一清理。

可回退的错误包括连接拒绝/断开、DNS 解析失败、socket 连接/读取超时，以及 Jindo
原生 `Failed to connect Http service:` 和 Arrow `NETWORK_CONNECTION` 诊断。
文件不存在、文件已存在、权限/鉴权失败、取消/中断以及无法确认是连接故障的一般 IO
错误均直接上抛，不触发回退或屏蔽。若 origin 重试也失败，最终抛出 origin 异常。

回退发生在 FileIO API 调用及 Arrow filesystem adapter 调用的边界，已返回的原生
输入/输出流不做透明重放。特别是已经开始写入后失败，不能安全地将部分写入或已消费
的迭代器重放到 origin；Python 格式写入接口仅在开始写入前探测失败时自动改用 origin，
写入途中失败仍报错，并在确认是连接故障时屏蔽后续缓存操作。调用方仍需处理这种写入
失败。此机制不新增客户端参数，仍按服务端下发的 policy 和 origin endpoint 工作。
Java `HadoopOptionsProvider` 与 Python Arrow filesystem adapter 使用同一套路径和操作规则。
Python 的 Vortex reader/writer 通过 `FileIO.for_path(path, operation)` 获取具体后端的
原生存储参数，同样遵守操作、路径白名单及缓存故障屏蔽状态。Arrow adapter 保留逻辑 URI，
每次操作先解析 `ResolvingFileIO`，再按选中的具体后端转换物理路径。
fork 后首次使用包装层时会重建锁及两个端点的进程内后端，避免继承其他线程持有的锁；
已经屏蔽缓存的状态继续保留。

## 服务端下发示例

只缓存指定表的两个 bucket 中的数据和文件索引读取，其余操作使用 origin：

```json
{
  "fs.oss.endpoint": "cache.example.internal",
  "io-cache.origin.endpoint": "oss-cn-hangzhou-internal.aliyuncs.com",
  "io-cache.policy": "read",
  "io-cache.whitelist": "data,file-index",
  "io-cache.whitelist-path": "/db/table/bucket-0/,/db/table/bucket-1/"
}
```

以 `oss://bucket/db/table` 为表路径，上述配置的结果如下：

| 操作和路径 | 访问端点 | 原因 |
| --- | --- | --- |
| 读取 `bucket-0/data-<id>-0.orc` | cache | `read`、类型和路径均命中。 |
| 读取 `bucket-1/data-<id>-0.orc.index` | cache | `file-index` 和路径命中。 |
| 读取 `bucket-2/data-<id>-0.orc` | origin | 路径未命中。 |
| 读取 `manifest/manifest-<id>-0` | origin | `meta` 文件类型未被允许。 |
| 读取 `snapshot/LATEST` | origin | 可变文件始终绕过。 |
| 查询数据文件大小或是否存在 | origin | 未启用 `meta` 操作标签。 |
| 写入数据文件 | origin | 未启用 `write` 操作标签。 |
| 列目录、删除文件、Java 两阶段上传 | origin | 固定走 origin。 |

允许所有不可变文件的读取与属性查询时，可以省略两个白名单：

```json
{
  "fs.oss.endpoint": "cache.example.internal",
  "io-cache.origin.endpoint": "oss-cn-hangzhou-internal.aliyuncs.com",
  "io-cache.policy": "meta,read"
}
```

停用缓存时，服务端将 `io-cache.policy` 下发为 `none` 或不下发该参数，
并提供期望使用的 origin endpoint；不需要额外的客户端关闭开关。

## 旧 JindoCache 兼容边界

Java `io-cache.enabled` 仍保留为旧 JindoCache 的客户端开关，默认 `false`，已标记废弃。
旧实现还依赖其 RPC 地址、policy 和路径白名单；其原有实现继续保留。
该开关只控制旧 JindoCache，不决定新的 endpoint policy 是否生效，
也不增加 VPC 校验要求。使用新的远端 cache policy 无需开启它。

相关实现：

- [Java CachePolicy](../paimon-common/src/main/java/org/apache/paimon/fs/cache/CachePolicy.java)
- [Java CachePolicyFileIO](../paimon-common/src/main/java/org/apache/paimon/fs/cache/CachePolicyFileIO.java)
- [Java 参数定义](../paimon-api/src/main/java/org/apache/paimon/rest/RESTCatalogOptions.java)
- [PyPaimon CachePolicyFileIO](../paimon-python/pypaimon/filesystem/cache_policy_file_io.py)
- [PyPaimon 参数定义](../paimon-python/pypaimon/common/options/config.py)
