---
title: Apache Paimon
type: docs
bookToc: false
---
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

# Apache Paimon

Apache Paimon（孵化中）是一个流式数据湖平台，支持高速数据摄取、变更数据追踪和高效的实时分析。

Paimon提供以下核心功能：

- 统一的批处理和流处理：Paimon支持批量写入和批量读取，同时支持以流式方式写入变更和以流式方式读取表的变更日志。
- 数据湖：作为数据湖存储，Paimon具有以下优势：低成本、高可靠性和可扩展的元数据。

* 合并引擎：Paimon支持丰富的合并引擎。默认情况下，保留主键的最后一个条目。您还可以使用“partial-update”或“aggregation”引擎。

* 变更日志生成器：Paimon支持丰富的变更日志生成器，例如“lookup”和“full-compaction”。正确的变更日志可以简化流水线的构建过程。

* 追加表：Paimon支持追加表，自动压缩小文件，并提供有序的流式读取。您可以使用它来替代消息队列。

{{< columns >}}
## 尝试 Paimon

如果您对Paimon感兴趣，可以查看我们与 [Flink]({{< ref "engines/flink" >}}), [Spark]({{< ref "engines/spark3" >}}) 或 [Hive]({{< ref "engines/hive" >}}).的快速入门指南。它提供了逐步介绍API并指导您进行实际应用程序的步骤。

<--->

## 获取 Paimon 的帮助

如果您遇到问题，可以订阅用户邮件列表（[user-subscribe@paimon.apache.org](mailto:user-subscribe@paimon.apache.org)），Paimon在GitHub上跟踪问题并更喜欢接收拉取请求作为贡献。您还可以创建一个问题。

{{< /columns >}}
