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

Apache Paimon（孵化中）是一个支持高速数据摄取、变更数据跟踪和高效实时分析的流数据湖平台。

Paimon提供以下核心功能：

* 统一批处理和流处理：Paimon支持批处理写入和批处理读取，以及流处理写入变更和流处理读取表格变更日志。
* 数据湖：作为数据湖存储，Paimon具有以下优势：低成本、高可靠性和可伸缩的元数据。
* 合并引擎：Paimon支持丰富的合并引擎。默认情况下，主键的最后一项被保留。您还可以使用“部分更新”或“聚合”引擎。
* 变更日志生产者：Paimon支持丰富的变更日志生产者，例如“查找”和“完全压缩”。正确的变更日志可以简化流水线的构建。
* 追加式表格：Paimon支持追加式表格，自动压缩小文件，并提供有序的流读取。您可以使用这个功能来替代消息队列。

{{< columns >}}

## 尝试Paimon

如果您对Paimon感兴趣，可以查看我们的 快速入门指南，使用 [Flink]({{< ref "engines/flink" >}})、[Spark]({{< ref "engines/spark" >}}) 或 [Hive]({{< ref "engines/hive" >}})。它提供了逐步介绍API并指导您进行实际应用的步骤。

<--->

## 获取Paimon的帮助

如果遇到困难，您可以订阅用户邮件列表（user-subscribe@paimon.apache.org）， Paimon在GitHub上跟踪问题并倾向于将贡献作为拉取请求接收。您还可以创建一个问题。

{{< /columns >}}
