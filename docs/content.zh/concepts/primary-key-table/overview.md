---
title: "Overview"
weight: 1
type: docs
aliases:
- /concepts/primary-key-table/overview.html
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

# 概述

主键表是创建表时的默认表类型。用户可以向表中插入、更新或删除记录。

主键由一组包含每个记录唯一值的列组成。Paimon通过在每个桶内对主键进行排序来强制数据排序，从而允许用户通过在主键上应用筛选条件来实现高性能。请参阅创建表。


