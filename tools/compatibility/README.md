<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Table compatibility test

Run `bash tools/compatibility/run-table-compatibility.sh` after installing the
current `paimon-bundle` into the local Maven repository. CI runs it in the Java 8
core job after the reactor build.

Each test invocation runs in its own JVM. Paimon 1.0.1 creates append and primary
key tables, then 1.4.2, 2.0.0, and the current build take turns reading and writing
each table with 1.0.1. The test also checks that 1.0.1 rejects the newer BLOB type.

This covers the tested operations and schemas only; it does not assert general
cross-version read/write compatibility.
