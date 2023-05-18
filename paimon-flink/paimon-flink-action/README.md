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

# Flink Action

This module contains one class FlinkActions.

The reason for extracting it as a separate module is that: When executing the Flink jar job, a jar must be specified.
If a `paimon-flink.jar` is specified, it may cause various classloader issues, as there are also `paimon-flink.jar`
in flink/lib and User Classloader, which will cause classes conflicts.
