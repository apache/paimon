# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Build and inspect public search tasks in focused execution tests."""

from pypaimon.ray.search_execution import RaySearchTaskExecutor
from pypaimon.table.source.search_execution import SearchTask


def task_parts(tasks):
    tasks = list(tasks)
    first = tasks[0]
    assert all(task.operation is first.operation and task.context is first.context for task in tasks)
    return first.operation, first.context, [task.item for task in tasks]


def execute_tasks(operation, context, items, concurrency, remote_args, ordered=False):
    executor = RaySearchTaskExecutor(concurrency, remote_args)
    return executor.map([SearchTask(operation, context, item) for item in items], ordered=ordered)
