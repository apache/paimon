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

"""Shared validation of scan sharding and row-position requests."""


def validate_shard(index: int, count: int) -> None:
    if not isinstance(count, int) or count <= 0:
        raise ValueError("number_of_para_subtasks must be a positive integer")
    if not isinstance(index, int) or index < 0:
        raise ValueError("idx_of_this_subtask must be a non-negative integer")
    if index >= count:
        raise ValueError("idx_of_this_subtask must be less than number_of_para_subtasks")


def validate_slice(start: int, end: int) -> None:
    if not isinstance(start, int) or not isinstance(end, int) or start < 0 or start >= end:
        raise ValueError("start_pos must be non-negative and less than end_pos; both must be integers")
