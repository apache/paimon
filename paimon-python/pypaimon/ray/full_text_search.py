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

"""Execute full-text and hybrid searches through public backend interfaces."""

from pypaimon.ray.search_execution import RaySearchTaskExecutor


def _execute_full_text_search(builder, *, concurrency=None, ray_remote_args=None):
    executor = RaySearchTaskExecutor(concurrency, ray_remote_args)
    reader = builder.new_full_text_read()
    plan = builder.new_full_text_scan().scan()
    return reader.read_plan(plan, executor=executor)


def _execute_hybrid_search(builder, *, concurrency=None, ray_remote_args=None):
    return builder.execute(executor=RaySearchTaskExecutor(concurrency, ray_remote_args))
