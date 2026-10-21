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

from pypaimon.write.bucket_plan import PostponeBucketPlan
from pypaimon.write.bucket_planning import PostponeBucketPlanning

__all__ = ["PostponeBucketPlan", "PostponeBucketPlanner"]


class PostponeBucketPlanner(PostponeBucketPlanning):
    """Plan fixed bucket counts for postpone batch writes."""

    @staticmethod
    def _load_bucket_metadata(table):
        from pypaimon.execution.python.write.postpone_bucket import PythonPostponeBucketPlanner
        return PythonPostponeBucketPlanner._load_bucket_metadata(table)
