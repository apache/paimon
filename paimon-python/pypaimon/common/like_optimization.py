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

"""Simple LIKE rewrites shared by scalar readers and search filtering."""


def try_optimize_like(literal):
    """Follow Java LikeOptimization, leaving escaped/underscore patterns intact."""
    if literal is None:
        return None
    pattern = str(literal)
    if "_" in pattern or "\\" in pattern:
        return None
    if "%" not in pattern and pattern:
        return "equal", pattern
    if (pattern.startswith("%") and pattern.endswith("%")
            and pattern.count("%") == 2 and pattern[1:-1]):
        return "contains", pattern[1:-1]
    if pattern.startswith("%") and pattern.count("%") == 1 and pattern[1:]:
        return "ends_with", pattern[1:]
    if pattern.endswith("%") and pattern.count("%") == 1 and pattern[:-1]:
        return "starts_with", pattern[:-1]
    return None
