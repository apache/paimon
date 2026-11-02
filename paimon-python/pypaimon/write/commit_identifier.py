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

"""Commit identifiers share Java long and Rust i64's signed integer domain."""

import operator


def validate_commit_identifier(value):
    """Normalize Python integer-like values before preparing or publishing files."""
    identifier = operator.index(value)
    if not -(1 << 63) <= identifier < (1 << 63):
        raise OverflowError('commit_identifier must fit in a signed 64-bit integer')
    return identifier
