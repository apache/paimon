################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

from pypaimon.read.split import Split


class QueryAuthSplit(Split):

    def __init__(self, split: Split, auth_result):
        self._split = split
        self._auth_result = auth_result

    @property
    def split(self) -> Split:
        return self._split

    @property
    def auth_result(self):
        return self._auth_result

    @property
    def row_count(self) -> int:
        return self._split.row_count

    @property
    def files(self):
        return self._split.files

    @property
    def partition(self):
        return self._split.partition

    @property
    def bucket(self) -> int:
        return self._split.bucket

    @property
    def raw_convertible(self) -> bool:
        return self._split.raw_convertible

    def merged_row_count(self):
        if self._auth_result.filter:
            return None
        return self._split.merged_row_count()

    def __getattr__(self, name):
        if name.startswith('_'):
            raise AttributeError(name)
        return getattr(self._split, name)
