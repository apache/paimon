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

from enum import Enum, auto


class BucketMode(Enum):
    def __str__(self):
        return self.value

    HASH_FIXED = auto()
    HASH_DYNAMIC = auto()
    CROSS_PARTITION = auto()
    BUCKET_UNAWARE = auto()
    POSTPONE_MODE = auto()

    POSTPONE_BUCKET = -2
