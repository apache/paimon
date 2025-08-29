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
#################################################################################

from typing import Optional, Dict


class Database:
    """Structure of a Database."""

    def __init__(self, name: str, options: Dict, comment: Optional[str] = None):
        self.name = name
        self.options = options
        self.comment = comment

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Database):
            return False
        return (self.name == other.name and
                self.options == other.options and
                self.comment == other.comment)
