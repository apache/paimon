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

from abc import ABC, abstractmethod
from typing import Any, List


class Predicate(ABC):
    """Predicate which evaluates to a boolean. Now it doesn't have
    any methods because only paimon_python_java implement it and
    the Java implementation convert it to Java object."""


class PredicateBuilder(ABC):
    """A utility class to create Predicate object for common filter conditions."""

    @abstractmethod
    def equal(self, field: str, literal: Any) -> Predicate:
        """field = literal"""

    @abstractmethod
    def not_equal(self, field: str, literal: Any) -> Predicate:
        """field <> literal"""

    @abstractmethod
    def less_than(self, field: str, literal: Any) -> Predicate:
        """field < literal"""

    @abstractmethod
    def less_or_equal(self, field: str, literal: Any) -> Predicate:
        """field <= literal"""

    @abstractmethod
    def greater_than(self, field: str, literal: Any) -> Predicate:
        """field > literal"""

    @abstractmethod
    def greater_or_equal(self, field: str, literal: Any) -> Predicate:
        """field >= literal"""

    @abstractmethod
    def is_null(self, field: str) -> Predicate:
        """field IS NULL"""

    @abstractmethod
    def is_not_null(self, field: str) -> Predicate:
        """field IS NOT NULL"""

    @abstractmethod
    def startswith(self, field: str, pattern_literal: Any) -> Predicate:
        """field.startswith"""

    @abstractmethod
    def endswith(self, field: str, pattern_literal: Any) -> Predicate:
        """field.endswith()"""

    @abstractmethod
    def contains(self, field: str, pattern_literal: Any) -> Predicate:
        """literal in field"""

    @abstractmethod
    def is_in(self, field: str, literals: List[Any]) -> Predicate:
        """field IN literals"""

    @abstractmethod
    def is_not_in(self, field: str, literals: List[Any]) -> Predicate:
        """field NOT IN literals"""

    @abstractmethod
    def between(self, field: str, included_lower_bound: Any, included_upper_bound: Any) \
            -> Predicate:
        """field BETWEEN included_lower_bound AND included_upper_bound"""

    @abstractmethod
    def and_predicates(self, predicates: List[Predicate]) -> Predicate:
        """predicate1 AND predicate2 AND ..."""

    @abstractmethod
    def or_predicates(self, predicates: List[Predicate]) -> Predicate:
        """predicate1 OR predicate2 OR ..."""
