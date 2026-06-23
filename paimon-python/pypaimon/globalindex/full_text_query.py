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

"""Structured full-text query DSL aligned with LanceDB FTS query JSON."""

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union


class FullTextQueryType(str, Enum):
    MATCH = "match"
    MATCH_PHRASE = "match_phrase"
    BOOST = "boost"
    MULTI_MATCH = "multi_match"
    BOOLEAN = "boolean"


class FullTextOperator(str, Enum):
    AND = "AND"
    OR = "OR"

    @staticmethod
    def normalize(value: Optional[Union[str, 'FullTextOperator']]) -> 'FullTextOperator':
        if value is None:
            return FullTextOperator.OR
        if isinstance(value, FullTextOperator):
            return value
        normalized = str(value).strip().upper()
        if normalized == "AND":
            return FullTextOperator.AND
        if normalized == "OR":
            return FullTextOperator.OR
        raise ValueError("Full-text query operator must be 'or' or 'and', got: %s" % value)

    def json_value(self) -> str:
        return "And" if self == FullTextOperator.AND else "Or"


class Occur(str, Enum):
    SHOULD = "SHOULD"
    MUST = "MUST"
    MUST_NOT = "MUST_NOT"

    @staticmethod
    def normalize(value: Union[str, 'Occur']) -> 'Occur':
        if isinstance(value, Occur):
            return value
        normalized = str(value).strip().upper()
        if normalized == "SHOULD":
            return Occur.SHOULD
        if normalized == "MUST":
            return Occur.MUST
        if normalized in ("MUST_NOT", "MUSTNOT"):
            return Occur.MUST_NOT
        raise ValueError("Unknown boolean query occur: %s" % value)


class FullTextQuery(ABC):
    """Base class for LanceDB-style full-text query objects."""

    @abstractmethod
    def query_type(self) -> FullTextQueryType:
        pass

    @abstractmethod
    def query_text(self) -> str:
        pass

    @abstractmethod
    def referenced_columns(self) -> List[str]:
        pass

    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        pass

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"))

    def single_column(self) -> str:
        columns = self.referenced_columns()
        if len(columns) != 1:
            raise ValueError(
                "Full-text query must reference exactly one column, but got: %s" % columns
            )
        return columns[0]

    def __and__(self, other: 'FullTextQuery') -> 'BooleanQuery':
        return BooleanQuery([(Occur.MUST, self), (Occur.MUST, other)])

    def __or__(self, other: 'FullTextQuery') -> 'BooleanQuery':
        return BooleanQuery([(Occur.SHOULD, self), (Occur.SHOULD, other)])

    @staticmethod
    def from_json(query_json: str) -> 'FullTextQuery':
        try:
            payload = json.loads(query_json)
        except Exception as e:
            raise ValueError("Failed to parse full-text query JSON: %s" % query_json) from e
        return FullTextQuery.from_dict(payload)

    @staticmethod
    def from_dict(payload: Dict[str, Any]) -> 'FullTextQuery':
        if not isinstance(payload, dict):
            raise ValueError("Full-text query JSON must be an object.")
        if "match" in payload:
            body = _object_value(payload, "match")
            return MatchQuery(
                _text_value_any(body, ("terms", "query")),
                _text_value(body, "column"),
                boost=_float_value(body, "boost", 1.0),
                fuzziness=_fuzziness_value(body),
                max_expansions=_int_value_any(body, ("max_expansions", "maxExpansions"), 50),
                operator=FullTextOperator.normalize(body.get("operator")),
                prefix_length=_int_value_any(body, ("prefix_length", "prefixLength"), 0),
            )
        if "match_phrase" in payload or "phrase" in payload:
            body = _object_value(payload, "match_phrase" if "match_phrase" in payload else "phrase")
            return PhraseQuery(
                _text_value_any(body, ("terms", "query")),
                _text_value(body, "column"),
                slop=_int_value(body, "slop", 0),
            )
        if "boost" in payload:
            body = _object_value(payload, "boost")
            return BoostQuery(
                FullTextQuery.from_dict(_object_value(body, "positive")),
                FullTextQuery.from_dict(_object_value(body, "negative")),
                negative_boost=_float_value_any(body, ("negative_boost", "negativeBoost"), 0.5),
            )
        if "multi_match" in payload:
            body = _object_value(payload, "multi_match")
            return MultiMatchQuery(
                _text_value(body, "query"),
                _string_list(body.get("columns"), "columns"),
                boosts=_optional_float_list(_field_value(body, ("boost", "boosts"))),
                operator=FullTextOperator.normalize(body.get("operator")),
            )
        if "boolean" in payload:
            body = _object_value(payload, "boolean")
            queries = []
            _add_boolean_queries(queries, Occur.SHOULD, body.get("should"))
            _add_boolean_queries(queries, Occur.MUST, body.get("must"))
            _add_boolean_queries(queries, Occur.MUST_NOT, body.get("must_not"))
            if "queries" in body:
                clauses = body.get("queries")
                if not isinstance(clauses, list):
                    raise ValueError("Boolean query 'queries' must be an array.")
                for clause in clauses:
                    if isinstance(clause, (list, tuple)) and len(clause) == 2:
                        occur = Occur.normalize(clause[0])
                        query = FullTextQuery.from_dict(clause[1])
                    elif isinstance(clause, dict):
                        occur = Occur.normalize(clause.get("occur"))
                        query = FullTextQuery.from_dict(_object_value(clause, "query"))
                    else:
                        raise ValueError("Invalid boolean query clause: %s" % (clause,))
                    queries.append((occur, query))
            return BooleanQuery(queries)
        raise ValueError("Unknown full-text query JSON: %s" % payload)


@dataclass(frozen=True)
class MatchQuery(FullTextQuery):
    query: str
    column: str
    boost: float = 1.0
    fuzziness: Optional[int] = 0
    max_expansions: int = 50
    operator: Union[str, FullTextOperator] = FullTextOperator.OR
    prefix_length: int = 0

    def __post_init__(self):
        _check_terms(self.query)
        _check_column(self.column)
        object.__setattr__(self, "query", str(self.query))
        object.__setattr__(self, "column", str(self.column))
        _check_positive(self.boost, "Boost factor")
        if self.fuzziness is not None:
            if self.fuzziness < 0 or self.fuzziness > 2:
                raise ValueError("fuzziness must be between 0 and 2, got: %s" % self.fuzziness)
        if self.max_expansions <= 0:
            raise ValueError("max_expansions must be positive, got: %s" % self.max_expansions)
        if self.prefix_length < 0:
            raise ValueError("prefix_length must be non-negative, got: %s" % self.prefix_length)
        object.__setattr__(self, "operator", FullTextOperator.normalize(self.operator))

    def query_type(self) -> FullTextQueryType:
        return FullTextQueryType.MATCH

    def query_text(self) -> str:
        return self.query

    def referenced_columns(self) -> List[str]:
        return [self.column]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "match": {
                "column": self.column,
                "terms": self.query,
                "boost": self.boost,
                "fuzziness": self.fuzziness,
                "max_expansions": self.max_expansions,
                "operator": self.operator.json_value(),
                "prefix_length": self.prefix_length,
            }
        }


@dataclass(frozen=True)
class PhraseQuery(FullTextQuery):
    query: str
    column: str
    slop: int = 0

    def __post_init__(self):
        _check_terms(self.query)
        _check_column(self.column)
        object.__setattr__(self, "query", str(self.query))
        object.__setattr__(self, "column", str(self.column))
        if self.slop < 0:
            raise ValueError("slop must be non-negative, got: %s" % self.slop)

    def query_type(self) -> FullTextQueryType:
        return FullTextQueryType.MATCH_PHRASE

    def query_text(self) -> str:
        return self.query

    def referenced_columns(self) -> List[str]:
        return [self.column]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "match_phrase": {
                "column": self.column,
                "terms": self.query,
                "slop": self.slop,
            }
        }


@dataclass(frozen=True)
class BoostQuery(FullTextQuery):
    positive: FullTextQuery
    negative: FullTextQuery
    negative_boost: float = 0.5

    def __post_init__(self):
        _check_positive(self.negative_boost, "Negative boost")

    def query_type(self) -> FullTextQueryType:
        return FullTextQueryType.BOOST

    def query_text(self) -> str:
        return self.positive.query_text()

    def referenced_columns(self) -> List[str]:
        return _distinct(
            self.positive.referenced_columns() + self.negative.referenced_columns()
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "boost": {
                "positive": self.positive.to_dict(),
                "negative": self.negative.to_dict(),
                "negative_boost": self.negative_boost,
            }
        }


@dataclass(frozen=True)
class MultiMatchQuery(FullTextQuery):
    query: str
    columns: List[str]
    boosts: Optional[List[float]] = None
    operator: Union[str, FullTextOperator] = FullTextOperator.OR

    def __init__(
        self,
        query: str,
        columns: Sequence[str],
        boosts: Optional[Sequence[float]] = None,
        operator: Union[str, FullTextOperator] = FullTextOperator.OR,
    ):
        object.__setattr__(self, "query", query)
        object.__setattr__(self, "columns", list(columns) if columns is not None else None)
        object.__setattr__(
            self, "boosts", list(boosts) if boosts is not None else None
        )
        object.__setattr__(self, "operator", operator)
        self.__post_init__()

    def __post_init__(self):
        _check_terms(self.query)
        if not self.columns:
            raise ValueError("columns cannot be None or empty")
        for column in self.columns:
            _check_column(column)
        object.__setattr__(self, "query", str(self.query))
        object.__setattr__(self, "columns", [str(column) for column in self.columns])
        boosts = self.boosts
        if boosts is None:
            boosts = [1.0] * len(self.columns)
        if len(boosts) != len(self.columns):
            raise ValueError("The number of boosts must match the number of columns.")
        for boost in boosts:
            _check_positive(boost, "Boost")
        object.__setattr__(self, "boosts", list(boosts))
        object.__setattr__(self, "operator", FullTextOperator.normalize(self.operator))

    def query_type(self) -> FullTextQueryType:
        return FullTextQueryType.MULTI_MATCH

    def query_text(self) -> str:
        return self.query

    def referenced_columns(self) -> List[str]:
        return list(self.columns)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "multi_match": {
                "query": self.query,
                "columns": list(self.columns),
                "boost": list(self.boosts),
                "operator": self.operator.json_value(),
            }
        }


@dataclass(frozen=True)
class BooleanQuery(FullTextQuery):
    queries: List[Tuple[Occur, FullTextQuery]]

    def __init__(self, queries: Sequence[Tuple[Union[str, Occur], FullTextQuery]]):
        checked = []
        for occur, query in queries or []:
            if not isinstance(query, FullTextQuery):
                raise ValueError("Boolean query must contain FullTextQuery objects.")
            checked.append((Occur.normalize(occur), query))
        if not checked:
            raise ValueError("Boolean query must contain at least one clause.")
        object.__setattr__(self, "queries", checked)

    def query_type(self) -> FullTextQueryType:
        return FullTextQueryType.BOOLEAN

    def query_text(self) -> str:
        return ""

    def referenced_columns(self) -> List[str]:
        columns = []
        for _, query in self.queries:
            columns.extend(query.referenced_columns())
        return _distinct(columns)

    def should(self) -> List[FullTextQuery]:
        return [query for occur, query in self.queries if occur == Occur.SHOULD]

    def must(self) -> List[FullTextQuery]:
        return [query for occur, query in self.queries if occur == Occur.MUST]

    def must_not(self) -> List[FullTextQuery]:
        return [query for occur, query in self.queries if occur == Occur.MUST_NOT]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "boolean": {
                "should": [query.to_dict() for query in self.should()],
                "must": [query.to_dict() for query in self.must()],
                "must_not": [query.to_dict() for query in self.must_not()],
            }
        }


def _add_boolean_queries(
    target: List[Tuple[Occur, FullTextQuery]],
    occur: Occur,
    values: Optional[Sequence[Dict[str, Any]]],
) -> None:
    if values is None:
        return
    if not isinstance(values, list):
        raise ValueError("Boolean query clauses must be arrays.")
    for value in values:
        target.append((occur, FullTextQuery.from_dict(value)))


def _object_value(payload: Dict[str, Any], field: str) -> Dict[str, Any]:
    value = payload.get(field)
    if not isinstance(value, dict):
        raise ValueError("Field '%s' must be an object." % field)
    return value


def _field_value(payload: Dict[str, Any], fields: Iterable[str]) -> Any:
    for field in fields:
        if field in payload:
            return payload[field]
    return None


def _text_value(payload: Dict[str, Any], field: str) -> str:
    value = payload.get(field)
    if value is None:
        raise ValueError("Field '%s' cannot be null or empty." % field)
    return str(value)


def _text_value_any(payload: Dict[str, Any], fields: Iterable[str]) -> str:
    value = _field_value(payload, fields)
    if value is None:
        raise ValueError("Query terms cannot be null or empty.")
    return str(value)


def _int_value(payload: Dict[str, Any], field: str, default: int) -> int:
    value = payload.get(field)
    if value is None:
        return default
    return int(value)


def _int_value_any(payload: Dict[str, Any], fields: Iterable[str], default: int) -> int:
    value = _field_value(payload, fields)
    if value is None:
        return default
    return int(value)


def _float_value(payload: Dict[str, Any], field: str, default: float) -> float:
    value = payload.get(field)
    if value is None:
        return default
    return float(value)


def _float_value_any(payload: Dict[str, Any], fields: Iterable[str], default: float) -> float:
    value = _field_value(payload, fields)
    if value is None:
        return default
    return float(value)


def _fuzziness_value(payload: Dict[str, Any]) -> Optional[int]:
    if "fuzziness" not in payload:
        return 0
    value = payload.get("fuzziness")
    if value is None:
        return None
    if isinstance(value, str) and value.lower() == "auto":
        return None
    return int(value)


def _string_list(value: Any, field: str) -> List[str]:
    if not isinstance(value, list):
        raise ValueError("%s must be an array." % field)
    return [str(item) for item in value]


def _optional_float_list(value: Any) -> Optional[List[float]]:
    if value is None:
        return None
    if not isinstance(value, list):
        raise ValueError("boost must be an array.")
    return [float(item) for item in value]


def _check_column(column: str) -> None:
    if column is None or str(column) == "":
        raise ValueError("Column cannot be None or empty.")


def _check_terms(terms: str) -> None:
    if terms is None or str(terms) == "":
        raise ValueError("Query terms cannot be None or empty.")


def _check_positive(value: float, name: str) -> None:
    if value is None or value <= 0:
        raise ValueError("%s must be positive, got: %s" % (name, value))


def _distinct(values: Sequence[str]) -> List[str]:
    result = []
    seen = set()
    for value in values:
        if value not in seen:
            seen.add(value)
            result.append(value)
    return result
