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

"""Lumina vector index options for Paimon tables.
Accepts Paimon table properties (with ``lumina.`` prefix), strips the prefix and provides typed accessors.
"""

LUMINA_PREFIX = "lumina."

# ConfigOption definitions: (paimon_key, default_value)
_ALL_OPTIONS = [
    ("lumina.index.dimension", "128"),
    ("lumina.index.type", "diskann"),
    ("lumina.distance.metric", "inner_product"),
    ("lumina.encoding.type", "pq"),
    ("lumina.pretrain.sample_ratio", "0.2"),
    ("lumina.diskann.build.ef_construction", "1024"),
    ("lumina.diskann.build.neighbor_count", "64"),
    ("lumina.diskann.build.thread_count", "32"),
    ("lumina.diskann.search.beam_width", "4"),
    ("lumina.encoding.pq.m", "64"),
    ("lumina.search.parallel_number", "5"),
]


def _strip_prefix(key):
    if key.startswith(LUMINA_PREFIX):
        return key[len(LUMINA_PREFIX):]
    return key


class LuminaVectorIndexOptions:

    def __init__(self, paimon_options):
        """Create from Paimon table options dict.

        Args:
            paimon_options: dict of table properties, e.g.
                {"lumina.index.dimension": "128", "lumina.distance.metric": "l2", ...}
                Non-lumina keys are ignored.
        """
        self._lumina_options = _build_lumina_options(paimon_options)
        self._dimension = _validate_positive(
            int(self._lumina_options["index.dimension"]), "lumina.index.dimension")
        from lumina_data import MetricType
        self._metric_type_class = MetricType
        self._metric = _parse_metric(MetricType, self._lumina_options["distance.metric"])
        self._index_type = self._lumina_options.get("index.type", "diskann")
        _validate_encoding_metric(
            MetricType, self._lumina_options.get("encoding.type", "pq"), self._metric)

    def to_lumina_options(self):
        """Return all lumina options as native keys (prefix stripped).
        """
        return dict(self._lumina_options)

    @property
    def dimension(self):
        return self._dimension

    @property
    def metric(self):
        return self._metric

    @property
    def index_type(self):
        return self._index_type

    @property
    def encoding_type(self):
        return self._lumina_options.get("encoding.type", "pq")


def _build_lumina_options(paimon_options):
    """Build native Lumina options from Paimon table options.
    1. Populate all known options with defaults
    2. Overlay user-specified lumina.* options
    3. Cap pq.m to dimension
    """
    result = {}

    # 1. Populate all known options with their resolved values (user-set or default)
    for paimon_key, default_value in _ALL_OPTIONS:
        user_value = paimon_options.get(paimon_key)
        value = user_value if user_value is not None else default_value
        result[_strip_prefix(paimon_key)] = str(value)

    # 2. Overlay any extra user-specified lumina.* options not in _ALL_OPTIONS
    known_keys = {k for k, _ in _ALL_OPTIONS}
    for key, value in paimon_options.items():
        if key.startswith(LUMINA_PREFIX) and key not in known_keys:
            native_key = _strip_prefix(key)
            result[native_key] = str(value)

    # 3. Cap pq.m to dimension
    _cap_pq_m(result)

    return result


def _cap_pq_m(opts):
    """Ensure encoding.pq.m does not exceed dimension."""
    encoding = opts.get("encoding.type")
    if encoding != "pq":
        return
    pq_m_str = opts.get("encoding.pq.m")
    if pq_m_str is not None:
        pq_m = int(pq_m_str)
        dim = int(opts.get("index.dimension", "128"))
        if pq_m > dim:
            opts["encoding.pq.m"] = str(dim)


def _parse_metric(MetricType, value):
    """Parse distance metric, accepting both native names and enum names."""
    try:
        return MetricType.from_lumina_name(value)
    except ValueError:
        return MetricType.from_string(value)


def _validate_encoding_metric(MetricType, encoding, metric):
    if encoding == "pq" and metric == MetricType.COSINE:
        raise ValueError(
            "Lumina does not support PQ encoding with cosine metric. "
            "Please use 'rawf32' or 'sq8' encoding, or switch to "
            "'l2' or 'inner_product' metric.")


def _validate_positive(value, key):
    if value <= 0:
        raise ValueError(
            "Invalid value for '%s': %d. Must be a positive integer." % (key, value))
    return value
