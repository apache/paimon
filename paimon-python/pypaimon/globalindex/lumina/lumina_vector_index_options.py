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

Handles the ``lumina.`` prefix used in Paimon table properties and provides
typed accessors. Mirrors ``LuminaVectorIndexOptions.java``.
"""

from lumina_data import MetricType

# Prefix used in Paimon table properties for Lumina options.
LUMINA_PREFIX = "lumina."

# Default option values (mirrors LuminaVectorIndexOptions.java)
DEFAULTS = {
    "index.dimension": "128",
    "index.type": "diskann",
    "distance.metric": "inner_product",
    "encoding.type": "pq",
    "pretrain.sample_ratio": "0.2",
    "diskann.build.ef_construction": "1024",
    "diskann.build.neighbor_count": "64",
    "diskann.build.thread_count": "32",
    "diskann.search.beam_width": "4",
    "encoding.pq.m": "64",
    "search.parallel_number": "5",
}


class LuminaVectorIndexOptions:
    """Configuration options for Lumina vector indexes in Paimon.

    Accepts Paimon table options (with ``lumina.`` prefix), strips the prefix
    to produce native Lumina keys.
    """

    def __init__(self, options=None):
        self._options = dict(DEFAULTS)
        if options:
            self._options.update(options)
        self._validate()

    def _validate(self):
        dim = self.dimension
        if dim <= 0:
            raise ValueError("Dimension must be positive, got: %d" % dim)

        encoding = self.encoding_type
        metric = self.metric
        if encoding == "pq" and metric == MetricType.COSINE:
            raise ValueError(
                "Lumina does not support PQ encoding with cosine metric. "
                "Use 'rawf32' or 'sq8' encoding, or switch to "
                "'l2' or 'inner_product' metric.")

        # Auto-cap pq.m to dimension
        if encoding == "pq":
            pq_m = int(self._options.get("encoding.pq.m", "64"))
            if pq_m > dim:
                self._options["encoding.pq.m"] = str(dim)

    @property
    def dimension(self):
        return int(self._options["index.dimension"])

    @property
    def index_type(self):
        return self._options.get("index.type", "diskann")

    @property
    def metric(self):
        name = self._options.get("distance.metric", "inner_product")
        try:
            return MetricType.from_lumina_name(name)
        except ValueError:
            return MetricType.from_string(name)

    @property
    def encoding_type(self):
        return self._options.get("encoding.type", "pq")

    def to_native_options(self):
        """Return a copy of all options as native Lumina keys."""
        return dict(self._options)

    @staticmethod
    def from_paimon_options(paimon_options):
        """Create from Paimon table properties (strips ``lumina.`` prefix)."""
        native = {}
        for key, value in paimon_options.items():
            if key.startswith(LUMINA_PREFIX):
                native[key[len(LUMINA_PREFIX):]] = value
        return LuminaVectorIndexOptions(native)
