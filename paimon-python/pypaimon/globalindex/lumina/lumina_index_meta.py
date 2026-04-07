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

"""Lumina index metadata serialization.

Serialized as a flat JSON ``Map<String, String>`` whose keys are native Lumina
option keys (with the ``lumina.`` prefix stripped).  This matches the format
used by both paimon-lumina (Java) and paimon-cpp, so indexes built by any
implementation can be read by all of them.
"""

import json


class LuminaIndexMeta:
    """Metadata for a Lumina vector index file."""

    KEY_DIMENSION = "index.dimension"
    KEY_DISTANCE_METRIC = "distance.metric"
    KEY_INDEX_TYPE = "index.type"

    def __init__(self, options):
        self._options = dict(options)

    @property
    def options(self):
        return self._options

    @property
    def dim(self):
        return int(self._options[self.KEY_DIMENSION])

    @property
    def distance_metric(self):
        return self._options[self.KEY_DISTANCE_METRIC]

    @property
    def metric(self):
        from lumina_data import MetricType
        return MetricType.from_lumina_name(self.distance_metric)

    @property
    def index_type(self):
        return self._options.get(self.KEY_INDEX_TYPE, "diskann")

    def serialize(self):
        """Serialize to UTF-8 JSON bytes."""
        return json.dumps(self._options).encode("utf-8")

    @staticmethod
    def deserialize(data):
        """Deserialize from UTF-8 JSON bytes."""
        options = json.loads(data.decode("utf-8"))
        if LuminaIndexMeta.KEY_DIMENSION not in options:
            raise ValueError(
                "Missing required key: %s" % LuminaIndexMeta.KEY_DIMENSION)
        if LuminaIndexMeta.KEY_DISTANCE_METRIC not in options:
            raise ValueError(
                "Missing required key: %s" % LuminaIndexMeta.KEY_DISTANCE_METRIC)
        return LuminaIndexMeta(options)
