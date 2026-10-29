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

"""Experimental, provider-neutral asynchronous prediction protocol."""

from dataclasses import dataclass
from typing import Dict, List, Optional

from pypaimon.api.api_request import RESTRequest
from pypaimon.api.api_response import RESTResponse
from pypaimon.common.json_util import optional_json_field


@dataclass
class PredictRequest(RESTRequest):
    model: str
    inputs: List[Dict[str, str]]
    result_columns: List[str]
    prompt: str
    response_format: str = "json_object"
    where: Optional[str] = optional_json_field("where", "non_null")
    snapshot_id: Optional[int] = optional_json_field("snapshot_id", "non_null")
    tag_name: Optional[str] = optional_json_field("tag_name", "non_null")

    def __post_init__(self):
        for name in ("model", "prompt", "response_format"):
            _nonblank(getattr(self, name), name)
        if self.where is not None:
            _nonblank(self.where, "where")
        if self.tag_name is not None:
            _nonblank(self.tag_name, "tag_name")
        if self.snapshot_id is not None and (
                type(self.snapshot_id) is not int or not 0 < self.snapshot_id < 2 ** 63):
            raise ValueError("snapshot_id must be a positive integer")
        if self.snapshot_id is not None and self.tag_name is not None:
            raise ValueError("Specify snapshot_id or tag_name, not both")
        if not isinstance(self.inputs, list) or not self.inputs:
            raise ValueError("inputs must be a non-empty list")
        for item in self.inputs:
            if not isinstance(item, dict) or set(item) != {"column", "modality"}:
                raise ValueError("Each input must contain only column and modality")
            _nonblank(item["column"], "column")
            if item["modality"] not in ("text", "image", "audio", "video"):
                raise ValueError("Unsupported input modality")
        if not isinstance(self.result_columns, list):
            raise ValueError("result_columns must be a list")
        for column in self.result_columns:
            _nonblank(column, "result column")
        if len(set(self.result_columns)) != len(self.result_columns):
            raise ValueError("Duplicate result columns")


@dataclass
class PredictResponse(RESTResponse):
    job_id: str
    result_table: str
    snapshot_id: int
    model_version: str

    def __post_init__(self):
        for name in ("job_id", "result_table", "model_version"):
            _nonblank(getattr(self, name), name)
        if type(self.snapshot_id) is not int or not 0 < self.snapshot_id < 2 ** 63:
            raise ValueError("Accepted prediction must identify a fixed snapshot")


def _nonblank(value, name):
    if not isinstance(value, str) or not value.strip():
        raise ValueError("%s must be a non-empty string" % name)
