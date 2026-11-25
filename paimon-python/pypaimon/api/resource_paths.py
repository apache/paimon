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

import urllib.parse
from typing import Optional

from pypaimon.api.rest_util import RESTUtil
from pypaimon.common.options import Options
from pypaimon.common.options.config import CatalogOptions
from pypaimon.management.java_string import is_blank


class ResourcePaths:
    V1 = "v1"
    DATABASES = "databases"
    TABLES = "tables"
    ID = "id"
    TABLE_DETAILS = "table-details"
    PARTITIONS = "partitions"
    FUNCTIONS = "functions"
    FUNCTION_DETAILS = "function-details"
    MODELS = "models"
    SEMANTIC_VIEWS = "semantic-views"
    PREDICTIONS = "predictions"
    TAGS = "tags"
    BRANCHES = "branches"
    FORWARD = "forward"
    PERMISSIONS = "permissions"
    POLICIES = "policies"

    def __init__(self, prefix: str):
        # Java encodes the prefix once with URLEncoder, so it stays one path segment. '~' stays raw:
        # requests un-escapes %7E before sending, and the signature must cover the path sent.
        if prefix:
            prefix = urllib.parse.quote_plus(prefix, safe="*~", errors="replace")
        self.base_path = "/{}/{}".format(self.V1, prefix).rstrip("/")
        # Semantic view requests preserve encoded dot segments in the HTTP client.
        self._semantic_view_base_path = "/{}/{}".format(self.V1, self._escape_dot_segment(prefix)).rstrip("/")

    @classmethod
    def for_catalog_properties(
            cls, options: Options) -> "ResourcePaths":
        prefix = options.get(CatalogOptions.PREFIX, "")
        return cls(prefix)

    @staticmethod
    def config() -> str:
        return "/{}/config".format(ResourcePaths.V1)

    def databases(self) -> str:
        return "{}/{}".format(self.base_path, self.DATABASES)

    def database(self, name: str) -> str:
        return "{}/{}/{}".format(self.base_path, self.DATABASES, RESTUtil.encode_string(name))

    def tables(self, database_name: Optional[str] = None) -> str:
        if database_name:
            return "{}/{}/{}/{}".format(self.base_path, self.DATABASES,
                                        RESTUtil.encode_string(database_name), self.TABLES)
        return "{}/{}".format(self.base_path, self.TABLES)

    def table(self, database_name: str, table_name: str) -> str:
        return ("{}/{}/{}/{}/{}".format(self.base_path, self.DATABASES, RESTUtil.encode_string(database_name),
                self.TABLES, RESTUtil.encode_string(table_name)))

    def table_by_id(self, table_id: str) -> str:
        # Keep '~' raw, as in table_details.
        table_id = urllib.parse.quote_plus(table_id, safe="*~", errors="replace")
        return "{}/{}/{}/{}".format(self.base_path, self.TABLES, self.ID, table_id)

    def table_details(self, database_name: str) -> str:
        database_name = urllib.parse.quote_plus(database_name, safe="*~", errors="replace")
        return "{}/{}/{}/{}".format(
            self.base_path, self.DATABASES, database_name, self.TABLE_DETAILS)

    def table_token(self, database_name: str, table_name: str) -> str:
        return ("{}/{}/{}/{}/{}/token".format(self.base_path, self.DATABASES, RESTUtil.encode_string(database_name),
                self.TABLES, RESTUtil.encode_string(table_name)))

    def rename_table(self) -> str:
        return "{}/{}/rename".format(self.base_path, self.TABLES)

    def commit_table(self, database_name: str, table_name: str) -> str:
        return ("{}/{}/{}/{}/{}/commit".format(self.base_path, self.DATABASES, RESTUtil.encode_string(database_name),
                self.TABLES, RESTUtil.encode_string(table_name)))

    def rollback_table(self, database_name: str, table_name: str) -> str:
        return ("{}/{}/{}/{}/{}/rollback".format(self.base_path, self.DATABASES, RESTUtil.encode_string(database_name),
                                                 self.TABLES, RESTUtil.encode_string(table_name)))

    def table_snapshot(self, database_name: str, table_name: str) -> str:
        return ("{}/{}/{}/{}/{}/snapshot".format(self.base_path, self.DATABASES, RESTUtil.encode_string(database_name),
                                                 self.TABLES, RESTUtil.encode_string(table_name)))

    def partitions(self, database_name: str, table_name: str) -> str:
        return ("{}/{}/{}/{}/{}/{}".format(self.base_path, self.DATABASES, RESTUtil.encode_string(database_name),
                                           self.TABLES, RESTUtil.encode_string(table_name), self.PARTITIONS))

    def functions(self, database_name: Optional[str] = None) -> str:
        if database_name:
            return "{}/{}/{}/{}".format(self.base_path, self.DATABASES,
                                        RESTUtil.encode_string(database_name), self.FUNCTIONS)
        return "{}/{}".format(self.base_path, self.FUNCTIONS)

    def function_details(self, database_name: str) -> str:
        return "{}/{}/{}/{}".format(self.base_path, self.DATABASES,
                                    RESTUtil.encode_string(database_name), self.FUNCTION_DETAILS)

    def function(self, database_name: str, function_name: str) -> str:
        return "{}/{}/{}/{}/{}".format(self.base_path, self.DATABASES, RESTUtil.encode_string(database_name),
                                       self.FUNCTIONS, RESTUtil.encode_string(function_name))

    def models(self, database_name: str) -> str:
        return "{}/{}/{}/{}".format(
            self.base_path,
            self.DATABASES,
            RESTUtil.encode_string(database_name),
            self.MODELS,
        )

    def model(self, database_name: str, model_name: str) -> str:
        return "{}/{}".format(
            self.models(database_name), RESTUtil.encode_string(model_name)
        )

    @staticmethod
    def _escape_dot_segment(segment: str) -> str:
        return segment.replace(".", "%2E") if segment in (".", "..") else segment

    def _semantic_view_segment(self, name: str, label: str) -> str:
        if not isinstance(name, str) or is_blank(name):
            raise ValueError("{} cannot be blank".format(label))
        # Requests leaves '~' unescaped on the wire, so sign it in that form.
        return self._escape_dot_segment(RESTUtil.encode_string(name).replace("%7E", "~"))

    def semantic_views(self, database_name: str) -> str:
        database = self._semantic_view_segment(database_name, "Database name")
        return "{}/{}/{}/{}".format(self._semantic_view_base_path, self.DATABASES, database, self.SEMANTIC_VIEWS)

    def semantic_view(self, database_name: str, view_name: str) -> str:
        view = self._semantic_view_segment(view_name, "Semantic view name")
        return "{}/{}".format(self.semantic_views(database_name), view)

    def predictions(self) -> str:
        return "{}/{}".format(self.base_path, self.PREDICTIONS)

    def prediction(self, prediction_id: str) -> str:
        if not isinstance(prediction_id, str) or not prediction_id.strip():
            raise ValueError("prediction_id must be a non-empty string")
        if prediction_id in (".", ".."):
            raise ValueError("prediction_id must not be a dot segment")
        return "{}/{}".format(
            self.predictions(), RESTUtil.encode_string(prediction_id))

    def cancel_prediction(self, prediction_id: str) -> str:
        return "{}/cancel".format(self.prediction(prediction_id))

    def tags(self, database_name: str, table_name: str) -> str:
        return "{}/{}/{}/{}/{}/{}".format(
            self.base_path, self.DATABASES, RESTUtil.encode_string(database_name),
            self.TABLES, RESTUtil.encode_string(table_name), self.TAGS)

    def tag(self, database_name: str, table_name: str, tag_name: str) -> str:
        return "{}/{}/{}/{}/{}/{}/{}".format(
            self.base_path, self.DATABASES, RESTUtil.encode_string(database_name),
            self.TABLES, RESTUtil.encode_string(table_name), self.TAGS,
            RESTUtil.encode_string(tag_name))

    def branches(self, database_name: str, table_name: str) -> str:
        return "{}/{}/{}/{}/{}/{}".format(
            self.base_path, self.DATABASES, RESTUtil.encode_string(database_name),
            self.TABLES, RESTUtil.encode_string(table_name), self.BRANCHES)

    def branch(self, database_name: str, table_name: str, branch_name: str) -> str:
        return "{}/{}/{}/{}/{}/{}/{}".format(
            self.base_path, self.DATABASES, RESTUtil.encode_string(database_name),
            self.TABLES, RESTUtil.encode_string(table_name), self.BRANCHES,
            RESTUtil.encode_string(branch_name))

    def forward_branch(self, database_name: str, table_name: str, branch_name: str) -> str:
        return "{}/{}".format(
            self.branch(database_name, table_name, branch_name), self.FORWARD)

    def auth_table(self, database_name: str, table_name: str) -> str:
        return "{}/{}/{}/{}/{}/auth".format(
            self.base_path, self.DATABASES, RESTUtil.encode_string(database_name),
            self.TABLES, RESTUtil.encode_string(table_name)
        )

    def permissions(self) -> str:
        return "{}/{}".format(self.base_path, self.PERMISSIONS)

    def grant_permission(self) -> str:
        return "{}/grant".format(self.permissions())

    def revoke_permission(self) -> str:
        return "{}/revoke".format(self.permissions())

    def policies(self, resource) -> str:
        resource.validate_policy_attachment()
        return "{}/{}".format(self.table(resource.get_database(), resource.get_table()), self.POLICIES)

    def drop_policy(self, resource) -> str:
        return "{}/drop".format(self.policies(resource))
