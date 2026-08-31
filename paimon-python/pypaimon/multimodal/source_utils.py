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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Shared validation for external multimodal sources."""

import os
from typing import Mapping
from urllib.parse import urlparse


def _source_path_text(value):
    try:
        path = os.fspath(value)
    except TypeError as error:
        raise ValueError(
            "paths must contain only filesystem paths or URIs.") from error
    if isinstance(path, bytes):
        raise ValueError("paths must contain only filesystem paths or URIs.")
    return path


def _validated_source_options(source_options):
    if source_options is None:
        return {}
    if not isinstance(source_options, Mapping):
        raise ValueError("source_options must be a mapping.")
    return dict(source_options)


def _validate_source_kerberos(paths, source_options, source_name="HDF5"):
    source_principal = (
        source_options.get("security.kerberos.login.principal")
        or source_options.get("security.principal")
    )
    source_keytab = (
        source_options.get("security.kerberos.login.keytab")
        or source_options.get("security.keytab")
    )
    if not source_principal and not source_keytab:
        return
    if bool(source_principal) != bool(source_keytab):
        raise ValueError(
            "Source Kerberos principal and keytab must be both set or both "
            "unset.")
    if not any(
            urlparse(_source_path_text(path)).scheme.lower()
            in ("hdfs", "viewfs") for path in paths):
        return
    raise ValueError(
        "%s sources cannot use an explicit Kerberos keytab in a shared "
        "process because kinit overwrites process-global credentials. "
        "Run the load in a process-isolated worker with a pre-acquired "
        "ticket cache and omit the source principal and keytab options."
        % source_name)
