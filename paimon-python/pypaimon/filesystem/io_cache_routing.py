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

"""Chooses the endpoint of each FileIO request from the io-cache.* keys of a table token."""

import os
import re
from enum import Enum
from typing import Dict, FrozenSet, List, Mapping, NamedTuple, Optional, Tuple

from pypaimon.common.options.config import CatalogOptions, OssOptions
from pypaimon.utils.file_type import FileType

FS_OSS_ENDPOINT = OssOptions.OSS_ENDPOINT.key()
FS_OSS_REGION = OssOptions.OSS_REGION.key()
FS_OSS_PATH_STYLE = OssOptions.OSS_SECOND_LEVEL_DOMAIN_ENABLE.key()
FS_OSS_HTTPS_ENABLE = "fs.oss.https.enable"
DLF_OSS_ENDPOINT = CatalogOptions.DLF_OSS_ENDPOINT.key()
IO_CACHE_ENABLED = CatalogOptions.IO_CACHE_ENABLED.key()
IO_CACHE_TARGET_PREFIX = "io-cache.target."

_TARGET_NAME = re.compile(r"[a-z][a-z0-9-]*")
# Data files are named {prefix}{uuid}-{count}.{extension}; this matches what follows the prefix.
_DATA_FILE_SUFFIX = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}-[0-9]+\..+")
_MANIFEST_SIDECAR_SUFFIX = ".avro.sidecar"
# JindoSDK cache client keys; the origin option set must not talk to a cache.
_ORIGIN_DROPPED_PREFIXES = ("fs.oss.dlf-cache.", "fs.jindocache.")


class Op(Enum):
    """FileIO operations; only READ, META, EXISTS, WRITE and TWO_PHASE_WRITE can go to a target."""
    READ = "read"
    META = "meta"
    EXISTS = "exists"
    WRITE = "write"
    LIST = "list"
    DELETE = "delete"
    RENAME = "rename"
    MKDIRS = "mkdirs"
    COPY = "copy"
    ATOMIC_WRITE = "atomic-write"
    TWO_PHASE_WRITE = "two-phase-write"
    PRESIGN = "presign"


def _flag(value) -> bool:
    return value is not None and str(value).strip().lower() == "true"


def _parse_policy(value) -> FrozenSet[Op]:
    # read, meta and write can use a target; none turns it off
    tokens = {token.strip().lower() for token in str(value or "").split(",")}
    if "none" in tokens:
        return frozenset()
    return frozenset(op for op in (Op.READ, Op.META, Op.WRITE) if op.value in tokens)


def _parse_routes(value: str) -> List[Tuple[set, str]]:
    # "types=target;...": the first rule containing a type picks its target
    rules = []
    for rule in value.lower().split(";"):
        types, eq, target = rule.partition("=")
        types = FileType.parse_whitelist(types) if eq and target.strip() else set()
        if types:
            rules.append((types, target.strip()))
    return rules


def _declared_targets(options: Mapping[str, str]) -> Dict[str, Optional[str]]:
    # io-cache.targets with io-cache.target.<name>.endpoint, else io-cache.endpoint as "default"
    names = options.get("io-cache.targets")
    if names is None:
        endpoint = options.get("io-cache.endpoint")
        return {} if endpoint is None else {"default": endpoint}
    endpoints = {}
    for name in str(names).lower().split(","):
        name = name.strip()
        if _TARGET_NAME.fullmatch(name) and name not in endpoints:
            endpoints[name] = options.get(IO_CACHE_TARGET_PREFIX + name + ".endpoint")
    return endpoints


def data_prefixes(options: Mapping[str, str]) -> Tuple[str, ...]:
    """Data file name prefixes: data- and changelog-, plus the table's own prefixes."""
    prefixes = ["data-", "changelog-"]
    for key in ("data-file.prefix", "changelog-file.prefix"):
        value = options.get(key)
        if value and value.strip() and value not in prefixes:
            prefixes.append(value)
    return tuple(prefixes)


def routable_type(path: str, prefixes: Tuple[str, ...]) -> Optional[FileType]:
    """The type a cache may serve, or None when the file must be read from the origin."""
    path = path.rstrip("/")
    name = os.path.basename(path)
    if _is_temporary(name) or FileType.is_mutable(path):
        return None
    if _is_data_file_name(name, prefixes):
        return FileType.FILE_INDEX if name.endswith(".index") else FileType.DATA
    # Sequential metadata may be read before it exists; cached NotFound would hide new commits.
    parent = os.path.basename(os.path.dirname(path))
    if name.startswith(("snapshot-", "schema-")) or (
            name.startswith("changelog-") and parent == "changelog"):
        return None
    file_type = FileType.classify(path)
    return None if file_type == FileType.DATA else file_type


def _is_data_file_name(name: str, prefixes: Tuple[str, ...]) -> bool:
    """Manifests, indexes and statistics share the uuid-count shape but have no extension."""
    if name.endswith(_MANIFEST_SIDECAR_SUFFIX):
        return False
    return any(name.startswith(p) and _DATA_FILE_SUFFIX.fullmatch(name[len(p):]) for p in prefixes)


def _is_temporary(name: str) -> bool:
    return name.endswith(".tmp") or ".tmp-" in name or ".tmp." in name


def _with_endpoint(options: Dict[str, str], endpoint: Optional[str]) -> Dict[str, str]:
    if not endpoint:
        return options
    # JindoSDK drops the scheme of a URL endpoint, so carry it in fs.oss.https.enable.
    scheme, separator, host = endpoint.partition("://")
    if separator:
        scheme = scheme.lower()
        options[FS_OSS_HTTPS_ENABLE] = "true" if scheme == "https" else "false"
        endpoint = scheme + separator + host
    options[FS_OSS_ENDPOINT] = endpoint.rstrip("/")
    return options


class CacheTarget(NamedTuple):
    """A named cache target with its endpoint and addressing settings."""
    name: str
    endpoint: str
    path_style_access: bool
    region: Optional[str]


class IoCacheRouting:
    """Per-request choice between the io-cache targets and the origin endpoint."""

    def __init__(self, options: Mapping[str, str], policy: FrozenSet[Op],
                 endpoints: Dict[str, Optional[str]]):
        origin = options.get("io-cache.origin.endpoint")
        self._origin = origin if origin and origin.strip() else options.get(FS_OSS_ENDPOINT)
        self._policy = policy
        self._targets = {
            name: CacheTarget(name, endpoint.strip(),
                              _flag(options.get(IO_CACHE_TARGET_PREFIX + name + ".path-style-access")),
                              options.get(IO_CACHE_TARGET_PREFIX + name + ".region"))
            for name, endpoint in endpoints.items() if endpoint and endpoint.strip()}
        whitelist = options.get("io-cache.whitelist")
        self._whitelist = set(FileType) if whitelist is None else FileType.parse_whitelist(
            str(whitelist).lower())
        routes = options.get("io-cache.routes")
        self._routes = None if routes is None else _parse_routes(str(routes))
        self._prefixes = data_prefixes(options)

    @staticmethod
    def create(options: Mapping[str, str]) -> Optional['IoCacheRouting']:
        """The routing of the options, or None when requests keep using fs.oss.endpoint."""
        if str(options.get(DLF_OSS_ENDPOINT) or "").strip() or not _flag(options.get(IO_CACHE_ENABLED)):
            return None
        policy = _parse_policy(options.get("io-cache.policy"))
        endpoints = _declared_targets(options)
        return IoCacheRouting(options, policy, endpoints) if policy and endpoints else None

    def targets(self) -> Dict[str, str]:
        """Endpoints of the targets that have one, by name, in declaration order."""
        return {name: target.endpoint for name, target in self._targets.items()}

    def origin_endpoint(self) -> Optional[str]:
        return self._origin

    def route(self, op: Op, path: str) -> Optional[str]:
        """Name of the target of one request, or None when it goes to origin."""
        # An existence check is a file status lookup, a two-phase write is a write.
        if op is Op.EXISTS:
            op = Op.META
        elif op is Op.TWO_PHASE_WRITE:
            op = Op.WRITE
        # Only OSS paths can use cache targets.
        if op not in self._policy or not path.startswith("oss://"):
            return None
        file_type = routable_type(path, self._prefixes)
        if file_type is None or file_type not in self._whitelist:
            return None
        if self._routes is None:
            return next(iter(self._targets), None)
        name = next((target for types, target in self._routes if file_type in types), None)
        return name if name in self._targets else None

    def origin_options(self, options: Mapping[str, str]) -> Dict[str, str]:
        """Options of the origin FileIO: the origin endpoint, without cache client keys."""
        result = {key: value for key, value in options.items()
                  if not str(key).startswith(_ORIGIN_DROPPED_PREFIXES)}
        return _with_endpoint(result, self._origin)

    def target_options(self, options: Mapping[str, str], name: str) -> Dict[str, str]:
        """Options of the FileIO of one target: its endpoint, addressing and region."""
        target = self._targets[name]
        endpoint = target.endpoint
        # A target endpoint without a scheme means https.
        if "://" not in endpoint:
            endpoint = "https://" + endpoint
        result = _with_endpoint(dict(options), endpoint)
        result[FS_OSS_PATH_STYLE] = "true" if target.path_style_access else "false"
        if target.region and target.region.strip():
            result[FS_OSS_REGION] = target.region
        return result
