#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
import re
from abc import ABC
from dataclasses import dataclass
from typing import Dict, Any, Optional, Tuple

import fsspec

from pypaimon.api import RESTApi, RESTCatalogOptions

PROTOCOL_NAME = "pvfs"


class PVFSIdentifier(ABC):
    name: str


@dataclass
class PVFSCatalogIdentifier(PVFSIdentifier):
    name: str


@dataclass
class PVFSDatabaseIdentifier(PVFSIdentifier):
    catalog: str
    name: str


@dataclass
class PVFSTableIdentifier(PVFSIdentifier):
    catalog: str
    database: str
    name: str


class PaimonVirtualFileSystem(fsspec.AbstractFileSystem):
    rest_api: RESTApi
    options: Dict[str, Any]

    _CATALOG_PATTERN = re.compile(r'^pvfs://([^/]+)/?$')
    _DATABASE_PATTERN = re.compile(r'^pvfs://([^/]+)//([^/]+)/?$')
    _TABLE_PATTERN = re.compile(r'^pvfs://([^/]+)//([^/]+)//([^/]+)/?$')

    def __init__(self, options: Dict = None, **kwargs):
        self.rest_api = RESTApi(options)
        self.options = options
        super().__init__(**kwargs)

    @property
    def fsid(self):
        return PROTOCOL_NAME

    def sign(self, path, expiration=None, **kwargs):
        """We do not support to create a signed URL representing the given path in gvfs."""
        raise Exception(
            "Sign is not implemented for Gravitino Virtual FileSystem."
        )

    @staticmethod
    def extract_pvfs_identifier(path: str) -> Optional['PVFSIdentifier']:
        if not isinstance(path, str) or not path.startswith('pvfs://'):
            return None

        match = PaimonVirtualFileSystem._TABLE_PATTERN.match(path)
        if match:
            catalog, database, table = match.groups()
            return PVFSTableIdentifier(catalog=catalog, database=database, table=table)

        match = PaimonVirtualFileSystem._DATABASE_PATTERN.match(path)
        if match:
            catalog, database = match.groups()
            return PVFSDatabaseIdentifier(catalog=catalog, database=database)

        match = PaimonVirtualFileSystem._CATALOG_PATTERN.match(path)
        if match:
            catalog = match.group(1)
            return PVFSCatalogIdentifier(catalog)
        return None
