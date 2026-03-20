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

"""Authentication module for pypaimon REST catalog."""

from pypaimon.api.auth.base import AuthProvider, RESTAuthFunction
from pypaimon.api.auth.bearer import BearTokenAuthProvider
from pypaimon.api.auth.dlf_provider import DLFAuthProvider
from pypaimon.api.auth.dlf_signer import (
    DLFDefaultSigner,
    DLFOpenApiSigner,
    DLFRequestSigner,
)
from pypaimon.api.auth.factory import AuthProviderFactory, DLFAuthProviderFactory

__all__ = [
    "AuthProvider",
    "RESTAuthFunction",
    "AuthProviderFactory",
    "DLFAuthProviderFactory",
    "BearTokenAuthProvider",
    "DLFRequestSigner",
    "DLFDefaultSigner",
    "DLFOpenApiSigner",
    "DLFAuthProvider",
]
