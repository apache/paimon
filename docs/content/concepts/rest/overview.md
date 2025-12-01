---
title: "Overview"
weight: 1
type: docs
aliases:
- /concepts/rest/overview.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# RESTCatalog

## Overview

Paimon REST Catalog provides a lightweight implementation to access the catalog service. Paimon could access the
catalog service through a catalog server which implements REST API. You can see all APIs in [REST API]({{< ref "concepts/rest/rest-api" >}}).

{{< img src="/img/rest-catalog.svg">}}

## Key Features

1. User Defined Technology-Specific Logic Implementation
    - All technology-specific logic within the catalog server.
    - This ensures that the user can define logic that could be owned by the user.
2. Decoupled Architecture
    - The REST Catalog interacts with the catalog server through a well-defined REST API.
    - This decoupling allows for independent evolution and scaling of the catalog server and clients.
3. Language Agnostic
    - Developers can implement the catalog server in any programming language, provided that it adheres to the specified REST API.
    - This flexibility enables teams to utilize their existing tech stacks and expertise.
4. Support for Any Catalog Backend
    - REST Catalog is designed to work with any catalog backend.
    - As long as they implement the relevant APIs, they can seamlessly integrate with REST Catalog.

## Conclusion

REST Catalog offers adaptable solution for accessing the catalog service. According to [REST API]({{< ref "concepts/rest/rest-api" >}}) is decoupled
from the catalog service.

Technology-specific Logic is encapsulated on the catalog server. At the same time, the catalog server supports any
backend and languages.

## Token Provider

RESTCatalog supports multiple access authentication methods, including the following:

1. [Bear Token]({{< ref "concepts/rest/bear" >}}).
2. [DLF Token]({{< ref "concepts/rest/dlf" >}}).

## REST Open API

See [REST API]({{< ref "concepts/rest/rest-api" >}}).

## REST Java API

See [REST Java API]({{< ref "program-api/rest-api" >}}).

## REST Python API

See [REST Python API]({{< ref "program-api/python-api" >}}).