---
title: "RESTCatalog"
weight: 4
type: docs
aliases:
- /concepts/rest-catalog.html
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

Paimon REST Catalog provides a flexible interface for accessing catalog services. It leverages a decoupled architecture by utilizing a RESTful API, allowing seamless integration between clients and the catalog server while enabling a wide range of backend implementations.

## Architecture:

{{< img src="/img/rest-catalog.png">}}

## Key Features

1. Centralized Technology-specific Logic Implementation
   All technology-specific logic within the catalog server. This ensures that these logic could be owned by the user.
2. Decoupled Architecture
   The REST Catalog interacts with the catalog server through a well-defined REST API. This decoupling allows for independent evolution and scaling of both the catalog server and clients.
3. Language Agnostic
   Developers can implement the catalog server in any programming language, provided that it adheres to the specified REST API. This flexibility enables teams to utilize their existing tech stacks and expertise for catalog server.
4. Support for Any Catalog Backend
   Paimon REST Catalog is designed to work with any catalog backend. As long as they implement the relevant APIs, they can seamlessly integrate with Paimon REST Catalog.

## Usage
- Bear token
```sql
CREATE CATALOG `paimon-rest-catalog`
WITH (
'type' = 'paimon',
'uri' = '<catalog server url>',
'metastore' = 'rest',
'token.provider' = 'bear'
'token' = '<token>'
);
```
- DLF ak
```sql
CREATE CATALOG `paimon-rest-catalog`
WITH (
'type' = 'paimon',
'uri' = '<catalog server url>',
'metastore' = 'rest',
'token.provider' = 'dlf',
'dlf.accessKeyId'='<accessKeyId>',
'dlf.accessKeySecret'='<accessKeySecret>',
);
```
- DLF token path
```sql
CREATE CATALOG `paimon-rest-catalog`
WITH (
'type' = 'paimon',
'uri' = '<catalog server url>',
'metastore' = 'rest',
'token.provider' = 'dlf',
'dlf.token-path' = '<token-path>'
);
```

## Conclusion

Paimon REST Catalog offers a powerful and adaptable solution for accessing catalog services. 
By technology-specific in the catalog server, decoupling through [REST API](https://github.com/apache/paimon/blob/master/paimon-open-api/rest-catalog-open-api.yaml), and supporting various implementations, it allows for diverse and efficient accessing catalog in Paimon.
Unlike other catalogs, the REST catalog's technology-specific logic is implemented on the server side.