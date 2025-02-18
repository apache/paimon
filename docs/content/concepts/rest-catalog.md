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

Paimon REST Catalog provides a lightweight implementation to access the catalog service. Paimon could access the catalog server, which implements REST API.

{{< img src="/img/rest-catalog.png">}}

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
   - Paimon REST Catalog is designed to work with any catalog backend. 
   - As long as they implement the relevant APIs, they can seamlessly integrate with Paimon REST Catalog.

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

Paimon REST Catalog offers adaptable solution for accessing the catalog service. According to [REST API](https://github.com/apache/paimon/blob/master/paimon-open-api/rest-catalog-open-api.yaml) is decoupled from the catalog service.
Technology-specific Logic is encapsulated on the catalog server. At the same time, the catalog server supports any backend and languages.