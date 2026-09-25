---
title: "Bearer Token"
sidebar_position: 2
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

<a id="bear-token"></a>

# Bearer Token

The bearer-token provider sends the configured token in the HTTP request header:

```http
Authorization: Bearer <token>
```

Obtain a token from your catalog service. The service defines how tokens are issued and validated;
the Paimon client supplies the token with each request.

## Flink SQL Example

```sql
CREATE CATALOG `paimon-rest-catalog` WITH (
    'type' = 'paimon',
    'uri' = '<catalog server url>',
    'metastore' = 'rest',
    'warehouse' = 'my_instance_name',
    'token.provider' = 'bear',
    'token' = '<token>'
);
```

The provider identifier is `bear`, even though the HTTP authentication scheme is called **Bearer**.
For Alibaba Cloud DLF authentication, see [DLF Token](./dlf).
