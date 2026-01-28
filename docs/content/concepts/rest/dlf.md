---
title: "DLF Token"
weight: 3
type: docs
aliases:
  - /concepts/rest/dlf.html
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

# DLF Token

DLF (Data Lake Formation) building is a fully-managed platform for unified metadata and data storage and management,
aiming to provide customers with functions such as metadata management, storage management, permission management,
storage analysis, and storage optimization.

DLF provides multiple authentication methods for different environments.

{{< hint info >}}
The `'warehouse'` is your catalog instance name on the server, not the path.
{{< /hint >}}

## Use the access key

```sql
CREATE CATALOG `paimon-rest-catalog`
WITH (
    'type' = 'paimon',
    'uri' = '<catalog server url>',
    'metastore' = 'rest',
    'warehouse' = 'my_instance_name',
    'token.provider' = 'dlf',
    'dlf.access-key-id'='<access-key-id>',
    'dlf.access-key-secret'='<access-key-secret>',
);
```

- `uri`: Access the URI of the DLF Rest Catalog Server.
- `warehouse`: DLF Catalog name
- `token.provider`: token provider
- `dlf.access-key-id`: The Access Key ID required to access the DLF service, usually referring to the AccessKey of your
  RAM user
- `dlf.access-key-secret`:The Access Key Secret required to access the DLF service

You can grant specific permissions to a RAM user and use the RAM user's access key for long-term access to your DLF
resources. Compared to using the Alibaba Cloud account access key, accessing DLF resources with a RAM user access key
is more secure.

## Use the STS temporary access token

Through the STS service, you can generate temporary access tokens for users, allowing them to access DLF resources
restricted by policies within the validity period.

```sql
CREATE CATALOG `paimon-rest-catalog`
WITH (
    'type' = 'paimon',
    'uri' = '<catalog server url>',
    'metastore' = 'rest',
    'warehouse' = 'my_instance_name',
    'token.provider' = 'dlf',
    'dlf.access-key-id'='<access-key-id>',
    'dlf.access-key-secret'='<access-key-secret>',
    'dlf.security-token'='<security-token>'
);
```

In some environments, temporary access token can be periodically refreshed by using a local file:

```sql
CREATE CATALOG `paimon-rest-catalog`
WITH (
    'type' = 'paimon',
    'uri' = '<catalog server url>',
    'metastore' = 'rest',
    'warehouse' = 'my_instance_name',
    'token.provider' = 'dlf',
    'dlf.token-path' = 'my_token_path_in_disk'
);
```

## Use the STS token from aliyun ecs role

An instance RAM role refers to a RAM role granted to an ECS instance. This RAM role is a standard service role
with the trusted entity being the cloud server. By using an instance RAM role, it is possible to obtain temporary
access token (STS Token) within the ECS instance without configuring an AccessKey.

```sql
CREATE CATALOG `paimon-rest-catalog`
WITH (
    'type' = 'paimon',
    'uri' = '<catalog server url>',
    'metastore' = 'rest',
    'warehouse' = 'my_instance_name',
    'token.provider' = 'dlf',
    'dlf.token-loader' = 'ecs'
    -- optional, loader can obtain it through ecs metadata service
    -- 'dlf.token-ecs-role-name' = 'my_ecs_role_name'
);
```

## Signing Algorithm Configuration

Paimon supports multiple signing algorithms for DLF authentication. You can configure the signing algorithm explicitly,
or let Paimon automatically select it based on the endpoint host.

### Automatic Selection (Recommended)

By default, Paimon automatically selects the appropriate signing algorithm based on the endpoint URI:

- **DLF endpoints** (e.g., `cn-hangzhou-vpc.dlf.aliyuncs.com`): Automatically uses `dlf-default`
  (backward compatible). Recommended for VPC environments with better performance.
- **OpenAPI endpoints** (e.g., `dlfnext.cn-hangzhou.aliyuncs.com`): Automatically uses
  `dlf-openapi` for DlfNext/2026-01-18 OpenAPI. Supports public network access through Alibaba Cloud API infrastructure
   for special scenarios.

```sql
CREATE CATALOG `paimon-rest-catalog`
WITH (
    'type' = 'paimon',
    'uri' = 'https://dlfnext.cn-hangzhou.aliyuncs.com',  -- Auto-detected as dlf-openapi
    'metastore' = 'rest',
    'warehouse' = 'my_instance_name',
    'token.provider' = 'dlf',
    'dlf.access-key-id'='<access-key-id>',
    'dlf.access-key-secret'='<access-key-secret>'
    -- 'dlf.signing-algorithm' is not set, will be auto-detected
);
```

### Explicit Configuration

You can explicitly specify the signing algorithm:

```sql
CREATE CATALOG `paimon-rest-catalog`
WITH (
    'type' = 'paimon',
    'uri' = '<catalog server url>',
    'metastore' = 'rest',
    'warehouse' = 'my_instance_name',
    'token.provider' = 'dlf',
    'dlf.access-key-id'='<access-key-id>',
    'dlf.access-key-secret'='<access-key-secret>',
    'dlf.signing-algorithm' = 'dlf-default'  -- or 'dlf-openapi'
);
```

**Available signing algorithms:**

- `dlf-default` (default): DLF4-HMAC-SHA256 signer for default VPC endpoint, backward compatible
  with existing DLF authentication
- `dlf-openapi`: ROA v2 style signer for DlfNext/2026-01-18 OpenAPI, implements HMAC-SHA1
  signature with ROA style canonicalization

**Note:** When `dlf.signing-algorithm` is explicitly configured, it takes precedence over automatic detection.
