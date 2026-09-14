---
title: "DLF Token"
sidebar_position: 3
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

Use `token.provider = dlf` to authenticate a Paimon REST Catalog client with Alibaba Cloud DLF.
The client signs requests using an access key and, for temporary credentials, an STS security token.

In the examples below, `uri` is the catalog service endpoint and `warehouse` is the **server-side
catalog instance name**, not a storage path or the local Flink catalog alias.

## Choose a Credential Source

| Source | Required options | Refresh behavior |
| --- | --- | --- |
| [Access key](#use-the-access-key) | `dlf.access-key-id`, `dlf.access-key-secret` | Uses the configured credentials. |
| [Inline STS token](#use-the-sts-temporary-access-token) | Access key options and `dlf.security-token` | Does not refresh automatically. |
| [Local token file](#local-token-file) | `dlf.token-path` | Reloads credentials according to the file's `Expiration`. |
| [ECS instance role](#use-the-sts-token-from-aliyun-ecs-role) | `dlf.token-loader = ecs` | Loads and refreshes credentials through the ECS metadata service. |

Choose one source. If several are configured, the client uses an explicit `dlf.token-loader`
first, then `dlf.token-path`, then the inline access key options.

## Use the access key

```sql
CREATE CATALOG `paimon-rest-catalog` WITH (
    'type' = 'paimon',
    'metastore' = 'rest',
    'uri' = 'https://cn-hangzhou-vpc.dlf.aliyuncs.com',
    'warehouse' = 'my_instance_name',
    'token.provider' = 'dlf',
    'dlf.access-key-id' = '<access-key-id>',
    'dlf.access-key-secret' = '<access-key-secret>'
);
```

Replace the endpoint and instance name with those of your DLF catalog. See
[endpoint configuration](#dlf-endpoint-configuration) for signing and region settings.

## Use the STS temporary access token

An inline STS credential consists of an access key ID, access key secret, and security token:

```sql
CREATE CATALOG `paimon-rest-catalog` WITH (
    'type' = 'paimon',
    'metastore' = 'rest',
    'uri' = 'https://cn-hangzhou-vpc.dlf.aliyuncs.com',
    'warehouse' = 'my_instance_name',
    'token.provider' = 'dlf',
    'dlf.access-key-id' = '<temporary-access-key-id>',
    'dlf.access-key-secret' = '<temporary-access-key-secret>',
    'dlf.security-token' = '<security-token>'
);
```

The client does not renew inline credentials. For a long-running client that needs refreshed STS
credentials, use a local token file or an ECS instance role.

### Local Token File

Set `dlf.token-path` to a local UTF-8 JSON file accessible to each process that uses the catalog.
This automatically selects the `local_file` token loader.

```sql
CREATE CATALOG `paimon-rest-catalog` WITH (
    'type' = 'paimon',
    'metastore' = 'rest',
    'uri' = 'https://cn-hangzhou-vpc.dlf.aliyuncs.com',
    'warehouse' = 'my_instance_name',
    'token.provider' = 'dlf',
    'dlf.token-path' = '/path/to/dlf-token.json'
);
```

The JSON field names are case-sensitive. Set `Expiration` to the actual UTC expiry of the issued
credentials, in `yyyy-MM-dd'T'HH:mm:ss'Z'` format:

```json
{
  "AccessKeyId": "<temporary-access-key-id>",
  "AccessKeySecret": "<temporary-access-key-secret>",
  "SecurityToken": "<security-token>",
  "Expiration": "2026-09-10T12:00:00Z"
}
```

Your credential provider must keep this file up to date. When signing a request, Paimon loads the
file if no token is cached, or reloads it when the cached token has less than one hour remaining.
Without `Expiration`, the cached token is treated as non-expiring and file changes do not trigger
a reload.

## Use the STS token from aliyun ecs role

On an ECS instance with an instance RAM role, the `ecs` loader retrieves temporary credentials
from the instance metadata service:

```sql
CREATE CATALOG `paimon-rest-catalog` WITH (
    'type' = 'paimon',
    'metastore' = 'rest',
    'uri' = 'https://cn-hangzhou-vpc.dlf.aliyuncs.com',
    'warehouse' = 'my_instance_name',
    'token.provider' = 'dlf',
    'dlf.token-loader' = 'ecs'
);
```

The loader discovers the role name through the metadata service. To specify it explicitly, add
`'dlf.token-ecs-role-name' = 'my_ecs_role_name'` as another catalog option. Credentials are refreshed
on request when they are within one hour of expiry.

## DLF Endpoint Configuration

The client selects a request signer from the configured endpoint unless
`dlf.signing-algorithm` is set explicitly:

| Example URI | Selected signer |
| --- | --- |
| `https://cn-hangzhou-vpc.dlf.aliyuncs.com` | `default` |
| `https://dlfnext.cn-hangzhou.aliyuncs.com` | `openapi` |

URIs containing `dlfnext` select the OpenAPI signer; other URIs select the default signer. The
client also infers the region from the URI. Set `dlf.region` explicitly if the endpoint does not
contain a recognizable region, for example when using a custom hostname.
