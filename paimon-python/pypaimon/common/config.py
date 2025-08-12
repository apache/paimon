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

class OssOptions:
    OSS_ACCESS_KEY_ID = "fs.oss.accessKeyId"
    OSS_ACCESS_KEY_SECRET = "fs.oss.accessKeySecret"
    OSS_SECURITY_TOKEN = "fs.oss.securityToken"
    OSS_ENDPOINT = "fs.oss.endpoint"
    OSS_REGION = "fs.oss.region"


class S3Options:
    S3_ACCESS_KEY_ID = "fs.s3.accessKeyId"
    S3_ACCESS_KEY_SECRET = "fs.s3.accessKeySecret"
    S3_SECURITY_TOKEN = "fs.s3.securityToken"
    S3_ENDPOINT = "fs.s3.endpoint"
    S3_REGION = "fs.s3.region"


class CatalogOptions:
    URI = "uri"
    METASTORE = "metastore"
    WAREHOUSE = "warehouse"
    TOKEN_PROVIDER = "token.provider"
    TOKEN = "token"
    DATA_TOKEN_ENABLED = "data-token.enabled"
    DLF_REGION = "dlf.region"
    DLF_ACCESS_KEY_ID = "dlf.access-key-id"
    DLF_ACCESS_KEY_SECRET = "dlf.access-key-secret"
    DLF_ACCESS_SECURITY_TOKEN = "dlf.security-token"
    DLF_TOKEN_LOADER = "dlf.token-loader"
    DLF_TOKEN_ECS_ROLE_NAME = "dlf.token-ecs-role-name"
    DLF_TOKEN_ECS_METADATA_URL = "dlf.token-ecs-metadata-url"
    PREFIX = 'prefix'
    HTTP_USER_AGENT_HEADER = 'header.HTTP_USER_AGENT'


class PVFSOptions:
    CACHE_ENABLED = "cache-enabled"
    TABLE_CACHE_TTL = "cache.expire-after-write"
    DEFAULT_TABLE_CACHE_TTL = 1800
    DEFAULT_CACHE_SIZE = 2**31 - 1
