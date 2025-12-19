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
from pypaimon.common.options.config_options import ConfigOptions


class OssOptions:
    OSS_ACCESS_KEY_ID = ConfigOptions.key("fs.oss.accessKeyId").string_type().no_default_value().with_description(
        "OSS access key ID")
    OSS_ACCESS_KEY_SECRET = ConfigOptions.key(
        "fs.oss.accessKeySecret").string_type().no_default_value().with_description("OSS access key secret")
    OSS_SECURITY_TOKEN = ConfigOptions.key("fs.oss.securityToken").string_type().no_default_value().with_description(
        "OSS security token")
    OSS_ENDPOINT = ConfigOptions.key("fs.oss.endpoint").string_type().no_default_value().with_description(
        "OSS endpoint")
    OSS_REGION = ConfigOptions.key("fs.oss.region").string_type().no_default_value().with_description("OSS region")


class S3Options:
    S3_ACCESS_KEY_ID = ConfigOptions.key("fs.s3.accessKeyId").string_type().no_default_value().with_description(
        "S3 access key ID")
    S3_ACCESS_KEY_SECRET = ConfigOptions.key("fs.s3.accessKeySecret").string_type().no_default_value().with_description(
        "S3 access key secret")
    S3_SECURITY_TOKEN = ConfigOptions.key("fs.s3.securityToken").string_type().no_default_value().with_description(
        "S3 security token")
    S3_ENDPOINT = ConfigOptions.key("fs.s3.endpoint").string_type().no_default_value().with_description("S3 endpoint")
    S3_REGION = ConfigOptions.key("fs.s3.region").string_type().no_default_value().with_description("S3 region")


class PVFSOptions:
    CACHE_ENABLED = ConfigOptions.key("cache-enabled").boolean_type().default_value("true").with_description(
        "Enable cache")
    TABLE_CACHE_TTL = ConfigOptions.key("cache.expire-after-write").int_type().default_value(1800).with_description(
        "Table cache TTL")
    DEFAULT_TABLE_CACHE_TTL = 1800
    DEFAULT_CACHE_SIZE = 2 ** 31 - 1


class CatalogOptions:
    URI = ConfigOptions.key("uri").string_type().no_default_value().with_description("Catalog URI")
    METASTORE = ConfigOptions.key("metastore").string_type().default_value("filesystem").with_description(
        "Metastore type")
    WAREHOUSE = ConfigOptions.key("warehouse").string_type().no_default_value().with_description("Warehouse path")
    TOKEN_PROVIDER = ConfigOptions.key("token.provider").string_type().no_default_value().with_description(
        "Token provider")
    TOKEN = ConfigOptions.key("token").string_type().no_default_value().with_description("Authentication token")
    DATA_TOKEN_ENABLED = ConfigOptions.key("data-token.enabled").boolean_type().default_value(False).with_description(
        "Enable data token")
    DLF_REGION = ConfigOptions.key("dlf.region").string_type().no_default_value().with_description("DLF region")
    DLF_ACCESS_KEY_ID = ConfigOptions.key("dlf.access-key-id").string_type().no_default_value().with_description(
        "DLF access key ID")
    DLF_ACCESS_KEY_SECRET = ConfigOptions.key(
        "dlf.access-key-secret").string_type().no_default_value().with_description("DLF access key secret")
    DLF_ACCESS_SECURITY_TOKEN = ConfigOptions.key(
        "dlf.security-token").string_type().no_default_value().with_description("DLF security token")
    DLF_OSS_ENDPOINT = ConfigOptions.key("dlf.oss-endpoint").string_type().no_default_value().with_description(
        "DLF OSS endpoint")
    DLF_TOKEN_LOADER = ConfigOptions.key("dlf.token-loader").string_type().no_default_value().with_description(
        "DLF token loader")
    DLF_TOKEN_ECS_ROLE_NAME = ConfigOptions.key(
        "dlf.token-ecs-role-name").string_type().no_default_value().with_description("DLF ECS role name")
    DLF_TOKEN_ECS_METADATA_URL = ConfigOptions.key(
        "dlf.token-ecs-metadata-url").string_type().no_default_value().with_description("DLF ECS metadata URL")
    PREFIX = ConfigOptions.key("prefix").string_type().no_default_value().with_description("Prefix")
    HTTP_USER_AGENT_HEADER = ConfigOptions.key(
        "header.HTTP_USER_AGENT").string_type().no_default_value().with_description("HTTP User Agent header")
    BLOB_FILE_IO_DEFAULT_CACHE_SIZE = 2 ** 31 - 1
