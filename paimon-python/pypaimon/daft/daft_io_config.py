################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

from __future__ import annotations

from urllib.parse import urlparse

from daft.io import IOConfig, S3Config


def serialize_io_config(io_config: IOConfig) -> bytes:
    """Serialize an IOConfig to the bytes layout Daft's File physical struct expects.

    Daft pickles IOConfig via ``__reduce__`` -> ``(IOConfig._from_serialized, (bytes,))``;
    those bytes are exactly what the File struct's ``io_config`` field is deserialized from.
    Validate that shape so a future Daft serialization change fails loudly, not silently.
    """
    reducer = io_config.__reduce__()
    if (reducer[0] is not IOConfig._from_serialized
            or not isinstance(reducer[1], tuple) or not reducer[1]
            or not isinstance(reducer[1][0], bytes)):
        raise TypeError(
            f"Unexpected IOConfig.__reduce__ shape ({reducer!r}); "
            "Daft may have changed its IOConfig serialization format."
        )
    return reducer[1][0]


def _convert_paimon_catalog_options_to_io_config(catalog_options: dict[str, str]) -> IOConfig | None:
    """Convert pypaimon catalog options to Daft IOConfig.

    pypaimon supports only S3-like (s3://, s3a://, s3n://, oss://), HDFS, and local (file://).

    OSS uses Daft's OpenDAL backend (opendal_backends={"oss": {...}}) rather than S3Config,
    because Daft routes oss:// URIs to OpenDAL's OSS operator, not the S3-compatible path.
    """
    warehouse = catalog_options.get("warehouse", "")
    scheme = urlparse(warehouse).scheme if warehouse else ""

    if scheme == "oss":
        parsed = urlparse(warehouse)
        oss_cfg: dict[str, str] = {}

        bucket = parsed.netloc
        if bucket:
            oss_cfg["bucket"] = bucket

        endpoint = catalog_options.get("fs.oss.endpoint")
        if endpoint:
            if not endpoint.startswith(("http://", "https://")):
                endpoint = f"https://{endpoint}"
            oss_cfg["endpoint"] = endpoint

        key_id = catalog_options.get("fs.oss.accessKeyId")
        if key_id:
            oss_cfg["access_key_id"] = key_id

        key_secret = catalog_options.get("fs.oss.accessKeySecret")
        if key_secret:
            oss_cfg["access_key_secret"] = key_secret

        region = catalog_options.get("fs.oss.region")
        if region:
            oss_cfg["region"] = region

        token = catalog_options.get("fs.oss.securityToken")
        if token:
            oss_cfg["security_token"] = token

        return IOConfig(opendal_backends={"oss": oss_cfg}) if oss_cfg else None

    # S3-compatible (s3://, s3a://, s3n://)
    any_props_set = False

    def get(key: str) -> str | None:
        nonlocal any_props_set
        val = catalog_options.get(key)
        if val is not None:
            any_props_set = True
        return val

    io_config = IOConfig(
        s3=S3Config(
            endpoint_url=get("fs.s3.endpoint"),
            region_name=get("fs.s3.region"),
            key_id=get("fs.s3.accessKeyId"),
            access_key=get("fs.s3.accessKeySecret"),
            session_token=get("fs.s3.securityToken"),
        ),
    )

    return io_config if any_props_set else None


def _convert_paimon_catalog_options_to_file_io_config(catalog_options: dict[str, str]) -> IOConfig | None:
    """IOConfig for Daft native File ops (open/read/as_image) on blob columns.

    Same as _convert_paimon_catalog_options_to_io_config, except OSS is routed through
    Daft's S3 client (oss:// aliased to s3, virtual-hosted) instead of the OpenDAL OSS
    backend: Daft's File.open() stat over OpenDAL/OSS fails to issue the request on some
    Daft builds, while the S3 client works (OSS is S3-API compatible). Other schemes
    (s3://, local) reuse the shared builder unchanged.
    """
    warehouse = catalog_options.get("warehouse", "")
    if (urlparse(warehouse).scheme if warehouse else "") != "oss":
        return _convert_paimon_catalog_options_to_io_config(catalog_options)

    endpoint = catalog_options.get("fs.oss.endpoint")
    if endpoint and not endpoint.startswith(("http://", "https://")):
        endpoint = f"https://{endpoint}"
    key_id = catalog_options.get("fs.oss.accessKeyId")
    if not endpoint and not key_id:
        return None
    return IOConfig(
        s3=S3Config(
            endpoint_url=endpoint,
            region_name=catalog_options.get("fs.oss.region"),
            key_id=key_id,
            access_key=catalog_options.get("fs.oss.accessKeySecret"),
            session_token=catalog_options.get("fs.oss.securityToken"),
            force_virtual_addressing=True,
        ),
        protocol_aliases={"oss": "s3"},
    )
