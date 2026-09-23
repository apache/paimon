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
    """Serialize an IOConfig to the bytes Daft's File struct embeds, via its __reduce__."""
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


def _convert_paimon_catalog_options_to_file_io_config(
    catalog_options: dict[str, str], require_credentials: bool = True
) -> IOConfig | None:
    """IOConfig for blob File ops. OSS routes through Daft's S3 client (oss:// aliased to s3,
    virtual-hosted) because File.open() over OpenDAL/OSS is broken on some Daft builds.
    require_credentials=False keeps a config without a key pair so the env chain is used."""
    warehouse = catalog_options.get("warehouse", "")
    if (urlparse(warehouse).scheme if warehouse else "") != "oss":
        if catalog_options.get("fs.s3.accessKeyId") and catalog_options.get("fs.s3.accessKeySecret"):
            return _convert_paimon_catalog_options_to_io_config(catalog_options)
        # No complete pair: None when required (explicit wins), else endpoint/region-only for env.
        if require_credentials:
            return None
        endpoint = catalog_options.get("fs.s3.endpoint")
        region = catalog_options.get("fs.s3.region")
        if not endpoint and not region:
            return None
        return IOConfig(s3=S3Config(endpoint_url=endpoint, region_name=region))

    endpoint = catalog_options.get("fs.oss.endpoint")
    if endpoint and not endpoint.startswith(("http://", "https://")):
        endpoint = f"https://{endpoint}"
    key_id = catalog_options.get("fs.oss.accessKeyId")
    access_key = catalog_options.get("fs.oss.accessKeySecret")
    token = catalog_options.get("fs.oss.securityToken")
    if not (key_id and access_key):
        # No complete pair: None when required (explicit wins), else alias only for the env chain.
        if require_credentials:
            return None
        key_id = access_key = token = None
    return IOConfig(
        s3=S3Config(
            endpoint_url=endpoint,
            region_name=catalog_options.get("fs.oss.region"),
            key_id=key_id,
            access_key=access_key,
            session_token=token,
            force_virtual_addressing=True,
        ),
        protocol_aliases={"oss": "s3"},
    )


def _with_oss_alias(io_config: IOConfig) -> IOConfig:
    """Add the oss->s3 alias + virtual-hosted to a caller's io_config, deriving creds from an
    OpenDAL OSS backend if the S3 config has none, so File.open() works on oss:// blobs."""
    s3 = io_config.s3
    if s3 is None or s3.key_id is None:
        oss = (io_config.opendal_backends or {}).get("oss") or {}
        if oss:
            s3 = S3Config(
                endpoint_url=oss.get("endpoint"), region_name=oss.get("region"),
                key_id=oss.get("access_key_id"), access_key=oss.get("access_key_secret"),
                session_token=oss.get("security_token"),
            )
    s3 = (s3 or S3Config()).replace(force_virtual_addressing=True)
    return io_config.replace(s3=s3, protocol_aliases={**dict(io_config.protocol_aliases or {}), "oss": "s3"})
