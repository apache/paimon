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

import pytest

pypaimon = pytest.importorskip("pypaimon")
daft = pytest.importorskip("daft")

from pypaimon.daft import daft_io_config


def _oss_options():
    return {
        "warehouse": "oss://test-bucket/warehouse",
        "fs.oss.endpoint": "oss-cn-hangzhou.aliyuncs.com",
        "fs.oss.region": "cn-hangzhou",
        "fs.oss.accessKeyId": "AK",
        "fs.oss.accessKeySecret": "SK",
        "fs.oss.securityToken": "TOKEN",
    }


def test_oss_io_config_preserves_opendal_backend():
    cfg = daft_io_config._convert_paimon_catalog_options_to_io_config(_oss_options())

    oss_cfg = cfg.opendal_backends["oss"]
    assert oss_cfg["bucket"] == "test-bucket"
    assert oss_cfg["endpoint"] == "https://oss-cn-hangzhou.aliyuncs.com"
    assert oss_cfg["region"] == "cn-hangzhou"
    assert oss_cfg["access_key_id"] == "AK"
    assert oss_cfg["access_key_secret"] == "SK"
    assert oss_cfg["security_token"] == "TOKEN"


def test_oss_io_config_adds_jindo_when_daft_supports_it(monkeypatch):
    class _FakeJindoConfig:
        def __init__(
            self,
            endpoint=None,
            region=None,
            access_key_id=None,
            access_key_secret=None,
            security_token=None,
        ):
            self.endpoint = endpoint
            self.region = region
            self.access_key_id = access_key_id
            self.access_key_secret = access_key_secret
            self.security_token = security_token

    class _FakeIOConfig:
        def __init__(self, jindo=None, opendal_backends=None, **_):
            self.jindo = jindo
            self.opendal_backends = opendal_backends or {}

    monkeypatch.setattr(daft_io_config, "JindoConfig", _FakeJindoConfig)
    monkeypatch.setattr(daft_io_config, "IOConfig", _FakeIOConfig)

    cfg = daft_io_config._convert_paimon_catalog_options_to_io_config(_oss_options())

    assert cfg.opendal_backends["oss"]["endpoint"] == "https://oss-cn-hangzhou.aliyuncs.com"
    assert cfg.jindo.endpoint == "oss-cn-hangzhou.aliyuncs.com"
    assert cfg.jindo.region == "cn-hangzhou"
    assert cfg.jindo.access_key_id == "AK"
    assert cfg.jindo.access_key_secret == "SK"
    assert cfg.jindo.security_token == "TOKEN"


def test_oss_io_config_keeps_legacy_daft_io_config_shape(monkeypatch):
    class _FakeIOConfig:
        def __init__(self, **kwargs):
            assert "jindo" not in kwargs
            self.opendal_backends = kwargs["opendal_backends"]

    monkeypatch.setattr(daft_io_config, "JindoConfig", None)
    monkeypatch.setattr(daft_io_config, "IOConfig", _FakeIOConfig)

    cfg = daft_io_config._convert_paimon_catalog_options_to_io_config(_oss_options())

    assert cfg.opendal_backends["oss"]["access_key_id"] == "AK"
