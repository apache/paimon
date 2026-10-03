# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Tests for the io-cache routing decision, including the shared routing cases."""

import json
import os
import unittest

import pytest

from pypaimon.filesystem.io_cache_routing import (IoCacheRouting, Op,
                                                  data_prefixes, routable_type)
from pypaimon.utils.file_type import FileType

_RESOURCES = os.path.join(os.path.dirname(__file__), "resources", "io_cache")

TYPE_NAMES = {
    FileType.META: "meta",
    FileType.DATA: "data",
    FileType.BUCKET_INDEX: "bucket-index",
    FileType.GLOBAL_INDEX: "global-index",
    FileType.FILE_INDEX: "file-index",
    None: None,
}


def load_vectors(name):
    with open(os.path.join(_RESOURCES, name), encoding="utf-8") as f:
        return json.load(f)


ROUTING = load_vectors("routing.json")
ROUTABLE_TYPES = load_vectors("routable-types.json")


def endpoint(options, op, path):
    """The endpoint of one request as a client sees it: override, old endpoint, origin or a target."""
    if options.get("dlf.oss-endpoint"):
        return options["dlf.oss-endpoint"]
    routing = IoCacheRouting.create(options)
    if routing is None:
        return options.get("fs.oss.endpoint")
    name = routing.route(op, path)
    return routing.origin_endpoint() if name is None else routing.targets()[name]


@pytest.mark.parametrize("case", ROUTING["cases"], ids=[c["name"] for c in ROUTING["cases"]])
def test_routing_vector(case):
    assert endpoint(case["options"], Op(case["op"]), case["path"]) == case["expect"]


@pytest.mark.parametrize("case", ROUTABLE_TYPES["cases"], ids=[c["path"] for c in ROUTABLE_TYPES["cases"]])
def test_routable_type_vector(case):
    assert TYPE_NAMES[routable_type(case["path"], data_prefixes(case["options"]))] == case["type"]


class IoCacheRoutingTest(unittest.TestCase):

    TABLE = "oss://bkt/db1.db/t1"
    DATA = TABLE + "/bucket-0/data-8b1f7c2e-3a4d-4e5f-9a0b-1c2d3e4f5a6b-0.parquet"
    MANIFEST = TABLE + "/manifest/manifest-8b1f7c2e-3a4d-4e5f-9a0b-1c2d3e4f5a6b-0"

    def _options(self, **overrides):
        options = {
            "fs.oss.endpoint": "http://legacy.example.com",
            "io-cache.enabled": "true",
            "io-cache.endpoint": "http://cache.example.com",
            "io-cache.origin.endpoint": "https://origin.example.com",
            "io-cache.policy": "meta,read",
        }
        options.update(overrides)
        return options

    def _multi(self, **overrides):
        options = self._options(**{
            "io-cache.targets": "accel,cluster",
            "io-cache.target.accel.endpoint": "http://accel.example.com",
            "io-cache.target.cluster.endpoint": "http://10.0.0.1:8080",
            "io-cache.routes": "meta=accel;data=cluster",
        })
        options.update(overrides)
        return options

    def test_create(self):
        self.assertIsNotNone(IoCacheRouting.create(self._options()))
        self.assertIsNotNone(IoCacheRouting.create(self._options(**{"io-cache.enabled": " TRUE "})))
        for overrides in ({"io-cache.enabled": "false"}, {"io-cache.enabled": "yes"},
                          {"io-cache.policy": "none"}, {"io-cache.policy": "read,none"},
                          {"io-cache.policy": "write,exists"}, {"io-cache.targets": ""},
                          {"io-cache.targets": "Bad_Name,1x"},
                          {"dlf.oss-endpoint": "oss-cn-hangzhou.aliyuncs.com"}):
            with self.subTest(overrides=overrides):
                self.assertIsNone(IoCacheRouting.create(self._options(**overrides)))
        options = self._options()
        del options["io-cache.enabled"]
        self.assertIsNone(IoCacheRouting.create(options))

    def test_policy_routes_only_read_and_meta(self):
        routing = IoCacheRouting.create(self._options(**{"io-cache.policy": " Meta , READ ,prefetch,write"}))
        for op in Op:
            with self.subTest(op=op):
                expected = "default" if op in (Op.READ, Op.META) else None
                self.assertEqual(expected, routing.route(op, self.DATA))
        read_only = IoCacheRouting.create(self._options(**{"io-cache.policy": "read"}))
        self.assertIsNone(read_only.route(Op.META, self.DATA))

    def test_whitelist(self):
        index = self.TABLE + "/index/btree-global-index-8b1f7c2e-3a4d-4e5f-9a0b-1c2d3e4f5a6b.index"
        routing = IoCacheRouting.create(self._options(**{"io-cache.whitelist": "Global-Index,DATA,video"}))
        self.assertEqual("default", routing.route(Op.READ, index))
        self.assertEqual("default", routing.route(Op.READ, self.DATA))
        self.assertIsNone(routing.route(Op.READ, self.MANIFEST))
        self.assertIsNone(IoCacheRouting.create(self._options(**{"io-cache.whitelist": ""})).route(
            Op.READ, self.DATA))

    def test_targets_and_routes(self):
        routing = IoCacheRouting.create(self._multi(**{
            "io-cache.targets": " Accel ,cluster,1x,accel,c_d,missing,",
            "io-cache.routes": " Meta = ACCEL ;; data= ; =x ; video=y ; data ; *=cluster",
        }))
        self.assertEqual({"accel": "http://accel.example.com", "cluster": "http://10.0.0.1:8080"},
                         routing.targets())
        self.assertEqual("accel", routing.route(Op.META, self.MANIFEST))
        self.assertEqual("cluster", routing.route(Op.READ, self.DATA))
        to_missing = IoCacheRouting.create(self._multi(**{"io-cache.routes": "data=missing"}))
        self.assertIsNone(to_missing.route(Op.READ, self.DATA))

    def test_first_target_with_an_endpoint_without_routes(self):
        options = self._multi()
        del options["io-cache.routes"]
        del options["io-cache.target.accel.endpoint"]
        routing = IoCacheRouting.create(options)
        self.assertEqual({"cluster": "http://10.0.0.1:8080"}, routing.targets())
        self.assertEqual("cluster", routing.route(Op.READ, self.MANIFEST))

    def test_target_without_endpoint_keeps_routing_on_origin(self):
        routing = IoCacheRouting.create(self._options(**{"io-cache.endpoint": ""}))
        self.assertEqual({}, routing.targets())
        self.assertIsNone(routing.route(Op.READ, self.DATA))
        self.assertEqual("https://origin.example.com", endpoint(
            self._options(**{"io-cache.endpoint": ""}), Op.READ, self.DATA))

    def test_origin_defaults_to_fs_oss_endpoint(self):
        options = self._options()
        del options["io-cache.origin.endpoint"]
        self.assertEqual("http://legacy.example.com", IoCacheRouting.create(options).origin_endpoint())

    def test_directory_with_trailing_slash_uses_its_name(self):
        routing = IoCacheRouting.create(self._options())
        self.assertIsNone(routing.route(Op.META, self.TABLE + "/tag/tag-1/"))
        self.assertEqual("default", routing.route(Op.META, self.MANIFEST + "/"))
        self.assertIsNone(routing.route(Op.META, self.TABLE + "/snapshot/snapshot-1/"))

    def test_table_prefixes_add_data_prefixes(self):
        path = self.TABLE + "/bucket-0/part-8b1f7c2e-3a4d-4e5f-9a0b-1c2d3e4f5a6b-0.parquet"
        self.assertIsNone(IoCacheRouting.create(self._options()).route(Op.READ, path))
        for key in ("data-file.prefix", "changelog-file.prefix"):
            routing = IoCacheRouting.create(self._options(**{key: "part-"}))
            self.assertEqual("default", routing.route(Op.READ, path))

    def test_origin_options(self):
        options = self._options(**{
            "fs.oss.dlf-cache.consistent-hash.enabled": "true",
            "fs.jindocache.namespace.rpc.address": "10.0.0.1:8101",
            "fs.oss.accessKeyId": "ak",
            "fs.oss.retry.count": "5",
        })
        origin = IoCacheRouting.create(options).origin_options(options)
        self.assertEqual("https://origin.example.com", origin["fs.oss.endpoint"])
        self.assertEqual("true", origin["fs.oss.https.enable"])
        self.assertEqual("ak", origin["fs.oss.accessKeyId"])
        self.assertEqual("5", origin["fs.oss.retry.count"])
        self.assertNotIn("fs.oss.dlf-cache.consistent-hash.enabled", origin)
        self.assertNotIn("fs.jindocache.namespace.rpc.address", origin)
        self.assertNotIn("fs.oss.second.level.domain.enable", origin)
        self.assertEqual("http://legacy.example.com", options["fs.oss.endpoint"])

    def test_target_options(self):
        options = self._multi(**{
            "io-cache.target.accel.endpoint": "HTTP://accel.example.com:8080/",
            "io-cache.target.cluster.endpoint": "10.0.0.1:8443",
            "io-cache.target.cluster.path-style-access": "true",
            "io-cache.target.cluster.region": "cn-shanghai",
            "fs.oss.region": "cn-hangzhou",
            "fs.oss.https.enable": "true",
            "fs.oss.second.level.domain.enable": "true",
            "fs.oss.dlf-cache.consistent-hash.enabled": "true",
        })
        routing = IoCacheRouting.create(options)
        accel = routing.target_options(options, "accel")
        self.assertEqual("http://accel.example.com:8080", accel["fs.oss.endpoint"])
        self.assertEqual("false", accel["fs.oss.https.enable"])
        self.assertEqual("false", accel["fs.oss.second.level.domain.enable"])
        self.assertEqual("cn-hangzhou", accel["fs.oss.region"])
        self.assertEqual("true", accel["fs.oss.dlf-cache.consistent-hash.enabled"])
        cluster = routing.target_options(options, "cluster")
        self.assertEqual("https://10.0.0.1:8443", cluster["fs.oss.endpoint"])
        self.assertEqual("true", cluster["fs.oss.https.enable"])
        self.assertEqual("true", cluster["fs.oss.second.level.domain.enable"])
        self.assertEqual("cn-shanghai", cluster["fs.oss.region"])
        self.assertEqual("cn-hangzhou", options["fs.oss.region"])

    def test_default_target_options(self):
        options = self._options(**{
            "io-cache.endpoint": "cache.example.com",
            "io-cache.target.default.path-style-access": "true",
            "io-cache.target.default.region": "cn-beijing",
            "fs.oss.https.enable": "false",
        })
        default = IoCacheRouting.create(options).target_options(options, "default")
        self.assertEqual("https://cache.example.com", default["fs.oss.endpoint"])
        self.assertEqual("true", default["fs.oss.https.enable"])
        self.assertEqual("true", default["fs.oss.second.level.domain.enable"])
        self.assertEqual("cn-beijing", default["fs.oss.region"])

    def test_target_keeps_sdk_settings(self):
        options = self._options(**{"fs.oss.retry.count": "5"})
        target = IoCacheRouting.create(options).target_options(options, "default")
        self.assertEqual("5", target["fs.oss.retry.count"])
