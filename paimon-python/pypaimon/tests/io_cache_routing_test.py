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

"""Tests for the io-cache routing decision."""

import unittest

from pypaimon.filesystem.io_cache_routing import (IoCacheRouting, Op,
                                                  data_prefixes, routable_type)
from pypaimon.utils.file_type import FileType

UUID = "8b1f7c2e-3a4d-4e5f-9a0b-1c2d3e4f5a6b"
TABLE_ROOT = "oss://bkt/db1.db/t1"

# paths below are relative to TABLE_ROOT, and {uuid} stands for UUID
DATA_PATH = "dt=1/bucket-0/data-{uuid}-0.parquet"
MANIFEST_PATH = "manifest/manifest-{uuid}-0"
INDEX_PATH = "index/index-{uuid}-0"
GLOBAL_INDEX_PATH = "index/btree-global-index-{uuid}.index"
TEMP_PATH = "dt=1/bucket-0/.data-{uuid}-0.parquet.{uuid}.tmp"
SNAPSHOT_PATH = "snapshot/snapshot-12"
LATEST_PATH = "snapshot/LATEST"

OSS = "https://oss-cn-hangzhou-internal.aliyuncs.com"
CACHE = "http://cache.example.com"
ACCEL = "https://accelerator.example.com"
CLUSTER = "http://cluster.example.com"
WRITE = "io-cache.policy=meta,read,write"
EXISTS = "io-cache.policy=meta,read,exists"
ORIGIN_OPS = (Op.LIST, Op.DELETE, Op.RENAME, Op.MKDIRS, Op.COPY, Op.ATOMIC_WRITE, Op.PRESIGN)


def with_changes(options, changes):
    """Applies changes: "key=value" sets an option and "-key" removes it."""
    for change in changes:
        if change.startswith("-"):
            options.pop(change[1:], None)
        else:
            key, value = change.split("=", 1)
            options[key] = value
    return options


def single(*changes):
    """One cache target, which older clients also get as fs.oss.endpoint."""
    return with_changes({
        "fs.oss.endpoint": CACHE,
        "io-cache.enabled": "true",
        "io-cache.endpoint": CACHE,
        "io-cache.origin.endpoint": OSS,
        "io-cache.policy": "meta,read",
        "io-cache.whitelist": "meta,data",
    }, changes)


def multi(*changes):
    """Metadata on an accelerator, data and indexes on a cache cluster."""
    return with_changes({
        "fs.oss.endpoint": ACCEL,
        "io-cache.enabled": "true",
        "io-cache.origin.endpoint": OSS,
        "io-cache.targets": "accel,cluster",
        "io-cache.target.accel.endpoint": ACCEL,
        "io-cache.target.accel.region": "cn-hangzhou",
        "io-cache.target.cluster.endpoint": CLUSTER,
        "io-cache.target.cluster.path-style-access": "true",
        "io-cache.policy": "meta,read",
        "io-cache.whitelist": "*",
        "io-cache.routes": "meta=accel;data,bucket-index,global-index,file-index=cluster",
    }, changes)


def resolve(path):
    path = path.replace("{uuid}", UUID)
    return path if "://" in path else TABLE_ROOT + "/" + path


def endpoint(options, op, path):
    """The endpoint of one request as a client sees it: override, old endpoint, origin or a target."""
    if options.get("dlf.oss-endpoint"):
        return options["dlf.oss-endpoint"]
    routing = IoCacheRouting.create(options)
    if routing is None:
        return options.get("fs.oss.endpoint")
    name = routing.route(op, path)
    return routing.origin_endpoint() if name is None else routing.targets()[name]


class IoCacheRoutingEndpointTest(unittest.TestCase):

    def assert_endpoint(self, options, op, path, expected):
        self.assertEqual(expected, endpoint(options, op, resolve(path)),
                         f"{op.value} {path} with {options}")

    def assert_type(self, path, file_type, *changes):
        prefixes = data_prefixes(with_changes({}, changes))
        self.assertEqual(file_type, routable_type(resolve(path), prefixes), f"{path} with {changes}")

    def test_one_cache_target(self):
        options = single()
        self.assert_endpoint(options, Op.READ, DATA_PATH, CACHE)
        self.assert_endpoint(options, Op.META, DATA_PATH, CACHE)
        # exists asks whether a file is still there, so it needs its own policy token
        self.assert_endpoint(options, Op.EXISTS, DATA_PATH, OSS)
        self.assert_endpoint(single(EXISTS), Op.EXISTS, DATA_PATH, CACHE)
        self.assert_endpoint(single(EXISTS), Op.EXISTS, SNAPSHOT_PATH, OSS)
        self.assert_endpoint(options, Op.READ, MANIFEST_PATH, CACHE)
        self.assert_endpoint(options, Op.READ, "manifest/manifest-list-{uuid}-1", CACHE)
        self.assert_endpoint(options, Op.READ, "oss://other-bkt/db1.db/t1/" + DATA_PATH, CACHE)
        self.assert_endpoint(options, Op.READ, SNAPSHOT_PATH, OSS)
        self.assert_endpoint(options, Op.META, SNAPSHOT_PATH, OSS)
        self.assert_endpoint(options, Op.EXISTS, SNAPSHOT_PATH, OSS)
        self.assert_endpoint(options, Op.READ, LATEST_PATH, OSS)
        self.assert_endpoint(options, Op.EXISTS, LATEST_PATH, OSS)
        self.assert_endpoint(options, Op.READ, TEMP_PATH, OSS)
        self.assert_endpoint(options, Op.READ, "dt=1/bucket-0/000000_0", OSS)
        self.assert_endpoint(options, Op.READ, "dls://bkt/db1.db/t1/" + DATA_PATH, OSS)
        # the whitelist has no index
        self.assert_endpoint(options, Op.READ, INDEX_PATH, OSS)
        # a Format Table file named like a manifest may be replaced in place
        self.assert_endpoint(options, Op.READ, "review_external/manifest.parquet", OSS)
        self.assert_endpoint(options, Op.META, "review_external/manifest.parquet", OSS)
        self.assert_endpoint(options, Op.WRITE, DATA_PATH, OSS)
        self.assert_endpoint(options, Op.TWO_PHASE_WRITE, DATA_PATH, OSS)
        for op in ORIGIN_OPS:
            self.assert_endpoint(options, op, DATA_PATH, OSS)

    def test_policy_and_whitelist(self):
        self.assert_endpoint(single("-io-cache.enabled"), Op.READ, DATA_PATH, CACHE)
        self.assert_endpoint(single("io-cache.enabled=false"), Op.EXISTS, DATA_PATH, CACHE)
        self.assert_endpoint(single("-io-cache.policy"), Op.EXISTS, DATA_PATH, CACHE)
        self.assert_endpoint(single("io-cache.policy=none"), Op.EXISTS, DATA_PATH, CACHE)
        self.assert_endpoint(single("io-cache.policy=read,NONE"), Op.EXISTS, DATA_PATH, CACHE)
        self.assert_endpoint(single("io-cache.policy=thread,metadata"), Op.EXISTS, DATA_PATH, CACHE)
        self.assert_endpoint(single("io-cache.policy= READ , Meta "), Op.META, DATA_PATH, CACHE)
        self.assert_endpoint(single("io-cache.policy=read,nonetheless"), Op.EXISTS, DATA_PATH, OSS)

        read_only = single("io-cache.policy=read")
        self.assert_endpoint(read_only, Op.READ, DATA_PATH, CACHE)
        self.assert_endpoint(read_only, Op.META, DATA_PATH, OSS)
        self.assert_endpoint(read_only, Op.EXISTS, DATA_PATH, OSS)
        write_only = single("io-cache.policy=write")
        self.assert_endpoint(write_only, Op.WRITE, DATA_PATH, CACHE)
        self.assert_endpoint(write_only, Op.EXISTS, DATA_PATH, OSS)
        exists_only = single("io-cache.policy=exists")
        self.assert_endpoint(exists_only, Op.EXISTS, DATA_PATH, CACHE)
        self.assert_endpoint(exists_only, Op.META, DATA_PATH, OSS)

        self.assert_endpoint(single("-io-cache.whitelist"), Op.READ, INDEX_PATH, CACHE)
        self.assert_endpoint(single("io-cache.whitelist=*"), Op.READ, GLOBAL_INDEX_PATH, CACHE)
        self.assert_endpoint(single("io-cache.whitelist=*"), Op.READ, DATA_PATH + ".index", CACHE)

    def test_endpoints(self):
        self.assert_endpoint(single("io-cache.endpoint=  "), Op.READ, DATA_PATH, OSS)
        self.assert_endpoint(
            single("io-cache.endpoint=", "io-cache.routes=*=default"), Op.READ, DATA_PATH, OSS)
        # the origin defaults to fs.oss.endpoint
        oss = "fs.oss.endpoint=" + OSS
        self.assert_endpoint(single(oss, "-io-cache.origin.endpoint"), Op.LIST, DATA_PATH, OSS)
        self.assert_endpoint(single(oss, "io-cache.origin.endpoint=  "), Op.LIST, DATA_PATH, OSS)
        # a client-side endpoint turns routing off
        override = "https://oss-cn-hangzhou.aliyuncs.com"
        self.assert_endpoint(single("dlf.oss-endpoint=" + override), Op.READ, DATA_PATH, override)

    def test_write_policy(self):
        options = single(WRITE)
        self.assert_endpoint(options, Op.WRITE, DATA_PATH, CACHE)
        self.assert_endpoint(options, Op.TWO_PHASE_WRITE, DATA_PATH, CACHE)
        self.assert_endpoint(options, Op.WRITE, MANIFEST_PATH, CACHE)
        self.assert_endpoint(options, Op.WRITE, SNAPSHOT_PATH, OSS)
        self.assert_endpoint(options, Op.WRITE, LATEST_PATH, OSS)
        self.assert_endpoint(options, Op.WRITE, TEMP_PATH, OSS)
        for op in ORIGIN_OPS:
            self.assert_endpoint(options, op, DATA_PATH, OSS)
        self.assert_endpoint(single(WRITE, "io-cache.whitelist=meta"), Op.WRITE, DATA_PATH, OSS)

        self.assert_endpoint(multi(WRITE), Op.WRITE, DATA_PATH, CLUSTER)
        self.assert_endpoint(multi(WRITE), Op.WRITE, MANIFEST_PATH, ACCEL)

    def test_two_cache_targets(self):
        options = multi()
        self.assert_endpoint(options, Op.READ, MANIFEST_PATH, ACCEL)
        self.assert_endpoint(options, Op.META, MANIFEST_PATH, ACCEL)
        self.assert_endpoint(options, Op.EXISTS, MANIFEST_PATH, OSS)
        self.assert_endpoint(multi(EXISTS), Op.EXISTS, MANIFEST_PATH, ACCEL)
        self.assert_endpoint(options, Op.READ, DATA_PATH, CLUSTER)
        self.assert_endpoint(multi(EXISTS), Op.EXISTS, DATA_PATH, CLUSTER)
        self.assert_endpoint(options, Op.READ, INDEX_PATH, CLUSTER)
        self.assert_endpoint(options, Op.READ, GLOBAL_INDEX_PATH, CLUSTER)
        self.assert_endpoint(options, Op.READ, SNAPSHOT_PATH, OSS)
        self.assert_endpoint(options, Op.WRITE, MANIFEST_PATH, OSS)
        self.assert_endpoint(options, Op.TWO_PHASE_WRITE, MANIFEST_PATH, OSS)
        for op in ORIGIN_OPS:
            self.assert_endpoint(options, op, MANIFEST_PATH, OSS)
        self.assert_endpoint(multi("io-cache.whitelist=meta"), Op.READ, DATA_PATH, OSS)

    def test_targets_and_routes(self):
        # without routes the first target with an endpoint takes every type
        no_routes = "-io-cache.routes"
        self.assert_endpoint(multi(no_routes), Op.READ, DATA_PATH, ACCEL)
        self.assert_endpoint(multi(no_routes, "io-cache.endpoint=" + CACHE), Op.READ, DATA_PATH, ACCEL)
        self.assert_endpoint(
            multi(no_routes, "-io-cache.target.accel.endpoint"), Op.READ, DATA_PATH, CLUSTER)
        self.assert_endpoint(
            multi(no_routes, "io-cache.target.accel.endpoint=  "), Op.READ, DATA_PATH, CLUSTER)
        self.assert_endpoint(
            multi(no_routes, "io-cache.targets=Bad_Name,cluster"), Op.READ, DATA_PATH, CLUSTER)
        self.assert_endpoint(
            multi(f"io-cache.target.cluster.endpoint=  {CLUSTER}  "), Op.READ, DATA_PATH, CLUSTER)

        # a type without a usable route goes to the origin
        self.assert_endpoint(multi("io-cache.routes=data=x"), Op.READ, DATA_PATH, OSS)
        self.assert_endpoint(multi("-io-cache.target.cluster.endpoint"), Op.READ, DATA_PATH, OSS)
        meta_and_data = "io-cache.routes=meta=accel;data=cluster"
        self.assert_endpoint(multi(meta_and_data), Op.READ, DATA_PATH + ".index", OSS)

        self.assert_endpoint(multi("io-cache.routes=*=cluster"), Op.READ, MANIFEST_PATH, CLUSTER)
        first_wins = "io-cache.routes=data=accel;data,meta=cluster"
        self.assert_endpoint(multi(first_wins), Op.READ, DATA_PATH, ACCEL)
        malformed = "io-cache.routes==cluster;meta=accel;junk"
        self.assert_endpoint(multi(malformed), Op.READ, MANIFEST_PATH, ACCEL)
        self.assert_endpoint(multi(malformed), Op.READ, DATA_PATH, OSS)

    def test_data_files_named_like_metadata(self):
        manifest = multi("data-file.prefix=manifest-")
        self.assert_endpoint(manifest, Op.READ, "dt=1/bucket-0/manifest-{uuid}-0.orc", CLUSTER)
        self.assert_endpoint(manifest, Op.READ, "dt=1/bucket-0/7f3a/manifest-{uuid}-0.orc", CLUSTER)
        self.assert_endpoint(manifest, Op.READ, MANIFEST_PATH, ACCEL)
        self.assert_endpoint(manifest, Op.READ, MANIFEST_PATH + ".avro.sidecar", ACCEL)
        snapshot = multi("data-file.prefix=snapshot-")
        self.assert_endpoint(snapshot, Op.READ, "dt=1/bucket-0/snapshot-{uuid}-0.orc", CLUSTER)
        stat = multi("data-file.prefix=stat-")
        self.assert_endpoint(stat, Op.READ, "dt=1/bucket-0/stat-{uuid}-0.orc", CLUSTER)
        index = multi("io-cache.routes=data=cluster;bucket-index=accel", "data-file.prefix=index-")
        self.assert_endpoint(index, Op.READ, "dt=1/bucket-0/index-{uuid}-0.orc", CLUSTER)
        self.assert_endpoint(index, Op.READ, INDEX_PATH, ACCEL)

        custom = "dt=1/bucket-0/custom-{uuid}-0.orc"
        self.assert_endpoint(single(), Op.READ, custom, OSS)
        self.assert_endpoint(single("data-file.prefix=custom-"), Op.READ, custom, CACHE)

    def test_routable_type(self):
        # named by sequence id or rewritten in place
        for path in ("snapshot/snapshot-12",
                     "snapshot/LATEST",
                     "snapshot/EARLIEST",
                     "branch/branch-dev/snapshot/LATEST",
                     "schema/schema-3",
                     "changelog/changelog-5",
                     "changelog/LATEST",
                     "tag/tag-2026-09-30",
                     "tag/tag-success-file/t1_SUCCESS",
                     "consumer/consumer-job1",
                     "service/service-primary-key-lookup",
                     "dt=1/_SUCCESS",
                     "oss://bkt/bucket-0/db/t/changelog/changelog-5"):
            self.assert_type(path, None)
        # temporary or not written by Paimon
        for path in ("snapshot/.snapshot-13.{uuid}.tmp",
                     "dt=1/bucket-0/.data-{uuid}-0.parquet.{uuid}.tmp",
                     "dt=1/bucket-0/data-{uuid}-0.parquet.tmp-{uuid}",
                     "dt=1/bucket-0/data-{uuid}-0.parquet.tmp.{uuid}",
                     "metadata/version-hint.text",
                     "metadata/v3.metadata.json",
                     "dt=1/bucket-0/000000_0",
                     "dt=1/part-00000-1a2b.snappy.parquet",
                     "dt=1/bucket-0/data-1.parquet",
                     "dt=1/bucket-0/custom-{uuid}-0.orc",
                     "README"):
            self.assert_type(path, None)
        # Format Table files named like metadata or indexes, which may be replaced in place
        for path in ("dt=1/manifest.parquet",
                     "dt=1/stat-2024.parquet",
                     "dt=1/index-a.csv",
                     "dt=1/foo.index",
                     "review_external/manifest.parquet",
                     "manifest/manifest-old",
                     "manifest/manifest-old.avro.sidecar",
                     "manifest/manifest-list-{uuid}-1.avro.sidecar",
                     "statistics/stat-old",
                     "index/index-old",
                     "index/my-global-index.index",
                     "index/btree-global-index-{uuid}-0.index"):
            self.assert_type(path, None)

        self.assert_type("manifest/manifest-{uuid}-0", FileType.META)
        self.assert_type("manifest/manifest-list-{uuid}-1", FileType.META)
        self.assert_type("manifest/index-manifest-{uuid}-0", FileType.META)
        self.assert_type("manifest/manifest-{uuid}-0.avro.sidecar", FileType.META)
        self.assert_type("statistics/stat-{uuid}-0", FileType.META)
        self.assert_type("dt=1/bucket-0/data-{uuid}-0.parquet", FileType.DATA)
        self.assert_type("dt=1/bucket-0/changelog-{uuid}-0.parquet", FileType.DATA)
        self.assert_type("dt=1/bucket-0/data-{uuid}-0.blob", FileType.DATA)
        self.assert_type("dt=1/8b1f7c2e/data-{uuid}-0.parquet", FileType.DATA)
        self.assert_type("dt=1/bucket-0/data-{uuid}-0.parquet.index", FileType.FILE_INDEX)
        self.assert_type("index/index-{uuid}-0", FileType.BUCKET_INDEX)
        self.assert_type("index/btree-global-index-{uuid}.index", FileType.GLOBAL_INDEX)
        self.assert_type("index/lumina-global-index-{uuid}.index", FileType.GLOBAL_INDEX)

    def test_routable_type_with_file_prefixes(self):
        self.assert_type("dt=1/bucket-0/custom-{uuid}-0.orc", FileType.DATA, "data-file.prefix=custom-")
        self.assert_type("dt=1/bucket-0/data-{uuid}-0.orc", FileType.DATA, "data-file.prefix=custom-")
        self.assert_type("dt=1/bucket-0/cl-{uuid}-0.orc", FileType.DATA, "changelog-file.prefix=cl-")
        self.assert_type("dt=1/bucket-0/  unknown.parquet", None, "data-file.prefix=  ")

        # the name decides, so data files may be named like metadata
        manifest = "data-file.prefix=manifest-"
        self.assert_type("dt=1/bucket-0/manifest-{uuid}-0.orc", FileType.DATA, manifest)
        self.assert_type("dt=1/bucket-0/7f3a/manifest-{uuid}-0.orc", FileType.DATA, manifest)
        self.assert_type("dt=1/bucket-0/manifest-{uuid}-0.orc.index", FileType.FILE_INDEX, manifest)
        self.assert_type("manifest/manifest-{uuid}-0", FileType.META, manifest)
        self.assert_type("manifest/manifest-{uuid}-0.avro.sidecar", FileType.META, manifest)
        index = "data-file.prefix=index-"
        self.assert_type("dt=1/bucket-0/index-{uuid}-0.orc", FileType.DATA, index)
        self.assert_type("bucket-postpone/index-{uuid}-0.orc", FileType.DATA, index)
        self.assert_type("index/index-{uuid}-0", FileType.BUCKET_INDEX, index)
        self.assert_type("dt=1/bucket-0/index-{uuid}-0", FileType.BUCKET_INDEX, index)
        stat = "data-file.prefix=stat-"
        self.assert_type("dt=1/bucket-0/stat-{uuid}-0.orc", FileType.DATA, stat)
        self.assert_type("postpone/stat-{uuid}-0.orc", FileType.DATA, stat)
        self.assert_type("statistics/stat-{uuid}-0", FileType.META, stat)
        snapshot = "data-file.prefix=snapshot-"
        self.assert_type("dt=1/bucket-0/snapshot-{uuid}-0.orc", FileType.DATA, snapshot)
        self.assert_type("dt=1/bucket-0/snapshot-{uuid}-0.orc.index", FileType.FILE_INDEX, snapshot)
        self.assert_type("dt=1/bucket-0/schema-{uuid}-0.orc", FileType.DATA, "data-file.prefix=schema-")
        self.assert_type("dt=1/bucket-0/global-index-{uuid}-0.orc.index", FileType.FILE_INDEX,
                         "data-file.prefix=global-index-")

        # names rewritten in place never use a cache, whatever the prefix
        self.assert_type("dt=1/bucket-0/tag-{uuid}-0.orc", None, "data-file.prefix=tag-")
        self.assert_type("dt=1/bucket-0/consumer-{uuid}-0.orc", None, "data-file.prefix=consumer-")
        warehouse = "oss://bkt/warehouse/bucket-0/db/t1/"
        self.assert_type(warehouse + "tag/tag-file", None, "data-file.prefix=tag-")
        self.assert_type(warehouse + "consumer/consumer-file", None, "data-file.prefix=consumer-")
        self.assert_type(warehouse + "service/service-file", None, "data-file.prefix=service-")
        self.assert_type(warehouse + "metadata/version-hint.text", None, "data-file.prefix=version-")
        self.assert_type(warehouse + "branch/branch-feature", None, "data-file.prefix=branch-")


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
                          {"io-cache.policy": "prefetch,metadata"}, {"io-cache.targets": ""},
                          {"io-cache.targets": "Bad_Name,1x"},
                          {"dlf.oss-endpoint": "oss-cn-hangzhou.aliyuncs.com"}):
            with self.subTest(overrides=overrides):
                self.assertIsNone(IoCacheRouting.create(self._options(**overrides)))
        options = self._options()
        del options["io-cache.enabled"]
        self.assertIsNone(IoCacheRouting.create(options))

    def test_policy_routes_read_meta_and_write(self):
        routing = IoCacheRouting.create(self._options(**{"io-cache.policy": " Meta , READ ,prefetch,write"}))
        routed = (Op.READ, Op.META, Op.WRITE, Op.TWO_PHASE_WRITE)
        for op in Op:
            with self.subTest(op=op):
                expected = "default" if op in routed else None
                self.assertEqual(expected, routing.route(op, self.DATA))
        write_only = IoCacheRouting.create(self._options(**{"io-cache.policy": "write"}))
        self.assertEqual("default", write_only.route(Op.WRITE, self.DATA))
        self.assertIsNone(write_only.route(Op.READ, self.DATA))
        exists_only = IoCacheRouting.create(self._options(**{"io-cache.policy": "exists"}))
        self.assertEqual("default", exists_only.route(Op.EXISTS, self.DATA))
        self.assertIsNone(exists_only.route(Op.META, self.DATA))
        read_only = IoCacheRouting.create(self._options(**{"io-cache.policy": "read"}))
        self.assertIsNone(read_only.route(Op.META, self.DATA))
        self.assertIsNone(read_only.route(Op.EXISTS, self.DATA))

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

    def test_blank_region_and_file_prefix_are_ignored(self):
        options = self._options(**{
            "io-cache.target.default.region": "  ",
            "fs.oss.region": "cn-hangzhou",
            "data-file.prefix": "  ",
        })
        routing = IoCacheRouting.create(options)
        self.assertEqual("cn-hangzhou", routing.target_options(options, "default")["fs.oss.region"])
        self.assertEqual(("data-", "changelog-"), data_prefixes(options))

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
