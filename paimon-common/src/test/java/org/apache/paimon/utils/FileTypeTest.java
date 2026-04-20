/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.utils;

import org.apache.paimon.fs.Path;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FileType}. */
public class FileTypeTest {

    private static final String TABLE_ROOT = "hdfs://cluster/warehouse/db.db/table";

    // ===== META files =====

    @Test
    public void testMetaFiles() {
        // snapshot
        assertThat(FileType.classify(new Path(TABLE_ROOT + "/snapshot/snapshot-1")))
                .isEqualTo(FileType.META);
        assertThat(FileType.classify(new Path(TABLE_ROOT + "/snapshot/snapshot-100")))
                .isEqualTo(FileType.META);
        // schema
        assertThat(FileType.classify(new Path(TABLE_ROOT + "/schema/schema-0")))
                .isEqualTo(FileType.META);
        assertThat(FileType.classify(new Path(TABLE_ROOT + "/schema/schema-5")))
                .isEqualTo(FileType.META);
        // manifest
        assertThat(FileType.classify(new Path(TABLE_ROOT + "/manifest/manifest-a1b2c3d4-0")))
                .isEqualTo(FileType.META);
        // manifest-list
        assertThat(FileType.classify(new Path(TABLE_ROOT + "/manifest/manifest-list-a1b2c3d4-0")))
                .isEqualTo(FileType.META);
        // index-manifest
        assertThat(FileType.classify(new Path(TABLE_ROOT + "/manifest/index-manifest-a1b2c3d4-0")))
                .isEqualTo(FileType.META);
        // statistics
        assertThat(FileType.classify(new Path(TABLE_ROOT + "/statistics/stat-a1b2c3d4-0")))
                .isEqualTo(FileType.META);
        // tag
        assertThat(FileType.classify(new Path(TABLE_ROOT + "/tag/tag-2024-01-01")))
                .isEqualTo(FileType.META);
        assertThat(FileType.classify(new Path(TABLE_ROOT + "/tag/tag-myTag")))
                .isEqualTo(FileType.META);
        // changelog metadata
        assertThat(FileType.classify(new Path(TABLE_ROOT + "/changelog/changelog-1")))
                .isEqualTo(FileType.META);
        assertThat(FileType.classify(new Path(TABLE_ROOT + "/changelog/changelog-100")))
                .isEqualTo(FileType.META);
        // hint files
        assertThat(FileType.classify(new Path(TABLE_ROOT + "/snapshot/EARLIEST")))
                .isEqualTo(FileType.META);
        assertThat(FileType.classify(new Path(TABLE_ROOT + "/snapshot/LATEST")))
                .isEqualTo(FileType.META);
        // success files
        assertThat(FileType.classify(new Path(TABLE_ROOT + "/dt=2024-01-01/bucket-0/_SUCCESS")))
                .isEqualTo(FileType.META);
        assertThat(FileType.classify(new Path(TABLE_ROOT + "/tag/tag-success-file/myTag_SUCCESS")))
                .isEqualTo(FileType.META);
        // consumer
        assertThat(FileType.classify(new Path(TABLE_ROOT + "/consumer/consumer-myGroup")))
                .isEqualTo(FileType.META);
        // service
        assertThat(FileType.classify(new Path(TABLE_ROOT + "/service/service-primary-key-lookup")))
                .isEqualTo(FileType.META);
    }

    // ===== BUCKET_INDEX files =====

    @Test
    public void testBucketIndexFiles() {
        // under /index/ dir
        assertThat(FileType.classify(new Path(TABLE_ROOT + "/index/index-a1b2c3d4-0")))
                .isEqualTo(FileType.BUCKET_INDEX);
        // under bucket dir
        assertThat(
                        FileType.classify(
                                new Path(TABLE_ROOT + "/dt=2024-01-01/bucket-0/index-a1b2c3d4-0")))
                .isEqualTo(FileType.BUCKET_INDEX);
    }

    // ===== GLOBAL_INDEX files =====

    @Test
    public void testGlobalIndexFiles() {
        // btree global index
        assertThat(
                        FileType.classify(
                                new Path(
                                        TABLE_ROOT
                                                + "/index/btree-global-index-a1b2c3d4-e5f6.index")))
                .isEqualTo(FileType.GLOBAL_INDEX);
        // bitmap global index
        assertThat(
                        FileType.classify(
                                new Path(
                                        TABLE_ROOT
                                                + "/index/bitmap-global-index-a1b2c3d4-e5f6.index")))
                .isEqualTo(FileType.GLOBAL_INDEX);
        // lumina vector global index
        assertThat(
                        FileType.classify(
                                new Path(
                                        TABLE_ROOT
                                                + "/index/lumina-vector-ann-global-index-a1b2c3d4.index")))
                .isEqualTo(FileType.GLOBAL_INDEX);
        // tantivy fulltext global index
        assertThat(
                        FileType.classify(
                                new Path(
                                        TABLE_ROOT
                                                + "/index/tantivy-fulltext-global-index-a1b2c3d4.index")))
                .isEqualTo(FileType.GLOBAL_INDEX);
    }

    // ===== FILE_INDEX files =====

    @Test
    public void testFileIndexFiles() {
        assertThat(
                        FileType.classify(
                                new Path(
                                        TABLE_ROOT
                                                + "/dt=2024-01-01/bucket-0/data-a1b2c3d4-0.orc.index")))
                .isEqualTo(FileType.FILE_INDEX);
        assertThat(
                        FileType.classify(
                                new Path(
                                        TABLE_ROOT
                                                + "/dt=2024-01-01/bucket-0/data-a1b2c3d4-0.parquet.index")))
                .isEqualTo(FileType.FILE_INDEX);
    }

    // ===== isIndex() =====

    @Test
    public void testIsIndex() {
        assertThat(FileType.BUCKET_INDEX.isIndex()).isTrue();
        assertThat(FileType.GLOBAL_INDEX.isIndex()).isTrue();
        assertThat(FileType.FILE_INDEX.isIndex()).isTrue();
        assertThat(FileType.META.isIndex()).isFalse();
        assertThat(FileType.DATA.isIndex()).isFalse();
    }

    // ===== DATA files =====

    @Test
    public void testDataFiles() {
        // orc data file
        assertThat(
                        FileType.classify(
                                new Path(
                                        TABLE_ROOT
                                                + "/dt=2024-01-01/bucket-0/data-a1b2c3d4-0.orc")))
                .isEqualTo(FileType.DATA);
        // parquet data file
        assertThat(
                        FileType.classify(
                                new Path(
                                        TABLE_ROOT
                                                + "/dt=2024-01-01/bucket-0/data-a1b2c3d4-0.parquet")))
                .isEqualTo(FileType.DATA);
        // changelog data file
        assertThat(
                        FileType.classify(
                                new Path(
                                        TABLE_ROOT
                                                + "/dt=2024-01-01/bucket-0/changelog-a1b2c3d4-0.orc")))
                .isEqualTo(FileType.DATA);
        // blob file
        assertThat(
                        FileType.classify(
                                new Path(
                                        TABLE_ROOT
                                                + "/dt=2024-01-01/bucket-0/data-a1b2c3d4-0.blob")))
                .isEqualTo(FileType.DATA);
        // vector file
        assertThat(
                        FileType.classify(
                                new Path(
                                        TABLE_ROOT
                                                + "/dt=2024-01-01/bucket-0/data-a1b2c3d4-0.vector.lance")))
                .isEqualTo(FileType.DATA);
        // unknown file defaults to DATA
        assertThat(
                        FileType.classify(
                                new Path(TABLE_ROOT + "/dt=2024-01-01/bucket-0/unknown-file.bin")))
                .isEqualTo(FileType.DATA);
    }

    // ===== Edge cases =====

    @Test
    public void testChangelogDirInParentPathNotMisjudged() {
        // table root path itself contains "changelog", should not be misjudged as META
        String tricky = "hdfs://cluster/changelog/warehouse/db.db/table";
        assertThat(
                        FileType.classify(
                                new Path(tricky + "/dt=2024-01-01/bucket-0/data-a1b2c3d4-0.orc")))
                .isEqualTo(FileType.DATA);
        assertThat(
                        FileType.classify(
                                new Path(
                                        tricky
                                                + "/dt=2024-01-01/bucket-0/changelog-a1b2c3d4-0.orc")))
                .isEqualTo(FileType.DATA);
    }

    @Test
    public void testBranchPaths() {
        String branchRoot = TABLE_ROOT + "/branch/branch-dev";
        assertThat(FileType.classify(new Path(branchRoot + "/snapshot/snapshot-1")))
                .isEqualTo(FileType.META);
        assertThat(FileType.classify(new Path(branchRoot + "/schema/schema-0")))
                .isEqualTo(FileType.META);
        assertThat(FileType.classify(new Path(branchRoot + "/changelog/changelog-1")))
                .isEqualTo(FileType.META);
        assertThat(FileType.classify(new Path(branchRoot + "/index/index-a1b2c3d4-0")))
                .isEqualTo(FileType.BUCKET_INDEX);
        assertThat(
                        FileType.classify(
                                new Path(branchRoot + "/index/btree-global-index-a1b2c3d4.index")))
                .isEqualTo(FileType.GLOBAL_INDEX);
    }

    // ===== Temporary file paths (from Path.createTempPath()) =====

    @Test
    public void testTempMetaFiles() {
        String uuid = UUID.randomUUID().toString();

        // snapshot tmp: .snapshot-1.{UUID}.tmp
        assertThat(
                        FileType.classify(
                                new Path(TABLE_ROOT + "/snapshot/.snapshot-1." + uuid + ".tmp")))
                .isEqualTo(FileType.META);

        // schema tmp
        assertThat(FileType.classify(new Path(TABLE_ROOT + "/schema/.schema-0." + uuid + ".tmp")))
                .isEqualTo(FileType.META);

        // tag tmp
        assertThat(FileType.classify(new Path(TABLE_ROOT + "/tag/.tag-myTag." + uuid + ".tmp")))
                .isEqualTo(FileType.META);

        // consumer tmp
        assertThat(
                        FileType.classify(
                                new Path(
                                        TABLE_ROOT
                                                + "/consumer/.consumer-myGroup."
                                                + uuid
                                                + ".tmp")))
                .isEqualTo(FileType.META);

        // service tmp
        assertThat(
                        FileType.classify(
                                new Path(
                                        TABLE_ROOT
                                                + "/service/.service-primary-key-lookup."
                                                + uuid
                                                + ".tmp")))
                .isEqualTo(FileType.META);

        // hint file tmp: .EARLIEST.{UUID}.tmp
        assertThat(FileType.classify(new Path(TABLE_ROOT + "/snapshot/.EARLIEST." + uuid + ".tmp")))
                .isEqualTo(FileType.META);
        assertThat(FileType.classify(new Path(TABLE_ROOT + "/snapshot/.LATEST." + uuid + ".tmp")))
                .isEqualTo(FileType.META);

        // _SUCCESS tmp
        assertThat(
                        FileType.classify(
                                new Path(
                                        TABLE_ROOT
                                                + "/dt=2024-01-01/bucket-0/._SUCCESS."
                                                + uuid
                                                + ".tmp")))
                .isEqualTo(FileType.META);

        // changelog metadata tmp
        assertThat(
                        FileType.classify(
                                new Path(TABLE_ROOT + "/changelog/.changelog-1." + uuid + ".tmp")))
                .isEqualTo(FileType.META);

        // statistics tmp
        assertThat(
                        FileType.classify(
                                new Path(
                                        TABLE_ROOT
                                                + "/statistics/.stat-a1b2c3d4-0."
                                                + uuid
                                                + ".tmp")))
                .isEqualTo(FileType.META);
    }

    @Test
    public void testUnwrapTempFileName() {
        String uuid = "a1b2c3d4-e5f6-7890-abcd-ef1234567890";

        // valid temp file names
        assertThat(FileType.unwrapTempFileName(".snapshot-1." + uuid + ".tmp"))
                .isEqualTo("snapshot-1");
        assertThat(FileType.unwrapTempFileName(".schema-0." + uuid + ".tmp")).isEqualTo("schema-0");
        assertThat(FileType.unwrapTempFileName(".EARLIEST." + uuid + ".tmp")).isEqualTo("EARLIEST");
        assertThat(FileType.unwrapTempFileName("._SUCCESS." + uuid + ".tmp")).isEqualTo("_SUCCESS");

        // not a temp file: no leading dot
        assertThat(FileType.unwrapTempFileName("snapshot-1." + uuid + ".tmp"))
                .isEqualTo("snapshot-1." + uuid + ".tmp");

        // not a temp file: no .tmp suffix
        assertThat(FileType.unwrapTempFileName(".snapshot-1." + uuid))
                .isEqualTo(".snapshot-1." + uuid);

        // not a temp file: invalid UUID
        assertThat(FileType.unwrapTempFileName(".snapshot-1.not-a-valid-uuid-string-here.tmp"))
                .isEqualTo(".snapshot-1.not-a-valid-uuid-string-here.tmp");

        // not a temp file: too short
        assertThat(FileType.unwrapTempFileName(".x.tmp")).isEqualTo(".x.tmp");

        // regular file names should pass through unchanged
        assertThat(FileType.unwrapTempFileName("snapshot-1")).isEqualTo("snapshot-1");
        assertThat(FileType.unwrapTempFileName("data-a1b2c3d4-0.orc"))
                .isEqualTo("data-a1b2c3d4-0.orc");
    }
}
