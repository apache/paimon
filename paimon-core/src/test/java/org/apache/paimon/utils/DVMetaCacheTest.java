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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ObjectsCache}. */
public class DVMetaCacheTest {

    @TempDir java.nio.file.Path tempDir;

    private static final Path TEST_PATH = new Path("/test/snapshot/snapshot-");
    //    private static final InternalRow PARTITION_ROW =
    // GenericRow.of(BinaryString.fromString("part-val"));

    private DeletionFile createDeletionFile(
            String dataFileName, long offset, long length, Long cardinality) {
        return new DeletionFile(dataFileName, offset, length, cardinality);
    }

    @Test
    public void testPutAndRead_SuccessfulRoundTrip() {
        DVMetaCache cache = new DVMetaCache(100);

        Map<String, DeletionFile> dvFilesMap = new HashMap<>();
        dvFilesMap.put("data1.parquet", new DeletionFile("dv1.parquet", 0L, 100L, 42L));
        dvFilesMap.put("data2.parquet", createDeletionFile("dv2.parquet", 100L, 500L, 42L));
        dvFilesMap.put("data3.parquet", createDeletionFile("dv3.parquet", 0L, 300L, null));
        dvFilesMap.put("data4.parquet", createDeletionFile("dv4.parquet", 0L, 200L, null));

        InternalRowSerializer serializer =
                new InternalRowSerializer(RowType.of(DataTypes.STRING()));
        BinaryRow p1 =
                serializer.toBinaryRow(GenericRow.of(BinaryString.fromString("partition1"))).copy();
        BinaryRow p2 =
                serializer.toBinaryRow(GenericRow.of(BinaryString.fromString("partition2"))).copy();
        BinaryRow p3 =
                serializer.toBinaryRow(GenericRow.of(BinaryString.fromString("partition3"))).copy();
        Path indexManifestPath = new Path("manifest/index-manifest");

        cache.put(
                indexManifestPath,
                p1,
                1,
                new HashMap<String, DeletionFile>() {
                    {
                        put("data1.parquet", new DeletionFile("dv1.parquet", 0L, 100L, 42L));
                        put("data2.parquet", new DeletionFile("dv2.parquet", 100L, 500L, null));
                    }
                });
        cache.put(
                indexManifestPath,
                p1,
                2,
                new HashMap<String, DeletionFile>() {
                    {
                        put("data3.parquet", new DeletionFile("dv3.parquet", 0L, 300L, 12L));
                    }
                });
        cache.put(
                indexManifestPath,
                p2,
                1,
                new HashMap<String, DeletionFile>() {
                    {
                        put("data4.parquet", new DeletionFile("dv4.parquet", 0L, 200L, 19L));
                    }
                });

        Map<String, DeletionFile> r1 = cache.read(indexManifestPath, p1, 1);
        assertThat(r1).isNotNull().hasSize(2);
        assertThat(r1).containsKeys("data1.parquet", "data2.parquet");

        DeletionFile f1 = r1.get("data1.parquet");
        assertThat(f1.path()).isEqualTo("dv1.parquet");
        assertThat(f1.offset()).isEqualTo(0L);
        assertThat(f1.length()).isEqualTo(100L);
        assertThat(f1.cardinality()).isEqualTo(42L);

        DeletionFile f2 = r1.get("data2.parquet");
        assertThat(f2.path()).isEqualTo("dv2.parquet");
        assertThat(f2.offset()).isEqualTo(100L);
        assertThat(f2.length()).isEqualTo(500L);
        assertThat(f2.cardinality()).isNull();

        Map<String, DeletionFile> r2 = cache.read(indexManifestPath, p1, 2);
        assertThat(r2).isNotNull().hasSize(1);
        assertThat(r2).containsKeys("data3.parquet");

        DeletionFile f3 = r2.get("data3.parquet");
        assertThat(f3.path()).isEqualTo("dv3.parquet");
        assertThat(f3.offset()).isEqualTo(0L);
        assertThat(f3.length()).isEqualTo(300L);
        assertThat(f3.cardinality()).isEqualTo(12L);

        Map<String, DeletionFile> r4 = cache.read(indexManifestPath, p3, 0);
        assertThat(r4).isNull();
    }
}
