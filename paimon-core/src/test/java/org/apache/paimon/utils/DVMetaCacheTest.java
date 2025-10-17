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

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DVMetaCache}. */
public class DVMetaCacheTest {

    @Test
    public void testPutAndRead() {
        DVMetaCache cache = new DVMetaCache(100);
        Path path = new Path("manifest/index-manifest");
        BinaryRow partition = partition("test-partition");

        // Put data for bucket 1 with multiple files
        Map<String, DeletionFile> dvFiles1 = new HashMap<>();
        dvFiles1.put("data1.parquet", new DeletionFile("dv1.parquet", 0L, 100L, 42L));
        dvFiles1.put("data2.parquet", new DeletionFile("dv2.parquet", 100L, 500L, null));
        cache.put(path, partition, 1, dvFiles1);

        // Put data for bucket 2 with single file
        Map<String, DeletionFile> dvFiles2 = new HashMap<>();
        dvFiles2.put("data3.parquet", new DeletionFile("dv3.parquet", 0L, 300L, 12L));
        cache.put(path, partition, 2, dvFiles2);

        // Read bucket 1 - verify multiple files
        Map<String, DeletionFile> result1 = cache.read(path, partition, 1);
        assertThat(result1).isNotNull().hasSize(2);
        assertThat(result1).containsKeys("data1.parquet", "data2.parquet");

        DeletionFile file1 = result1.get("data1.parquet");
        assertThat(file1.path()).isEqualTo("dv1.parquet");
        assertThat(file1.offset()).isEqualTo(0L);
        assertThat(file1.length()).isEqualTo(100L);
        assertThat(file1.cardinality()).isEqualTo(42L);

        DeletionFile file2 = result1.get("data2.parquet");
        assertThat(file2.path()).isEqualTo("dv2.parquet");
        assertThat(file2.cardinality()).isNull();

        // Read bucket 2 - verify single file
        Map<String, DeletionFile> result2 = cache.read(path, partition, 2);
        assertThat(result2).isNotNull().hasSize(1);
        assertThat(result2).containsKey("data3.parquet");

        // Read non-existent key
        assertThat(cache.read(path, partition("other"), 0)).isNull();
        assertThat(cache.read(path, partition, 999)).isNull();
    }

    @Test
    public void testEmptyMap() {
        DVMetaCache cache = new DVMetaCache(100);
        Path path = new Path("test");
        BinaryRow partition = partition("test");

        // Put empty map
        cache.put(path, partition, 1, new HashMap<>());

        // Should return empty map, not null
        Map<String, DeletionFile> result = cache.read(path, partition, 1);
        assertThat(result).isNotNull().isEmpty();
    }

    @Test
    public void testOverwrite() {
        DVMetaCache cache = new DVMetaCache(100);
        Path path = new Path("test");
        BinaryRow partition = partition("test");

        // Put initial data
        Map<String, DeletionFile> dvFiles1 = new HashMap<>();
        dvFiles1.put("data1.parquet", new DeletionFile("dv1.parquet", 0L, 100L, 1L));
        cache.put(path, partition, 1, dvFiles1);

        // Overwrite with new data
        Map<String, DeletionFile> dvFiles2 = new HashMap<>();
        dvFiles2.put("data2.parquet", new DeletionFile("dv2.parquet", 0L, 200L, 2L));
        cache.put(path, partition, 1, dvFiles2);

        // Should return new data
        Map<String, DeletionFile> result = cache.read(path, partition, 1);
        assertThat(result).hasSize(1);
        assertThat(result).containsKey("data2.parquet");
        assertThat(result).doesNotContainKey("data1.parquet");
    }

    @Test
    public void testCacheEviction() {
        DVMetaCache cache = new DVMetaCache(2); // Small cache size
        Path path = new Path("test");
        BinaryRow partition = partition("test");

        // Fill cache to capacity
        Map<String, DeletionFile> dvFiles1 = new HashMap<>();
        dvFiles1.put("data1.parquet", new DeletionFile("dv1.parquet", 0L, 100L, 1L));
        cache.put(path, partition, 1, dvFiles1);

        Map<String, DeletionFile> dvFiles2 = new HashMap<>();
        dvFiles2.put("data2.parquet", new DeletionFile("dv2.parquet", 0L, 100L, 2L));
        cache.put(path, partition, 2, dvFiles2);

        // Verify both entries are cached
        assertThat(cache.read(path, partition, 1)).isNotNull();
        assertThat(cache.read(path, partition, 2)).isNotNull();

        // Add third entry, should evict first one
        Map<String, DeletionFile> dvFiles3 = new HashMap<>();
        dvFiles3.put("data3.parquet", new DeletionFile("dv3.parquet", 0L, 100L, 3L));
        cache.put(path, partition, 3, dvFiles3);

        // First entry should be evicted
        assertThat(cache.read(path, partition, 1)).isNull();
        assertThat(cache.read(path, partition, 3)).isNotNull();
    }

    // ============================ Test utils ===================================

    private BinaryRow partition(String partitionValue) {
        InternalRowSerializer serializer =
                new InternalRowSerializer(RowType.of(DataTypes.STRING()));
        return serializer
                .toBinaryRow(GenericRow.of(BinaryString.fromString(partitionValue)))
                .copy();
    }
}
