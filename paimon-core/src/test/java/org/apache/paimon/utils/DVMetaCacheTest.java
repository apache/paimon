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
        Path path = new Path("manifest/index-manifest-00001");
        BinaryRow partition = partition("year=2023/month=12");

        // Put data for bucket 1 with multiple files
        Map<String, DeletionFile> dvFiles1 = new HashMap<>();
        dvFiles1.put(
                "data-a1b2c3d4-e5f6-7890-abcd-ef1234567890-1.parquet",
                new DeletionFile("index-a1b2c3d4-e5f6-7890-abcd-ef1234567890-1", 0L, 100L, 42L));
        dvFiles1.put(
                "data-a1b2c3d4-e5f6-7890-abcd-ef1234567890-2.parquet",
                new DeletionFile("index-a1b2c3d4-e5f6-7890-abcd-ef1234567890-1", 100L, 500L, null));
        cache.put(path, partition, 1, dvFiles1);

        // Put data for bucket 2 with single file
        Map<String, DeletionFile> dvFiles2 = new HashMap<>();
        dvFiles2.put(
                "data-b2c3d4e5-f6g7-8901-bcde-f23456789012-1.parquet",
                new DeletionFile("index-b2c3d4e5-f6g7-8901-bcde-f23456789012-1", 0L, 300L, 12L));
        cache.put(path, partition, 2, dvFiles2);

        // Read bucket 1 - verify multiple files
        Map<String, DeletionFile> result1 = cache.read(path, partition, 1);
        assertThat(result1).isNotNull().hasSize(2);
        assertThat(result1)
                .containsKeys(
                        "data-a1b2c3d4-e5f6-7890-abcd-ef1234567890-1.parquet",
                        "data-a1b2c3d4-e5f6-7890-abcd-ef1234567890-2.parquet");

        DeletionFile file1 = result1.get("data-a1b2c3d4-e5f6-7890-abcd-ef1234567890-1.parquet");
        assertThat(file1.path()).isEqualTo("index-a1b2c3d4-e5f6-7890-abcd-ef1234567890-1");
        assertThat(file1.offset()).isEqualTo(0L);
        assertThat(file1.length()).isEqualTo(100L);
        assertThat(file1.cardinality()).isEqualTo(42L);

        DeletionFile file2 = result1.get("data-a1b2c3d4-e5f6-7890-abcd-ef1234567890-2.parquet");
        assertThat(file2.path()).isEqualTo("index-a1b2c3d4-e5f6-7890-abcd-ef1234567890-1");
        assertThat(file2.cardinality()).isNull();

        // Read bucket 2 - verify single file
        Map<String, DeletionFile> result2 = cache.read(path, partition, 2);
        assertThat(result2).isNotNull().hasSize(1);
        assertThat(result2).containsKey("data-b2c3d4e5-f6g7-8901-bcde-f23456789012-1.parquet");

        // Read non-existent key
        assertThat(cache.read(path, partition("year=2024/month=01"), 0)).isNull();
        assertThat(cache.read(path, partition, 999)).isNull();
    }

    @Test
    public void testEmptyMap() {
        DVMetaCache cache = new DVMetaCache(100);
        Path path = new Path("manifest/index-manifest-00002");
        BinaryRow partition = partition("year=2023/month=11");
        cache.put(path, partition, 1, new HashMap<>());

        // Should return empty map, not null
        Map<String, DeletionFile> result = cache.read(path, partition, 1);
        assertThat(result).isNotNull().isEmpty();
    }

    @Test
    public void testCacheEviction() {
        DVMetaCache cache = new DVMetaCache(2);
        Path path = new Path("manifest/index-manifest-00004");
        BinaryRow partition = partition("year=2023/month=09");

        // Fill cache to capacity
        Map<String, DeletionFile> dvFiles1 = new HashMap<>();
        dvFiles1.put(
                "data-e5f6g7h8-i9j0-1234-efgh-567890123456-1.parquet",
                new DeletionFile("index-e5f6g7h8-i9j0-1234-efgh-567890123456-1", 0L, 100L, 1L));
        dvFiles1.put(
                "data-e5f6g7h8-i9j0-1234-efgh-567890123456-2.parquet",
                new DeletionFile("index-e5f6g7h8-i9j0-1234-efgh-567890123456-1", 100L, 200L, 2L));
        cache.put(path, partition, 1, dvFiles1);

        Map<String, DeletionFile> dvFiles2 = new HashMap<>();
        dvFiles2.put(
                "data-f6g7h8i9-j0k1-2345-fghi-678901234567-1.parquet",
                new DeletionFile("index-f6g7h8i9-j0k1-2345-fghi-678901234567-1", 0L, 100L, 2L));
        cache.put(path, partition, 2, dvFiles2);

        // Verify both buckets are cached
        assertThat(cache.read(path, partition, 1)).isNotNull();
        assertThat(cache.read(path, partition, 2)).isNotNull();

        // Add third entry, should evict first one
        Map<String, DeletionFile> dvFiles3 = new HashMap<>();
        dvFiles3.put(
                "data-g7h8i9j0-k1l2-3456-ghij-789012345678-1.parquet",
                new DeletionFile("index-g7h8i9j0-k1l2-3456-ghij-789012345678-1", 0L, 100L, 3L));
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
