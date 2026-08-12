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

package org.apache.paimon.manifest;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ManifestEntrySortKey}. */
public class ManifestEntrySortKeyTest {

    // partition type: (dt INT, region STRING)
    private final List<DataType> partitionFieldTypes =
            RowType.of(DataTypes.INT(), DataTypes.STRING()).getFieldTypes();

    private ManifestEntrySortKey key(int dt, String region, int bucket, int level) {
        return key(dt, region, bucket, level, "f1");
    }

    private ManifestEntrySortKey key(int dt, String region, int bucket, int level, String file) {
        return new ManifestEntrySortKey(
                partition(dt, region), bucket, level, file, partitionFieldTypes);
    }

    private BinaryRow partition(int dt, String region) {
        BinaryRow row = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, dt);
        writer.writeString(1, BinaryString.fromString(region));
        writer.complete();
        return row;
    }

    @Test
    public void testOrderByPartitionFirst() {
        // same bucket & level, different partition -> ordered by partition
        assertThat(key(1, "a", 0, 0)).isLessThan(key(2, "a", 0, 0));
        assertThat(key(2, "a", 0, 0)).isGreaterThan(key(1, "a", 0, 0));
    }

    @Test
    public void testPartitionComparesAllFields() {
        // dt equal, region differs -> ordered by region (second partition field)
        assertThat(key(1, "a", 0, 0)).isLessThan(key(1, "b", 0, 0));
        assertThat(key(1, "b", 0, 0)).isGreaterThan(key(1, "a", 0, 0));
        // dt differs -> dt wins regardless of region
        assertThat(key(2, "a", 0, 0)).isGreaterThan(key(1, "z", 0, 0));
    }

    @Test
    public void testOrderByBucketWhenPartitionEquals() {
        assertThat(key(1, "a", 0, 0)).isLessThan(key(1, "a", 1, 0));
        assertThat(key(1, "a", 1, 0)).isGreaterThan(key(1, "a", 0, 0));
    }

    @Test
    public void testOrderByLevelWhenPartitionAndBucketEqual() {
        assertThat(key(1, "a", 0, 0)).isLessThan(key(1, "a", 0, 1));
        assertThat(key(1, "a", 0, 1)).isGreaterThan(key(1, "a", 0, 0));
    }

    @Test
    public void testOrderByFileNameWhenPartitionBucketLevelEqual() {
        // same (p,b,l), different fileName -> ordered by fileName
        assertThat(key(1, "a", 0, 0, "f1")).isLessThan(key(1, "a", 0, 0, "f2"));
        assertThat(key(1, "a", 0, 0, "f2")).isGreaterThan(key(1, "a", 0, 0, "f1"));
    }

    @Test
    public void testAddAndDeleteShareSameKey() {
        // ADD and DELETE of the same file share the same sort key, so they always land in the
        // same Spark partition after sortByKey and can be cancelled.
        ManifestEntrySortKey add = key(1, "a", 0, 0, "f1");
        ManifestEntrySortKey delete = key(1, "a", 0, 0, "f1");
        assertThat(add.compareTo(delete)).isEqualTo(0);
        assertThat(add).isEqualTo(delete);
    }

    @Test
    public void testGlobalSort() {
        // build keys in a deliberately shuffled order
        List<ManifestEntrySortKey> keys =
                new ArrayList<>(
                        Arrays.asList(
                                key(2, "a", 1, 0, "f1"),
                                key(1, "b", 0, 1, "f1"),
                                key(1, "a", 0, 0, "f2"),
                                key(1, "a", 0, 0, "f1"),
                                key(2, "a", 0, 0, "f2"),
                                key(1, "a", 0, 1, "f1"),
                                key(2, "a", 0, 0, "f1"),
                                key(1, "a", 1, 0, "f1")));
        Collections.sort(keys);

        // expected: partition asc -> bucket asc -> level asc -> fileName asc
        List<ManifestEntrySortKey> expected =
                Arrays.asList(
                        key(1, "a", 0, 0, "f1"),
                        key(1, "a", 0, 0, "f2"),
                        key(1, "a", 0, 1, "f1"),
                        key(1, "a", 1, 0, "f1"),
                        key(1, "b", 0, 1, "f1"),
                        key(2, "a", 0, 0, "f1"),
                        key(2, "a", 0, 0, "f2"),
                        key(2, "a", 1, 0, "f1"));
        assertThat(keys).isEqualTo(expected);
    }

    @Test
    public void testEqualsAndHashCode() {
        // keys with the same partition/bucket/level/fileName are equal even if built from different
        // BinaryRow instances, because the partition is compared by serialized bytes
        ManifestEntrySortKey a = key(1, "a", 3, 5, "f1");
        ManifestEntrySortKey b = key(1, "a", 3, 5, "f1");
        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());

        // any field difference breaks equality
        assertThat(a).isNotEqualTo(key(2, "a", 3, 5, "f1"));
        assertThat(a).isNotEqualTo(key(1, "b", 3, 5, "f1"));
        assertThat(a).isNotEqualTo(key(1, "a", 4, 5, "f1"));
        assertThat(a).isNotEqualTo(key(1, "a", 3, 6, "f1"));
        assertThat(a).isNotEqualTo(key(1, "a", 3, 5, "f2"));
    }

    @Test
    public void testSurvivesSerialization() throws Exception {
        // the comparator and partition are transient; after Java serialization they must be
        // rebuilt lazily and comparison must still work
        ManifestEntrySortKey original = key(1, "a", 0, 0, "f1");

        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        try (java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(baos)) {
            oos.writeObject(original);
        }
        try (java.io.ObjectInputStream ois =
                new java.io.ObjectInputStream(
                        new java.io.ByteArrayInputStream(baos.toByteArray()))) {
            ManifestEntrySortKey roundTripped = (ManifestEntrySortKey) ois.readObject();
            assertThat(roundTripped.compareTo(key(1, "a", 0, 0, "f1"))).isEqualTo(0);
            assertThat(roundTripped).isLessThan(key(2, "a", 0, 0, "f1"));
            assertThat(roundTripped).isLessThan(key(1, "a", 0, 0, "f2"));
        }
    }
}
