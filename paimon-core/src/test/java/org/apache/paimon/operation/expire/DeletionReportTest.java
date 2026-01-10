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

package org.apache.paimon.operation.expire;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.utils.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DeletionReport}. */
public class DeletionReportTest {

    @Test
    public void testSerialization() throws IOException, ClassNotFoundException {
        DeletionReport report = new DeletionReport(1L);
        report.setDataFilesDeleted(true);

        DeletionReport deserialized = InstantiationUtil.clone(report);
        assertThat(deserialized.snapshotId()).isEqualTo(1L);
        assertThat(deserialized.deletionBuckets()).isEmpty();
    }

    @Test
    public void testSerializationWithBuckets() throws IOException, ClassNotFoundException {
        DeletionReport report = new DeletionReport(2L);
        Map<BinaryRow, Set<Integer>> buckets = new HashMap<>();

        BinaryRow partition1 = createBinaryRow(10);
        buckets.put(partition1, Collections.singleton(1));

        report.setDeletionBuckets(buckets);

        // BinaryRow supports standard Java serialization via BinarySection's
        // writeObject/readObject.
        // This test confirms that DeletionReport with BinaryRow keys can be serialized.
        DeletionReport deserialized = InstantiationUtil.clone(report);
        assertThat(deserialized.snapshotId()).isEqualTo(2L);
        assertThat(deserialized.deletionBuckets()).hasSize(1);
        Map.Entry<BinaryRow, Set<Integer>> entry =
                deserialized.deletionBuckets().entrySet().iterator().next();
        assertThat(entry.getKey()).isEqualTo(partition1);
        assertThat(entry.getValue()).containsExactly(1);
    }

    private BinaryRow createBinaryRow(int i) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, i);
        writer.complete();
        return row;
    }
}
