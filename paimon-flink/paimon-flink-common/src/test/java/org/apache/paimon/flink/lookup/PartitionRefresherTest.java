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

package org.apache.paimon.flink.lookup;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.utils.Filter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PartitionRefresher}. */
class PartitionRefresherTest {

    @TempDir private Path tempDir;

    @Test
    void testRefreshResultKeepsGenerationConsistent() throws Exception {
        List<BinaryRow> initialPartitions = partitions(1);
        List<BinaryRow> refreshedPartitions = partitions(2);
        File refreshPath = tempDir.resolve("refresh-2").toFile();
        TestingLookupTable refreshedTable = new TestingLookupTable();
        PartitionRefresher refresher = createRefresher(initialPartitions);

        try {
            refresher.publishRefreshResult(refreshedTable, refreshedPartitions, refreshPath);

            assertThat(refresher.getNewLookupTable()).isSameAs(refreshedTable);
            assertThat(refresher.currentPartitions()).isSameAs(refreshedPartitions);
            assertThat(refresher.path()).isEqualTo(refreshPath);
            assertThat(refreshedTable.closed).isFalse();
        } finally {
            refresher.close();
            refreshedTable.close();
        }
    }

    @Test
    void testPublishingNewResultClosesUnconsumedTable() throws Exception {
        List<BinaryRow> partitionsB = partitions(2);
        List<BinaryRow> partitionsC = partitions(3);
        File pathB = tempDir.resolve("refresh-2").toFile();
        File pathC = tempDir.resolve("refresh-3").toFile();
        TestingLookupTable tableB = new TestingLookupTable();
        TestingLookupTable tableC = new TestingLookupTable();
        PartitionRefresher refresher = createRefresher(partitions(1));

        try {
            refresher.publishRefreshResult(tableB, partitionsB, pathB);
            refresher.publishRefreshResult(tableC, partitionsC, pathC);

            assertThat(tableB.closed).isTrue();
            assertThat(tableC.closed).isFalse();
            assertThat(refresher.getNewLookupTable()).isSameAs(tableC);
            assertThat(refresher.currentPartitions()).isSameAs(partitionsC);
            assertThat(refresher.path()).isEqualTo(pathC);
        } finally {
            refresher.close();
            tableB.close();
            tableC.close();
        }
    }

    private PartitionRefresher createRefresher(List<BinaryRow> initialPartitions) {
        return new PartitionRefresher(true, "table", tempDir.toString(), initialPartitions);
    }

    private static List<BinaryRow> partitions(int value) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, value);
        writer.complete();
        return Collections.singletonList(row);
    }

    private static class TestingLookupTable implements LookupTable {

        private boolean closed;

        @Override
        public void specifyPartitions(List<BinaryRow> scanPartitions, Predicate partitionFilter) {}

        @Override
        public void open() {}

        @Override
        public List<InternalRow> get(InternalRow key) {
            return Collections.emptyList();
        }

        @Override
        public void refresh() {}

        @Override
        public void specifyCacheRowFilter(Filter<InternalRow> filter) {}

        @Override
        public Long nextSnapshotId() {
            return null;
        }

        @Override
        public void close() throws IOException {
            closed = true;
        }
    }
}
