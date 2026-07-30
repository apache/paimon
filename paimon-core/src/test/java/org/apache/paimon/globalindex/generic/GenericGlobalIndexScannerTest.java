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

package org.apache.paimon.globalindex.generic;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.globalindex.ScanResult;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link GenericGlobalIndexScanner}. */
public class GenericGlobalIndexScannerTest extends TableTestBase {

    @Override
    public Schema schemaDefault() {
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("v", DataTypes.STRING())
                .option(CoreOptions.BUCKET.key(), "-1")
                .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                .build();
    }

    @Test
    public void testScan() throws Exception {
        FileStoreTable table = writeRows();
        long snapshotId = table.snapshotManager().latestSnapshot().id();

        ScanResult<ManifestEntry> scanResult =
                new GenericGlobalIndexScanner(table)
                        .scan()
                        .orElseThrow(() -> new IllegalStateException("Expected scan result."));

        assertThat(scanResult.scanSnapshotId()).isEqualTo(snapshotId);
        assertThat(scanResult.entries()).isNotEmpty();
        assertThat(scanResult.rowRangeIndex().ranges()).containsExactly(new Range(0, 9));
        assertThat(scanResult.deletedIndexEntries()).isEmpty();
    }

    @Test
    public void testIncrementalScanWithoutExistingIndex() throws Exception {
        FileStoreTable table = writeRows();

        ScanResult<ManifestEntry> scanResult =
                new GenericGlobalIndexScanner(table)
                        .withIndex("test-index", Collections.singletonList("v"), new Options())
                        .incrementalScan()
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Expected incremental scan result."));

        assertThat(scanResult.entries()).isNotEmpty();
        assertThat(scanResult.rowRangeIndex().ranges()).containsExactly(new Range(0, 9));
        assertThat(scanResult.deletedIndexEntries()).isEmpty();
    }

    @Test
    public void testScanEmptyTable() throws Exception {
        createTableDefault();

        assertThat(new GenericGlobalIndexScanner(getTableDefault()).scan()).isEmpty();
    }

    private FileStoreTable writeRows() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite()) {
            for (int i = 0; i < 10; i++) {
                write.write(GenericRow.of(i, BinaryString.fromString("v-" + i)));
            }
            try (BatchTableCommit commit = builder.newCommit()) {
                commit.commit(write.prepareCommit());
            }
        }
        return table;
    }
}
