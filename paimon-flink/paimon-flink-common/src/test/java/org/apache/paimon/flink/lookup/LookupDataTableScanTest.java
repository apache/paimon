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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.OutOfRangeException;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.snapshot.ScannerTestBase;
import org.apache.paimon.utils.NextSnapshotFetcher;
import org.apache.paimon.utils.ProcedureUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;

import static org.apache.paimon.flink.lookup.LookupFileStoreTable.LookupStreamScanMode.CHANGELOG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests retained-history discovery boundaries for lookup scans. */
class LookupDataTableScanTest extends ScannerTestBase {

    @Override
    protected FileStoreTable createFileStoreTable() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.WRITE_ONLY, true);
        options.set(CoreOptions.CHANGELOG_PRODUCER, CoreOptions.ChangelogProducer.INPUT);
        options.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN, 1);
        options.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX, 4);
        options.set(CoreOptions.CHANGELOG_NUM_RETAINED_MAX, 4);
        return createFileStoreTable(options);
    }

    @Test
    void testAutomaticLookupFileMonitoringKeepsSnapshotExpiration() throws Exception {
        writeFourCommits();
        expireSnapshots();
        FileStoreTable reloaded = FileStoreTableFactory.create(fileIO, tablePath);
        LookupFileStoreTable lookup = new LookupFileStoreTable(reloaded, reloaded.primaryKeys());
        assertExpired(lookup.newStreamScan());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testLookupChangelogKeepsConfiguredDiscovery(boolean independent) throws Exception {
        writeFourCommits();
        expireSnapshots();
        FileStoreTable reloaded = FileStoreTableFactory.create(fileIO, tablePath);
        if (independent) {
            reloaded =
                    reloaded.copy(
                            Collections.singletonMap(
                                    CoreOptions.CHANGELOG_NUM_RETAINED_MAX.key(), "5"));
        }
        StreamTableScan scan = new LookupFileStoreTable(reloaded, CHANGELOG).newStreamScan();
        if (independent) {
            scan.restore(1L);
            for (int i = 1; i <= 4; i++) {
                assertThat(getResult(reloaded.newRead(), scan.plan().splits()))
                        .containsExactly("+I 1|" + i + "|" + i);
            }
            assertThat(scan.checkpoint()).isEqualTo(5L);
        } else {
            assertExpired(scan);
        }
    }

    @Test
    void testLookupFileMonitoringReadsAvailableSnapshots() throws Exception {
        writeFourCommits();
        FileStoreTable reloaded = FileStoreTableFactory.create(fileIO, tablePath);
        StreamTableScan scan =
                new LookupFileStoreTable(reloaded, reloaded.primaryKeys()).newStreamScan();
        scan.restore(1L);
        for (int i = 1; i <= 4; i++) {
            assertThat(getResult(reloaded.newRead(), scan.plan().splits()))
                    .containsExactly("+I 1|" + i + "|" + i);
        }
        assertThat(scan.checkpoint()).isEqualTo(5L);
    }

    private void writeFourCommits() throws Exception {
        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            for (int i = 1; i <= 4; i++) {
                write.write(rowData(1, i, (long) i));
                commit.commit(i, write.prepareCommit(true, i));
            }
        }
    }

    private void expireSnapshots() {
        table.newExpireSnapshots()
                .config(
                        ProcedureUtils.fillInSnapshotOptions(
                                        table.coreOptions(), 1, null, null, null)
                                .build())
                .expire();
        assertThat(table.snapshotManager().earliestSnapshotId()).isEqualTo(4L);
        assertThat(table.changelogManager().longLivedChangelogExists(1L)).isTrue();
    }

    private void assertExpired(StreamTableScan scan) {
        scan.restore(1L);
        assertThatThrownBy(
                        () -> {
                            for (int i = 0; i < NextSnapshotFetcher.RANGE_CHECK_INTERVAL; i++) {
                                assertThat(scan.plan().splits()).isEmpty();
                            }
                        })
                .isInstanceOf(OutOfRangeException.class);
        assertThat(scan.checkpoint()).isEqualTo(1L);
    }
}
