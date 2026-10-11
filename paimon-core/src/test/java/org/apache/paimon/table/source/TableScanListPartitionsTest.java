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

package org.apache.paimon.table.source;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.source.snapshot.ScannerTestBase;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TableScan} listPartitions. */
public class TableScanListPartitionsTest extends ScannerTestBase {

    @Test
    public void testListPartitions() throws Exception {
        BatchTableWrite write = table.newWrite(commitUser);

        for (int i = 0; i < 1000; i++) {
            InternalRow row = GenericRow.of(i, i, Long.valueOf(i));
            write.write(row);
        }
        List<CommitMessage> result = write.prepareCommit();
        TableCommitImpl commit = table.newCommit(commitUser);
        commit.commit(result);

        AtomicInteger ai = new AtomicInteger(0);

        BinaryRow[] rows =
                table.newReadBuilder().newScan().listPartitions().toArray(new BinaryRow[0]);
        assertThat(rows.length).isEqualTo(1000);
        Arrays.sort(rows, Comparator.comparing(o -> o.getInt(0)));

        for (BinaryRow row : rows) {
            assertThat(row.getInt(0)).isEqualTo(ai.getAndIncrement());
        }
        commit.close();
    }

    @Test
    public void testTopNPartitions() throws Exception {
        createAppendOnlyTable();

        BatchTableWrite write = table.newWrite(commitUser);
        write.write(GenericRow.of(null, 1, 1L));
        TableCommitImpl commit = table.newCommit(commitUser);
        commit.commit(write.prepareCommit());
        write.close();
        commit.close();

        assertThat(table.newReadBuilder().newScan().topNPartitions(1, 1))
                .singleElement()
                .satisfies(partition -> assertThat(partition.isNullAt(0)).isTrue());

        write = table.newWrite(commitUser);
        write.write(GenericRow.of(9, 1, 1L));
        write.write(GenericRow.of(10, 1, 1L));
        write.write(GenericRow.of(2, 1, 1L));
        commit = table.newCommit(commitUser);
        commit.commit(write.prepareCommit());
        write.close();
        commit.close();

        assertThat(table.newReadBuilder().newScan().topNPartitions(2, 1))
                .extracting(row -> row.getInt(0))
                .containsExactly(10, 9);

        assertThat(
                        table.newReadBuilder()
                                .withPartitionFilter(Collections.singletonMap("pt", "9"))
                                .newScan()
                                .topNPartitions(1, 1))
                .extracting(row -> row.getInt(0))
                .containsExactly(9);

        TableScan scan = table.newReadBuilder().newScan();
        assertThatThrownBy(() -> scan.topNPartitions(0, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> scan.topNPartitions(1, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> scan.topNPartitions(1, 2))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
