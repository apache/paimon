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
import org.apache.paimon.table.sink.TableCommitApi;
import org.apache.paimon.table.source.snapshot.ScannerTestBase;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

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
        TableCommitApi commit = table.newCommit(commitUser);
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
}
