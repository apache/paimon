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

package org.apache.paimon.table.source.snapshot;

import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link StaticFromTagStartingScanner}. */
public class StaticFromTagStartingScannerTest extends ScannerTestBase {

    @Test
    public void testScan() throws Exception {
        SnapshotManager snapshotManager = table.snapshotManager();
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);

        write.write(rowData(1, 10, 100L));
        write.write(rowData(1, 20, 200L));
        commit.commit(0, write.prepareCommit(true, 0));

        write.write(rowData(2, 30, 101L));
        write.write(rowData(2, 40, 201L));
        commit.commit(1, write.prepareCommit(true, 1));

        write.write(rowData(3, 50, 500L));
        write.write(rowData(3, 60, 600L));
        commit.commit(2, write.prepareCommit(true, 2));

        table.createTag("tag2", 2);

        StaticFromTagStartingScanner scanner =
                new StaticFromTagStartingScanner(snapshotManager, "tag2");
        StartingScanner.ScannedResult result =
                (StartingScanner.ScannedResult) scanner.scan(snapshotReader);
        assertThat(result.currentSnapshotId()).isEqualTo(2);
        assertThat(getResult(table.newRead(), toSplits(result.splits())))
                .hasSameElementsAs(
                        Arrays.asList("+I 1|10|100", "+I 1|20|200", "+I 2|30|101", "+I 2|40|201"));

        write.close();
        commit.close();
    }

    @Test
    public void testNonExistingTag() {
        SnapshotManager snapshotManager = table.snapshotManager();
        assertThatThrownBy(
                        () ->
                                new StaticFromTagStartingScanner(snapshotManager, "non-existing")
                                        .scan(snapshotReader))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Tag 'non-existing' doesn't exist"));
    }
}
