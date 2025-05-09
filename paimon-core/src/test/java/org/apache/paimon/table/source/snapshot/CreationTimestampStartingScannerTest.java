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

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CreationTimestampStartingScanner}. */
public class CreationTimestampStartingScannerTest extends ScannerTestBase {

    @Test
    public void test() throws Exception {
        SnapshotManager snapshotManager = table.snapshotManager();
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);

        write.write(rowData(1, 10, 100L));
        write.write(rowData(1, 20, 200L));
        write.write(rowData(1, 40, 400L));

        long t1 = System.currentTimeMillis();
        commit.commit(0, write.prepareCommit(true, 0));
        long t2 = System.currentTimeMillis();

        write.write(rowData(1, 11, 100L));
        write.write(rowData(1, 21, 200L));
        write.write(rowData(1, 41, 400L));
        commit.commit(0, write.prepareCommit(true, 0));
        long t3 = System.currentTimeMillis();
        write.close();
        commit.close();

        CreationTimestampStartingScanner scanner =
                new CreationTimestampStartingScanner(
                        snapshotManager, table.changelogManager(), t1, false, true);
        assertThat(scanner.scanner() instanceof FileCreationTimeStartingScanner).isTrue();
        scanner =
                new CreationTimestampStartingScanner(
                        snapshotManager, table.changelogManager(), t1, false, false);
        assertThat(scanner.scanner() instanceof FileCreationTimeStartingScanner).isTrue();

        scanner =
                new CreationTimestampStartingScanner(
                        snapshotManager, table.changelogManager(), t2, false, true);
        assertThat(scanner.scanner() instanceof ContinuousFromTimestampStartingScanner).isTrue();
        scanner =
                new CreationTimestampStartingScanner(
                        snapshotManager, table.changelogManager(), t2, false, false);
        assertThat(scanner.scanner() instanceof StaticFromTimestampStartingScanner).isTrue();

        scanner =
                new CreationTimestampStartingScanner(
                        snapshotManager, table.changelogManager(), t3, false, true);
        assertThat(scanner.scanner() instanceof FileCreationTimeStartingScanner).isTrue();
        scanner =
                new CreationTimestampStartingScanner(
                        snapshotManager, table.changelogManager(), t3, false, false);
        assertThat(scanner.scanner() instanceof FileCreationTimeStartingScanner).isTrue();
    }
}
