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

package org.apache.flink.table.store.connector;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.store.file.utils.SnapshotFinder;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.store.file.FileStoreOptions.relativeTablePath;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for 'ALTER TABLE ... COMPACT'. */
public class AlterTableCompactITCase extends FileStoreTableITCase {

    @Override
    protected List<String> ddl() {
        return Arrays.asList(
                "CREATE TABLE IF NOT EXISTS T0 (f0 INT, f1 STRING, f2 DOUBLE)",
                "CREATE TABLE IF NOT EXISTS T1 ("
                        + "f0 INT, f1 STRING, f2 STRING) PARTITIONED BY (f1)",
                "CREATE TABLE IF NOT EXISTS T2 ("
                        + "f0 INT, f1 STRING, f2 STRING) PARTITIONED BY (f1, f0)");
    }

    @Test
    public void testNonPartitioned() throws Exception {
        batchSql("INSERT INTO T0 VALUES (1, 'Pride and Prejudice', 9.0), (2, 'Emma', 8.5)");
        batchSql(
                "INSERT INTO T0 VALUES (3, 'The Mansfield Park', 7.0), (4, 'Sense and Sensibility', 9.0)");
        batchSql("INSERT INTO T0 VALUES (5, 'Northanger Abby', 8.6)");
        batchSql(
                "INSERT INTO T0 VALUES (6, 'Jane Eyre', 9.9), (1, 'Pride and Prejudice', 9.0), (2, 'Emma', 8.5)");

        Long snapshot = findLatestSnapshotId("T0");
        List<Row> expected = batchSql("SELECT * FROM T0", 6);

        batchSql("ALTER TABLE T0 COMPACT");

        assertThat(batchSql("SELECT * FROM T0", 6)).containsExactlyInAnyOrderElementsOf(expected);
        assertThat(findLatestSnapshotId("T0")).isEqualTo(snapshot + 1);
    }

    @Test
    public void testSinglePartitioned() throws Exception {
        // increase trigger to avoid compaction
        batchSql("ALTER TABLE T1 SET ('num-sorted-run.compaction-trigger' = '20')");
        batchSql("ALTER TABLE T1 SET ('num-sorted-run.stop-trigger' = '20')");
        batchSql(
                "INSERT INTO T1 VALUES (1, 'Winter', 'Winter is Coming'),"
                        + " (2, 'Winter', 'The First Snowflake'),"
                        + " (2, 'Spring', 'The First Rose in Spring'),"
                        + " (7, 'Summer', 'Summertime Sadness')");
        batchSql("INSERT INTO T1 VALUES (12, 'Winter', 'Last Christmas')");
        batchSql("INSERT INTO T1 VALUES (11, 'Winter', 'Winter is Coming')");
        batchSql("INSERT INTO T1 VALUES (10, 'Autumn', 'Refrain')");
        batchSql(
                "INSERT INTO T1 VALUES (6, 'Summer', 'Watermelon Sugar'),"
                        + " (4, 'Spring', 'Spring Water')");
        batchSql(
                "INSERT INTO T1 VALUES (66, 'Summer', 'Summer Vibe'),"
                        + " (9, 'Autumn', 'Wake Me Up When September Ends')");
        batchSql(
                "INSERT INTO T1 VALUES (666, 'Summer', 'Summer Vibe'),"
                        + " (9, 'Autumn', 'Wake Me Up When September Ends')");
        batchSql(
                "INSERT INTO T1 VALUES (6666, 'Summer', 'Summer Vibe'),"
                        + " (9, 'Autumn', 'Wake Me Up When September Ends')");
        batchSql(
                "INSERT INTO T1 VALUES (66666, 'Summer', 'Summer Vibe'),"
                        + " (7, 'Summer', 'Summertime Sadness')");
        batchSql(
                "INSERT INTO T1 VALUES (66666, 'Summer', 'Summer Vibe'),"
                        + " (9, 'Autumn', 'Wake Me Up When September Ends')");
        batchSql(
                "INSERT INTO T1 VALUES (66666, 'Summer', 'Summer Vibe'),"
                        + " (7, 'Summer', 'Summertime Sadness')");

        Long snapshot = findLatestSnapshotId("T1");
        List<Row> expectedSummer = batchSql("SELECT * FROM T1 WHERE f1 = 'Summer'", 8);
        List<Row> expectedFourSeasons = batchSql("SELECT * FROM T1", 21);

        // compact a single partition
        batchSql("ALTER TABLE T1 PARTITION (f1 = 'Summer') COMPACT");
        assertThat(batchSql("SELECT * FROM T1 WHERE f1 = 'Summer'", 8))
                .containsExactlyElementsOf(expectedSummer);
        assertThat(findLatestSnapshotId("T1")).isEqualTo(snapshot + 1);

        // compact whole table
        batchSql("ALTER TABLE T1 COMPACT");
        assertThat(batchSql("SELECT * FROM T1", 21))
                .containsExactlyInAnyOrderElementsOf(expectedFourSeasons);
        assertThat(findLatestSnapshotId("T1")).isEqualTo(snapshot + 2);

        batchSql("ALTER TABLE T1 COMPACT");
        assertThat(findLatestSnapshotId("T1")).isEqualTo(snapshot + 2);
    }

    @Test
    public void testMultiPartitioned() throws Exception {
        // increase trigger to avoid compaction
        batchSql("ALTER TABLE T2 SET ('num-sorted-run.compaction-trigger' = '20')");
        batchSql("ALTER TABLE T2 SET ('num-sorted-run.stop-trigger' = '20')");

        batchSql(
                "INSERT INTO T2 VALUES (1, '2022-05-19', 'Hello'),"
                        + " (2, '2022-05-19', 'Bye'),"
                        + " (3, '2022-05-19', 'Meow')");

        batchSql(
                "INSERT INTO T2 VALUES (1, '2022-05-19', 'Bonjour'),"
                        + " (2, '2022-05-19', 'Ciao'),"
                        + " (3, '2022-05-19', 'Bark')");

        batchSql(
                "INSERT INTO T2 VALUES (1, '2022-05-20', 'Hasta la vista'),"
                        + " (1, '2022-05-19', 'Bonjour'),"
                        + " (2, '2022-05-19', 'Ciao'),"
                        + " (3, '2022-05-19', 'Bark')");

        Long snapshot = findLatestSnapshotId("T2");
        List<Row> theSecond = batchSql("SELECT * FROM T2 WHERE f0 = 2", 3);

        batchSql("ALTER TABLE T2 PARTITION (f0 = 2) COMPACT");
        assertThat(batchSql("SELECT * FROM T2 WHERE f0 = 2", 3))
                .containsExactlyInAnyOrderElementsOf(theSecond);
        assertThat(findLatestSnapshotId("T2")).isEqualTo(snapshot + 1);
    }

    private Long findLatestSnapshotId(String tableName) throws IOException {
        return SnapshotFinder.findLatest(
                Path.fromLocalFile(
                        new File(
                                URI.create(
                                        path
                                                + relativeTablePath(
                                                        ObjectIdentifier.of(
                                                                bEnv.getCurrentCatalog(),
                                                                bEnv.getCurrentDatabase(),
                                                                tableName))
                                                + "/snapshot"))));
    }
}
