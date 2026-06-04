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

package org.apache.paimon.flink;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.List;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Batch IT for LIMIT push-down on primary key tables with multiple level-0 files (merge read path).
 */
public class MergeFileSplitReadLimitITCase extends CatalogITCaseBase {

    private static final int TOTAL_ROWS = 100;
    private static final int COMMITS = 5;
    private static final int LIMIT = 10;

    @Override
    protected List<String> ddl() {
        return singletonList(
                "CREATE TABLE IF NOT EXISTS pk_l0_limit ("
                        + "id INT, v INT, PRIMARY KEY (id) NOT ENFORCED) "
                        + "WITH ("
                        + "'bucket' = '1', "
                        + "'write-only' = 'true', "
                        + "'changelog-producer' = 'none', "
                        + "'merge-engine' = 'deduplicate')");
    }

    @Nullable
    @Override
    protected Boolean sqlSyncMode() {
        return true;
    }

    @Test
    public void testLimitOnMultiLevel0PrimaryKeyTable() {
        writeMultipleCommits();

        long level0FileCount =
                batchSql("SELECT file_path FROM `pk_l0_limit$files` WHERE level = 0").size();
        assertThat(level0FileCount)
                .as("Need multiple level-0 files to exercise merge read path")
                .isGreaterThan(1);

        List<Row> limited =
                batchSql(
                        String.format(
                                "SELECT * FROM pk_l0_limit /*+ OPTIONS('scan.infer-parallelism'='false', 'scan.parallelism'='1') */ LIMIT %d",
                                LIMIT));
        assertThat(limited).hasSize(LIMIT);

        List<Row> all = batchSql("SELECT * FROM pk_l0_limit");
        assertThat(all).hasSize(TOTAL_ROWS);
    }

    @Test
    public void testLimitWithValueFilterReturnsCorrectRows() {
        writeMultipleCommits();

        List<Row> filtered =
                batchSql(
                        String.format(
                                "SELECT * FROM pk_l0_limit /*+ OPTIONS('scan.infer-parallelism'='false', 'scan.parallelism'='1') */ "
                                        + "WHERE v > %d LIMIT %d",
                                TOTAL_ROWS / 2, LIMIT));
        assertThat(filtered).hasSize(LIMIT);
        for (Row row : filtered) {
            assertThat((int) row.getField(1)).isGreaterThan(TOTAL_ROWS / 2);
        }
    }

    private void writeMultipleCommits() {
        int rowsPerCommit = TOTAL_ROWS / COMMITS;
        IntStream.range(0, COMMITS)
                .forEach(
                        commit -> {
                            StringBuilder values = new StringBuilder();
                            for (int i = 0; i < rowsPerCommit; i++) {
                                int id = commit * rowsPerCommit + i;
                                if (i > 0) {
                                    values.append(", ");
                                }
                                values.append(String.format("(%d, %d)", id, id));
                            }
                            batchSql("INSERT INTO pk_l0_limit VALUES " + values);
                        });
    }
}
