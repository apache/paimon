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

import org.apache.paimon.Snapshot;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL;
import static org.assertj.core.api.Assertions.assertThat;

/** Test case for append-only managed unaware-bucket table. */
public class UnawareBucketAppendOnlyTableITCase extends CatalogITCaseBase {

    @Override
    protected List<String> ddl() {
        return Collections.singletonList(
                "CREATE TABLE IF NOT EXISTS append_table (id INT, data STRING) WITH ('bucket' = '-1')");
    }

    @Test
    public void testCompactionInStreamingMode() throws Exception {
        batchSql("ALTER TABLE append_table SET ('compaction.min.file-num' = '2')");
        batchSql("ALTER TABLE append_table SET ('compaction.early-max.file-num' = '4')");
        batchSql("ALTER TABLE append_table SET ('continuous.discovery-interval' = '1 s')");

        sEnv.getConfig().getConfiguration().set(CHECKPOINTING_INTERVAL, Duration.ofMillis(500));
        sEnv.executeSql(
                "CREATE TEMPORARY TABLE Orders_in (\n"
                        + "    f0        INT,\n"
                        + "    f1        STRING\n"
                        + ") WITH (\n"
                        + "    'connector' = 'datagen',\n"
                        + "    'rows-per-second' = '1',\n"
                        + "    'number-of-rows' = '10'\n"
                        + ")");

        assertStreamingHasCompact("INSERT INTO append_table SELECT * FROM Orders_in", 60000);
        // ensure data gen finished
        Thread.sleep(5000);

        List<Row> rows = batchSql("SELECT * FROM append_table");
        assertThat(rows.size()).isEqualTo(10);
    }

    private void assertStreamingHasCompact(String sql, long timeout) throws Exception {
        long start = System.currentTimeMillis();
        long currentId = 1;
        sEnv.executeSql(sql);
        Snapshot snapshot;
        while (true) {
            snapshot = findSnapshot("append_table", currentId);
            if (snapshot != null) {
                if (snapshot.commitKind() == Snapshot.CommitKind.COMPACT) {
                    break;
                }
                currentId++;
            }
            long now = System.currentTimeMillis();
            if (now - start > timeout) {
                throw new RuntimeException(
                        "Time up for streaming execute, don't get expected result.");
            }
            Thread.sleep(1000);
        }
    }
}
