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

import org.apache.paimon.flink.util.AbstractTestBase;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for postpone bucket tables. */
public class PostponeBucketTableITCase extends AbstractTestBase {

    @Test
    public void testWriteThenCompact() throws Exception {
        String warehouse = getTempDirPath();
        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();

        tEnv.executeSql(
                "CREATE CATALOG mycat WITH (\n"
                        + "  'type' = 'paimon',\n"
                        + "  'warehouse' = '"
                        + warehouse
                        + "'\n"
                        + ")");
        tEnv.executeSql("USE CATALOG mycat");
        tEnv.executeSql(
                "CREATE TABLE T (\n"
                        + "  pt INT,\n"
                        + "  k INT,\n"
                        + "  v INT,\n"
                        + "  PRIMARY KEY (pt, k) NOT ENFORCED\n"
                        + ") PARTITIONED BY (pt) WITH (\n"
                        + "  'bucket' = '-2'\n"
                        + ")");

        int numPartitions = 3;
        int numKeys = 100;
        List<String> values = new ArrayList<>();
        for (int i = 0; i < numPartitions; i++) {
            for (int j = 0; j < numKeys; j++) {
                values.add(String.format("(%d, %d, %d)", i, j, i * numKeys + j));
            }
        }
        tEnv.executeSql("INSERT INTO T VALUES " + String.join(", ", values)).await();
        assertThat(collect(tEnv.executeSql("SELECT * FROM T"))).isEmpty();
    }

    private List<Row> collect(TableResult result) throws Exception {
        List<Row> ret = new ArrayList<>();
        try (CloseableIterator<Row> it = result.collect()) {
            while (it.hasNext()) {
                ret.add(it.next());
            }
        }
        return ret;
    }
}
