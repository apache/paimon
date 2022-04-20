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

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import org.apache.commons.compress.utils.Lists;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.List;

/** Test case for append-only managed table. */
public class TestAppendOnlyTable {
    @ClassRule public static final TemporaryFolder TEMP = new TemporaryFolder();

    @ClassRule
    public static final MiniClusterWithClientResource MINI_CLUSTER =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(4)
                            .build());

    private volatile TableEnvironment tEnv;

    @Test
    public void testReadWrite() throws IOException {
        sql(
                "CREATE TABLE append_table (id INT, data STRING) WITH ('write-mode'='append-only', 'path'='%s')",
                TEMP.newFolder().getAbsolutePath());
        sql("INSERT INTO append_table VALUES (1, 'AAA'), (2, 'BBB')");

        List<Row> rows = sql("SELECT * FROM append_table");
        Assert.assertEquals(2, rows.size());
        Assert.assertEquals(ImmutableList.of(Row.of(1, "AAA"), Row.of(2, "BBB")), rows);

        rows = sql("SELECT id FROM append_table");
        Assert.assertEquals(2, rows.size());
        Assert.assertEquals(ImmutableList.of(Row.of(1), Row.of(2)), rows);

        rows = sql("SELECT data from append_table");
        Assert.assertEquals(2, rows.size());
        Assert.assertEquals(ImmutableList.of(Row.of("AAA"), Row.of("BBB")), rows);
    }

    private List<Row> sql(String query, Object... args) {
        TableResult tableResult = tableEnv().executeSql(String.format(query, args));

        try (CloseableIterator<Row> iter = tableResult.collect()) {
            return Lists.newArrayList(iter);
        } catch (Exception e) {
            throw new RuntimeException("Failed to collect the table result.", e);
        }
    }

    private TableEnvironment tableEnv() {
        if (tEnv == null) {
            synchronized (this) {
                if (tEnv == null) {
                    EnvironmentSettings.Builder settingsBuilder =
                            EnvironmentSettings.newInstance().inBatchMode();
                    tEnv = TableEnvironment.create(settingsBuilder.build());
                }
            }
        }
        return tEnv;
    }
}
