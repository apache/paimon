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

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.store.file.catalog.FileSystemCatalog;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.util.CollectionUtil.iteratorToList;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for {@link FileSystemCatalog}. */
public class FileSystemCatalogITCase extends AbstractTestBase {

    private TableEnvironment tEnv;

    @Before
    public void before() throws IOException {
        tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        String path = TEMPORARY_FOLDER.newFolder().toURI().toString();
        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG fs WITH ('type'='table-store', 'root-path'='%s')", path));
        tEnv.useCatalog("fs");
    }

    @Test
    public void testWriteRead() throws ExecutionException, InterruptedException {
        tEnv.executeSql("CREATE TABLE T (a STRING, b STRING, c STRING)");
        tEnv.executeSql("INSERT INTO T VALUES ('1', '2', '3'), ('4', '5', '6')").await();
        List<Row> result = iteratorToList(tEnv.from("T").execute().collect());
        assertThat(result).containsExactlyInAnyOrder(Row.of("1", "2", "3"), Row.of("4", "5", "6"));
    }
}
