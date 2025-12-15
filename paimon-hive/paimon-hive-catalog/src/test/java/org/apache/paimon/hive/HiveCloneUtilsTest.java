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

package org.apache.paimon.hive;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.hive.clone.HiveCloneUtils;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import static org.assertj.core.api.Assertions.assertThatList;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Test for {@link HiveCloneUtils}. */
public class HiveCloneUtilsTest {

    @Test
    public void listAllTablesForCatalog() throws Exception {
        HiveCatalog mockHiveCatalog = mock(HiveCatalog.class);
        IMetaStoreClient mockClient = mock(IMetaStoreClient.class);
        when(mockHiveCatalog.getHmsClient()).thenReturn(mockClient);
        when(mockClient.getAllDatabases()).thenReturn(Arrays.asList("db1", "db2"));
        when(mockClient.getAllTables("db1")).thenReturn(Arrays.asList("tbl1", "tbl2", "tbl3"));
        when(mockClient.getAllTables("db2")).thenReturn(Arrays.asList("tbl1", "tbl2", "tbl3"));

        List<Identifier> sourceTables = HiveCloneUtils.listTables(mockHiveCatalog, null, null);
        List<Identifier> expectedTables =
                Arrays.asList(
                        Identifier.create("db1", "tbl1"),
                        Identifier.create("db1", "tbl2"),
                        Identifier.create("db1", "tbl3"),
                        Identifier.create("db2", "tbl1"),
                        Identifier.create("db2", "tbl2"),
                        Identifier.create("db2", "tbl3"));
        assertThatList(sourceTables).containsExactlyInAnyOrderElementsOf(expectedTables);

        sourceTables =
                HiveCloneUtils.listTables(
                        mockHiveCatalog, null, Arrays.asList("db1.tbl3", "db2.tbl1"));
        expectedTables =
                Arrays.asList(
                        Identifier.create("db1", "tbl1"),
                        Identifier.create("db1", "tbl2"),
                        Identifier.create("db2", "tbl2"),
                        Identifier.create("db2", "tbl3"));
        assertThatList(sourceTables).containsExactlyInAnyOrderElementsOf(expectedTables);

        sourceTables =
                HiveCloneUtils.listTables(
                        mockHiveCatalog, Arrays.asList("db1.tbl3", "db2.tbl1"), null);
        expectedTables =
                Arrays.asList(Identifier.create("db1", "tbl3"), Identifier.create("db2", "tbl1"));
        assertThatList(sourceTables).containsExactlyInAnyOrderElementsOf(expectedTables);

        sourceTables =
                HiveCloneUtils.listTables(
                        mockHiveCatalog,
                        Arrays.asList("db1.tbl3", "db2.tbl1"),
                        Arrays.asList("db1.tbl3"));
        expectedTables = Arrays.asList(Identifier.create("db2", "tbl1"));
        assertThatList(sourceTables).containsExactlyInAnyOrderElementsOf(expectedTables);
    }

    @Test
    public void listAllTablesForDatabase() throws Exception {
        HiveCatalog mockHiveCatalog = mock(HiveCatalog.class);
        IMetaStoreClient mockClient = mock(IMetaStoreClient.class);
        when(mockHiveCatalog.getHmsClient()).thenReturn(mockClient);
        when(mockClient.getAllTables("db1")).thenReturn(Arrays.asList("tbl1", "tbl2", "tbl3"));

        List<Identifier> sourceTables =
                HiveCloneUtils.listTables(mockHiveCatalog, "db1", null, null);
        List<Identifier> expectedTables =
                Arrays.asList(
                        Identifier.create("db1", "tbl1"),
                        Identifier.create("db1", "tbl2"),
                        Identifier.create("db1", "tbl3"));
        assertThatList(sourceTables).containsExactlyInAnyOrderElementsOf(expectedTables);

        sourceTables =
                HiveCloneUtils.listTables(mockHiveCatalog, "db1", null, Arrays.asList("db1.tbl1"));
        expectedTables =
                Arrays.asList(Identifier.create("db1", "tbl2"), Identifier.create("db1", "tbl3"));
        assertThatList(sourceTables).containsExactlyInAnyOrderElementsOf(expectedTables);

        sourceTables =
                HiveCloneUtils.listTables(mockHiveCatalog, "db1", Arrays.asList("db1.tbl1"), null);
        expectedTables = Arrays.asList(Identifier.create("db1", "tbl1"));
        assertThatList(sourceTables).containsExactlyInAnyOrderElementsOf(expectedTables);

        sourceTables =
                HiveCloneUtils.listTables(
                        mockHiveCatalog,
                        "db1",
                        Arrays.asList("db1.tbl1", "db1.tbl2"),
                        Arrays.asList("db1.tbl1"));
        expectedTables = Arrays.asList(Identifier.create("db1", "tbl2"));
        assertThatList(sourceTables).containsExactlyInAnyOrderElementsOf(expectedTables);
    }
}
