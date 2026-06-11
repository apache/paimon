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

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/** Tests for {@link HiveAlterTableUtils}. */
public class HiveAlterTableUtilsTest {

    @Test
    public void testAlterTableWithoutCascade() throws Exception {
        IMetaStoreClient client = mock(IMetaStoreClient.class);
        Identifier identifier = Identifier.create("db", "tbl");
        Table table = new Table();
        table.setParameters(new HashMap<>());

        HiveAlterTableUtils.alterTable(client, identifier, table, true, false);

        EnvironmentContext context = captureEnvironmentContext(client, table);
        assertThat(context.getProperties())
                .containsEntry(StatsSetupConst.DO_NOT_UPDATE_STATS, "true")
                .doesNotContainKey(StatsSetupConst.CASCADE);
    }

    @Test
    public void testAlterTableWithCascade() throws Exception {
        IMetaStoreClient client = mock(IMetaStoreClient.class);
        Identifier identifier = Identifier.create("db", "tbl");
        Table table = new Table();
        table.setParameters(new HashMap<>());

        HiveAlterTableUtils.alterTable(client, identifier, table, true, true);

        EnvironmentContext context = captureEnvironmentContext(client, table);
        assertThat(context.getProperties())
                .containsEntry(StatsSetupConst.DO_NOT_UPDATE_STATS, "true")
                .containsEntry(StatsSetupConst.CASCADE, "true");
    }

    private EnvironmentContext captureEnvironmentContext(IMetaStoreClient client, Table table)
            throws Exception {
        ArgumentCaptor<EnvironmentContext> captor =
                ArgumentCaptor.forClass(EnvironmentContext.class);
        verify(client)
                .alter_table_with_environmentContext(
                        eq("db"), eq("tbl"), same(table), captor.capture());
        return captor.getValue();
    }
}
