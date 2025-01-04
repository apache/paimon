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

package org.apache.paimon.flink.sink.partition;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataTypes;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.apache.paimon.CoreOptions.PARTITION_MARK_DONE_ACTION;
import static org.apache.paimon.CoreOptions.PARTITION_MARK_DONE_CUSTOM_CLASS;
import static org.apache.paimon.CoreOptions.PARTITION_MARK_DONE_WHEN_END_INPUT;
import static org.apache.paimon.flink.sink.partition.PartitionMarkDoneTest.notifyCommits;
import static org.apache.paimon.partition.actions.PartitionMarkDoneAction.CUSTOM;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for custom PartitionMarkDoneAction. */
public class CustomPartitionMarkDoneActionTest extends TableTestBase {

    @Test
    public void testCustomPartitionMarkDoneAction() throws Exception {

        Identifier identifier = identifier("T");
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .column("c", DataTypes.INT())
                        .partitionKeys("a")
                        .primaryKey("a", "b")
                        .option(PARTITION_MARK_DONE_WHEN_END_INPUT.key(), "true")
                        .option(PARTITION_MARK_DONE_ACTION.key(), "success-file,custom")
                        .build();
        catalog.createTable(identifier, schema, true);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
        Path location = table.location();
        Path successFile = new Path(location, "a=0/_SUCCESS");

        // Throwing the exception, if the parameter 'partition.mark-done-action.custom.class' is not
        // set.
        Assertions.assertThatThrownBy(
                        () ->
                                PartitionMarkDone.create(
                                        getClass().getClassLoader(),
                                        false,
                                        false,
                                        new PartitionMarkDoneTest.MockOperatorStateStore(),
                                        table))
                .hasMessageContaining(
                        String.format(
                                "You need to set [%s] when you add [%s] mark done action in your property [%s].",
                                PARTITION_MARK_DONE_CUSTOM_CLASS.key(),
                                CUSTOM,
                                PARTITION_MARK_DONE_ACTION.key()));

        // Set parameter 'partition.mark-done-action.custom.class'.
        catalog.alterTable(
                identifier,
                SchemaChange.setOption(
                        PARTITION_MARK_DONE_CUSTOM_CLASS.key(),
                        MockCustomPartitionMarkDoneAction.class.getName()),
                true);

        FileStoreTable table2 = (FileStoreTable) catalog.getTable(identifier);

        PartitionMarkDone markDone =
                PartitionMarkDone.create(
                                getClass().getClassLoader(),
                                false,
                                false,
                                new PartitionMarkDoneTest.MockOperatorStateStore(),
                                table2)
                        .get();

        notifyCommits(markDone, false);

        assertThat(table2.fileIO().exists(successFile)).isEqualTo(true);

        assertThat(MockCustomPartitionMarkDoneAction.getMarkedDonePartitions().iterator().next())
                .isEqualTo("a=0/");
    }
}
