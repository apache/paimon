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

package org.apache.paimon.flink.sink.listener;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.streaming.api.operators.collect.utils.MockOperatorStateStore;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.CoreOptions.DELETION_VECTORS_ENABLED;
import static org.apache.paimon.CoreOptions.PARTITION_MARK_DONE_ACTION;
import static org.apache.paimon.CoreOptions.PARTITION_MARK_DONE_WHEN_END_INPUT;
import static org.apache.paimon.flink.sink.listener.ListenerTestUtils.notifyCommits;
import static org.assertj.core.api.Assertions.assertThat;

class PartitionMarkDoneTest extends TableTestBase {

    @Test
    public void testTriggerByCompaction() throws Exception {
        innerTest(options -> options.set(DELETION_VECTORS_ENABLED, true), true, false);
    }

    @Test
    public void testTriggerByCompaction2() throws Exception {
        innerTest(options -> options.set(BUCKET, -2), true, false);
    }

    @Test
    public void testNotTriggerByCompaction() throws Exception {
        innerTest(options -> {}, true, true);
    }

    @Test
    public void testNotTriggerWhenRecoveryFromState() throws Exception {
        innerTest(options -> {}, false, true);
    }

    private void innerTest(
            Consumer<Options> config, boolean recoverFromState, boolean shouldMarkDone)
            throws Exception {
        Identifier identifier = identifier("T");
        Options options = new Options();
        options.set(PARTITION_MARK_DONE_WHEN_END_INPUT, true);
        options.set(PARTITION_MARK_DONE_ACTION, "success-file");
        config.accept(options);
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .column("c", DataTypes.INT())
                        .partitionKeys("a")
                        .primaryKey("a", "b")
                        .options(options.toMap())
                        .build();
        catalog.createTable(identifier, schema, true);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
        Path location = table.location();
        Path successFile = new Path(location, "a=0/_SUCCESS");
        PartitionMarkDoneListener markDone =
                PartitionMarkDoneListener.create(
                                getClass().getClassLoader(),
                                false,
                                false,
                                new MockOperatorStateStore(),
                                table)
                        .get();

        if (!recoverFromState) {
            notifyCommits(markDone, false, false);
            assertThat(table.fileIO().exists(successFile)).isEqualTo(false);
            return;
        }

        notifyCommits(markDone, true, true);
        assertThat(table.fileIO().exists(successFile)).isEqualTo(!shouldMarkDone);

        if (shouldMarkDone) {
            notifyCommits(markDone, false, true);
            assertThat(table.fileIO().exists(successFile)).isEqualTo(true);
        }
    }
}
