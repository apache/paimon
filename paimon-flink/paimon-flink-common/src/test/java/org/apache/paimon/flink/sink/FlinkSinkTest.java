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

package org.apache.paimon.flink.sink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FlinkSink}. */
public class FlinkSinkTest extends CommitterOperatorTestBase {

    private static final RowType ROW_TYPE =
            RowType.of(
                    new DataType[] {DataTypes.INT(), DataTypes.BIGINT()}, new String[] {"a", "b"});

    @Test
    public void testCoordinatorCommitPreconditionsHappyPath() throws Exception {
        FileStoreTable table = createUnawareBucketTable(options -> {});
        FlinkSink.checkCoordinatorCommitPreconditions(table, newCheckpointConfig(1), true);
    }

    @Test
    public void testCoordinatorCommitPreconditionsRejectsBatchOrNoCheckpoint() throws Exception {
        FileStoreTable table = createUnawareBucketTable(options -> {});
        assertThatThrownBy(
                        () ->
                                FlinkSink.checkCoordinatorCommitPreconditions(
                                        table, newCheckpointConfig(1), false))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testCoordinatorCommitPreconditionsRejectsPrimaryKeyTable() throws Exception {
        FileStoreTable table = createPrimaryKeyTable();
        assertThatThrownBy(
                        () ->
                                FlinkSink.checkCoordinatorCommitPreconditions(
                                        table, newCheckpointConfig(1), true))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testCoordinatorCommitPreconditionsRejectsFixedBucket() throws Exception {
        FileStoreTable table = createFileStoreTable();
        assertThatThrownBy(
                        () ->
                                FlinkSink.checkCoordinatorCommitPreconditions(
                                        table, newCheckpointConfig(1), true))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testCoordinatorCommitPreconditionsRejectsNonWriteOnly() throws Exception {
        FileStoreTable table =
                createUnawareBucketTable(options -> options.set(CoreOptions.WRITE_ONLY, false));
        assertThatThrownBy(
                        () ->
                                FlinkSink.checkCoordinatorCommitPreconditions(
                                        table, newCheckpointConfig(1), true))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testCoordinatorCommitPreconditionsRejectsPrecommitCompact() throws Exception {
        FileStoreTable table =
                createUnawareBucketTable(
                        options -> options.set(FlinkConnectorOptions.PRECOMMIT_COMPACT, true));
        assertThatThrownBy(
                        () ->
                                FlinkSink.checkCoordinatorCommitPreconditions(
                                        table, newCheckpointConfig(1), true))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testCoordinatorCommitPreconditionsRejectsAutoTagForSavepoint() throws Exception {
        FileStoreTable table =
                createUnawareBucketTable(
                        options ->
                                options.set(
                                        FlinkConnectorOptions.SINK_AUTO_TAG_FOR_SAVEPOINT, true));
        assertThatThrownBy(
                        () ->
                                FlinkSink.checkCoordinatorCommitPreconditions(
                                        table, newCheckpointConfig(1), true))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testCoordinatorCommitPreconditionsRejectsConcurrentCheckpoints() throws Exception {
        FileStoreTable table = createUnawareBucketTable(options -> {});
        assertThatThrownBy(
                        () ->
                                FlinkSink.checkCoordinatorCommitPreconditions(
                                        table, newCheckpointConfig(2), true))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private FileStoreTable createUnawareBucketTable(Consumer<Options> setOptions) throws Exception {
        return createFileStoreTable(
                options -> {
                    options.set(CoreOptions.BUCKET, -1);
                    options.remove("bucket-key");
                    options.set(CoreOptions.WRITE_ONLY, true);
                    setOptions.accept(options);
                });
    }

    private FileStoreTable createPrimaryKeyTable() throws Exception {
        Options conf = new Options();
        conf.set(CoreOptions.PATH, tablePath.toString());
        conf.setString("bucket", "1");
        SchemaManager schemaManager = new SchemaManager(LocalFileIO.create(), tablePath);
        schemaManager.createTable(
                new Schema(
                        ROW_TYPE.getFields(),
                        Collections.emptyList(),
                        Collections.singletonList("a"),
                        conf.toMap(),
                        ""));
        return FileStoreTableFactory.create(LocalFileIO.create(), conf);
    }

    private static CheckpointConfig newCheckpointConfig(int maxConcurrentCheckpoints) {
        CheckpointConfig config = new CheckpointConfig();
        config.setMaxConcurrentCheckpoints(maxConcurrentCheckpoints);
        return config;
    }
}
