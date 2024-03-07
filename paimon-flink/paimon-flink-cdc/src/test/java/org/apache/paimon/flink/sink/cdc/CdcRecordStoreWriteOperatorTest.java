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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.CommittableTypeInfo;
import org.apache.paimon.flink.sink.StoreSinkWriteImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.TraceableFileIO;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.JavaSerializer;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CdcRecordStoreWriteOperator}. */
public class CdcRecordStoreWriteOperatorTest {

    @TempDir java.nio.file.Path tempDir;

    private Path tablePath;
    private String commitUser;

    @BeforeEach
    public void before() {
        tablePath = new Path(TraceableFileIO.SCHEME + "://" + tempDir.toString());
        commitUser = UUID.randomUUID().toString();
    }

    @AfterEach
    public void after() {
        // assert all connections are closed
        Predicate<Path> pathPredicate = path -> path.toString().contains(tempDir.toString());
        assertThat(TraceableFileIO.openInputStreams(pathPredicate)).isEmpty();
        assertThat(TraceableFileIO.openOutputStreams(pathPredicate)).isEmpty();
    }

    @Test
    @Timeout(30)
    public void testAddColumn() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.BIGINT(), DataTypes.STRING()},
                        new String[] {"pt", "k", "v"});

        FileStoreTable table =
                createFileStoreTable(
                        rowType, Collections.singletonList("pt"), Arrays.asList("pt", "k"));
        OneInputStreamOperatorTestHarness<CdcRecord, Committable> harness =
                createTestHarness(table);
        harness.open();

        Runner runner = new Runner(harness);
        Thread t = new Thread(runner);
        t.start();

        // check that records with compatible schema can be processed immediately

        Map<String, String> fields = new HashMap<>();
        fields.put("pt", "0");
        fields.put("k", "1");
        fields.put("v", "10");
        CdcRecord expected = new CdcRecord(RowKind.INSERT, fields);
        runner.offer(expected);
        CdcRecord actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        fields = new HashMap<>();
        fields.put("pt", "0");
        fields.put("k", "2");
        expected = new CdcRecord(RowKind.INSERT, fields);
        runner.offer(expected);
        actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        // check that records with new fields should be processed after schema is updated

        fields = new HashMap<>();
        fields.put("pt", "0");
        fields.put("k", "3");
        fields.put("v", "30");
        fields.put("v2", "300");
        expected = new CdcRecord(RowKind.INSERT, fields);
        runner.offer(expected);
        actual = runner.poll(1);
        assertThat(actual).isNull();

        SchemaManager schemaManager = new SchemaManager(table.fileIO(), table.location());
        schemaManager.commitChanges(SchemaChange.addColumn("v2", DataTypes.INT()));
        actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        runner.stop();
        t.join();
        harness.close();
    }

    @Test
    @Timeout(30)
    public void testUpdateColumnType() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT(),
                            DataTypes.INT(),
                            DataTypes.FLOAT(),
                            DataTypes.VARCHAR(5),
                            DataTypes.VARBINARY(5)
                        },
                        new String[] {"k", "v1", "v2", "v3", "v4"});

        FileStoreTable table =
                createFileStoreTable(
                        rowType, Collections.emptyList(), Collections.singletonList("k"));
        OneInputStreamOperatorTestHarness<CdcRecord, Committable> harness =
                createTestHarness(table);
        harness.open();

        Runner runner = new Runner(harness);
        Thread t = new Thread(runner);
        t.start();

        // check that records with compatible schema can be processed immediately

        Map<String, String> fields = new HashMap<>();
        fields.put("k", "1");
        fields.put("v1", "10");
        fields.put("v2", "0.625");
        fields.put("v3", "one");
        fields.put("v4", "b_one");
        CdcRecord expected = new CdcRecord(RowKind.INSERT, fields);
        runner.offer(expected);
        CdcRecord actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        // check that records with new fields should be processed after schema is updated

        // int -> bigint

        fields = new HashMap<>();
        fields.put("k", "2");
        fields.put("v1", "12345678987654321");
        fields.put("v2", "0.25");
        expected = new CdcRecord(RowKind.INSERT, fields);
        runner.offer(expected);
        actual = runner.poll(1);
        assertThat(actual).isNull();

        SchemaManager schemaManager = new SchemaManager(table.fileIO(), table.location());
        schemaManager.commitChanges(SchemaChange.updateColumnType("v1", DataTypes.BIGINT()));
        actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        // float -> double

        fields = new HashMap<>();
        fields.put("k", "3");
        fields.put("v1", "100");
        fields.put("v2", "1.0000000000009095");
        expected = new CdcRecord(RowKind.INSERT, fields);
        runner.offer(expected);
        actual = runner.poll(1);
        assertThat(actual).isNull();

        schemaManager.commitChanges(SchemaChange.updateColumnType("v2", DataTypes.DOUBLE()));
        actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        // varchar(5) -> varchar(10)

        fields = new HashMap<>();
        fields.put("k", "4");
        fields.put("v1", "40");
        fields.put("v3", "long four");
        expected = new CdcRecord(RowKind.INSERT, fields);
        runner.offer(expected);
        actual = runner.poll(1);
        assertThat(actual).isNull();

        schemaManager.commitChanges(SchemaChange.updateColumnType("v3", DataTypes.VARCHAR(10)));
        actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        // varbinary(5) -> varbinary(10)

        fields = new HashMap<>();
        fields.put("k", "5");
        fields.put("v1", "50");
        fields.put("v4", "long five~");
        expected = new CdcRecord(RowKind.INSERT, fields);
        runner.offer(expected);
        actual = runner.poll(1);
        assertThat(actual).isNull();

        schemaManager.commitChanges(SchemaChange.updateColumnType("v4", DataTypes.VARBINARY(10)));
        actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        runner.stop();
        t.join();
        harness.close();
    }

    private OneInputStreamOperatorTestHarness<CdcRecord, Committable> createTestHarness(
            FileStoreTable table) throws Exception {
        CdcRecordStoreWriteOperator operator =
                new CdcRecordStoreWriteOperator(
                        table,
                        (t, commitUser, state, ioManager, memoryPool, metricGroup) ->
                                new StoreSinkWriteImpl(
                                        t,
                                        commitUser,
                                        state,
                                        ioManager,
                                        false,
                                        false,
                                        true,
                                        memoryPool,
                                        metricGroup),
                        commitUser);
        TypeSerializer<CdcRecord> inputSerializer = new JavaSerializer<>();
        TypeSerializer<Committable> outputSerializer =
                new CommittableTypeInfo().createSerializer(new ExecutionConfig());
        OneInputStreamOperatorTestHarness<CdcRecord, Committable> harness =
                new OneInputStreamOperatorTestHarness<>(operator, inputSerializer);
        harness.setup(outputSerializer);
        return harness;
    }

    private FileStoreTable createFileStoreTable(
            RowType rowType, List<String> partitions, List<String> primaryKeys) throws Exception {
        Options conf = new Options();
        conf.set(CdcRecordStoreWriteOperator.RETRY_SLEEP_TIME, Duration.ofMillis(10));
        conf.set(CoreOptions.BUCKET, 1);

        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(LocalFileIO.create(), tablePath),
                        new Schema(rowType.getFields(), partitions, primaryKeys, conf.toMap(), ""));
        return FileStoreTableFactory.create(LocalFileIO.create(), tablePath, tableSchema);
    }

    private static class Runner implements Runnable {

        private final OneInputStreamOperatorTestHarness<CdcRecord, Committable> harness;
        private final BlockingQueue<CdcRecord> toProcess = new LinkedBlockingQueue<>();
        private final BlockingQueue<CdcRecord> processed = new LinkedBlockingQueue<>();
        private final AtomicBoolean running = new AtomicBoolean(true);

        private Runner(OneInputStreamOperatorTestHarness<CdcRecord, Committable> harness) {
            this.harness = harness;
        }

        private void offer(CdcRecord record) {
            toProcess.offer(record);
        }

        private CdcRecord take() throws Exception {
            return processed.take();
        }

        private CdcRecord poll(long seconds) throws Exception {
            return processed.poll(seconds, TimeUnit.SECONDS);
        }

        private void stop() {
            running.set(false);
        }

        @Override
        public void run() {
            long timestamp = 0;
            try {
                while (running.get()) {
                    if (toProcess.isEmpty()) {
                        Thread.sleep(10);
                        continue;
                    }

                    CdcRecord record = toProcess.poll();
                    harness.processElement(record, ++timestamp);
                    processed.offer(record);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
