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

import org.apache.paimon.cdc.CdcRecord;
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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SchemaAwareStoreWriteOperator}. */
public class SchemaAwareStoreWriteOperatorTest {

    private static final RowType ROW_TYPE =
            RowType.of(
                    new DataType[] {DataTypes.INT(), DataTypes.BIGINT(), DataTypes.STRING()},
                    new String[] {"pt", "k", "v"});

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
    public void testProcessRecord() throws Exception {
        FileStoreTable table = createFileStoreTable();
        OneInputStreamOperatorTestHarness<CdcRecord, Committable> harness =
                createTestHarness(table);
        harness.open();

        BlockingQueue<CdcRecord> toProcess = new LinkedBlockingQueue<>();
        BlockingQueue<CdcRecord> processed = new LinkedBlockingQueue<>();
        AtomicBoolean running = new AtomicBoolean(true);
        Runnable r =
                () -> {
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
                };

        Thread t = new Thread(r);
        t.start();

        // check that records with compatible schema can be processed immediately

        Map<String, String> fields = new HashMap<>();
        fields.put("pt", "0");
        fields.put("k", "1");
        fields.put("v", "10");
        CdcRecord expected = new CdcRecord(RowKind.INSERT, fields);
        toProcess.offer(expected);
        CdcRecord actual = processed.take();
        assertThat(actual).isEqualTo(expected);

        fields = new HashMap<>();
        fields.put("pt", "0");
        fields.put("k", "2");
        expected = new CdcRecord(RowKind.INSERT, fields);
        toProcess.offer(expected);
        actual = processed.take();
        assertThat(actual).isEqualTo(expected);

        // check that records with new fields should be processed after schema is updated

        fields = new HashMap<>();
        fields.put("pt", "0");
        fields.put("k", "3");
        fields.put("v", "30");
        fields.put("v2", "300");
        expected = new CdcRecord(RowKind.INSERT, fields);
        toProcess.offer(expected);
        actual = processed.poll(1, TimeUnit.SECONDS);
        assertThat(actual).isNull();

        SchemaManager schemaManager = new SchemaManager(table.fileIO(), table.location());
        schemaManager.commitChanges(SchemaChange.addColumn("v2", DataTypes.INT()));
        actual = processed.take();
        assertThat(actual).isEqualTo(expected);

        running.set(false);
        t.join();
        harness.close();
    }

    private OneInputStreamOperatorTestHarness<CdcRecord, Committable> createTestHarness(
            FileStoreTable table) throws Exception {
        SchemaAwareStoreWriteOperator operator =
                new SchemaAwareStoreWriteOperator(
                        table,
                        null,
                        (t, context, ioManager) ->
                                new StoreSinkWriteImpl(t, context, commitUser, ioManager, false));
        TypeSerializer<CdcRecord> inputSerializer = new JavaSerializer<>();
        TypeSerializer<Committable> outputSerializer =
                new CommittableTypeInfo().createSerializer(new ExecutionConfig());
        OneInputStreamOperatorTestHarness<CdcRecord, Committable> harness =
                new OneInputStreamOperatorTestHarness<>(operator, inputSerializer);
        harness.setup(outputSerializer);
        return harness;
    }

    private FileStoreTable createFileStoreTable() throws Exception {
        Options conf = new Options();
        conf.set(SchemaAwareStoreWriteOperator.RETRY_SLEEP_TIME, Duration.ofMillis(10));

        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(LocalFileIO.create(), tablePath),
                        new Schema(
                                ROW_TYPE.getFields(),
                                Collections.singletonList("pt"),
                                Arrays.asList("pt", "k"),
                                conf.toMap(),
                                ""));
        return FileStoreTableFactory.create(LocalFileIO.create(), tablePath, tableSchema);
    }
}
