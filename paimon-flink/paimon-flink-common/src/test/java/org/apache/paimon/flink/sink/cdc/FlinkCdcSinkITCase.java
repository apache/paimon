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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.util.AbstractTestBase;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FailingFileIO;
import org.apache.paimon.utils.TraceableFileIO;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link FlinkCdcSink}. */
public class FlinkCdcSinkITCase extends AbstractTestBase {

    @TempDir java.nio.file.Path tempDir;

    @Test
    @Timeout(120)
    public void testRandomCdcEvents() throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        int numEvents = random.nextInt(1500) + 1;
        int numSchemaChanges = random.nextInt(10) + 1;
        int numPartitions = random.nextInt(3) + 1;
        int numKeys = random.nextInt(150) + 1;
        int numBucket = random.nextInt(5) + 1;
        boolean shuffleByPartitionEnable = random.nextBoolean();
        boolean enableFailure = random.nextBoolean();

        TestCdcEvent[] events = new TestCdcEvent[numEvents];

        Set<Integer> schemaChangePositions = new HashSet<>();
        for (int i = 0; i < numSchemaChanges; i++) {
            int pos;
            do {
                pos = random.nextInt(numEvents);
            } while (schemaChangePositions.contains(pos));
            schemaChangePositions.add(pos);
        }

        Map<Integer, Map<String, String>> expected = new HashMap<>();
        List<String> fieldNames = new ArrayList<>();
        List<Boolean> isBigInt = new ArrayList<>();
        fieldNames.add("v0");
        isBigInt.add(false);
        int suffixId = 0;
        for (int i = 0; i < numEvents; i++) {
            if (schemaChangePositions.contains(i)) {
                if (random.nextBoolean()) {
                    int idx = random.nextInt(fieldNames.size());
                    isBigInt.set(idx, true);
                    events[i] =
                            new TestCdcEvent(
                                    SchemaChange.updateColumnType(
                                            fieldNames.get(idx), DataTypes.BIGINT()));
                } else {
                    suffixId++;
                    String newName = "v" + suffixId;
                    fieldNames.add(newName);
                    isBigInt.add(false);
                    events[i] = new TestCdcEvent(SchemaChange.addColumn(newName, DataTypes.INT()));
                }
            } else {
                Map<String, String> fields = new HashMap<>();
                int key = random.nextInt(numKeys);
                fields.put("k", String.valueOf(key));
                int pt = key % numPartitions;
                fields.put("pt", String.valueOf(pt));
                for (int j = 0; j < fieldNames.size(); j++) {
                    String fieldName = fieldNames.get(j);
                    if (isBigInt.get(j)) {
                        fields.put(fieldName, String.valueOf(random.nextLong()));
                    } else {
                        fields.put(fieldName, String.valueOf(random.nextInt()));
                    }
                }

                List<CdcRecord> records = new ArrayList<>();
                if (expected.containsKey(key)) {
                    records.add(new CdcRecord(RowKind.DELETE, expected.get(key)));
                }
                records.add(new CdcRecord(RowKind.INSERT, fields));
                events[i] = new TestCdcEvent(records);
                expected.put(key, fields);
            }
        }

        Path tablePath;
        FileIO fileIO;
        String failingName = UUID.randomUUID().toString();
        if (enableFailure) {
            tablePath = new Path(FailingFileIO.getFailingPath(failingName, tempDir.toString()));
            fileIO = new FailingFileIO();
        } else {
            tablePath = new Path(TraceableFileIO.SCHEME + "://" + tempDir.toString());
            fileIO = LocalFileIO.create();
        }

        // no failure when creating table
        FailingFileIO.reset(failingName, 0, 1);

        FileStoreTable table =
                createFileStoreTable(
                        tablePath,
                        fileIO,
                        RowType.of(
                                new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.INT()},
                                new String[] {"pt", "k", "v0"}),
                        Collections.singletonList("pt"),
                        Arrays.asList("pt", "k"),
                        numBucket,
                        shuffleByPartitionEnable);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointInterval(100);
        TestCdcSourceFunction sourceFunction =
                new TestCdcSourceFunction(
                        events, record -> Integer.valueOf(record.fields().get("k")));
        DataStreamSource<TestCdcEvent> source = env.addSource(sourceFunction);
        source.setParallelism(2);
        new FlinkCdcSinkBuilder<TestCdcEvent>()
                .withInput(source)
                .withParserFactory(TestCdcEventParser::new)
                .withTable(table)
                .withParallelism(3)
                .build();

        // enable failure when running jobs if needed
        FailingFileIO.reset(failingName, 10, 10000);

        env.execute();

        // no failure when checking results
        FailingFileIO.reset(failingName, 0, 1);

        table = table.copyWithLatestSchema();
        SchemaManager schemaManager = new SchemaManager(table.fileIO(), table.location());
        TableSchema schema = schemaManager.latest().get();

        Map<Integer, Map<String, String>> actual = new HashMap<>();
        TableScan.Plan plan = table.newScan().plan();
        try (RecordReaderIterator<InternalRow> it =
                new RecordReaderIterator<>(table.newRead().createReader(plan))) {
            while (it.hasNext()) {
                InternalRow row = it.next();
                Map<String, String> fields = new HashMap<>();
                for (int i = 0; i < schema.fieldNames().size(); i++) {
                    if (!row.isNullAt(i)) {
                        fields.put(
                                schema.fieldNames().get(i),
                                String.valueOf(
                                        schema.fields().get(i).type().equals(DataTypes.BIGINT())
                                                ? row.getLong(i)
                                                : row.getInt(i)));
                    }
                }
                actual.put(Integer.valueOf(fields.get("k")), fields);
            }
        }
        assertThat(actual).isEqualTo(expected);
    }

    private FileStoreTable createFileStoreTable(
            Path tablePath,
            FileIO fileIO,
            RowType rowType,
            List<String> partitions,
            List<String> primaryKeys,
            int numBucket,
            boolean shuffleByPartitionEnable)
            throws Exception {
        Options conf = new Options();
        conf.set(CoreOptions.BUCKET, numBucket);
        conf.set(CoreOptions.WRITE_BUFFER_SIZE, new MemorySize(4096 * 3));
        conf.set(CoreOptions.PAGE_SIZE, new MemorySize(4096));
        conf.set(FlinkConnectorOptions.SINK_SHUFFLE_BY_PARTITION, shuffleByPartitionEnable);

        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(fileIO, tablePath),
                        new Schema(rowType.getFields(), partitions, primaryKeys, conf.toMap(), ""));
        return FileStoreTableFactory.create(fileIO, tablePath, tableSchema);
    }
}
