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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.FiniteTestSource;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.store.connector.sink.StoreSink;
import org.apache.flink.table.store.connector.sink.global.GlobalCommittingSinkTranslator;
import org.apache.flink.table.store.connector.source.FileStoreSource;
import org.apache.flink.table.store.file.FileStore;
import org.apache.flink.table.store.file.FileStoreImpl;
import org.apache.flink.table.store.file.mergetree.compact.DeduplicateAccumulator;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.store.file.FileStoreOptions.BUCKET;
import static org.apache.flink.table.store.file.FileStoreOptions.FILE_FORMAT;
import static org.apache.flink.table.store.file.FileStoreOptions.FILE_PATH;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for {@link FileStoreSource} and {@link StoreSink}. */
@RunWith(Parameterized.class)
public class FileStoreITCase extends AbstractTestBase {

    private static final RowType PARTITION_TYPE =
            new RowType(Collections.singletonList(new RowType.RowField("p", new VarCharType())));

    private static final RowType KEY_TYPE =
            new RowType(Collections.singletonList(new RowType.RowField("k", new IntType())));

    private static final RowType VALUE_TYPE =
            new RowType(
                    Arrays.asList(
                            new RowType.RowField("v", new IntType()),
                            new RowType.RowField("p", new VarCharType()),
                            // rename key
                            new RowType.RowField("_k", new IntType())));

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static final DataStructureConverter<RowData, Row> CONVERTER =
            (DataStructureConverter)
                    DataStructureConverters.getConverter(
                            TypeConversions.fromLogicalToDataType(VALUE_TYPE));

    private static final int NUM_BUCKET = 3;

    private static final List<RowData> SOURCE_DATA =
            Arrays.asList(
                    wrap(GenericRowData.of(0, StringData.fromString("p1"), 1)),
                    wrap(GenericRowData.of(0, StringData.fromString("p1"), 2)),
                    wrap(GenericRowData.of(5, StringData.fromString("p1"), 1)),
                    wrap(GenericRowData.of(6, StringData.fromString("p2"), 1)),
                    wrap(GenericRowData.of(3, StringData.fromString("p2"), 5)),
                    wrap(GenericRowData.of(5, StringData.fromString("p2"), 1)));

    private Configuration options;

    private final boolean isBounded;

    public FileStoreITCase(boolean isBounded) {
        this.isBounded = isBounded;
    }

    @Parameterized.Parameters(name = "isBounded-{0}")
    public static List<Boolean> getVarSeg() {
        return Arrays.asList(true, false);
    }

    private static SerializableRowData wrap(RowData row) {
        return new SerializableRowData(row, InternalSerializers.create(VALUE_TYPE));
    }

    @Before
    public void before() throws IOException {
        options = new Configuration();
        options.set(BUCKET, NUM_BUCKET);
        options.set(FILE_PATH, TEMPORARY_FOLDER.newFolder().toURI().toString());
        options.set(FILE_FORMAT, "avro");
    }

    @Test
    public void testPartitioned() throws Exception {
        innerTest(true);
    }

    @Test
    public void testNonPartitioned() throws Exception {
        innerTest(false);
    }

    private void innerTest(boolean partitioned) throws Exception {
        int[] partitions = partitioned ? new int[] {1} : new int[0];
        int[] keys = new int[] {2};
        InternalTypeInfo<RowData> typeInfo = InternalTypeInfo.of(VALUE_TYPE);

        StreamExecutionEnvironment env = isBounded ? buildBatchEnv() : buildStreamEnv();
        env.setParallelism(1); // no key by, must single parallelism
        FileStore fileStore =
                new FileStoreImpl(
                        options,
                        "user",
                        partitioned ? PARTITION_TYPE : RowType.of(),
                        KEY_TYPE,
                        VALUE_TYPE,
                        new DeduplicateAccumulator());

        // sink
        StoreSink<?, ?> sink =
                new StoreSink<>(
                        null, fileStore, VALUE_TYPE, partitions, keys, NUM_BUCKET, null, null);
        DataStreamSource<RowData> finiteSource =
                isBounded
                        ? env.fromCollection(SOURCE_DATA, InternalTypeInfo.of(VALUE_TYPE))
                        : env.addSource(new FiniteTestSource<>(null, SOURCE_DATA), typeInfo);
        GlobalCommittingSinkTranslator.translate(finiteSource, isBounded, sink);
        env.execute();

        // source
        FileStoreSource source =
                new FileStoreSource(fileStore, VALUE_TYPE, partitions, keys, null, null, null);
        CloseableIterator<RowData> iterator =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "source", typeInfo)
                        .executeAndCollect();
        List<Row> results = new ArrayList<>();
        while (iterator.hasNext()) {
            results.add(CONVERTER.toExternal(iterator.next()));
        }
        iterator.close();

        Row[] expected =
                partitioned
                        ? new Row[] {
                            Row.of(5, "p2", 1),
                            Row.of(3, "p2", 5),
                            Row.of(5, "p1", 1),
                            Row.of(0, "p1", 2)
                        }
                        : new Row[] {Row.of(5, "p2", 1), Row.of(0, "p1", 2), Row.of(3, "p2", 5)};
        assertThat(results).containsExactlyInAnyOrder(expected);
    }

    private StreamExecutionEnvironment buildStreamEnv() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(100);
        return env;
    }

    private StreamExecutionEnvironment buildBatchEnv() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        return env;
    }
}
