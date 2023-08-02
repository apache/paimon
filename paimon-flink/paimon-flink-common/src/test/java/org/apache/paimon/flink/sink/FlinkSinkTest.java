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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.operation.KeyValueFileStoreWrite;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.state.StateInitializationContextImpl;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.collect.utils.MockOperatorStateStore;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Test class for {@link FlinkSink}. */
public class FlinkSinkTest {

    @TempDir Path tempPath;

    @Test
    public void testOptimizeKeyValueWriterForBatch() throws Exception {
        // test for batch mode auto enable spillable
        FileStoreTable fileStoreTable = createFileStoreTable();
        StreamExecutionEnvironment streamExecutionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // set this when batch executing
        streamExecutionEnvironment.setRuntimeMode(RuntimeExecutionMode.BATCH);
        assertThat(testSpillable(streamExecutionEnvironment, fileStoreTable)).isTrue();

        // set this to streaming, we should get a false then
        streamExecutionEnvironment.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        assertThat(testSpillable(streamExecutionEnvironment, fileStoreTable)).isFalse();
    }

    private boolean testSpillable(
            StreamExecutionEnvironment streamExecutionEnvironment, FileStoreTable fileStoreTable)
            throws Exception {
        DataStreamSource<RowData> source =
                streamExecutionEnvironment.fromCollection(
                        Collections.singletonList(new FlinkRowData(GenericRow.of(1, 1))));
        FlinkSink<RowData> flinkSink = new FileStoreSink(fileStoreTable, null, null);
        SingleOutputStreamOperator<Committable> written = flinkSink.doWrite(source, "123", 1);
        RowDataStoreWriteOperator operator =
                ((RowDataStoreWriteOperator)
                        ((SimpleOperatorFactory)
                                        ((OneInputTransformation) written.getTransformation())
                                                .getOperatorFactory())
                                .getOperator());
        StateInitializationContextImpl context =
                new StateInitializationContextImpl(
                        null,
                        new MockOperatorStateStore() {
                            @Override
                            public <S> ListState<S> getUnionListState(
                                    ListStateDescriptor<S> stateDescriptor) throws Exception {
                                return getListState(stateDescriptor);
                            }
                        },
                        null,
                        null,
                        null);
        operator.initStateAndWriter(context, (a, b, c) -> true, new IOManagerAsync(), "123");
        return ((KeyValueFileStoreWrite) ((StoreSinkWriteImpl) operator.write).write.getWrite())
                .bufferSpillable();
    }

    protected static final RowType ROW_TYPE =
            RowType.of(
                    new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"pk", "pt0"});

    private FileStoreTable createFileStoreTable() throws Exception {
        org.apache.paimon.fs.Path tablePath = new org.apache.paimon.fs.Path(tempPath.toString());
        Options conf = new Options();
        conf.set(CoreOptions.PATH, tablePath.toString());
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(LocalFileIO.create(), tablePath),
                        new Schema(
                                ROW_TYPE.getFields(),
                                Collections.emptyList(),
                                Arrays.asList("pk"),
                                conf.toMap(),
                                ""));
        return FileStoreTableFactory.create(
                FileIOFinder.find(tablePath),
                tablePath,
                tableSchema,
                conf,
                new CatalogEnvironment(Lock.emptyFactory(), null, null));
    }
}
