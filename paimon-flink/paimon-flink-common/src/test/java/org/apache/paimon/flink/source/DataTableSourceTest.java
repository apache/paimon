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

package org.apache.paimon.flink.source;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.PaimonDataStreamScanProvider;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.InnerTableWrite;
import org.apache.paimon.table.sink.TableCommitApi;
import org.apache.paimon.table.system.ReadOptimizedTable;
import org.apache.paimon.types.DataTypes;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.options.OptionsUtils.PAIMON_PREFIX;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Tests for {@link DataTableSource}. */
class DataTableSourceTest {

    @TempDir java.nio.file.Path path;

    @Test
    void testInferScanParallelism() throws Exception {
        FileStoreTable fileStoreTable = createTable(ImmutableMap.of("bucket", "1"));
        writeData(fileStoreTable);

        DataTableSource tableSource =
                new DataTableSource(
                        ObjectIdentifier.of("cat", "db", "table"),
                        fileStoreTable,
                        true,
                        null,
                        null);
        PaimonDataStreamScanProvider runtimeProvider = runtimeProvider(tableSource);
        StreamExecutionEnvironment sEnv1 = StreamExecutionEnvironment.createLocalEnvironment();
        DataStream<RowData> sourceStream1 =
                runtimeProvider.produceDataStream(s -> Optional.empty(), sEnv1);
        assertThat(sourceStream1.getParallelism()).isEqualTo(1);

        StreamExecutionEnvironment sEnv2 =
                StreamExecutionEnvironment.createLocalEnvironment(
                        Configuration.fromMap(
                                Collections.singletonMap(
                                        String.format(
                                                "%s%s",
                                                PAIMON_PREFIX,
                                                FlinkConnectorOptions.INFER_SCAN_PARALLELISM.key()),
                                        "false")));
        DataStream<RowData> sourceStream2 =
                runtimeProvider.produceDataStream(s -> Optional.empty(), sEnv2);
        // The default parallelism is not 1
        assertThat(sourceStream2.getParallelism()).isNotEqualTo(1);
        assertThat(sourceStream2.getParallelism()).isEqualTo(sEnv2.getParallelism());
    }

    @Test
    public void testInferStreamParallelism() throws Exception {
        FileStoreTable fileStoreTable = createTable(ImmutableMap.of("bucket", "-1"));

        DataTableSource tableSource =
                new DataTableSource(
                        ObjectIdentifier.of("cat", "db", "table"),
                        fileStoreTable,
                        true,
                        null,
                        null);
        PaimonDataStreamScanProvider runtimeProvider = runtimeProvider(tableSource);

        StreamExecutionEnvironment sEnv1 = StreamExecutionEnvironment.createLocalEnvironment();
        DataStream<RowData> sourceStream1 =
                runtimeProvider.produceDataStream(s -> Optional.empty(), sEnv1);
        // parallelism = 1 for table with -1 bucket.
        assertThat(sourceStream1.getParallelism()).isEqualTo(1);
    }

    @Test
    public void testSystemTableParallelism() throws Exception {
        FileStoreTable fileStoreTable =
                createTable(ImmutableMap.of("bucket", "1", "scan.parallelism", "3"));

        ReadOptimizedTable ro = new ReadOptimizedTable(fileStoreTable);

        SystemTableSource tableSource =
                new SystemTableSource(ro, false, ObjectIdentifier.of("cat", "db", "table"));
        PaimonDataStreamScanProvider runtimeProvider = runtimeProvider(tableSource);

        Configuration configuration = new Configuration();
        configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        StreamExecutionEnvironment sEnv1 = StreamExecutionEnvironment.createLocalEnvironment();
        DataStream<RowData> sourceStream1 =
                runtimeProvider.produceDataStream(s -> Optional.empty(), sEnv1);
        assertThat(sourceStream1.getParallelism()).isEqualTo(3);
    }

    private PaimonDataStreamScanProvider runtimeProvider(FlinkTableSource tableSource) {
        return (PaimonDataStreamScanProvider)
                tableSource.getScanRuntimeProvider(
                        new ScanTableSource.ScanContext() {
                            @Override
                            public <T> TypeInformation<T> createTypeInformation(DataType dataType) {
                                throw new UnsupportedOperationException();
                            }

                            @Override
                            public <T> TypeInformation<T> createTypeInformation(
                                    LogicalType logicalType) {
                                throw new UnsupportedOperationException();
                            }

                            @Override
                            public DynamicTableSource.DataStructureConverter
                                    createDataStructureConverter(DataType dataType) {
                                throw new UnsupportedOperationException();
                            }
                        });
    }

    private FileStoreTable createTable(Map<String, String> options) throws Exception {
        FileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(path.toString());
        SchemaManager schemaManager = new SchemaManager(fileIO, tablePath);
        TableSchema tableSchema =
                schemaManager.createTable(
                        Schema.newBuilder()
                                .column("a", DataTypes.INT())
                                .column("b", DataTypes.BIGINT())
                                .options(options)
                                .build());
        return FileStoreTableFactory.create(fileIO, tablePath, tableSchema);
    }

    private void writeData(FileStoreTable table) throws Exception {
        InnerTableWrite writer = table.newWrite("test");
        TableCommitApi commit = table.newCommit("test");
        writer.write(GenericRow.of(1, 2L));
        commit.commit(writer.prepareCommit());
        commit.close();
        writer.close();
    }
}
