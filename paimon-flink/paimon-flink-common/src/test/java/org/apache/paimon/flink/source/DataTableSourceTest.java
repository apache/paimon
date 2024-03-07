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
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
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
import java.util.Optional;

import static org.apache.paimon.options.OptionsUtils.PAIMON_PREFIX;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Tests for {@link DataTableSource}. */
class DataTableSourceTest {

    @Test
    void testInferScanParallelism(@TempDir java.nio.file.Path path) throws Exception {
        FileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(path.toString());
        SchemaManager schemaManager = new SchemaManager(fileIO, tablePath);
        TableSchema tableSchema =
                schemaManager.createTable(
                        Schema.newBuilder()
                                .column("a", DataTypes.INT())
                                .column("b", DataTypes.BIGINT())
                                .option("bucket", "1")
                                .build());
        FileStoreTable fileStoreTable =
                FileStoreTableFactory.create(fileIO, tablePath, tableSchema);
        InnerTableWrite writer = fileStoreTable.newWrite("test");
        TableCommitImpl commit = fileStoreTable.newCommit("test");
        writer.write(GenericRow.of(1, 2L));
        writer.write(GenericRow.of(3, 4L));
        writer.write(GenericRow.of(5, 6L));
        writer.write(GenericRow.of(7, 8L));
        writer.write(GenericRow.of(9, 10L));
        writer.write(GenericRow.of(11, 12L));
        writer.write(GenericRow.of(13, 14L));
        writer.write(GenericRow.of(15, 16L));
        writer.write(GenericRow.of(17, 18L));
        commit.commit(writer.prepareCommit());

        commit.close();
        writer.close();

        DataTableSource tableSource =
                new DataTableSource(
                        ObjectIdentifier.of("cat", "db", "table"),
                        fileStoreTable,
                        true,
                        null,
                        null);
        PaimonDataStreamScanProvider runtimeProvider =
                (PaimonDataStreamScanProvider)
                        tableSource.getScanRuntimeProvider(
                                new ScanTableSource.ScanContext() {
                                    @Override
                                    public <T> TypeInformation<T> createTypeInformation(
                                            DataType dataType) {
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
}
