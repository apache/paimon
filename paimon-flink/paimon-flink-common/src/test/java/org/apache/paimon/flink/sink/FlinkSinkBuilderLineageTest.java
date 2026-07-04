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
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.flink.LogicalTypeConversion.toLogicalType;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for sink lineage in {@link FlinkSinkBuilder}. */
class FlinkSinkBuilderLineageTest {

    @TempDir java.nio.file.Path temp;

    @Test
    void testFlinkSinkBuilderUsesPaimonDiscardingSinkForLineage() throws Exception {
        FileStoreTable table = createTable();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<RowData> input =
                env.fromCollection(
                        Collections.singletonList((RowData) GenericRowData.of(1)),
                        InternalTypeInfo.of(toLogicalType(table.rowType())));

        DataStreamSink<?> sink = new FlinkSinkBuilder(table).forRowData(input).build();

        assertThat(sink.getTransformation()).isInstanceOf(SinkTransformation.class);
        SinkTransformation<?, ?> transformation =
                (SinkTransformation<?, ?>) sink.getTransformation();
        assertThat(transformation.getSink()).isInstanceOf(PaimonDiscardingSink.class);
    }

    private FileStoreTable createTable() throws Exception {
        Path tablePath = new Path(temp.toUri().toString());
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "-1");
        new SchemaManager(LocalFileIO.create(), tablePath)
                .createTable(
                        new Schema(
                                RowType.of(new IntType()).getFields(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                options,
                                ""));
        return FileStoreTableFactory.create(LocalFileIO.create(), tablePath);
    }
}
