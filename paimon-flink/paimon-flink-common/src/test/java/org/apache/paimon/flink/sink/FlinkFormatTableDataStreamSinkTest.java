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

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;

import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FlinkFormatTableDataStreamSink}. */
class FlinkFormatTableDataStreamSinkTest {

    @TempDir java.nio.file.Path temp;

    @Test
    void testFormatTableSinkLineageVertex() throws Exception {
        FormatTable table =
                FormatTable.builder()
                        .fileIO(LocalFileIO.create())
                        .identifier(Identifier.create("test_db", "test_table"))
                        .rowType(RowType.of(new IntType()))
                        .partitionKeys(Collections.emptyList())
                        .location(new Path(temp.toUri().toString()).toString())
                        .format(FormatTable.Format.PARQUET)
                        .options(Collections.singletonMap("path", temp.toUri().toString()))
                        .catalogContext(CatalogContext.create(new Options()))
                        .build();

        Class<?> sinkClass =
                Class.forName(
                        "org.apache.paimon.flink.sink.FlinkFormatTableDataStreamSink$FormatTableSink");
        Constructor<?> constructor =
                sinkClass.getDeclaredConstructor(FormatTable.class, boolean.class, Map.class);
        constructor.setAccessible(true);
        Object sink = constructor.newInstance(table, false, Collections.emptyMap());

        assertThat(sink).isInstanceOf(LineageVertexProvider.class);
        LineageVertex vertex = ((LineageVertexProvider) sink).getLineageVertex();
        assertThat(vertex.datasets()).hasSize(1);
        assertThat(vertex.datasets().get(0).name()).isEqualTo("paimon." + table.fullName());
    }
}
