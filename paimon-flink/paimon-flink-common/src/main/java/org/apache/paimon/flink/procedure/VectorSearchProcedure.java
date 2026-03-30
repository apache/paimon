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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.json.JsonFormatWriter;
import org.apache.paimon.format.json.JsonOptions;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.StringUtils;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Vector search procedure. This procedure takes one vector and searches for topK nearest vectors.
 * Usage:
 *
 * <pre><code>
 *  CALL sys.vector_search(
 *    `table` => 'tableId',
 *    vector_column => 'v',
 *    query_vector => '1.0,2.0,3.0',
 *    top_k => 5
 *  )
 *
 *  -- with projection and options
 *  CALL sys.vector_search(
 *    `table` => 'tableId',
 *    vector_column => 'v',
 *    query_vector => '1.0,2.0,3.0',
 *    top_k => 5,
 *    projection => 'id,name',
 *    options => 'k1=v1;k2=v2'
 *  )
 * </code></pre>
 */
public class VectorSearchProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "vector_search";

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "vector_column", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "query_vector", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "top_k", type = @DataTypeHint("INT")),
                @ArgumentHint(
                        name = "projection",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(name = "options", type = @DataTypeHint("STRING"), isOptional = true)
            })
    public String[] call(
            ProcedureContext procedureContext,
            String tableId,
            String vectorColumn,
            String queryVectorStr,
            Integer topK,
            String projection,
            String options)
            throws Exception {
        Table table = table(tableId);

        Map<String, String> optionsMap = optionalConfigMap(options);
        if (!optionsMap.isEmpty()) {
            table = table.copy(optionsMap);
        }

        float[] queryVector = parseVector(queryVectorStr);

        GlobalIndexResult result =
                table.newVectorSearchBuilder()
                        .withVector(queryVector)
                        .withVectorColumn(vectorColumn)
                        .withLimit(topK)
                        .executeLocal();

        RowType tableRowType = table.rowType();
        int[] projectionIndices = parseProjection(projection, tableRowType);

        ReadBuilder readBuilder = table.newReadBuilder();
        if (projectionIndices != null) {
            readBuilder.withProjection(projectionIndices);
        }

        TableScan.Plan plan = readBuilder.newScan().withGlobalIndexResult(result).plan();

        RowType readType =
                projectionIndices != null ? tableRowType.project(projectionIndices) : tableRowType;

        ByteArrayOutputStream byteOut = new ByteArrayOutputStream(1024);
        JsonOptions jsonOptions = new JsonOptions(new Options());
        try (JsonFormatWriter jsonWriter =
                        new JsonFormatWriter(
                                new ByteArrayPositionOutputStream(byteOut),
                                readType,
                                jsonOptions,
                                "none");
                RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
            reader.forEachRemaining(
                    row -> {
                        try {
                            jsonWriter.addElement(row);
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to convert row to JSON string", e);
                        }
                    });
        }

        String[] lines =
                StringUtils.split(byteOut.toString("UTF-8"), jsonOptions.getLineDelimiter());
        List<String> rows = new ArrayList<>(lines.length);
        for (String line : lines) {
            String trimmed = line.trim();
            if (!trimmed.isEmpty()) {
                rows.add(trimmed);
            }
        }
        return rows.toArray(new String[0]);
    }

    private static float[] parseVector(String vectorStr) {
        String[] parts = StringUtils.split(vectorStr, ",");
        float[] vector = new float[parts.length];
        for (int i = 0; i < parts.length; i++) {
            vector[i] = Float.parseFloat(parts[i].trim());
        }
        return vector;
    }

    private static int[] parseProjection(String projection, RowType rowType) {
        if (StringUtils.isNullOrWhitespaceOnly(projection)) {
            return null;
        }
        String[] projectionNames = StringUtils.split(projection, ",");
        return rowType.getFieldIndices(Arrays.stream(projectionNames).collect(Collectors.toList()));
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    /** A {@link PositionOutputStream} wrapping a {@link ByteArrayOutputStream}. */
    private static class ByteArrayPositionOutputStream extends PositionOutputStream {

        private final ByteArrayOutputStream out;

        private ByteArrayPositionOutputStream(ByteArrayOutputStream out) {
            this.out = out;
        }

        @Override
        public long getPos() {
            return out.size();
        }

        @Override
        public void write(int b) {
            out.write(b);
        }

        @Override
        public void write(byte[] b) throws IOException {
            out.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) {
            out.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            out.flush();
        }

        @Override
        public void close() throws IOException {
            out.close();
        }
    }
}
