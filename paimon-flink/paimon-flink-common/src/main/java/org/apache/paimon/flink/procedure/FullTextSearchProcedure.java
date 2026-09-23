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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.json.JsonFormatWriter;
import org.apache.paimon.format.json.JsonOptions;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.ScoreRecordIterator;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowUtils;
import org.apache.paimon.utils.StringUtils;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Procedure for local full-text search with optional JSON projection and search scores. */
public class FullTextSearchProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "full_text_search";
    public static final String SEARCH_SCORE = "__paimon_search_score";

    private static final int MAX_TOP_K = 10_000;

    private static final DataField SEARCH_SCORE_FIELD =
            new DataField(Integer.MAX_VALUE, SEARCH_SCORE, DataTypes.FLOAT());

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "column", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "query", type = @DataTypeHint("STRING")),
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
            String column,
            String query,
            Integer topK,
            String projection,
            String options)
            throws Exception {
        validateSearch(column, query, topK);

        Table table = table(tableId);
        Map<String, String> optionsMap = optionalConfigMap(options);
        String queryAuthOption = CoreOptions.QUERY_AUTH_ENABLED.key();
        if (optionsMap.containsKey(queryAuthOption)) {
            throw new IllegalArgumentException(
                    String.format("Option '%s' is not allowed", queryAuthOption));
        }
        if (!optionsMap.isEmpty()) {
            table = table.copy(optionsMap);
        }

        Projection parsedProjection = Projection.parse(projection, table.rowType());
        GlobalIndexResult result =
                table.newFullTextSearchBuilder()
                        .withQuery(column, query)
                        .withLimit(topK)
                        .executeLocal();

        ReadBuilder readBuilder = table.newReadBuilder();
        if (parsedProjection.dataProjection != null) {
            readBuilder.withProjection(parsedProjection.dataProjection);
        }
        TableScan.Plan plan = readBuilder.newScan().withGlobalIndexResult(result).plan();
        return readRows(readBuilder, plan, parsedProjection);
    }

    private static void validateSearch(String column, String query, Integer topK) {
        if (StringUtils.isNullOrWhitespaceOnly(column)) {
            throw new IllegalArgumentException("column must not be blank");
        }
        if (StringUtils.isNullOrWhitespaceOnly(query)) {
            throw new IllegalArgumentException("query must not be blank");
        }
        if (topK == null || topK <= 0) {
            throw new IllegalArgumentException("top_k must be positive");
        }
        if (topK > MAX_TOP_K) {
            throw new IllegalArgumentException("top_k must not exceed " + MAX_TOP_K);
        }
    }

    private static String[] readRows(
            ReadBuilder readBuilder, TableScan.Plan plan, Projection projection)
            throws IOException {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream(1024);
        JsonOptions jsonOptions = new JsonOptions(new Options());
        List<Float> scores = new ArrayList<>();
        try (JsonFormatWriter jsonWriter =
                        new JsonFormatWriter(
                                new ByteArrayPositionOutputStream(byteOut),
                                projection.outputType,
                                jsonOptions,
                                "none");
                RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                try {
                    if (!(batch instanceof ScoreRecordIterator)) {
                        throw new IllegalStateException(
                                "Full-text search reader did not expose search scores.");
                    }
                    ScoreRecordIterator<InternalRow> scoredBatch =
                            (ScoreRecordIterator<InternalRow>) batch;
                    InternalRow row;
                    while ((row = scoredBatch.next()) != null) {
                        float score = scoredBatch.returnedScore();
                        if (Float.isNaN(score)) {
                            throw new IllegalStateException(
                                    "Full-text search reader returned a missing score.");
                        }
                        jsonWriter.addElement(projection.output(row, score));
                        scores.add(score);
                    }
                } finally {
                    batch.releaseBatch();
                }
            }
        }

        String[] lines =
                StringUtils.split(byteOut.toString("UTF-8"), jsonOptions.getLineDelimiter());
        List<ScoredJson> rows = new ArrayList<>(lines.length);
        int scoreIndex = 0;
        for (String line : lines) {
            String trimmed = line.trim();
            if (!trimmed.isEmpty()) {
                if (scoreIndex >= scores.size()) {
                    throw new IllegalStateException(
                            "Full-text JSON rows and scores are misaligned.");
                }
                rows.add(new ScoredJson(trimmed, scores.get(scoreIndex++)));
            }
        }
        if (scoreIndex != scores.size()) {
            throw new IllegalStateException("Full-text JSON rows and scores are misaligned.");
        }
        Collections.sort(
                rows,
                (left, right) -> {
                    int scoreOrder = Float.compare(right.score, left.score);
                    return scoreOrder != 0 ? scoreOrder : left.json.compareTo(right.json);
                });
        return rows.stream().map(row -> row.json).toArray(String[]::new);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    private static class Projection {

        private final RowType readType;
        private final RowType outputType;
        private final int[] dataProjection;
        private final int[] outputToRead;

        private Projection(
                RowType readType, RowType outputType, int[] dataProjection, int[] outputToRead) {
            this.readType = readType;
            this.outputType = outputType;
            this.dataProjection = dataProjection;
            this.outputToRead = outputToRead;
        }

        private static Projection parse(String projection, RowType tableType) {
            if (StringUtils.isNullOrWhitespaceOnly(projection)) {
                return new Projection(tableType, tableType, null, null);
            }

            String[] names = projection.split(",", -1);
            List<Integer> dataIndices = new ArrayList<>();
            List<DataField> outputFields = new ArrayList<>();
            int[] outputToRead = new int[names.length];
            Set<String> seen = new HashSet<>();
            for (int i = 0; i < names.length; i++) {
                String name = names[i].trim();
                if (name.isEmpty()) {
                    throw new IllegalArgumentException("Projection column must not be blank");
                }
                if (!seen.add(name)) {
                    throw new IllegalArgumentException("Duplicate projection column: " + name);
                }
                if (SEARCH_SCORE.equals(name)) {
                    outputFields.add(SEARCH_SCORE_FIELD);
                    outputToRead[i] = -1;
                    continue;
                }

                int tableIndex = tableType.getFieldIndex(name);
                if (tableIndex < 0) {
                    throw new IllegalArgumentException("Unknown projection column: " + name);
                }
                outputFields.add(tableType.getFields().get(tableIndex));
                outputToRead[i] = dataIndices.size();
                dataIndices.add(tableIndex);
            }

            int[] dataProjection = dataIndices.stream().mapToInt(Integer::intValue).toArray();
            return new Projection(
                    tableType.project(dataProjection),
                    new RowType(outputFields),
                    dataProjection,
                    outputToRead);
        }

        private InternalRow output(InternalRow row, float score) {
            if (outputToRead == null) {
                return row;
            }
            GenericRow output = new GenericRow(outputToRead.length);
            for (int i = 0; i < outputToRead.length; i++) {
                int readIndex = outputToRead[i];
                output.setField(
                        i,
                        readIndex < 0
                                ? score
                                : InternalRowUtils.get(
                                        row, readIndex, readType.getTypeAt(readIndex)));
            }
            return output;
        }
    }

    private static class ScoredJson {

        private final String json;
        private final float score;

        private ScoredJson(String json, float score) {
            this.json = json;
            this.score = score;
        }
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
