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
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.predicate.SimpleSqlPredicateConvertor;
import org.apache.paimon.flink.vectorsearch.FlinkVectorSearchBuilderImpl;
import org.apache.paimon.format.json.JsonFormatWriter;
import org.apache.paimon.format.json.JsonOptions;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.ScoreRecordIterator;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.table.source.VectorScan;
import org.apache.paimon.table.source.VectorSearchBuilder;
import org.apache.paimon.table.source.snapshot.TimeTravelUtil;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowUtils;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.StringUtils;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.paimon.CoreOptions.SCAN_MODE;
import static org.apache.paimon.CoreOptions.SCAN_SNAPSHOT_ID;
import static org.apache.paimon.CoreOptions.SCAN_TAG_NAME;
import static org.apache.paimon.CoreOptions.SCAN_TIMESTAMP;
import static org.apache.paimon.CoreOptions.SCAN_TIMESTAMP_MILLIS;
import static org.apache.paimon.CoreOptions.SCAN_VERSION;
import static org.apache.paimon.CoreOptions.SCAN_WATERMARK;
import static org.apache.paimon.partition.PartitionPredicate.splitPartitionPredicatesAndDataPredicates;
import static org.apache.paimon.predicate.PredicateVisitor.collectFieldNames;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Procedure for local or distributed vector search with optional filtering and scores. */
public class VectorSearchProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "vector_search";
    public static final String SEARCH_SCORE = "__paimon_search_score";

    private static final int MAX_TOP_K = 10_000;
    private static final DataField SEARCH_SCORE_FIELD =
            new DataField(Integer.MAX_VALUE, SEARCH_SCORE, DataTypes.FLOAT());

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
                @ArgumentHint(name = "options", type = @DataTypeHint("STRING"), isOptional = true),
                @ArgumentHint(name = "where", type = @DataTypeHint("STRING"), isOptional = true),
                @ArgumentHint(
                        name = "partitions",
                        type = @DataTypeHint("STRING"),
                        isOptional = true)
            })
    public String[] call(
            ProcedureContext procedureContext,
            String tableId,
            String vectorColumn,
            String queryVectorStr,
            Integer topK,
            String projection,
            String options,
            String where,
            String partitions)
            throws Exception {
        validateSearch(vectorColumn, queryVectorStr, topK);

        Table table = table(tableId);
        Map<String, String> optionsMap = optionalConfigMap(options);
        String queryAuthOption = CoreOptions.QUERY_AUTH_ENABLED.key();
        checkArgument(
                !optionsMap.containsKey(queryAuthOption),
                "Option '%s' is not allowed",
                queryAuthOption);
        if (!optionsMap.isEmpty()) {
            table = table.copy(optionsMap);
        }
        checkArgument(
                table instanceof FileStoreTable, "Vector search requires a file store table.");

        FileStoreTable fileStoreTable = (FileStoreTable) table;
        checkArgument(
                !fileStoreTable.coreOptions().queryAuthEnabled(),
                "Vector search does not support tables with query auth enabled.");
        RowType tableType = fileStoreTable.rowType();
        checkArgument(
                tableType.containsField(vectorColumn),
                "Vector column '%s' does not exist in table '%s'.",
                vectorColumn,
                tableId);
        checkArgument(
                tableType.notContainsField(SEARCH_SCORE),
                "Table column '%s' conflicts with vector-search metadata.",
                SEARCH_SCORE);

        float[] queryVector = parseVector(queryVectorStr);
        Predicate filter = parseFilter(where, tableType);
        PartitionPredicate partitionFilter = parsePartitions(partitions, fileStoreTable);
        Projection parsedProjection = Projection.parse(projection, tableType, filter);
        FilterParts filterParts = FilterParts.from(filter, fileStoreTable);
        validatePrimaryKeyFilter(fileStoreTable, vectorColumn, filterParts);

        fileStoreTable = resolveAndPinSnapshot(fileStoreTable);
        if (fileStoreTable == null) {
            return new String[0];
        }

        VectorSearchBuilder builder =
                newVectorSearchBuilder(procedureContext, fileStoreTable)
                        .withVector(queryVector)
                        .withVectorColumn(vectorColumn)
                        .withLimit(topK)
                        .withOptions(optionsMap);
        if (filter != null) {
            builder.withFilter(filter);
        }
        if (partitionFilter != null) {
            builder.withPartitionFilter(partitionFilter);
        }

        VectorScan.Plan vectorPlan = builder.newVectorScan().scan();
        GlobalIndexResult result = builder.newVectorRead().read(vectorPlan);

        ReadBuilder readBuilder = fileStoreTable.newReadBuilder();
        if (filter != null) {
            readBuilder.withFilter(filter);
        }
        PartitionPredicate effectivePartitionFilter =
                filterParts.mergePartitionFilter(partitionFilter);
        if (effectivePartitionFilter != null) {
            readBuilder.withPartitionFilter(effectivePartitionFilter);
        }
        if (parsedProjection.readProjection != null) {
            readBuilder.withProjection(parsedProjection.readProjection);
        }
        TableScan.Plan readPlan = readBuilder.newScan().withGlobalIndexResult(result).plan();
        return readRows(readBuilder, readPlan, parsedProjection);
    }

    private static VectorSearchBuilder newVectorSearchBuilder(
            ProcedureContext context, FileStoreTable table) {
        if (!table.coreOptions().vectorSearchDistributeEnabled()) {
            return table.newVectorSearchBuilder();
        }
        return new FlinkVectorSearchBuilderImpl(table, context.getExecutionEnvironment());
    }

    @Nullable
    static FileStoreTable resolveAndPinSnapshot(FileStoreTable table) {
        Snapshot snapshot = TimeTravelUtil.tryTravelOrLatest(table);
        if (snapshot == null) {
            return null;
        }

        Map<String, String> snapshotOptions = new LinkedHashMap<>();
        snapshotOptions.put(SCAN_VERSION.key(), null);
        snapshotOptions.put(SCAN_TAG_NAME.key(), null);
        snapshotOptions.put(SCAN_WATERMARK.key(), null);
        snapshotOptions.put(SCAN_TIMESTAMP.key(), null);
        snapshotOptions.put(SCAN_TIMESTAMP_MILLIS.key(), null);
        snapshotOptions.put(SCAN_MODE.key(), CoreOptions.StartupMode.FROM_SNAPSHOT.toString());
        snapshotOptions.put(SCAN_SNAPSHOT_ID.key(), String.valueOf(snapshot.id()));
        return table.copyWithoutTimeTravel(snapshotOptions);
    }

    private static void validateSearch(
            String vectorColumn, String queryVector, @Nullable Integer topK) {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(vectorColumn),
                "vector_column must not be blank");
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(queryVector), "query_vector must not be blank");
        checkArgument(topK != null && topK > 0, "top_k must be positive");
        checkArgument(topK <= MAX_TOP_K, "top_k must not exceed %s", MAX_TOP_K);
    }

    private static float[] parseVector(String vectorStr) {
        String[] parts = vectorStr.split(",", -1);
        float[] vector = new float[parts.length];
        for (int i = 0; i < parts.length; i++) {
            String part = parts[i].trim();
            checkArgument(!part.isEmpty(), "query_vector contains an empty value");
            try {
                vector[i] = Float.parseFloat(part);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        String.format("Invalid query vector value: '%s'", part), e);
            }
            checkArgument(
                    !Float.isNaN(vector[i]) && !Float.isInfinite(vector[i]),
                    "query_vector values must be finite");
        }
        return vector;
    }

    @Nullable
    private static Predicate parseFilter(@Nullable String where, RowType tableType)
            throws Exception {
        if (StringUtils.isNullOrWhitespaceOnly(where)) {
            return null;
        }
        return new SimpleSqlPredicateConvertor(tableType).convertSqlToPredicate(where);
    }

    @Nullable
    private static PartitionPredicate parsePartitions(
            @Nullable String partitions, FileStoreTable table) {
        if (StringUtils.isNullOrWhitespaceOnly(partitions)) {
            return null;
        }

        List<String> partitionKeys = table.partitionKeys();
        checkArgument(
                !partitionKeys.isEmpty(),
                "partitions can only be used with a partitioned table '%s'.",
                table.name());
        List<Map<String, String>> specs = new ArrayList<>();
        for (String rawSpec : partitions.split(";", -1)) {
            checkArgument(
                    !StringUtils.isNullOrWhitespaceOnly(rawSpec),
                    "Partition spec must not be blank in partitions '%s'.",
                    partitions);
            specs.add(parsePartitionSpec(rawSpec));
        }
        Set<String> expectedKeys = new HashSet<>(partitionKeys);
        for (Map<String, String> spec : specs) {
            checkArgument(
                    spec.keySet().equals(expectedKeys),
                    "Partition spec for table '%s' must exactly match partition keys %s, but was %s.",
                    table.name(),
                    partitionKeys,
                    spec);
        }
        return PartitionPredicate.fromMaps(
                table.schema().logicalPartitionType(),
                specs,
                table.coreOptions().partitionDefaultName());
    }

    private static Map<String, String> parsePartitionSpec(String rawSpec) {
        Map<String, String> spec = new LinkedHashMap<>();
        for (String rawKeyValue : rawSpec.split(",", -1)) {
            checkArgument(
                    !StringUtils.isNullOrWhitespaceOnly(rawKeyValue),
                    "Partition key-value must not be blank in spec '%s'.",
                    rawSpec);
            String[] keyValue = rawKeyValue.split("=", 2);
            checkArgument(
                    keyValue.length == 2,
                    "Invalid partition key-value '%s'. Please use format 'key=value'.",
                    rawKeyValue);
            String key = keyValue[0].trim();
            checkArgument(!key.isEmpty(), "Partition key must not be blank in spec '%s'.", rawSpec);
            checkArgument(
                    !spec.containsKey(key),
                    "Duplicate partition key '%s' in spec '%s'.",
                    key,
                    rawSpec);
            spec.put(key, keyValue[1].trim());
        }
        return spec;
    }

    private static void validatePrimaryKeyFilter(
            FileStoreTable table, String vectorColumn, FilterParts filterParts) {
        if (!table.coreOptions().primaryKeyVectorIndexColumns().contains(vectorColumn)
                || !filterParts.hasDataFilter()) {
            return;
        }
        checkArgument(
                table.coreOptions().deletionVectorsEnabled()
                        && !table.coreOptions().deletionVectorsMergeOnRead(),
                "Primary-key vector where filter on non-partition columns requires "
                        + "deletion-vectors.enabled = true and "
                        + "deletion-vectors.merge-on-read = false.");
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
                RecordReader<InternalRow> reader =
                        readBuilder.newRead().executeFilter().createReader(plan)) {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                try {
                    checkArgument(
                            batch instanceof ScoreRecordIterator,
                            "Vector-search reader did not expose search scores.");
                    ScoreRecordIterator<InternalRow> scoredBatch =
                            (ScoreRecordIterator<InternalRow>) batch;
                    InternalRow row;
                    while ((row = scoredBatch.next()) != null) {
                        float score = scoredBatch.returnedScore();
                        checkArgument(
                                !Float.isNaN(score),
                                "Vector-search reader returned a missing score.");
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
                checkArgument(
                        scoreIndex < scores.size(),
                        "Vector-search JSON rows and scores are misaligned.");
                rows.add(new ScoredJson(trimmed, scores.get(scoreIndex++)));
            }
        }
        checkArgument(
                scoreIndex == scores.size(), "Vector-search JSON rows and scores are misaligned.");
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
        @Nullable private final int[] readProjection;
        @Nullable private final int[] outputToRead;

        private Projection(
                RowType readType,
                RowType outputType,
                @Nullable int[] readProjection,
                @Nullable int[] outputToRead) {
            this.readType = readType;
            this.outputType = outputType;
            this.readProjection = readProjection;
            this.outputToRead = outputToRead;
        }

        private static Projection parse(
                @Nullable String projection, RowType tableType, @Nullable Predicate filter) {
            if (StringUtils.isNullOrWhitespaceOnly(projection)) {
                return new Projection(tableType, tableType, null, null);
            }

            String[] names = projection.split(",", -1);
            List<Integer> readIndices = new ArrayList<>();
            List<DataField> outputFields = new ArrayList<>();
            int[] outputToRead = new int[names.length];
            Set<String> seen = new HashSet<>();
            for (int i = 0; i < names.length; i++) {
                String name = names[i].trim();
                checkArgument(!name.isEmpty(), "Projection column must not be blank");
                checkArgument(seen.add(name), "Duplicate projection column: %s", name);
                if (SEARCH_SCORE.equals(name)) {
                    outputFields.add(SEARCH_SCORE_FIELD);
                    outputToRead[i] = -1;
                    continue;
                }

                int tableIndex = tableType.getFieldIndex(name);
                checkArgument(tableIndex >= 0, "Unknown projection column: %s", name);
                outputFields.add(tableType.getFields().get(tableIndex));
                outputToRead[i] = readIndices.size();
                readIndices.add(tableIndex);
            }

            if (filter != null) {
                Set<String> filterFields = collectFieldNames(filter);
                Set<Integer> included = new HashSet<>(readIndices);
                List<String> tableFields = tableType.getFieldNames();
                for (int i = 0; i < tableFields.size(); i++) {
                    if (filterFields.contains(tableFields.get(i)) && included.add(i)) {
                        readIndices.add(i);
                    }
                }
            }

            int[] readProjection = readIndices.stream().mapToInt(Integer::intValue).toArray();
            return new Projection(
                    tableType.project(readProjection),
                    new RowType(outputFields),
                    readProjection,
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

    private static class FilterParts {

        @Nullable private final PartitionPredicate partitionFilter;
        private final boolean hasDataFilter;

        private FilterParts(@Nullable PartitionPredicate partitionFilter, boolean hasDataFilter) {
            this.partitionFilter = partitionFilter;
            this.hasDataFilter = hasDataFilter;
        }

        private static FilterParts from(@Nullable Predicate filter, FileStoreTable table) {
            if (filter == null) {
                return new FilterParts(null, false);
            }
            Pair<Optional<PartitionPredicate>, List<Predicate>> parts =
                    splitPartitionPredicatesAndDataPredicates(
                            filter, table.rowType(), table.partitionKeys());
            return new FilterParts(parts.getLeft().orElse(null), !parts.getRight().isEmpty());
        }

        private boolean hasDataFilter() {
            return hasDataFilter;
        }

        @Nullable
        private PartitionPredicate mergePartitionFilter(
                @Nullable PartitionPredicate explicitPartitionFilter) {
            List<PartitionPredicate> filters = new ArrayList<>(2);
            if (partitionFilter != null) {
                filters.add(partitionFilter);
            }
            if (explicitPartitionFilter != null) {
                filters.add(explicitPartitionFilter);
            }
            return PartitionPredicate.and(filters);
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
