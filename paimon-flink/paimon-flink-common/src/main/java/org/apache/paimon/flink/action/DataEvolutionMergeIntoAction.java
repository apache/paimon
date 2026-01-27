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

package org.apache.paimon.flink.action;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.FlinkRowWrapper;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.dataevolution.DataEvolutionPartialWriteOperator;
import org.apache.paimon.flink.dataevolution.FirstRowIdAssigner;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.CommittableTypeInfo;
import org.apache.paimon.flink.sink.CommitterOperatorFactory;
import org.apache.paimon.flink.sink.NoopCommittableStateManager;
import org.apache.paimon.flink.sink.StoreCommitter;
import org.apache.paimon.flink.sorter.SortOperator;
import org.apache.paimon.flink.utils.InternalTypeInfo;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeCasts;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.StringUtils;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.utils.ParameterUtils.parseCommaSeparatedKeyValues;

/**
 * The flink action for 'MERGE INTO' on Data-Evolution Table. This action is specially implemented
 * for the Data-Evolution pattern which can batch insert and update columns without rewriting
 * existing data files. This is a simplified version of standard 'MERGE INTO': we do not support
 * deleting or appending data now.
 *
 * <pre><code>
 *  MERGE INTO target-table
 *  USING source-table | source-expr AS source-alias
 *  ON merge-condition
 *  WHEN MATCHED
 *    THEN UPDATE SET xxx
 * </code></pre>
 */
public class DataEvolutionMergeIntoAction extends TableActionBase {

    private static final Logger LOG = LoggerFactory.getLogger(DataEvolutionMergeIntoAction.class);
    public static final String IDENTIFIER_QUOTE = "`";

    private final CoreOptions coreOptions;

    // field names of target table
    private final List<String> targetFieldNames;

    // target table alias
    @Nullable private String targetAlias;

    // source table name
    private String sourceTable;

    // sqls to config environment and create source table
    @Nullable private String[] sourceSqls;

    // merge condition
    private String mergeCondition;

    // set statement
    private String matchedUpdateSet;

    private int sinkParallelism;

    public DataEvolutionMergeIntoAction(
            String databaseName, String tableName, Map<String, String> catalogConfig) {
        super(databaseName, tableName, catalogConfig);

        if (!(table instanceof FileStoreTable)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Only FileStoreTable supports merge-into action. The table type is '%s'.",
                            table.getClass().getName()));
        }

        this.coreOptions = ((FileStoreTable) table).coreOptions();

        if (!coreOptions.dataEvolutionEnabled()) {
            throw new UnsupportedOperationException(
                    "Only DataEvolutionTable supports data-evolution merge-into action.");
        }

        // init field names of target table
        targetFieldNames =
                table.rowType().getFields().stream()
                        .map(DataField::name)
                        .collect(Collectors.toList());

        System.out.println(table.fullName());
    }

    public DataEvolutionMergeIntoAction withSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
        return this;
    }

    public DataEvolutionMergeIntoAction withSourceSqls(String... sourceSqls) {
        this.sourceSqls = sourceSqls;
        return this;
    }

    public DataEvolutionMergeIntoAction withTargetAlias(String targetAlias) {
        this.targetAlias = targetAlias;
        return this;
    }

    public DataEvolutionMergeIntoAction withMergeCondition(String mergeCondition) {
        this.mergeCondition = mergeCondition;
        return this;
    }

    public DataEvolutionMergeIntoAction withMatchedUpdateSet(String matchedUpdateSet) {
        this.matchedUpdateSet = matchedUpdateSet;
        return this;
    }

    public DataEvolutionMergeIntoAction withSinkParallelism(int sinkParallelism) {
        this.sinkParallelism = sinkParallelism;
        return this;
    }

    @Override
    public void run() throws Exception {
        // 1. build source
        Tuple2<DataStream<RowData>, RowType> sourceWithType = buildSource();
        // 2. shuffle by firstRowId
        DataStream<Tuple2<Long, RowData>> shuffled =
                shuffleByFirstRowId(sourceWithType.f0, sourceWithType.f1);
        // 3. write partial columns
        DataStream<Committable> written =
                writePartialColumns(shuffled, sourceWithType.f1, sinkParallelism);
        // 4. commit
        DataStream<?> committed = commit(written);

        // execute internal
        Transformation<?> transformations =
                committed
                        .sinkTo(new DiscardingSink<>())
                        .name("END")
                        .setParallelism(1)
                        .getTransformation();

        executeInternal(
                        Collections.singletonList(transformations),
                        Collections.singletonList(identifier.getFullName()))
                .await();
    }

    public Tuple2<DataStream<RowData>, RowType> buildSource() {
        // handle aliases
        handleTargetAlias();

        // handle sqls
        handleSqls();

        // assign row id for each source row
        List<String> project;
        if (matchedUpdateSet.equals("*")) {
            // if sourceName is qualified like 'default.S', we should build a project like S.*
            String[] splits = sourceTable.split("\\.");
            project = Collections.singletonList(splits[splits.length - 1] + ".*");
        } else {
            // validate upsert changes
            Map<String, String> changes = parseCommaSeparatedKeyValues(matchedUpdateSet);
            System.out.println("Target fieldNames: " + targetFieldNames);
            System.out.println("Changes: " + changes);
            for (String targetField : changes.keySet()) {
                if (!targetFieldNames.contains(extractFieldName(targetField))) {
                    throw new RuntimeException(
                            String.format(
                                    "Invalid column reference '%s' of table '%s' at matched-upsert action.",
                                    targetField, identifier.getFullName()));
                }
            }

            // rename source table's selected columns according to SET statement
            project =
                    changes.entrySet().stream()
                            .map(
                                    entry ->
                                            String.format(
                                                    "%s.`%s` AS `%s`",
                                                    escapedSourceName(),
                                                    extractFieldName(entry.getValue()),
                                                    extractFieldName(entry.getKey())))
                            .collect(Collectors.toList());
        }

        // use join to find matched rows and assign row id for each source row.
        // _ROW_ID is the first field of joined table.
        String query =
                String.format(
                        "SELECT %s, %s FROM %s INNER JOIN %s AS RT ON %s",
                        "`RT`.`_ROW_ID` as `_ROW_ID`",
                        String.join(",", project),
                        escapedSourceName(),
                        escapedRowTrackingTargetName(),
                        rewriteMergeCondition(mergeCondition));

        LOG.info("Source query: {}", query);

        System.out.println("Source query: " + query);

        Table source = batchTEnv.sqlQuery(query);

        System.out.println("source schema:" + source.getResolvedSchema().toString());
        checkSchema(source);
        RowType sourceType =
                SpecialFields.rowTypeWithRowId(table.rowType())
                        .project(source.getResolvedSchema().getColumnNames());

        return Tuple2.of(toDataStream(source), sourceType);
    }

    public DataStream<Tuple2<Long, RowData>> shuffleByFirstRowId(
            DataStream<RowData> source, RowType sourceType) {
        Transformation<RowData> sourceTransformation = source.getTransformation();
        List<Long> firstRowIds =
                ((FileStoreTable) table)
                        .store().newScan()
                                .withManifestEntryFilter(
                                        entry ->
                                                entry.file().firstRowId() != null
                                                        && !isBlobFile(entry.file().fileName()))
                                .plan().files().stream()
                                .map(entry -> entry.file().nonNullFirstRowId())
                                .sorted()
                                .collect(Collectors.toList());

        Preconditions.checkState(
                !firstRowIds.isEmpty(), "Should not MERGE INTO an empty target table.");

        System.out.print("First RowIDs: " + firstRowIds);

        OneInputTransformation<RowData, Tuple2<Long, RowData>> assignedFirstRowId =
                new OneInputTransformation<>(
                        sourceTransformation,
                        "ASSIGN FIRST_ROW_ID",
                        new StreamMap<>(new FirstRowIdAssigner(firstRowIds, sourceType)),
                        new TupleTypeInfo<>(
                                BasicTypeInfo.LONG_TYPE_INFO, sourceTransformation.getOutputType()),
                        sourceTransformation.getParallelism(),
                        sourceTransformation.isParallelismConfigured());

        // shuffle by firstRowId
        return new DataStream<>(source.getExecutionEnvironment(), assignedFirstRowId)
                .partitionCustom(
                        new FirstRowIdAssigner.FirstRowIdPartitioner(),
                        new FirstRowIdAssigner.FirstRowIdKeySelector());
    }

    public DataStream<Committable> writePartialColumns(
            DataStream<Tuple2<Long, RowData>> shuffled, RowType rowType, int sinkParallelism) {
        // 1. sort data by row id
        InternalTypeInfo<InternalRow> typeInfo = InternalTypeInfo.fromRowType(rowType);
        RowType sortType = rowType.project(SpecialFields.ROW_ID.name());
        DataStream<InternalRow> sorted =
                shuffled.map(t -> new FlinkRowWrapper(t.f1), typeInfo)
                        .setParallelism(sinkParallelism)
                        .transform(
                                "SORT BY _ROW_ID",
                                typeInfo,
                                new SortOperator(
                                        sortType,
                                        rowType,
                                        coreOptions.writeBufferSize(),
                                        coreOptions.pageSize(),
                                        coreOptions.localSortMaxNumFileHandles(),
                                        coreOptions.spillCompressOptions(),
                                        sinkParallelism,
                                        coreOptions.writeBufferSpillDiskSize(),
                                        coreOptions.sequenceFieldSortOrderIsAscending()))
                        .setParallelism(sinkParallelism);

        return sorted.transform(
                        "PARTIAL WRITE COLUMNS",
                        new CommittableTypeInfo(),
                        new DataEvolutionPartialWriteOperator((FileStoreTable) table, rowType))
                .setParallelism(sinkParallelism);
    }

    public DataStream<Committable> commit(DataStream<Committable> written) {
        FileStoreTable storeTable = (FileStoreTable) table;
        OneInputStreamOperatorFactory<Committable, Committable> committerOperator =
                new CommitterOperatorFactory<>(
                        false,
                        true,
                        "DataEvolutionMergeInto",
                        context ->
                                new StoreCommitter(
                                        storeTable,
                                        storeTable.newCommit(context.commitUser()),
                                        context),
                        new NoopCommittableStateManager());

        return written.transform("COMMIT OPERATOR", new CommittableTypeInfo(), committerOperator)
                .setParallelism(1)
                .setMaxParallelism(1);
    }

    private DataStream<RowData> toDataStream(Table source) {
        List<DataStructureConverter<Object, Object>> converters =
                source.getResolvedSchema().getColumns().stream()
                        .map(Column::getDataType)
                        .map(DataStructureConverters::getConverter)
                        .collect(Collectors.toList());

        return batchTEnv
                .toDataStream(source)
                .map(
                        row -> {
                            int arity = row.getArity();
                            GenericRowData rowData = new GenericRowData(row.getKind(), arity);
                            for (int i = 0; i < arity; i++) {
                                rowData.setField(
                                        i, converters.get(i).toInternalOrNull(row.getField(i)));
                            }
                            return rowData;
                        });
    }

    /**
     * Rewrite merge condition, replacing all references to target table with the alias 'RT'. This
     * is necessary because in Flink, row-tracking metadata columns (e.g. _ROW_ID, SEQUENCE_NUMBER)
     * are exposed through system table (i.e. {@code SELECT * FROM T$row_tracking}), we use 'RT' to
     * simplify it.
     */
    @VisibleForTesting
    public String rewriteMergeCondition(String mergeCondition) {
        // skip single and double-quoted chunks
        String skipQuoted = "'(?:''|[^'])*'" + "|\"(?:\"\"|[^\"])*\"";
        String targetTableRegex =
                "(?i)(?:\\b"
                        + Pattern.quote(targetTableName())
                        + "\\b|`"
                        + Pattern.quote(targetTableName())
                        + "`)\\s*\\.";

        Pattern pattern = Pattern.compile(skipQuoted + "|(" + targetTableRegex + ")");
        Matcher matcher = pattern.matcher(mergeCondition);

        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            if (matcher.group(1) != null) {
                matcher.appendReplacement(sb, Matcher.quoteReplacement("`RT`."));
            } else {
                matcher.appendReplacement(sb, Matcher.quoteReplacement(matcher.group(0)));
            }
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    /**
     * Check the schema of generated source data. All columns of source table should be present in
     * target table, and it should also contain a _ROW_ID column.
     *
     * @param source source table
     */
    private void checkSchema(Table source) {
        Map<String, DataField> targetFields =
                table.rowType().getFields().stream()
                        .collect(Collectors.toMap(DataField::name, Function.identity()));
        List<String> partitionKeys = ((FileStoreTable) table).schema().partitionKeys();

        List<Column> flinkColumns = source.getResolvedSchema().getColumns();
        boolean foundRowIdColumn = false;
        for (Column flinkColumn : flinkColumns) {
            if (partitionKeys.contains(flinkColumn.getName())) {
                throw new IllegalStateException(
                        "User should not update partition columns: " + flinkColumn.getName());
            }
            if (flinkColumn.getName().equals("_ROW_ID")) {
                foundRowIdColumn = true;
                Preconditions.checkState(
                        flinkColumn.getDataType().getLogicalType().getTypeRoot()
                                == LogicalTypeRoot.BIGINT);
            } else {
                DataField targetField = targetFields.get(flinkColumn.getName());
                if (targetField == null) {
                    throw new IllegalStateException(
                            "Column not found in target table: " + flinkColumn.getName());
                }
                if (targetField.type().getTypeRoot() == DataTypeRoot.BLOB) {
                    throw new IllegalStateException(
                            "Should not append/update new BLOB column through MERGE INTO.");
                }

                DataType paimonType =
                        LogicalTypeConversion.toDataType(
                                flinkColumn.getDataType().getLogicalType());
                if (!DataTypeCasts.supportsCompatibleCast(paimonType, targetField.type())) {
                    throw new IllegalStateException(
                            String.format(
                                    "DataType incompatible of field %s: %s is not compatible with %s",
                                    flinkColumn.getName(), paimonType, targetField.type()));
                }
            }
        }
        if (!foundRowIdColumn) {
            throw new IllegalStateException("_ROW_ID column not found in generated source.");
        }
    }

    private void handleTargetAlias() {
        if (targetAlias != null) {
            // create a view 'targetAlias' in the path of target table, then we can find it with the
            // qualified name
            batchTEnv.createTemporaryView(
                    escapedTargetName(), batchTEnv.from(identifier.getFullName()));
        }
    }

    private void handleSqls() {
        // NOTE: sql may change current catalog and database
        if (sourceSqls != null) {
            for (String sql : sourceSqls) {
                try {
                    batchTEnv.executeSql(sql).await();
                } catch (Throwable t) {
                    String errMsg = "Error occurs when executing sql:\n%s";
                    LOG.error(String.format(errMsg, sql), t);
                    throw new RuntimeException(String.format(errMsg, sql), t);
                }
            }
        }
    }

    private String targetTableName() {
        return targetAlias == null ? identifier.getObjectName() : targetAlias;
    }

    private String escapedSourceName() {
        return Arrays.stream(sourceTable.split("\\."))
                .map(s -> String.format("`%s`", s))
                .collect(Collectors.joining("."));
    }

    private String extractFieldName(String sourceField) {
        String[] fieldPath = sourceField.split("\\.");
        return fieldPath[fieldPath.length - 1];
    }

    private String escapedRowTrackingTargetName() {
        return String.format(
                "`%s`.`%s`.`%s$row_tracking`",
                catalogName, identifier.getDatabaseName(), targetTableName());
    }

    private String escapedTargetName() {
        return String.format(
                "`%s`.`%s`.`%s`", catalogName, identifier.getDatabaseName(), targetTableName());
    }

    private List<String> normalizeFieldName(List<String> fieldNames) {
        return fieldNames.stream().map(this::normalizeFieldName).collect(Collectors.toList());
    }

    private String normalizeFieldName(String fieldName) {
        if (StringUtils.isNullOrWhitespaceOnly(fieldName) || fieldName.endsWith(IDENTIFIER_QUOTE)) {
            return fieldName;
        }

        String[] splitFieldNames = fieldName.split("\\.");
        if (!targetFieldNames.contains(splitFieldNames[splitFieldNames.length - 1])) {
            return fieldName;
        }

        return String.join(
                ".",
                Arrays.stream(splitFieldNames)
                        .map(
                                part ->
                                        part.endsWith(IDENTIFIER_QUOTE)
                                                ? part
                                                : IDENTIFIER_QUOTE + part + IDENTIFIER_QUOTE)
                        .toArray(String[]::new));
    }
}
