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
import org.apache.paimon.flink.dataevolution.MergeIntoCommitterOperatorFactory;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.CommittableTypeInfo;
import org.apache.paimon.flink.sorter.SortOperator;
import org.apache.paimon.flink.utils.FlinkCalciteClasses;
import org.apache.paimon.flink.utils.InternalTypeInfo;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeCasts;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
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
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
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

    private final CoreOptions coreOptions;

    // field names of target table
    private final List<String> targetFieldNames;

    /**
     * Target Table's alias. The alias is implemented through rewriting merge-condition. For
     * example, if the original condition is `TempT.id=S.id`, it will be rewritten to `RT.id=S.id`.
     * `RT` means 'row-tracking target'. The reason is that _ROW_ID metadata field is exposed via
     * system table like `T$row_tracking`, so we have to rewrite merge condition. Moreover, if we
     * still create a temporary view such as `viewT`, then `viewT$row_tracking` is not a valid
     * table.
     */
    @Nullable private String targetAlias;

    // source table name
    private String sourceTable;

    // sqls to config environment and create source table
    @Nullable private String[] sourceSqls;

    // merge condition
    private String mergeCondition;
    private MergeConditionParser mergeConditionParser;

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
        try {
            this.mergeConditionParser = new MergeConditionParser(mergeCondition);
        } catch (Exception e) {
            LOG.error("Failed to parse merge condition: {}", mergeCondition, e);
            throw new RuntimeException("Failed to parse merge condition " + mergeCondition, e);
        }
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
        runInternal().await();
    }

    public TableResult runInternal() {
        // 1. build source
        Tuple2<DataStream<RowData>, RowType> sourceWithType = buildSource();
        // 2. shuffle by firstRowId
        DataStream<Tuple2<Long, RowData>> shuffled =
                shuffleByFirstRowId(sourceWithType.f0, sourceWithType.f1);
        // 3. write partial columns
        DataStream<Committable> written =
                writePartialColumns(shuffled, sourceWithType.f1, sinkParallelism);
        // 4. commit
        Set<String> updatedColumns =
                sourceWithType.f1.getFields().stream()
                        .map(DataField::name)
                        .filter(name -> !SpecialFields.ROW_ID.name().equals(name))
                        .collect(Collectors.toSet());
        DataStream<?> committed = commit(written, updatedColumns);

        // execute internal
        Transformation<?> transformations =
                committed
                        .sinkTo(new DiscardingSink<>())
                        .name("END")
                        .setParallelism(1)
                        .getTransformation();

        return executeInternal(
                Collections.singletonList(transformations),
                Collections.singletonList(identifier.getFullName()));
    }

    public Tuple2<DataStream<RowData>, RowType> buildSource() {
        // handle sqls
        handleSqls();

        // assign row id for each source row
        List<String> project;
        if (matchedUpdateSet.equals("*")) {
            // if sourceName is qualified like 'default.S', we should build a project like S.*
            project = Collections.singletonList(sourceTableName() + ".*");
        } else {
            // validate upsert changes
            Map<String, String> changes = parseCommaSeparatedKeyValues(matchedUpdateSet);
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
                                                    "%s AS `%s`",
                                                    entry.getValue(),
                                                    extractFieldName(entry.getKey())))
                            .collect(Collectors.toList());
        }

        String query;
        Optional<String> sourceRowIdField;
        try {
            sourceRowIdField = mergeConditionParser.extractRowIdFieldFromSource(targetTableName());
        } catch (Exception e) {
            LOG.error("Error happened when extract row id field from source table.", e);
            throw new RuntimeException(
                    "Error happened when extract row id field from source table.", e);
        }

        // if source table already contains _ROW_ID field, we could avoid join
        if (sourceRowIdField.isPresent()) {
            query =
                    String.format(
                            // cast _ROW_ID to BIGINT
                            "SELECT CAST(`%s`.`%s` AS BIGINT) AS `_ROW_ID`, %s FROM %s",
                            sourceTableName(),
                            sourceRowIdField.get(),
                            String.join(",", project),
                            escapedSourceName());
        } else {
            // use join to find matched rows and assign row id for each source row.
            // _ROW_ID is the first field of joined table.
            query =
                    String.format(
                            "SELECT %s, %s FROM %s INNER JOIN %s AS RT ON %s",
                            "`RT`.`_ROW_ID` as `_ROW_ID`",
                            String.join(",", project),
                            escapedSourceName(),
                            escapedRowTrackingTargetName(),
                            rewriteMergeCondition(mergeCondition));
        }

        LOG.info("Source query: {}", query);

        Table source = batchTEnv.sqlQuery(query);

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

        // if firstRowIds is not empty, there must be a valid nextRowId
        long maxRowId = table.latestSnapshot().get().nextRowId() - 1;

        OneInputTransformation<RowData, Tuple2<Long, RowData>> assignedFirstRowId =
                new OneInputTransformation<>(
                        sourceTransformation,
                        "ASSIGN FIRST_ROW_ID",
                        new StreamFlatMap<>(
                                new FirstRowIdAssigner(firstRowIds, maxRowId, sourceType)),
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

        // 2. write partial columns
        return sorted.transform(
                        "PARTIAL WRITE COLUMNS",
                        new CommittableTypeInfo(),
                        new DataEvolutionPartialWriteOperator((FileStoreTable) table, rowType))
                .setParallelism(sinkParallelism);
    }

    public DataStream<Committable> commit(
            DataStream<Committable> written, Set<String> updatedColumns) {
        FileStoreTable storeTable = (FileStoreTable) table;

        MergeIntoCommitterOperatorFactory factory =
                new MergeIntoCommitterOperatorFactory(storeTable, updatedColumns);

        return written.transform("COMMIT OPERATOR", new CommittableTypeInfo(), factory)
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
     * simplify its representation.
     */
    @VisibleForTesting
    public String rewriteMergeCondition(String mergeCondition) {
        try {
            Object rewrittenNode = mergeConditionParser.rewriteSqlNode(targetTableName(), "RT");
            return rewrittenNode.toString();
        } catch (Exception e) {
            LOG.error("Failed to rewrite merge condition: {}", mergeCondition, e);
            throw new RuntimeException("Failed to rewrite merge condition " + mergeCondition, e);
        }
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

        // Get updatable BLOB fields (descriptor-based fields that support partial updates)
        Set<String> updatableBlobFields = coreOptions.updatableBlobFields();

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
                                == LogicalTypeRoot.BIGINT,
                        "_ROW_ID field should be BIGINT type.");
            } else {
                DataField targetField = targetFields.get(flinkColumn.getName());
                if (targetField == null) {
                    throw new IllegalStateException(
                            "Column not found in target table: " + flinkColumn.getName());
                }
                if (targetField.type().getTypeRoot() == DataTypeRoot.BLOB
                        && !updatableBlobFields.contains(flinkColumn.getName())) {
                    throw new IllegalStateException(
                            "Should not append/update raw-data BLOB column '"
                                    + flinkColumn.getName()
                                    + "' through MERGE INTO. "
                                    + "Only descriptor-based BLOB columns (configured via '"
                                    + CoreOptions.BLOB_DESCRIPTOR_FIELD.key()
                                    + "' or '"
                                    + CoreOptions.BLOB_EXTERNAL_STORAGE_FIELD.key()
                                    + "') can be updated.");
                }

                DataType paimonType =
                        LogicalTypeConversion.toDataType(
                                flinkColumn.getDataType().getLogicalType());
                // For descriptor-based BLOB fields, allow BYTES (VARBINARY/BINARY) source
                // since BLOB is represented as BYTES in Flink SQL
                boolean blobCompatible =
                        targetField.type().getTypeRoot() == DataTypeRoot.BLOB
                                && updatableBlobFields.contains(flinkColumn.getName())
                                && paimonType
                                        .getTypeRoot()
                                        .getFamilies()
                                        .contains(DataTypeFamily.BINARY_STRING);
                if (!blobCompatible
                        && !DataTypeCasts.supportsCompatibleCast(paimonType, targetField.type())) {
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

    private String sourceTableName() {
        String[] splits = sourceTable.split("\\.");
        return splits[splits.length - 1];
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
                catalogName, identifier.getDatabaseName(), identifier.getObjectName());
    }

    /** The parser to parse merge condition through calcite sql parser. */
    static class MergeConditionParser {

        private final FlinkCalciteClasses calciteClasses;
        private final Object sqlNode;

        MergeConditionParser(String mergeCondition) throws Exception {
            this.calciteClasses = new FlinkCalciteClasses();
            this.sqlNode = initializeSqlNode(mergeCondition);
        }

        private Object initializeSqlNode(String mergeCondition) throws Exception {
            Object config =
                    calciteClasses
                            .configDelegate()
                            .withLex(
                                    calciteClasses.sqlParserDelegate().config(),
                                    calciteClasses.lexDelegate().java());
            Object sqlParser = calciteClasses.sqlParserDelegate().create(mergeCondition, config);
            return calciteClasses.sqlParserDelegate().parseExpression(sqlParser);
        }

        /**
         * Rewrite the SQL node, replacing all references from the 'from' table to the 'to' table.
         */
        public Object rewriteSqlNode(String from, String to) throws Exception {
            return rewriteNode(sqlNode, from, to);
        }

        private Object rewriteNode(Object node, String from, String to) throws Exception {
            // It's a SqlBasicCall, recursively rewrite children operands
            if (calciteClasses.sqlBasicCallDelegate().instanceOfSqlBasicCall(node)) {
                List<?> operandList = calciteClasses.sqlBasicCallDelegate().getOperandList(node);
                List<Object> newNodes = new java.util.ArrayList<>();
                for (Object operand : operandList) {
                    newNodes.add(rewriteNode(operand, from, to));
                }

                Object operator = calciteClasses.sqlBasicCallDelegate().getOperator(node);
                Object parserPos = calciteClasses.sqlBasicCallDelegate().getParserPosition(node);
                Object functionQuantifier =
                        calciteClasses.sqlBasicCallDelegate().getFunctionQuantifier(node);
                return calciteClasses
                        .sqlBasicCallDelegate()
                        .create(operator, newNodes, parserPos, functionQuantifier);
            } else if (calciteClasses.sqlIndentifierDelegate().instanceOfSqlIdentifier(node)) {
                // It's a sql identifier, try to replace the table name
                List<String> names = calciteClasses.sqlIndentifierDelegate().getNames(node);
                Preconditions.checkState(
                        names.size() >= 2, "Please specify the table name for the column: " + node);
                int nameLen = names.size();
                if (names.get(nameLen - 2).equals(from)) {
                    return calciteClasses.sqlIndentifierDelegate().setName(node, nameLen - 2, to);
                }
                return node;
            } else {
                return node;
            }
        }

        /**
         * Find the row id field in source table. This method looks for an equality condition like
         * `target_table._ROW_ID = source_table.some_field` or `source_table.some_field =
         * target_table._ROW_ID`, and returns the field name that is paired with _ROW_ID.
         */
        public Optional<String> extractRowIdFieldFromSource(String targetTable) throws Exception {
            Object operator = calciteClasses.sqlBasicCallDelegate().getOperator(sqlNode);
            Object kind = calciteClasses.sqlOperatorDelegate().getKind(operator);

            if (kind == calciteClasses.sqlKindDelegate().equals()) {
                List<?> operandList = calciteClasses.sqlBasicCallDelegate().getOperandList(sqlNode);

                Object left = operandList.get(0);
                Object right = operandList.get(1);

                if (calciteClasses.sqlIndentifierDelegate().instanceOfSqlIdentifier(left)
                        && calciteClasses.sqlIndentifierDelegate().instanceOfSqlIdentifier(right)) {

                    List<String> leftNames = calciteClasses.sqlIndentifierDelegate().getNames(left);
                    List<String> rightNames =
                            calciteClasses.sqlIndentifierDelegate().getNames(right);
                    Preconditions.checkState(
                            leftNames.size() >= 2,
                            "Please specify the table name for the column: " + left);
                    Preconditions.checkState(
                            rightNames.size() >= 2,
                            "Please specify the table name for the column: " + right);

                    if (leftNames.get(leftNames.size() - 1).equals(SpecialFields.ROW_ID.name())
                            && leftNames.get(leftNames.size() - 2).equals(targetTable)) {
                        return Optional.of(rightNames.get(rightNames.size() - 1));
                    } else if (rightNames
                                    .get(rightNames.size() - 1)
                                    .equals(SpecialFields.ROW_ID.name())
                            && rightNames.get(rightNames.size() - 2).equals(targetTable)) {
                        return Optional.of(leftNames.get(leftNames.size() - 1));
                    }
                    return Optional.empty();
                }
            }

            return Optional.empty();
        }
    }
}
