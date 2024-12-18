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
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.paimon.CoreOptions.MergeEngine.DEDUPLICATE;
import static org.apache.paimon.CoreOptions.MergeEngine.PARTIAL_UPDATE;
import static org.apache.paimon.utils.ParameterUtils.parseCommaSeparatedKeyValues;

/**
 * Flink action for 'MERGE INTO', which references the syntax as follows (we use 'upsert' semantics
 * instead of 'update'):
 *
 * <pre><code>
 *  MERGE INTO target-table
 *  USING source-table | source-expr AS source-alias
 *  ON merge-condition
 *  WHEN MATCHED [AND matched-condition]
 *    THEN UPDATE SET xxx
 *  WHEN MATCHED [AND matched-condition]
 *    THEN DELETE
 *  WHEN NOT MATCHED [AND not-matched-condition]
 *    THEN INSERT VALUES (xxx)
 *  WHEN NOT MATCHED BY SOURCE [AND not-matched-by-source-condition]
 *    THEN UPDATE SET xxx
 *  WHEN NOT MATCHED BY SOURCE [AND not-matched-by-source-condition]
 *    THEN DELETE
 * </code></pre>
 *
 * <p>It builds a query to find the rows to be changed. INNER JOIN with merge-condition is used to
 * find MATCHED rows, and NOT EXISTS with merge-condition is used to find NOT MATCHED rows, then the
 * condition of each action is used to filter the rows.
 */
public class MergeIntoAction extends TableActionBase {

    private static final Logger LOG = LoggerFactory.getLogger(MergeIntoAction.class);

    // primary keys of target table
    private final List<String> primaryKeys;

    // converters for Row to RowData
    private final List<DataStructureConverter<Object, Object>> converters;

    // field names of target table
    private final List<String> targetFieldNames;

    // target table
    @Nullable private String targetAlias;

    // source table
    private String sourceTable;

    // sqls to config environment and create source table
    @Nullable private String[] sourceSqls;

    // merge condition
    private String mergeCondition;

    // actions to be taken
    private boolean matchedUpsert;
    private boolean notMatchedUpsert;
    private boolean matchedDelete;
    private boolean notMatchedDelete;
    private boolean insert;

    // upsert
    @Nullable String matchedUpsertCondition;
    @Nullable private String matchedUpsertSet;

    @Nullable String notMatchedBySourceUpsertCondition;
    @Nullable String notMatchedBySourceUpsertSet;

    // delete
    @Nullable String matchedDeleteCondition;
    @Nullable String notMatchedBySourceDeleteCondition;

    // insert
    @Nullable private String notMatchedInsertCondition;
    @Nullable private String notMatchedInsertValues;

    public MergeIntoAction(String database, String tableName, Map<String, String> catalogConfig) {
        super(database, tableName, catalogConfig);

        if (!(table instanceof FileStoreTable)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Only FileStoreTable supports merge-into action. The table type is '%s'.",
                            table.getClass().getName()));
        }

        // init primaryKeys of target table
        primaryKeys = ((FileStoreTable) table).schema().primaryKeys();
        if (primaryKeys.isEmpty()) {
            throw new UnsupportedOperationException(
                    "merge-into action doesn't support table with no primary keys defined.");
        }

        // init DataStructureConverters
        converters =
                table.rowType().getFieldTypes().stream()
                        .map(LogicalTypeConversion::toLogicalType)
                        .map(TypeConversions::fromLogicalToDataType)
                        .map(DataStructureConverters::getConverter)
                        .collect(Collectors.toList());

        // init field names of target table
        targetFieldNames =
                table.rowType().getFields().stream()
                        .map(DataField::name)
                        .collect(Collectors.toList());
    }

    public MergeIntoAction withTargetAlias(String targetAlias) {
        this.targetAlias = targetAlias;
        return this;
    }

    public MergeIntoAction withSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
        return this;
    }

    public MergeIntoAction withSourceSqls(String... sourceSqls) {
        this.sourceSqls = sourceSqls;
        return this;
    }

    public MergeIntoAction withMergeCondition(String mergeCondition) {
        this.mergeCondition = mergeCondition;
        return this;
    }

    public MergeIntoAction withMatchedUpsert(
            @Nullable String matchedUpsertCondition, String matchedUpsertSet) {
        this.matchedUpsert = true;
        this.matchedUpsertCondition = matchedUpsertCondition;
        this.matchedUpsertSet = matchedUpsertSet;
        return this;
    }

    public MergeIntoAction withNotMatchedBySourceUpsert(
            @Nullable String notMatchedBySourceUpsertCondition,
            String notMatchedBySourceUpsertSet) {
        this.notMatchedUpsert = true;
        this.notMatchedBySourceUpsertCondition = notMatchedBySourceUpsertCondition;
        this.notMatchedBySourceUpsertSet = notMatchedBySourceUpsertSet;
        return this;
    }

    public MergeIntoAction withMatchedDelete(@Nullable String matchedDeleteCondition) {
        this.matchedDelete = true;
        this.matchedDeleteCondition = matchedDeleteCondition;
        return this;
    }

    public MergeIntoAction withNotMatchedBySourceDelete(
            @Nullable String notMatchedBySourceDeleteCondition) {
        this.notMatchedDelete = true;
        this.notMatchedBySourceDeleteCondition = notMatchedBySourceDeleteCondition;
        return this;
    }

    public MergeIntoAction withNotMatchedInsert(
            @Nullable String notMatchedInsertCondition, String notMatchedInsertValues) {
        this.insert = true;
        this.notMatchedInsertCondition = notMatchedInsertCondition;
        this.notMatchedInsertValues = notMatchedInsertValues;
        return this;
    }

    public void validate() {
        if (!matchedUpsert && !notMatchedUpsert && !matchedDelete && !notMatchedDelete && !insert) {
            throw new IllegalArgumentException(
                    "Must specify at least one merge action. Run 'merge_into --help' for help.");
        }

        CoreOptions.MergeEngine mergeEngine = CoreOptions.fromMap(table.options()).mergeEngine();
        boolean supportMergeInto = mergeEngine == DEDUPLICATE || mergeEngine == PARTIAL_UPDATE;
        if (!supportMergeInto) {
            throw new UnsupportedOperationException(
                    String.format("Merge engine %s can not support merge-into.", mergeEngine));
        }

        if ((matchedUpsert && matchedDelete)
                && (matchedUpsertCondition == null || matchedDeleteCondition == null)) {
            throw new IllegalArgumentException(
                    "If both matched-upsert and matched-delete actions are present, their conditions must both be present too.");
        }

        if ((notMatchedUpsert && notMatchedDelete)
                && (notMatchedBySourceUpsertCondition == null
                        || notMatchedBySourceDeleteCondition == null)) {
            throw new IllegalArgumentException(
                    "If both not-matched-by-source-upsert and not-matched-by--source-delete actions are present, "
                            + "their conditions must both be present too.\n");
        }

        if (notMatchedBySourceUpsertSet != null && notMatchedBySourceUpsertSet.equals("*")) {
            throw new IllegalArgumentException(
                    "The '*' cannot be used in not_matched_by_source_upsert_set");
        }
    }

    @Override
    public void run() throws Exception {
        DataStream<RowData> dataStream = buildDataStream();
        batchSink(dataStream).await();
    }

    public DataStream<RowData> buildDataStream() {
        // handle aliases
        handleTargetAlias();

        // handle sqls
        handleSqls();

        // get data streams for all actions
        List<DataStream<RowData>> dataStreams =
                Stream.of(
                                getMatchedUpsertDataStream(),
                                getNotMatchedUpsertDataStream(),
                                getMatchedDeleteDataStream(),
                                getNotMatchedDeleteDataStream(),
                                getInsertDataStream())
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .collect(Collectors.toList());

        DataStream<RowData> firstDs = dataStreams.get(0);
        return firstDs.union(dataStreams.stream().skip(1).toArray(DataStream[]::new));
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

    private Optional<DataStream<RowData>> getMatchedUpsertDataStream() {
        if (!matchedUpsert) {
            return Optional.empty();
        }

        List<String> project;
        // extract project
        if (matchedUpsertSet.equals("*")) {
            // if sourceName is qualified like 'default.S', we should build a project like S.*
            String[] splits = sourceTable.split("\\.");
            project = Collections.singletonList(splits[splits.length - 1] + ".*");
        } else {
            // validate upsert changes
            // no need to check primary keys changes because merge condition must contain all pks
            // of the target table
            Map<String, String> changes = parseCommaSeparatedKeyValues(matchedUpsertSet);
            for (String targetField : changes.keySet()) {
                if (!targetFieldNames.contains(targetField)) {
                    throw new RuntimeException(
                            String.format(
                                    "Invalid column reference '%s' of table '%s' at matched-upsert action.",
                                    targetField, identifier.getFullName()));
                }
            }

            // replace field names
            // the table name is added before column name to avoid ambiguous column reference
            project =
                    targetFieldNames.stream()
                            .map(name -> changes.getOrDefault(name, targetTableName() + "." + name))
                            .collect(Collectors.toList());
        }

        // use inner join to find matched records
        String query =
                String.format(
                        "SELECT %s FROM %s INNER JOIN %s ON %s %s",
                        String.join(",", project),
                        escapedTargetName(),
                        escapedSourceName(),
                        mergeCondition,
                        matchedUpsertCondition == null ? "" : "WHERE " + matchedUpsertCondition);
        LOG.info("Query used for matched-update:\n{}", query);

        Table source = batchTEnv.sqlQuery(query);
        checkSchema("matched-upsert", source);

        return Optional.of(toDataStream(source, RowKind.UPDATE_AFTER, converters));
    }

    private Optional<DataStream<RowData>> getNotMatchedUpsertDataStream() {
        if (!notMatchedUpsert) {
            return Optional.empty();
        }

        // validate upsert change
        Map<String, String> changes = parseCommaSeparatedKeyValues(notMatchedBySourceUpsertSet);
        for (String targetField : changes.keySet()) {
            if (!targetFieldNames.contains(targetField)) {
                throw new RuntimeException(
                        String.format(
                                "Invalid column reference '%s' of table '%s' at not-matched-by-source-upsert action.\nRun <action> --help for help.",
                                targetField, identifier.getFullName()));
            }

            if (primaryKeys.contains(targetField)) {
                throw new RuntimeException(
                        "Not allowed to change primary key in not-matched-by-source-upsert-set.\nRun <action> --help for help.");
            }
        }

        // replace field names (won't be ambiguous here)
        List<String> project =
                targetFieldNames.stream()
                        .map(name -> changes.getOrDefault(name, name))
                        .collect(Collectors.toList());

        // use not exists to find not matched records
        String query =
                String.format(
                        "SELECT %s FROM %s WHERE NOT EXISTS (SELECT * FROM %s WHERE %s) %s",
                        String.join(",", project),
                        escapedTargetName(),
                        escapedSourceName(),
                        mergeCondition,
                        notMatchedBySourceUpsertCondition == null
                                ? ""
                                : String.format("AND (%s)", notMatchedBySourceUpsertCondition));

        LOG.info("Query used for not-matched-by-source-upsert:\n{}", query);

        Table source = batchTEnv.sqlQuery(query);
        checkSchema("not-matched-by-source-upsert", source);

        return Optional.of(toDataStream(source, RowKind.UPDATE_AFTER, converters));
    }

    private Optional<DataStream<RowData>> getMatchedDeleteDataStream() {
        if (!matchedDelete) {
            return Optional.empty();
        }

        // the table name is added before column name to avoid ambiguous column reference
        List<String> project =
                targetFieldNames.stream()
                        .map(name -> targetTableName() + "." + name)
                        .collect(Collectors.toList());

        // use inner join to find matched records
        String query =
                String.format(
                        "SELECT %s FROM %s INNER JOIN %s ON %s %s",
                        String.join(",", project),
                        escapedTargetName(),
                        escapedSourceName(),
                        mergeCondition,
                        matchedDeleteCondition == null ? "" : "WHERE " + matchedDeleteCondition);
        LOG.info("Query used by matched-delete:\n{}", query);

        Table source = batchTEnv.sqlQuery(query);
        checkSchema("matched-delete", source);

        return Optional.of(toDataStream(source, RowKind.DELETE, converters));
    }

    private Optional<DataStream<RowData>> getNotMatchedDeleteDataStream() {
        if (!notMatchedDelete) {
            return Optional.empty();
        }

        // use not exists to find not matched records
        String query =
                String.format(
                        "SELECT %s FROM %s WHERE NOT EXISTS (SELECT * FROM %s WHERE %s) %s",
                        String.join(",", targetFieldNames),
                        escapedTargetName(),
                        escapedSourceName(),
                        mergeCondition,
                        notMatchedBySourceDeleteCondition == null
                                ? ""
                                : String.format("AND (%s)", notMatchedBySourceDeleteCondition));
        LOG.info("Query used by not-matched-by-source-delete:\n{}", query);

        Table source = batchTEnv.sqlQuery(query);
        checkSchema("not-matched-by-source-delete", source);

        return Optional.of(toDataStream(source, RowKind.DELETE, converters));
    }

    private Optional<DataStream<RowData>> getInsertDataStream() {
        if (!insert) {
            return Optional.empty();
        }

        // use not exist to find rows to insert
        String query =
                String.format(
                        "SELECT %s FROM %s WHERE NOT EXISTS (SELECT * FROM %s WHERE %s) %s",
                        notMatchedInsertValues,
                        escapedSourceName(),
                        escapedTargetName(),
                        mergeCondition,
                        notMatchedInsertCondition == null
                                ? ""
                                : String.format("AND (%s)", notMatchedInsertCondition));
        LOG.info("Query used by not-matched-insert:\n{}", query);

        Table source = batchTEnv.sqlQuery(query);
        checkSchema("not-matched-insert", source);

        return Optional.of(toDataStream(source, RowKind.INSERT, converters));
    }

    private void checkSchema(String action, Table source) {
        List<DataType> actualTypes = toPaimonTypes(source.getResolvedSchema().getColumnDataTypes());
        List<DataType> expectedTypes = this.table.rowType().getFieldTypes();
        if (!compatibleCheck(actualTypes, expectedTypes)) {
            throw new IllegalStateException(
                    String.format(
                            "The schema of result in action '%s' is invalid.\n"
                                    + "Result schema:   [%s]\n"
                                    + "Expected schema: [%s]",
                            action,
                            actualTypes.stream()
                                    .map(DataType::asSQLString)
                                    .collect(Collectors.joining(", ")),
                            expectedTypes.stream()
                                    .map(DataType::asSQLString)
                                    .collect(Collectors.joining(", "))));
        }
    }

    // pass converters to avoid "not serializable" exception
    private DataStream<RowData> toDataStream(
            Table source, RowKind kind, List<DataStructureConverter<Object, Object>> converters) {
        return batchTEnv
                .toChangelogStream(source)
                .map(
                        row -> {
                            int arity = row.getArity();
                            GenericRowData rowData = new GenericRowData(kind, arity);
                            for (int i = 0; i < arity; i++) {
                                rowData.setField(
                                        i, converters.get(i).toInternalOrNull(row.getField(i)));
                            }
                            return rowData;
                        });
    }

    private String targetTableName() {
        return targetAlias == null ? identifier.getObjectName() : targetAlias;
    }

    private String escapedTargetName() {
        return String.format(
                "`%s`.`%s`.`%s`", catalogName, identifier.getDatabaseName(), targetTableName());
    }

    private String escapedSourceName() {
        return Arrays.stream(sourceTable.split("\\."))
                .map(s -> String.format("`%s`", s))
                .collect(Collectors.joining("."));
    }
}
