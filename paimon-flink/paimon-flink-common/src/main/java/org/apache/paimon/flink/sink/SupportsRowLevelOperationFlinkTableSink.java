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
import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.PredicateConverter;
import org.apache.paimon.flink.log.LogStoreTableFactory;
import org.apache.paimon.operation.FileStoreCommit;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.OnlyPartitionKeyEqualVisitor;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchWriteBuilder;

import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.RowLevelModificationScanContext;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsDeletePushDown;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelDelete;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelUpdate;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.MERGE_ENGINE;
import static org.apache.paimon.CoreOptions.MergeEngine.DEDUPLICATE;
import static org.apache.paimon.CoreOptions.MergeEngine.PARTIAL_UPDATE;
import static org.apache.paimon.CoreOptions.createCommitUser;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Flink table sink that supports row level update and delete. */
public abstract class SupportsRowLevelOperationFlinkTableSink extends FlinkTableSinkBase
        implements SupportsRowLevelUpdate, SupportsRowLevelDelete, SupportsDeletePushDown {

    @Nullable protected Predicate deletePredicate;

    public SupportsRowLevelOperationFlinkTableSink(
            ObjectIdentifier tableIdentifier,
            Table table,
            DynamicTableFactory.Context context,
            @Nullable LogStoreTableFactory logStoreTableFactory) {
        super(tableIdentifier, table, context, logStoreTableFactory);
    }

    @Override
    public DynamicTableSink copy() {
        FlinkTableSink copied =
                new FlinkTableSink(tableIdentifier, table, context, logStoreTableFactory);
        copied.staticPartitions = new HashMap<>(staticPartitions);
        copied.overwrite = overwrite;
        copied.deletePredicate = deletePredicate;
        return copied;
    }

    @Override
    public RowLevelUpdateInfo applyRowLevelUpdate(
            List<Column> updatedColumns, @Nullable RowLevelModificationScanContext context) {
        // Since only UPDATE_AFTER type messages can be received at present,
        // AppendOnlyFileStoreTable cannot correctly handle old data, so they are marked as
        // unsupported. Similarly, it is not allowed to update the primary key column when updating
        // the column of PrimaryKeyFileStoreTable, because the old data cannot be handled correctly.
        if (table.primaryKeys().isEmpty()) {
            throw new UnsupportedOperationException(
                    String.format(
                            "%s can not support update, because there is no primary key.",
                            table.getClass().getName()));
        }

        Options options = Options.fromMap(table.options());
        Set<String> primaryKeys = new HashSet<>(table.primaryKeys());
        updatedColumns.forEach(
                column -> {
                    if (primaryKeys.contains(column.getName())) {
                        String errMsg =
                                String.format(
                                        "Updates to primary keys are not supported, primaryKeys (%s), updatedColumns (%s)",
                                        primaryKeys,
                                        updatedColumns.stream()
                                                .map(Column::getName)
                                                .collect(Collectors.toList()));
                        throw new UnsupportedOperationException(errMsg);
                    }
                });

        MergeEngine mergeEngine = options.get(MERGE_ENGINE);
        boolean supportUpdate = mergeEngine == DEDUPLICATE || mergeEngine == PARTIAL_UPDATE;
        if (!supportUpdate) {
            throw new UnsupportedOperationException(
                    String.format("Merge engine %s can not support batch update.", mergeEngine));
        }

        // Even with partial-update we still need all columns. Because the topology
        // structure is source -> cal -> constraintEnforcer -> sink, in the
        // constraintEnforcer operator, the constraint check will be performed according to
        // the index, not according to the column. So we can't return only some columns,
        // which will cause problems like ArrayIndexOutOfBoundsException.
        // TODO: return partial columns after FLINK-32001 is resolved.
        return new RowLevelUpdateInfo() {};
    }

    @Override
    public RowLevelDeleteInfo applyRowLevelDelete(
            @Nullable RowLevelModificationScanContext rowLevelModificationScanContext) {
        validateDeletable();
        return new RowLevelDeleteInfo() {};
    }

    // supported filters push down please refer DeletePushDownVisitorTest

    @Override
    public boolean applyDeleteFilters(List<ResolvedExpression> list) {
        validateDeletable();
        List<Predicate> predicates = new ArrayList<>();
        RowType rowType = LogicalTypeConversion.toLogicalType(table.rowType());
        for (ResolvedExpression filter : list) {
            Optional<Predicate> predicate = PredicateConverter.convert(rowType, filter);
            if (predicate.isPresent()) {
                predicates.add(predicate.get());
            } else {
                // convert failed, leave it to flink
                return false;
            }
        }
        deletePredicate = predicates.isEmpty() ? null : PredicateBuilder.and(predicates);
        return canPushDownDeleteFilter();
    }

    @Override
    public Optional<Long> executeDeletion() {
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        try (FileStoreCommit commit =
                fileStoreTable
                        .store()
                        .newCommit(
                                createCommitUser(fileStoreTable.coreOptions().toConfiguration()))) {
            long identifier = BatchWriteBuilder.COMMIT_IDENTIFIER;
            if (deletePredicate == null) {
                commit.truncateTable(identifier);
            } else {
                checkArgument(deleteIsDropPartition());
                commit.dropPartitions(Collections.singletonList(deletePartitions()), identifier);
            }
            return Optional.empty();
        }
    }

    private void validateDeletable() {
        if (table.primaryKeys().isEmpty()) {
            throw new UnsupportedOperationException(
                    String.format(
                            "table '%s' can not support delete, because there is no primary key.",
                            table.getClass().getName()));
        }

        CoreOptions coreOptions = CoreOptions.fromMap(table.options());
        if (coreOptions.mergeEngine() == DEDUPLICATE
                || coreOptions.ignoreDelete()
                || (coreOptions.mergeEngine() == PARTIAL_UPDATE
                        && coreOptions.partialUpdateRemoveRecordOnDelete())) {
            return;
        }

        throw new UnsupportedOperationException(
                String.format(
                        "Merge engine %s can not support batch delete.",
                        coreOptions.mergeEngine()));
    }

    private boolean canPushDownDeleteFilter() {
        CoreOptions options = CoreOptions.fromMap(table.options());
        return (deletePredicate == null || deleteIsDropPartition())
                && !options.deleteForceProduceChangelog();
    }

    private boolean deleteIsDropPartition() {
        if (deletePredicate == null) {
            return false;
        }
        return deletePredicate.visit(new OnlyPartitionKeyEqualVisitor(table.partitionKeys()));
    }

    private Map<String, String> deletePartitions() {
        if (deletePredicate == null) {
            return null;
        }
        OnlyPartitionKeyEqualVisitor visitor =
                new OnlyPartitionKeyEqualVisitor(table.partitionKeys());
        deletePredicate.visit(visitor);
        return visitor.partitions();
    }
}
