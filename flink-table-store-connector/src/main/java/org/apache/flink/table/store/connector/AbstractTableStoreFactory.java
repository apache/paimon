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

package org.apache.flink.table.store.connector;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.store.CoreOptions.LogChangelogMode;
import org.apache.flink.table.store.CoreOptions.LogConsistency;
import org.apache.flink.table.store.annotation.VisibleForTesting;
import org.apache.flink.table.store.connector.sink.TableStoreSink;
import org.apache.flink.table.store.connector.source.SystemTableSource;
import org.apache.flink.table.store.connector.source.TableStoreSource;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.file.schema.UpdateSchema;
import org.apache.flink.table.store.log.LogStoreTableFactory;
import org.apache.flink.table.store.options.CatalogOptions;
import org.apache.flink.table.store.options.Options;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.FileStoreTableFactory;
import org.apache.flink.table.store.utils.Preconditions;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.table.store.CoreOptions.LOG_CHANGELOG_MODE;
import static org.apache.flink.table.store.CoreOptions.LOG_CONSISTENCY;
import static org.apache.flink.table.store.connector.FlinkConnectorOptions.LOG_SYSTEM;
import static org.apache.flink.table.store.connector.FlinkConnectorOptions.NONE;
import static org.apache.flink.table.store.connector.LogicalTypeConversion.toLogicalType;
import static org.apache.flink.table.store.log.LogStoreTableFactory.discoverLogStoreFactory;

/** Abstract table store factory to create table source and table sink. */
public abstract class AbstractTableStoreFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        CatalogTable origin = context.getCatalogTable().getOrigin();
        boolean isStreamingMode =
                context.getConfiguration().get(ExecutionOptions.RUNTIME_MODE)
                        == RuntimeExecutionMode.STREAMING;
        if (origin instanceof SystemCatalogTable) {
            return new SystemTableSource(((SystemCatalogTable) origin).table(), isStreamingMode);
        } else {
            return new TableStoreSource(
                    context.getObjectIdentifier(),
                    buildFileStoreTable(context),
                    isStreamingMode,
                    context,
                    createOptionalLogStoreFactory(context).orElse(null));
        }
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        return new TableStoreSink(
                context.getObjectIdentifier(),
                buildFileStoreTable(context),
                context,
                createOptionalLogStoreFactory(context).orElse(null));
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>();
    }

    // ~ Tools ------------------------------------------------------------------

    static Optional<LogStoreTableFactory> createOptionalLogStoreFactory(
            DynamicTableFactory.Context context) {
        return createOptionalLogStoreFactory(
                context.getClassLoader(), context.getCatalogTable().getOptions());
    }

    static Optional<LogStoreTableFactory> createOptionalLogStoreFactory(
            ClassLoader classLoader, Map<String, String> options) {
        Options configOptions = new Options();
        options.forEach(configOptions::setString);

        if (configOptions.get(LOG_SYSTEM).equalsIgnoreCase(NONE)) {
            // Use file store continuous reading
            validateFileStoreContinuous(configOptions);
            return Optional.empty();
        }

        return Optional.of(discoverLogStoreFactory(classLoader, configOptions.get(LOG_SYSTEM)));
    }

    private static void validateFileStoreContinuous(Options options) {
        LogChangelogMode changelogMode = options.get(LOG_CHANGELOG_MODE);
        if (changelogMode == LogChangelogMode.UPSERT) {
            throw new ValidationException(
                    "File store continuous reading dose not support upsert changelog mode.");
        }
        LogConsistency consistency = options.get(LOG_CONSISTENCY);
        if (consistency == LogConsistency.EVENTUAL) {
            throw new ValidationException(
                    "File store continuous reading dose not support eventual consistency mode.");
        }
    }

    static CatalogOptions createCatalogOptions(DynamicTableFactory.Context context) {
        return FlinkUtils.catalogOptions(
                context.getCatalogTable().getOptions(), context.getConfiguration());
    }

    static FileStoreTable buildFileStoreTable(DynamicTableFactory.Context context) {
        CatalogTable origin = context.getCatalogTable().getOrigin();
        FileStoreTable table;
        if (origin instanceof DataCatalogTable) {
            table = ((DataCatalogTable) origin).table().copy(origin.getOptions());
        } else {
            table = FileStoreTableFactory.create(createCatalogOptions(context));
        }

        TableSchema tableSchema = table.schema();
        UpdateSchema updateSchema = FlinkCatalog.fromCatalogTable(context.getCatalogTable());

        RowType rowType = toLogicalType(updateSchema.rowType());
        List<String> partitionKeys = updateSchema.partitionKeys();
        List<String> primaryKeys = updateSchema.primaryKeys();

        // compare fields to ignore the outside nullability and nested fields' comments
        Preconditions.checkArgument(
                schemaEquals(toLogicalType(tableSchema.logicalRowType()), rowType),
                "Flink schema and store schema are not the same, "
                        + "store schema is %s, Flink schema is %s",
                tableSchema.logicalRowType(),
                rowType);

        Preconditions.checkArgument(
                tableSchema.partitionKeys().equals(partitionKeys),
                "Flink partitionKeys and store partitionKeys are not the same, "
                        + "store partitionKeys is %s, Flink partitionKeys is %s",
                tableSchema.partitionKeys(),
                partitionKeys);

        Preconditions.checkArgument(
                tableSchema.primaryKeys().equals(primaryKeys),
                "Flink primaryKeys and store primaryKeys are not the same, "
                        + "store primaryKeys is %s, Flink primaryKeys is %s",
                tableSchema.primaryKeys(),
                primaryKeys);

        return table;
    }

    @VisibleForTesting
    static boolean schemaEquals(RowType rowType1, RowType rowType2) {
        List<RowType.RowField> fieldList1 = rowType1.getFields();
        List<RowType.RowField> fieldList2 = rowType2.getFields();
        if (fieldList1.size() != fieldList2.size()) {
            return false;
        }
        for (int i = 0; i < fieldList1.size(); i++) {
            RowType.RowField f1 = fieldList1.get(i);
            RowType.RowField f2 = fieldList2.get(i);
            if (!f1.getName().equals(f2.getName()) || !f1.getType().equals(f2.getType())) {
                return false;
            }
        }
        return true;
    }
}
