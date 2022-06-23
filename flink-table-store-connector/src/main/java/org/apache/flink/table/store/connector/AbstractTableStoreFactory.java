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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.store.connector.sink.TableStoreSink;
import org.apache.flink.table.store.connector.source.TableStoreSource;
import org.apache.flink.table.store.file.FileStoreOptions;
import org.apache.flink.table.store.file.mergetree.MergeTreeOptions;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.file.schema.UpdateSchema;
import org.apache.flink.table.store.log.LogOptions;
import org.apache.flink.table.store.log.LogStoreTableFactory;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.FileStoreTableFactory;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.store.connector.TableStoreFactoryOptions.LOG_SYSTEM;
import static org.apache.flink.table.store.log.LogOptions.CHANGELOG_MODE;
import static org.apache.flink.table.store.log.LogOptions.CONSISTENCY;
import static org.apache.flink.table.store.log.LogOptions.LOG_PREFIX;
import static org.apache.flink.table.store.log.LogOptions.SCAN;
import static org.apache.flink.table.store.log.LogStoreTableFactory.discoverLogStoreFactory;

/** Abstract table store factory to create table source and table sink. */
public abstract class AbstractTableStoreFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    @Override
    public TableStoreSource createDynamicTableSource(Context context) {
        return new TableStoreSource(
                context.getObjectIdentifier(),
                buildFileStoreTable(context),
                context.getConfiguration().get(ExecutionOptions.RUNTIME_MODE)
                        == RuntimeExecutionMode.STREAMING,
                createLogContext(context),
                createOptionalLogStoreFactory(context).orElse(null));
    }

    @Override
    public TableStoreSink createDynamicTableSink(Context context) {
        return new TableStoreSink(
                context.getObjectIdentifier(),
                buildFileStoreTable(context),
                createLogContext(context),
                createOptionalLogStoreFactory(context).orElse(null));
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = FileStoreOptions.allOptions();
        options.addAll(MergeTreeOptions.allOptions());
        options.addAll(TableStoreFactoryOptions.allOptions());
        return options;
    }

    // ~ Tools ------------------------------------------------------------------

    static Optional<LogStoreTableFactory> createOptionalLogStoreFactory(
            DynamicTableFactory.Context context) {
        return createOptionalLogStoreFactory(
                context.getClassLoader(), context.getCatalogTable().getOptions());
    }

    static Optional<LogStoreTableFactory> createOptionalLogStoreFactory(
            ClassLoader classLoader, Map<String, String> options) {
        Configuration configOptions = new Configuration();
        options.forEach(configOptions::setString);

        if (configOptions.get(LOG_SYSTEM) == null) {
            // Use file store continuous reading
            validateFileStoreContinuous(configOptions);
            return Optional.empty();
        }

        return Optional.of(discoverLogStoreFactory(classLoader, configOptions.get(LOG_SYSTEM)));
    }

    private static void validateFileStoreContinuous(Configuration options) {
        Configuration logOptions = new DelegatingConfiguration(options, LOG_PREFIX);
        LogOptions.LogChangelogMode changelogMode = logOptions.get(CHANGELOG_MODE);
        if (changelogMode == LogOptions.LogChangelogMode.UPSERT) {
            throw new ValidationException(
                    "File store continuous reading dose not support upsert changelog mode.");
        }
        LogOptions.LogConsistency consistency = logOptions.get(CONSISTENCY);
        if (consistency == LogOptions.LogConsistency.EVENTUAL) {
            throw new ValidationException(
                    "File store continuous reading dose not support eventual consistency mode.");
        }
        LogOptions.LogStartupMode startupMode = logOptions.get(SCAN);
        if (startupMode == LogOptions.LogStartupMode.FROM_TIMESTAMP) {
            throw new ValidationException(
                    "File store continuous reading dose not support from_timestamp scan mode, "
                            + "you can add timestamp filters instead.");
        }
    }

    static DynamicTableFactory.Context createLogContext(DynamicTableFactory.Context context) {
        return createLogContext(context, context.getCatalogTable().getOptions());
    }

    static DynamicTableFactory.Context createLogContext(
            DynamicTableFactory.Context context, Map<String, String> options) {
        return new FactoryUtil.DefaultDynamicTableContext(
                context.getObjectIdentifier(),
                context.getCatalogTable().copy(filterLogStoreOptions(options)),
                filterLogStoreOptions(context.getEnrichmentOptions()),
                context.getConfiguration(),
                context.getClassLoader(),
                context.isTemporary());
    }

    static Map<String, String> filterLogStoreOptions(Map<String, String> options) {
        return options.entrySet().stream()
                .filter(entry -> !entry.getKey().equals(LOG_SYSTEM.key())) // exclude log.system
                .filter(entry -> entry.getKey().startsWith(LOG_PREFIX))
                .collect(
                        Collectors.toMap(
                                entry -> entry.getKey().substring(LOG_PREFIX.length()),
                                Map.Entry::getValue));
    }

    static FileStoreTable buildFileStoreTable(DynamicTableFactory.Context context) {
        FileStoreTable table =
                FileStoreTableFactory.create(
                        Configuration.fromMap(context.getCatalogTable().getOptions()));

        TableSchema tableSchema = table.schema();
        UpdateSchema updateSchema = UpdateSchema.fromCatalogTable(context.getCatalogTable());

        RowType rowType = updateSchema.rowType();
        List<String> partitionKeys = updateSchema.partitionKeys();
        List<String> primaryKeys = updateSchema.primaryKeys();

        // compare fields to ignore isNullable for row type
        Preconditions.checkArgument(
                tableSchema.logicalRowType().getFields().equals(rowType.getFields()),
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
}
