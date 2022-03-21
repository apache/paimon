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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.ManagedTableFactory;
import org.apache.flink.table.store.connector.sink.TableStoreSink;
import org.apache.flink.table.store.connector.source.TableStoreSource;
import org.apache.flink.table.store.connector.utils.TableConfigUtils;
import org.apache.flink.table.store.file.FileStoreOptions;
import org.apache.flink.table.store.file.mergetree.MergeTreeOptions;
import org.apache.flink.table.store.log.LogOptions.LogChangelogMode;
import org.apache.flink.table.store.log.LogOptions.LogConsistency;
import org.apache.flink.table.store.log.LogOptions.LogStartupMode;
import org.apache.flink.table.store.log.LogStoreTableFactory;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.store.connector.TableStoreFactoryOptions.LOG_SYSTEM;
import static org.apache.flink.table.store.file.FileStoreOptions.BUCKET;
import static org.apache.flink.table.store.file.FileStoreOptions.TABLE_STORE_PREFIX;
import static org.apache.flink.table.store.log.LogOptions.CHANGELOG_MODE;
import static org.apache.flink.table.store.log.LogOptions.CONSISTENCY;
import static org.apache.flink.table.store.log.LogOptions.LOG_PREFIX;
import static org.apache.flink.table.store.log.LogOptions.SCAN;
import static org.apache.flink.table.store.log.LogStoreTableFactory.discoverLogStoreFactory;

/** Default implementation of {@link ManagedTableFactory}. */
public class TableStoreFactory
        implements ManagedTableFactory, DynamicTableSourceFactory, DynamicTableSinkFactory {

    @Override
    public Map<String, String> enrichOptions(Context context) {
        Map<String, String> enrichedOptions = new HashMap<>(context.getCatalogTable().getOptions());
        TableConfigUtils.extractConfiguration(context.getConfiguration())
                .toMap()
                .forEach(
                        (k, v) -> {
                            if (k.startsWith(TABLE_STORE_PREFIX)) {
                                enrichedOptions.putIfAbsent(
                                        k.substring(TABLE_STORE_PREFIX.length()), v);
                            }
                        });
        return enrichedOptions;
    }

    @Override
    public void onCreateTable(Context context, boolean ignoreIfExists) {
        Map<String, String> options = context.getCatalogTable().getOptions();
        Path path = FileStoreOptions.path(options, context.getObjectIdentifier());
        try {
            if (path.getFileSystem().exists(path) && !ignoreIfExists) {
                throw new TableException(
                        String.format(
                                "Failed to create file store path. "
                                        + "Reason: directory %s exists for table %s. "
                                        + "Suggestion: please try `DESCRIBE TABLE %s` to "
                                        + "first check whether table exists in current catalog. "
                                        + "If table exists in catalog, and data files under current path "
                                        + "are valid, please use `CREATE TABLE IF NOT EXISTS` ddl instead. "
                                        + "Otherwise, please choose another table name "
                                        + "or manually delete the current path and try again.",
                                path,
                                context.getObjectIdentifier().asSerializableString(),
                                context.getObjectIdentifier().asSerializableString()));
            }
            path.getFileSystem().mkdirs(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        createOptionalLogStoreFactory(context)
                .ifPresent(
                        factory ->
                                factory.onCreateTable(
                                        createLogContext(context),
                                        Integer.parseInt(
                                                options.getOrDefault(
                                                        BUCKET.key(),
                                                        BUCKET.defaultValue().toString())),
                                        ignoreIfExists));
    }

    @Override
    public void onDropTable(Context context, boolean ignoreIfNotExists) {
        Map<String, String> options = context.getCatalogTable().getOptions();
        Path path = FileStoreOptions.path(options, context.getObjectIdentifier());
        try {
            if (path.getFileSystem().exists(path)) {
                path.getFileSystem().delete(path, true);
            } else if (!ignoreIfNotExists) {
                throw new TableException(
                        String.format(
                                "Failed to delete file store path. "
                                        + "Reason: directory %s doesn't exist for table %s. "
                                        + "Suggestion: please try `DROP TABLE IF EXISTS` ddl instead.",
                                path, context.getObjectIdentifier().asSerializableString()));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        createOptionalLogStoreFactory(context)
                .ifPresent(
                        factory ->
                                factory.onDropTable(createLogContext(context), ignoreIfNotExists));
    }

    @Override
    public Map<String, String> onCompactTable(
            Context context, CatalogPartitionSpec catalogPartitionSpec) {
        throw new UnsupportedOperationException("Not implement yet");
    }

    @Override
    public TableStoreSource createDynamicTableSource(Context context) {
        return new TableStoreSource(
                buildTableStore(context),
                context.getConfiguration().get(ExecutionOptions.RUNTIME_MODE)
                        == RuntimeExecutionMode.STREAMING,
                createLogContext(context),
                createOptionalLogStoreFactory(context).orElse(null));
    }

    @Override
    public TableStoreSink createDynamicTableSink(Context context) {
        return new TableStoreSink(
                buildTableStore(context),
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
        return options;
    }

    // ~ Tools ------------------------------------------------------------------

    private static Optional<LogStoreTableFactory> createOptionalLogStoreFactory(Context context) {
        Configuration options = new Configuration();
        context.getCatalogTable().getOptions().forEach(options::setString);

        if (options.get(LOG_SYSTEM) == null) {
            // Use file store continuous reading
            validateFileStoreContinuous(options);
            return Optional.empty();
        }

        return Optional.of(
                discoverLogStoreFactory(context.getClassLoader(), options.get(LOG_SYSTEM)));
    }

    private static void validateFileStoreContinuous(Configuration options) {
        Configuration logOptions = new DelegatingConfiguration(options, LOG_PREFIX);
        LogChangelogMode changelogMode = logOptions.get(CHANGELOG_MODE);
        if (changelogMode == LogChangelogMode.UPSERT) {
            throw new ValidationException(
                    "File store continuous reading dose not support upsert changelog mode.");
        }
        LogConsistency consistency = logOptions.get(CONSISTENCY);
        if (consistency == LogConsistency.EVENTUAL) {
            throw new ValidationException(
                    "File store continuous reading dose not support eventual consistency mode.");
        }
        LogStartupMode startupMode = logOptions.get(SCAN);
        if (startupMode == LogStartupMode.FROM_TIMESTAMP) {
            throw new ValidationException(
                    "File store continuous reading dose not support from_timestamp scan mode, "
                            + "you can add timestamp filters instead.");
        }
    }

    private static Context createLogContext(Context context) {
        return new FactoryUtil.DefaultDynamicTableContext(
                context.getObjectIdentifier(),
                context.getCatalogTable()
                        .copy(filterLogStoreOptions(context.getCatalogTable().getOptions())),
                filterLogStoreOptions(context.getEnrichmentOptions()),
                context.getConfiguration(),
                context.getClassLoader(),
                context.isTemporary());
    }

    @VisibleForTesting
    static Map<String, String> filterLogStoreOptions(Map<String, String> options) {
        return options.entrySet().stream()
                .filter(entry -> !entry.getKey().equals(LOG_SYSTEM.key())) // exclude log.system
                .filter(entry -> entry.getKey().startsWith(LOG_PREFIX))
                .collect(
                        Collectors.toMap(
                                entry -> entry.getKey().substring(LOG_PREFIX.length()),
                                Map.Entry::getValue));
    }

    @VisibleForTesting
    TableStore buildTableStore(Context context) {
        ResolvedCatalogTable catalogTable = context.getCatalogTable();
        ResolvedSchema schema = catalogTable.getResolvedSchema();
        RowType rowType = (RowType) schema.toPhysicalRowDataType().getLogicalType();
        List<String> partitionKeys = catalogTable.getPartitionKeys();
        int[] pkIndex = new int[0];
        if (schema.getPrimaryKey().isPresent()) {
            List<String> pkCols = schema.getPrimaryKey().get().getColumns();
            Preconditions.checkState(
                    new HashSet<>(pkCols).containsAll(partitionKeys),
                    String.format(
                            "Primary key constraint %s should include partition key %s",
                            pkCols, partitionKeys));
            Set<String> partFilter = new HashSet<>(partitionKeys);
            pkIndex =
                    schema.getPrimaryKey().get().getColumns().stream()
                            .filter(pk -> !partFilter.contains(pk))
                            .mapToInt(rowType.getFieldNames()::indexOf)
                            .toArray();
            if (pkIndex.length == 0) {
                throw new TableException(
                        String.format(
                                "Primary key constraint %s should not be same with partition key %s",
                                pkCols, partitionKeys));
            }
        }
        return new TableStore(Configuration.fromMap(catalogTable.getOptions()))
                .withTableIdentifier(context.getObjectIdentifier())
                .withSchema(rowType)
                .withPrimaryKeys(pkIndex)
                .withPartitions(
                        partitionKeys.stream()
                                .mapToInt(rowType.getFieldNames()::indexOf)
                                .toArray());
    }
}
