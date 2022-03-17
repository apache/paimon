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
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.ManagedTableFactory;
import org.apache.flink.table.store.connector.sink.TableStoreSink;
import org.apache.flink.table.store.connector.source.TableStoreSource;
import org.apache.flink.table.store.connector.utils.TableConfigUtils;
import org.apache.flink.table.store.file.FileStoreOptions;
import org.apache.flink.table.store.file.mergetree.MergeTreeOptions;
import org.apache.flink.table.store.log.LogOptions;
import org.apache.flink.table.store.log.LogStoreTableFactory;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.store.connector.TableStoreFactoryOptions.CHANGE_TRACKING;
import static org.apache.flink.table.store.connector.TableStoreFactoryOptions.LOG_SYSTEM;
import static org.apache.flink.table.store.file.FileStoreOptions.BUCKET;
import static org.apache.flink.table.store.file.FileStoreOptions.FILE_PATH;
import static org.apache.flink.table.store.file.FileStoreOptions.TABLE_STORE_PREFIX;
import static org.apache.flink.table.store.log.LogOptions.CHANGELOG_MODE;
import static org.apache.flink.table.store.log.LogOptions.LOG_PREFIX;
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
        Path path = tablePath(options, context.getObjectIdentifier());
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

        if (enableChangeTracking(options)) {
            createLogStoreTableFactory(context)
                    .onCreateTable(
                            createLogContext(context),
                            Integer.parseInt(
                                    options.getOrDefault(
                                            BUCKET.key(), BUCKET.defaultValue().toString())),
                            ignoreIfExists);
        }
    }

    @Override
    public void onDropTable(Context context, boolean ignoreIfNotExists) {
        Map<String, String> options = context.getCatalogTable().getOptions();
        Path path = tablePath(options, context.getObjectIdentifier());
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
        if (enableChangeTracking(options)) {
            createLogStoreTableFactory(context)
                    .onDropTable(createLogContext(context), ignoreIfNotExists);
        }
    }

    @Override
    public Map<String, String> onCompactTable(
            Context context, CatalogPartitionSpec catalogPartitionSpec) {
        throw new UnsupportedOperationException("Not implement yet");
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        boolean streaming =
                context.getConfiguration().get(ExecutionOptions.RUNTIME_MODE)
                        == RuntimeExecutionMode.STREAMING;
        Context logStoreContext = null;
        LogStoreTableFactory logStoreTableFactory = null;

        if (enableChangeTracking(context.getCatalogTable().getOptions())) {
            logStoreContext = createLogContext(context);
            logStoreTableFactory = createLogStoreTableFactory(context);
        }
        return new TableStoreSource(
                buildTableStore(context), streaming, logStoreContext, logStoreTableFactory);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        Map<String, String> options = context.getCatalogTable().getOptions();
        Context logStoreContext = null;
        LogStoreTableFactory logStoreTableFactory = null;
        if (enableChangeTracking(options)) {
            logStoreContext = createLogContext(context);
            logStoreTableFactory = createLogStoreTableFactory(context);
        }
        return new TableStoreSink(
                buildTableStore(context),
                LogOptions.LogChangelogMode.valueOf(
                        options.getOrDefault(
                                LOG_PREFIX + CHANGELOG_MODE.key(),
                                CHANGELOG_MODE.defaultValue().toString().toUpperCase())),
                logStoreContext,
                logStoreTableFactory);
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = FileStoreOptions.allOptions();
        options.addAll(MergeTreeOptions.allOptions());
        options.add(CHANGE_TRACKING);
        return options;
    }

    // ~ Tools ------------------------------------------------------------------

    private static LogStoreTableFactory createLogStoreTableFactory(Context context) {
        return discoverLogStoreFactory(
                context.getClassLoader(),
                context.getCatalogTable()
                        .getOptions()
                        .getOrDefault(LOG_SYSTEM.key(), LOG_SYSTEM.defaultValue()));
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
                .filter(entry -> entry.getKey().startsWith(LOG_PREFIX))
                .collect(
                        Collectors.toMap(
                                entry -> entry.getKey().substring(LOG_PREFIX.length()),
                                Map.Entry::getValue));
    }

    @VisibleForTesting
    static Map<String, String> filterFileStoreOptions(Map<String, String> options) {
        return options.entrySet().stream()
                .filter(entry -> !entry.getKey().startsWith(LOG_PREFIX))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @VisibleForTesting
    static Path tablePath(Map<String, String> options, ObjectIdentifier identifier) {
        Preconditions.checkArgument(
                options.containsKey(FILE_PATH.key()),
                String.format(
                        "Failed to create file store path. "
                                + "Please specify a root dir by setting session level configuration "
                                + "as `SET 'table-store.%s' = '...'`. "
                                + "Alternatively, you can use a per-table root dir "
                                + "as `CREATE TABLE ${table} (...) WITH ('%s' = '...')`",
                        FILE_PATH.key(), FILE_PATH.key()));
        return new Path(
                options.get(FILE_PATH.key()),
                String.format(
                        "root/%s.catalog/%s.db/%s",
                        identifier.getCatalogName(),
                        identifier.getDatabaseName(),
                        identifier.getObjectName()));
    }

    @VisibleForTesting
    static boolean enableChangeTracking(Map<String, String> options) {
        return Boolean.parseBoolean(
                options.getOrDefault(
                        CHANGE_TRACKING.key(), CHANGE_TRACKING.defaultValue().toString()));
    }

    private TableStore buildTableStore(Context context) {
        ResolvedCatalogTable catalogTable = context.getCatalogTable();
        ResolvedSchema schema = catalogTable.getResolvedSchema();
        RowType rowType = (RowType) schema.toPhysicalRowDataType().getLogicalType();
        int[] primaryKeys = new int[0];
        if (schema.getPrimaryKey().isPresent()) {
            primaryKeys =
                    schema.getPrimaryKey().get().getColumns().stream()
                            .mapToInt(rowType.getFieldNames()::indexOf)
                            .toArray();
        }
        return new TableStore(
                        Configuration.fromMap(filterFileStoreOptions(catalogTable.getOptions())))
                .withTableIdentifier(context.getObjectIdentifier())
                .withSchema(rowType)
                .withPrimaryKeys(primaryKeys)
                .withPartitions(
                        catalogTable.getPartitionKeys().stream()
                                .mapToInt(rowType.getFieldNames()::indexOf)
                                .toArray());
    }
}
