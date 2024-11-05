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

package org.apache.paimon.flink;

import org.apache.paimon.CoreOptions.LogChangelogMode;
import org.apache.paimon.CoreOptions.LogConsistency;
import org.apache.paimon.CoreOptions.StreamingReadMode;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.flink.log.LogStoreTableFactory;
import org.apache.paimon.flink.sink.FlinkTableSink;
import org.apache.paimon.flink.source.DataTableSource;
import org.apache.paimon.flink.source.SystemTableSource;
import org.apache.paimon.lineage.LineageMeta;
import org.apache.paimon.lineage.LineageMetaFactory;
import org.apache.paimon.lineage.TableLineageEntity;
import org.apache.paimon.lineage.TableLineageEntityImpl;
import org.apache.paimon.options.Options;
import org.apache.paimon.options.OptionsUtils;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;

import static org.apache.paimon.CoreOptions.LOG_CHANGELOG_MODE;
import static org.apache.paimon.CoreOptions.LOG_CONSISTENCY;
import static org.apache.paimon.CoreOptions.SCAN_MODE;
import static org.apache.paimon.CoreOptions.STREAMING_READ_MODE;
import static org.apache.paimon.CoreOptions.StartupMode.FROM_SNAPSHOT;
import static org.apache.paimon.CoreOptions.StartupMode.FROM_SNAPSHOT_FULL;
import static org.apache.paimon.flink.FlinkConnectorOptions.LOG_SYSTEM;
import static org.apache.paimon.flink.FlinkConnectorOptions.NONE;
import static org.apache.paimon.flink.LogicalTypeConversion.toLogicalType;
import static org.apache.paimon.flink.log.LogStoreTableFactory.discoverLogStoreFactory;

/** Abstract paimon factory to create table source and table sink. */
public abstract class AbstractFlinkTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractFlinkTableFactory.class);

    @Nullable private final FlinkCatalog flinkCatalog;

    public AbstractFlinkTableFactory(@Nullable FlinkCatalog flinkCatalog) {
        this.flinkCatalog = flinkCatalog;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        CatalogTable origin = context.getCatalogTable().getOrigin();
        boolean isStreamingMode =
                context.getConfiguration().get(ExecutionOptions.RUNTIME_MODE)
                        == RuntimeExecutionMode.STREAMING;
        if (origin instanceof SystemCatalogTable) {
            return new SystemTableSource(
                    ((SystemCatalogTable) origin).table(),
                    isStreamingMode,
                    context.getObjectIdentifier());
        } else {
            Table table = buildPaimonTable(context);
            if (table instanceof FileStoreTable) {
                storeTableLineage(
                        ((FileStoreTable) table).catalogEnvironment().lineageMetaFactory(),
                        context,
                        (entity, lineageFactory) -> {
                            try (LineageMeta lineage =
                                    lineageFactory.create(() -> Options.fromMap(table.options()))) {
                                lineage.saveSourceTableLineage(entity);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
            }
            return new DataTableSource(
                    context.getObjectIdentifier(),
                    table,
                    isStreamingMode,
                    context,
                    createOptionalLogStoreFactory(context).orElse(null));
        }
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        Table table = buildPaimonTable(context);
        if (table instanceof FileStoreTable) {
            storeTableLineage(
                    ((FileStoreTable) table).catalogEnvironment().lineageMetaFactory(),
                    context,
                    (entity, lineageFactory) -> {
                        try (LineageMeta lineage =
                                lineageFactory.create(() -> Options.fromMap(table.options()))) {
                            lineage.saveSinkTableLineage(entity);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
        return new FlinkTableSink(
                context.getObjectIdentifier(),
                table,
                context,
                createOptionalLogStoreFactory(context).orElse(null));
    }

    private void storeTableLineage(
            @Nullable LineageMetaFactory lineageMetaFactory,
            Context context,
            BiConsumer<TableLineageEntity, LineageMetaFactory> tableLineage) {
        if (lineageMetaFactory != null) {
            String pipelineName = context.getConfiguration().get(PipelineOptions.NAME);
            if (pipelineName == null) {
                throw new ValidationException("Cannot get pipeline name for lineage meta.");
            }
            tableLineage.accept(
                    new TableLineageEntityImpl(
                            context.getObjectIdentifier().getDatabaseName(),
                            context.getObjectIdentifier().getObjectName(),
                            pipelineName,
                            Timestamp.fromEpochMillis(System.currentTimeMillis())),
                    lineageMetaFactory);
        }
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

    public static Optional<LogStoreTableFactory> createOptionalLogStoreFactory(
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
        } else if (configOptions.get(SCAN_MODE) == FROM_SNAPSHOT
                || configOptions.get(SCAN_MODE) == FROM_SNAPSHOT_FULL) {
            throw new ValidationException(
                    String.format(
                            "Log system does not support %s and %s scan mode",
                            FROM_SNAPSHOT, FROM_SNAPSHOT_FULL));
        }

        return Optional.of(discoverLogStoreFactory(classLoader, configOptions.get(LOG_SYSTEM)));
    }

    private static void validateFileStoreContinuous(Options options) {
        LogChangelogMode changelogMode = options.get(LOG_CHANGELOG_MODE);
        StreamingReadMode streamingReadMode = options.get(STREAMING_READ_MODE);
        if (changelogMode == LogChangelogMode.UPSERT) {
            throw new ValidationException(
                    "File store continuous reading does not support upsert changelog mode.");
        }
        LogConsistency consistency = options.get(LOG_CONSISTENCY);
        if (consistency == LogConsistency.EVENTUAL) {
            throw new ValidationException(
                    "File store continuous reading does not support eventual consistency mode.");
        }
        if (streamingReadMode == StreamingReadMode.LOG) {
            throw new ValidationException(
                    "File store continuous reading does not support the log streaming read mode.");
        }
    }

    static CatalogContext createCatalogContext(DynamicTableFactory.Context context) {
        return CatalogContext.create(
                Options.fromMap(context.getCatalogTable().getOptions()), new FlinkFileIOLoader());
    }

    Table buildPaimonTable(DynamicTableFactory.Context context) {
        CatalogTable origin = context.getCatalogTable().getOrigin();
        Table table;

        Map<String, String> dynamicOptions = getDynamicConfigOptions(context);
        dynamicOptions.forEach(
                (key, newValue) -> {
                    String oldValue = origin.getOptions().get(key);
                    if (!Objects.equals(oldValue, newValue)) {
                        SchemaManager.checkAlterTableOption(key, oldValue, newValue, true);
                    }
                });
        Map<String, String> newOptions = new HashMap<>();
        newOptions.putAll(origin.getOptions());
        // dynamic options should override origin options
        newOptions.putAll(dynamicOptions);

        FileStoreTable fileStoreTable;
        if (origin instanceof DataCatalogTable) {
            fileStoreTable = (FileStoreTable) ((DataCatalogTable) origin).table();
        } else if (flinkCatalog == null) {
            // In case Paimon is directly used as a Flink connector, instead of through catalog.
            fileStoreTable = FileStoreTableFactory.create(createCatalogContext(context));
        } else {
            // In cases like materialized table, the Paimon table might not be DataCatalogTable,
            // but can still be acquired through the catalog.
            Identifier identifier =
                    Identifier.create(
                            context.getObjectIdentifier().getDatabaseName(),
                            context.getObjectIdentifier().getObjectName());
            try {
                fileStoreTable = (FileStoreTable) flinkCatalog.catalog().getTable(identifier);
            } catch (Catalog.TableNotExistException e) {
                throw new RuntimeException(e);
            }
        }
        table = fileStoreTable.copyWithoutTimeTravel(newOptions);

        // notice that the Paimon table schema must be the same with the Flink's
        Schema schema = FlinkCatalog.fromCatalogTable(context.getCatalogTable());

        RowType rowType = toLogicalType(schema.rowType());
        List<String> partitionKeys = schema.partitionKeys();
        List<String> primaryKeys = schema.primaryKeys();

        // compare fields to ignore the outside nullability and nested fields' comments
        Preconditions.checkArgument(
                schemaEquals(toLogicalType(table.rowType()), rowType),
                "Flink schema and store schema are not the same, "
                        + "store schema is %s, Flink schema is %s",
                table.rowType(),
                rowType);

        Preconditions.checkArgument(
                table.partitionKeys().equals(partitionKeys),
                "Flink partitionKeys and store partitionKeys are not the same, "
                        + "store partitionKeys is %s, Flink partitionKeys is %s",
                table.partitionKeys(),
                partitionKeys);

        Preconditions.checkArgument(
                table.primaryKeys().equals(primaryKeys),
                "Flink primaryKeys and store primaryKeys are not the same, "
                        + "store primaryKeys is %s, Flink primaryKeys is %s",
                table.primaryKeys(),
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

    /**
     * The dynamic option's format is:
     *
     * <p>Global Options: key = value .
     *
     * <p>Table Options: {@link
     * FlinkConnectorOptions#TABLE_DYNAMIC_OPTION_PREFIX}${catalog}.${database}.${tableName}.key =
     * value.
     *
     * <p>These job level options will be extracted and injected into the target table option. Table
     * options will override global options if there are conflicts.
     *
     * @param context The table factory context.
     * @return The dynamic options of this target table.
     */
    static Map<String, String> getDynamicConfigOptions(DynamicTableFactory.Context context) {

        ReadableConfig config = context.getConfiguration();

        Map<String, String> conf;

        if (config instanceof Configuration) {
            conf = ((Configuration) config).toMap();
        } else if (config instanceof TableConfig) {
            conf = ((TableConfig) config).getConfiguration().toMap();
        } else {
            throw new IllegalArgumentException("Unexpected config: " + config.getClass());
        }

        String template =
                String.format(
                        "(%s)(%s|\\*)\\.(%s|\\*)\\.(%s|\\*)\\.(.+)",
                        FlinkConnectorOptions.TABLE_DYNAMIC_OPTION_PREFIX,
                        context.getObjectIdentifier().getCatalogName(),
                        context.getObjectIdentifier().getDatabaseName(),
                        context.getObjectIdentifier().getObjectName());
        Pattern pattern = Pattern.compile(template);
        Map<String, String> optionsFromTableConfig =
                OptionsUtils.convertToDynamicTableProperties(conf, "", pattern, 5);

        if (!optionsFromTableConfig.isEmpty()) {
            LOG.info(
                    "Loading dynamic table options for {} in table config: {}",
                    context.getObjectIdentifier().getObjectName(),
                    optionsFromTableConfig);
        }
        return optionsFromTableConfig;
    }
}
