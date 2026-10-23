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

package org.apache.paimon.flink.lakestream;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.AbstractFlinkTableFactory;
import org.apache.paimon.flink.FlinkCatalog;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.Table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.Factory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Builds the source and the sink of a lake-stream table by delegating to the Lakestream Store's own
 * table factory. Not registered with Flink's factory SPI: {@link #factoryIdentifier} exists because
 * the interface asks for it, and nothing discovers this object by it.
 */
public class LakeStreamTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private final FlinkCatalog catalog;

    public LakeStreamTableFactory(FlinkCatalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public String factoryIdentifier() {
        return FlinkCatalogFactory.IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }

    /** Whether the read is a {@code $rt} read of a lake-stream table. */
    public static boolean isLakeStreamRead(
            Catalog paimonCatalog, DynamicTableFactory.Context context) {
        return realTimeReadBaseTable(paimonCatalog, context) != null;
    }

    /** Whether the write is to a lake-stream table, or through the read-only {@code $rt} name. */
    public static boolean isLakeStreamWrite(
            Catalog paimonCatalog, DynamicTableFactory.Context context) {
        Identifier identifier = identifierOf(context);
        return LakeStreamUtils.isRealTimeTable(identifier)
                || persistedLakeStreamTable(paimonCatalog, identifier) != null;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(DynamicTableFactory.Context context) {
        Table baseTable = realTimeReadBaseTable(catalog.catalog(), context);
        ObjectIdentifier baseIdentifier =
                LakeStreamUtils.stripRealTimeSuffix(context.getObjectIdentifier());
        checkNotFormatTable(baseTable, baseIdentifier);
        org.apache.flink.table.catalog.Catalog storeCatalog =
                catalog.getOrCreateLakeStreamStoreCatalog();
        // the catalog already resolved this read against the store, so the planner's table is the
        // store's table with any query hints applied on top of it
        CatalogBaseTable storeTable = context.getCatalogTable();
        DynamicTableSourceFactory sourceFactory =
                storeFactory(
                        storeCatalog,
                        DynamicTableSourceFactory.class,
                        "create a source for '$rt'-suffixed reads");
        return sourceFactory.createDynamicTableSource(
                delegatedContext(context, baseIdentifier, storeTable));
    }

    @Override
    public DynamicTableSink createDynamicTableSink(DynamicTableFactory.Context context) {
        checkNotRealTimeWrite(context);
        Table table = persistedLakeStreamTable(catalog.catalog(), identifierOf(context));
        checkNotFormatTable(table, context.getObjectIdentifier());
        ObjectIdentifier identifier = context.getObjectIdentifier();
        org.apache.flink.table.catalog.Catalog storeCatalog =
                catalog.getOrCreateLakeStreamStoreCatalog();
        // the store table is fetched from the store catalog because the store's sink factory may
        // require provider-specific table options (PIP-39 L289)
        CatalogBaseTable storeTable =
                catalog.requireLakeStreamStoreTable(identifier.toObjectPath());
        DynamicTableSinkFactory sinkFactory =
                storeFactory(
                        storeCatalog, DynamicTableSinkFactory.class, "create a sink for writes");
        return sinkFactory.createDynamicTableSink(
                delegatedContext(context, identifier, storeTable));
    }

    /** A format table has no Paimon history for the store to tier into, so it cannot be one. */
    private void checkNotFormatTable(@Nullable Table table, ObjectIdentifier identifier) {
        if (table instanceof FormatTable) {
            throw new ValidationException(
                    String.format(
                            "Table '%s' cannot be a lake-stream table: '%s' is only supported on "
                                    + "Paimon tables, not on format tables.",
                            identifier.asSummaryString(), CoreOptions.LAKE_STREAM_ENABLED.key()));
        }
    }

    /** The base table of a {@code $rt} read, or {@code null} when the read stays on Paimon. */
    @Nullable
    private static Table realTimeReadBaseTable(
            Catalog paimonCatalog, DynamicTableFactory.Context context) {
        Identifier identifier = identifierOf(context);
        if (!LakeStreamUtils.isRealTimeTable(identifier)) {
            return null;
        }
        return persistedLakeStreamTable(
                paimonCatalog,
                Identifier.create(identifier.getDatabaseName(), identifier.getTableName()));
    }

    /**
     * The persisted Paimon table when lake-stream mode is enabled on it, otherwise {@code null}.
     * Read from the catalog because Flink merges query hints into the planner-resolved table, and a
     * hint must not move a table's writes off the store that holds its recent data.
     */
    @Nullable
    private static Table persistedLakeStreamTable(Catalog paimonCatalog, Identifier identifier) {
        try {
            Table table = paimonCatalog.getTable(identifier);
            return Options.fromMap(table.options()).get(CoreOptions.LAKE_STREAM_ENABLED)
                    ? table
                    : null;
        } catch (Catalog.TableNotExistException e) {
            return null;
        }
    }

    private static Identifier identifierOf(DynamicTableFactory.Context context) {
        ObjectIdentifier identifier = context.getObjectIdentifier();
        return new Identifier(identifier.getDatabaseName(), identifier.getObjectName());
    }

    /**
     * The failure reported for a lake-stream table when no store was configured. Serving it from
     * Paimon instead would leave the store and Paimon holding different values for the same keys.
     *
     * @param operation what the statement was trying to do, e.g. {@code "write to"}
     */
    public static ValidationException missingStoreError(
            ObjectIdentifier identifier, String operation) {
        return new ValidationException(
                String.format(
                        "Cannot %s lake-stream table '%s': the catalog was created without "
                                + "'%s', so there is no Lakestream Store to serve it, and serving it "
                                + "from Paimon instead would leave the store and Paimon holding "
                                + "different data for the same keys. Create the catalog with '%s' and "
                                + "the store's connection options, or set '%s' to false first.",
                        operation,
                        identifier.asSummaryString(),
                        CatalogOptions.LAKE_STREAM_STORE.key(),
                        CatalogOptions.LAKE_STREAM_STORE.key(),
                        CoreOptions.LAKE_STREAM_ENABLED.key()));
    }

    /** Rejects a write through the read-only {@code $rt} name, which the store serves. */
    private static void checkNotRealTimeWrite(DynamicTableFactory.Context context) {
        ObjectIdentifier identifier = context.getObjectIdentifier();
        if (LakeStreamUtils.isRealTimeTable(identifierOf(context))) {
            throw new ValidationException(
                    String.format(
                            "Cannot write to '%s': the '$%s' suffix names the Lakestream "
                                    + "Store's read-only view of a lake-stream table. Write to "
                                    + "'%s' instead.",
                            identifier.asSummaryString(),
                            LakeStreamUtils.REAL_TIME_TABLE_SUFFIX,
                            LakeStreamUtils.stripRealTimeSuffix(identifier).asSummaryString()));
        }
    }

    /** The store's factory of the given type, failing with an error naming the store. */
    private <T> T storeFactory(
            org.apache.flink.table.catalog.Catalog storeCatalog,
            Class<T> factoryClass,
            String purpose) {
        String store = catalog.lakeStreamStoreIdentifier();
        Optional<Factory> factory = storeCatalog.getFactory();
        if (!factory.isPresent() || !factoryClass.isInstance(factory.get())) {
            throw new ValidationException(
                    String.format(
                            "The Lakestream Store '%s' (configured via '%s' = '%s') does not "
                                    + "provide a %s through its catalog factory, so Paimon cannot "
                                    + "delegate lake-stream operations to it and cannot %s.",
                            store,
                            CatalogOptions.LAKE_STREAM_STORE.key(),
                            store,
                            factoryClass.getSimpleName(),
                            purpose));
        }
        return factoryClass.cast(factory.get());
    }

    /**
     * The planner's context adapted for the store's factory: the base identifier, the store table's
     * options, and the store's catalog options after prefix removal.
     */
    private DynamicTableFactory.Context delegatedContext(
            DynamicTableFactory.Context original,
            ObjectIdentifier identifier,
            CatalogBaseTable storeTable) {
        if (!(storeTable instanceof CatalogTable)) {
            throw new ValidationException(
                    String.format(
                            "Expected a table from the Lakestream Store '%s' for '%s', but got %s.",
                            catalog.lakeStreamStoreIdentifier(),
                            identifier.asSummaryString(),
                            storeTable.getClass().getName()));
        }
        ResolvedCatalogTable resolvedStoreTable =
                new ResolvedCatalogTable(
                        (CatalogTable) storeTable, original.getCatalogTable().getResolvedSchema());

        Configuration configuration = new Configuration();
        ReadableConfig originalConfiguration = original.getConfiguration();
        if (originalConfiguration instanceof Configuration) {
            configuration.addAll((Configuration) originalConfiguration);
        } else {
            for (Map.Entry<String, String> entry :
                    AbstractFlinkTableFactory.getAllOptions(original).entrySet()) {
                configuration.setString(entry.getKey(), entry.getValue());
            }
        }
        Map<String, String> storeConnectionOptions =
                LakeStreamUtils.extractPrefixedOptions(
                        catalog.catalog().options(), catalog.lakeStreamStoreIdentifier() + ".");
        for (Map.Entry<String, String> entry : storeConnectionOptions.entrySet()) {
            configuration.setString(entry.getKey(), entry.getValue());
        }

        return new DynamicTableFactory.Context() {
            @Override
            public ObjectIdentifier getObjectIdentifier() {
                return identifier;
            }

            @Override
            public ResolvedCatalogTable getCatalogTable() {
                return resolvedStoreTable;
            }

            @Override
            public boolean isTemporary() {
                return false;
            }

            @Override
            public ReadableConfig getConfiguration() {
                return configuration;
            }

            @Override
            public ClassLoader getClassLoader() {
                return original.getClassLoader();
            }
        };
    }
}
