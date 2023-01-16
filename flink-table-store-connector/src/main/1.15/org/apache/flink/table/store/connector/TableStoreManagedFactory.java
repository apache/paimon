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

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.factories.ManagedTableFactory;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.UpdateSchema;
import org.apache.flink.table.store.log.LogStoreTableFactory;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.table.store.CoreOptions.BUCKET;
import static org.apache.flink.table.store.CoreOptions.PATH;
import static org.apache.flink.table.store.CoreOptions.WRITE_MODE;
import static org.apache.flink.table.store.connector.FlinkConnectorOptions.ROOT_PATH;
import static org.apache.flink.table.store.connector.FlinkConnectorOptions.TABLE_STORE_PREFIX;
import static org.apache.flink.table.store.connector.FlinkConnectorOptions.relativeTablePath;
import static org.apache.flink.table.store.file.WriteMode.APPEND_ONLY;

/** Default implementation of {@link ManagedTableFactory}. */
public class TableStoreManagedFactory extends AbstractTableStoreFactory
        implements ManagedTableFactory {

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

        String rootPath = enrichedOptions.remove(ROOT_PATH.key());
        Preconditions.checkArgument(
                rootPath != null,
                String.format(
                        "Please specify a root path by setting session level configuration "
                                + "as `SET 'table-store.%s' = '...'`.",
                        ROOT_PATH.key()));

        Preconditions.checkArgument(
                !enrichedOptions.containsKey(PATH.key()),
                "Managed table can not contain table path. "
                        + "You need to remove path in table options or session config.");

        Path path = new Path(rootPath, relativeTablePath(context.getObjectIdentifier()));
        enrichedOptions.put(PATH.key(), path.toString());

        Optional<LogStoreTableFactory> logFactory =
                createOptionalLogStoreFactory(context.getClassLoader(), enrichedOptions);
        logFactory.ifPresent(
                factory ->
                        factory.enrichOptions(
                                        new TableStoreDynamicContext(context, enrichedOptions))
                                .forEach(enrichedOptions::putIfAbsent));

        return enrichedOptions;
    }

    @Override
    public void onCreateTable(Context context, boolean ignoreIfExists) {
        Map<String, String> options = context.getCatalogTable().getOptions();
        Path path = CoreOptions.path(options);
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

        // Cannot define any primary key in an append-only table.
        if (context.getCatalogTable().getResolvedSchema().getPrimaryKey().isPresent()) {
            if (Objects.equals(
                    APPEND_ONLY.toString(),
                    options.getOrDefault(WRITE_MODE.key(), WRITE_MODE.defaultValue().toString()))) {
                throw new TableException(
                        "Cannot define any primary key in an append-only table. Set 'write-mode'='change-log' if "
                                + "still want to keep the primary key definition.");
            }
        }

        // update schema
        // TODO pass lock
        try {
            new SchemaManager(path)
                    .commitNewVersion(FlinkCatalog.fromCatalogTable(context.getCatalogTable()));
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        createOptionalLogStoreFactory(context)
                .ifPresent(
                        factory ->
                                factory.onCreateTable(
                                        context,
                                        Integer.parseInt(
                                                options.getOrDefault(
                                                        BUCKET.key(),
                                                        BUCKET.defaultValue().toString())),
                                        ignoreIfExists));
    }

    @Override
    public void onDropTable(Context context, boolean ignoreIfNotExists) {
        Path path = CoreOptions.path(context.getCatalogTable().getOptions());
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
                .ifPresent(factory -> factory.onDropTable(context, ignoreIfNotExists));
    }

    @Override
    public Map<String, String> onCompactTable(
            Context context, CatalogPartitionSpec catalogPartitionSpec) {
        throw new UnsupportedOperationException(
                "Table store does not support ALTER TABLE ... COMPACT ...");
    }
}
