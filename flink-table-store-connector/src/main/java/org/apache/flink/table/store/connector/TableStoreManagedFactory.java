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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.factories.ManagedTableFactory;
import org.apache.flink.table.store.connector.utils.TableConfigUtils;
import org.apache.flink.table.store.file.FileStore;
import org.apache.flink.table.store.file.FileStoreOptions;
import org.apache.flink.table.store.file.WriteMode;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.mergetree.Levels;
import org.apache.flink.table.store.file.mergetree.MergeTreeOptions;
import org.apache.flink.table.store.file.mergetree.compact.UniversalCompaction;
import org.apache.flink.table.store.file.operation.FileStoreScan;
import org.apache.flink.table.store.file.predicate.PredicateConverter;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.UpdateSchema;
import org.apache.flink.table.store.file.utils.JsonSerdeUtil;
import org.apache.flink.table.store.file.utils.KeyComparatorSupplier;
import org.apache.flink.table.store.file.utils.PartitionedManifestMeta;
import org.apache.flink.table.store.log.LogStoreTableFactory;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.table.store.connector.TableStoreFactoryOptions.COMPACTION_RESCALE_BUCKET;
import static org.apache.flink.table.store.connector.TableStoreFactoryOptions.COMPACTION_SCANNED_MANIFEST;
import static org.apache.flink.table.store.connector.TableStoreFactoryOptions.ROOT_PATH;
import static org.apache.flink.table.store.connector.TableStoreFactoryOptions.WRITE_MODE;
import static org.apache.flink.table.store.file.FileStoreOptions.BUCKET;
import static org.apache.flink.table.store.file.FileStoreOptions.PATH;
import static org.apache.flink.table.store.file.FileStoreOptions.TABLE_STORE_PREFIX;
import static org.apache.flink.table.store.log.LogOptions.LOG_PREFIX;

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
                        factory.enrichOptions(createLogContext(context, enrichedOptions))
                                .forEach((k, v) -> enrichedOptions.putIfAbsent(LOG_PREFIX + k, v)));

        return enrichedOptions;
    }

    @VisibleForTesting
    static String relativeTablePath(ObjectIdentifier tableIdentifier) {
        return String.format(
                "%s.catalog/%s.db/%s",
                tableIdentifier.getCatalogName(),
                tableIdentifier.getDatabaseName(),
                tableIdentifier.getObjectName());
    }

    @Override
    public void onCreateTable(Context context, boolean ignoreIfExists) {
        Map<String, String> options = context.getCatalogTable().getOptions();
        Path path = FileStoreOptions.path(options);
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
                    WriteMode.APPEND_ONLY.toString(),
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
                    .commitNewVersion(UpdateSchema.fromCatalogTable(context.getCatalogTable()));
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
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
        Path path = FileStoreOptions.path(context.getCatalogTable().getOptions());
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
        Map<String, String> newOptions = new HashMap<>(context.getCatalogTable().getOptions());
        FileStore fileStore = buildTableStore(context).buildFileStore();
        FileStoreScan.Plan plan =
                fileStore
                        .newScan()
                        .withPartitionFilter(
                                PredicateConverter.CONVERTER.fromMap(
                                        catalogPartitionSpec.getPartitionSpec(),
                                        fileStore.partitionType()))
                        .plan();

        Preconditions.checkState(
                plan.snapshotId() != null && !plan.files().isEmpty(),
                "The specified %s to compact does not exist any snapshot",
                catalogPartitionSpec.getPartitionSpec().isEmpty()
                        ? "table"
                        : String.format("partition %s", catalogPartitionSpec.getPartitionSpec()));
        Map<BinaryRowData, Map<Integer, List<DataFileMeta>>> groupBy = plan.groupByPartFiles();
        if (!Boolean.parseBoolean(newOptions.get(COMPACTION_RESCALE_BUCKET.key()))) {
            groupBy =
                    pickManifest(
                            groupBy,
                            new FileStoreOptions(Configuration.fromMap(newOptions))
                                    .mergeTreeOptions(),
                            new KeyComparatorSupplier(fileStore.partitionType()).get());
        }
        newOptions.put(
                COMPACTION_SCANNED_MANIFEST.key(),
                JsonSerdeUtil.toJson(new PartitionedManifestMeta(plan.snapshotId(), groupBy)));
        return newOptions;
    }

    @VisibleForTesting
    Map<BinaryRowData, Map<Integer, List<DataFileMeta>>> pickManifest(
            Map<BinaryRowData, Map<Integer, List<DataFileMeta>>> groupBy,
            MergeTreeOptions options,
            Comparator<RowData> keyComparator) {
        Map<BinaryRowData, Map<Integer, List<DataFileMeta>>> filtered = new HashMap<>();
        UniversalCompaction compaction =
                new UniversalCompaction(
                        options.maxSizeAmplificationPercent,
                        options.sizeRatio,
                        options.numSortedRunCompactionTrigger);

        for (Map.Entry<BinaryRowData, Map<Integer, List<DataFileMeta>>> partEntry :
                groupBy.entrySet()) {
            Map<Integer, List<DataFileMeta>> manifests = new HashMap<>();
            for (Map.Entry<Integer, List<DataFileMeta>> bucketEntry :
                    partEntry.getValue().entrySet()) {
                Levels levels =
                        new Levels(keyComparator, bucketEntry.getValue(), options.numLevels);
                compaction
                        .pick(levels.numberOfLevels(), levels.levelSortedRuns())
                        .ifPresent(
                                unit -> {
                                    if (unit.files().size() > 0) {
                                        manifests.put(bucketEntry.getKey(), unit.files());
                                    }
                                });
            }
            if (manifests.size() > 0) {
                filtered.put(partEntry.getKey(), manifests);
            }
        }
        return filtered;
    }
}
