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

package org.apache.paimon.table.system;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.casting.CastExecutor;
import org.apache.paimon.casting.CastExecutors;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.predicate.And;
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.GreaterOrEqual;
import org.apache.paimon.predicate.GreaterThan;
import org.apache.paimon.predicate.InPredicateVisitor;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.LeafPredicateExtractor;
import org.apache.paimon.predicate.LessOrEqual;
import org.apache.paimon.predicate.LessThan;
import org.apache.paimon.predicate.Or;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.ReadonlyTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadOnceTableScan;
import org.apache.paimon.table.source.SingletonSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.snapshot.TimeTravelUtil;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.SerializationUtils;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static org.apache.paimon.catalog.Identifier.SYSTEM_TABLE_SPLITTER;

/** A {@link Table} for showing committing snapshots of table. */
public class ManifestsTable implements ReadonlyTable {

    private static final Logger LOG = LoggerFactory.getLogger(ManifestsTable.class);

    private static final long serialVersionUID = 2L;

    public static final String MANIFESTS = "manifests";

    public static final RowType TABLE_TYPE =
            new RowType(
                    Arrays.asList(
                            new DataField(0, "file_name", SerializationUtils.newStringType(false)),
                            new DataField(1, "file_size", new BigIntType(false)),
                            new DataField(2, "num_added_files", new BigIntType(false)),
                            new DataField(3, "num_deleted_files", new BigIntType(false)),
                            new DataField(4, "schema_id", new BigIntType(false)),
                            new DataField(
                                    5,
                                    "min_partition_stats",
                                    SerializationUtils.newStringType(true)),
                            new DataField(
                                    6,
                                    "max_partition_stats",
                                    SerializationUtils.newStringType(true)),
                            new DataField(7, "min_row_id", new BigIntType(true)),
                            new DataField(8, "max_row_id", new BigIntType(true))));

    private final FileStoreTable dataTable;

    public ManifestsTable(FileStoreTable dataTable) {
        this.dataTable = dataTable;
    }

    @Override
    public InnerTableScan newScan() {
        return new ManifestsScan();
    }

    @Override
    public InnerTableRead newRead() {
        return new ManifestsRead(dataTable);
    }

    @Override
    public String name() {
        return dataTable.name() + SYSTEM_TABLE_SPLITTER + MANIFESTS;
    }

    @Override
    public RowType rowType() {
        return TABLE_TYPE;
    }

    @Override
    public List<String> primaryKeys() {
        return Collections.singletonList("file_name");
    }

    @Override
    public FileIO fileIO() {
        return dataTable.fileIO();
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new ManifestsTable(dataTable.copy(dynamicOptions));
    }

    private static class ManifestsScan extends ReadOnceTableScan {

        @Override
        public InnerTableScan withFilter(Predicate predicate) {
            // filter is handled in ManifestsRead
            return this;
        }

        @Override
        protected Plan innerPlan() {
            return () -> Collections.singletonList(new ManifestsSplit());
        }
    }

    private static class ManifestsSplit extends SingletonSplit {

        private static final long serialVersionUID = 1L;

        private ManifestsSplit() {}

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            return o != null && getClass() == o.getClass();
        }

        @Override
        public OptionalLong mergedRowCount() {
            return OptionalLong.empty();
        }
    }

    private static class ManifestsRead implements InnerTableRead {

        private static final String LEAF_NAME = "schema_id";

        private RowType readType;

        private final FileStoreTable dataTable;

        private @Nullable Long schemaIdMin = null;
        private @Nullable Long schemaIdMax = null;
        private final List<Long> schemaIds = new ArrayList<>();

        public ManifestsRead(FileStoreTable dataTable) {
            this.dataTable = dataTable;
        }

        @Override
        public InnerTableRead withFilter(Predicate predicate) {
            if (predicate == null) {
                return this;
            }

            if (predicate instanceof CompoundPredicate) {
                CompoundPredicate compoundPredicate = (CompoundPredicate) predicate;
                if ((compoundPredicate.function()) instanceof And) {
                    List<Predicate> children = compoundPredicate.children();
                    for (Predicate leaf : children) {
                        handleLeafPredicate(leaf, LEAF_NAME);
                    }
                }

                // optimize for IN filter
                if ((compoundPredicate.function()) instanceof Or) {
                    InPredicateVisitor.extractInElements(predicate, LEAF_NAME)
                            .ifPresent(
                                    leafs ->
                                            leafs.forEach(
                                                    leaf ->
                                                            schemaIds.add(
                                                                    Long.parseLong(
                                                                            leaf.toString()))));
                }
            } else {
                handleLeafPredicate(predicate, LEAF_NAME);
            }

            return this;
        }

        private void handleLeafPredicate(Predicate predicate, String leafName) {
            LeafPredicate schemaPred =
                    predicate.visit(LeafPredicateExtractor.INSTANCE).get(leafName);
            if (schemaPred != null) {
                if (schemaPred.function() instanceof Equal) {
                    schemaIdMin = (Long) schemaPred.literals().get(0);
                    schemaIdMax = (Long) schemaPred.literals().get(0);
                }

                if (schemaPred.function() instanceof GreaterThan) {
                    schemaIdMin = (Long) schemaPred.literals().get(0) + 1;
                }

                if (schemaPred.function() instanceof GreaterOrEqual) {
                    schemaIdMin = (Long) schemaPred.literals().get(0);
                }

                if (schemaPred.function() instanceof LessThan) {
                    schemaIdMax = (Long) schemaPred.literals().get(0) - 1;
                }

                if (schemaPred.function() instanceof LessOrEqual) {
                    schemaIdMax = (Long) schemaPred.literals().get(0);
                }
            }
        }

        @Override
        public InnerTableRead withReadType(RowType readType) {
            this.readType = readType;
            return this;
        }

        @Override
        public TableRead withIOManager(IOManager ioManager) {
            return this;
        }

        @Override
        public RecordReader<InternalRow> createReader(Split split) {
            if (!(split instanceof ManifestsSplit)) {
                throw new IllegalArgumentException("Unsupported split: " + split.getClass());
            }
            List<ManifestFileMeta> manifestFileMetas = allManifests(dataTable);

            // Apply schema_id filter
            if (!schemaIds.isEmpty()) {
                manifestFileMetas = filterBySchemaIds(manifestFileMetas, schemaIds);
            } else if (schemaIdMin != null || schemaIdMax != null) {
                manifestFileMetas =
                        filterBySchemaIdRange(
                                manifestFileMetas,
                                Optional.ofNullable(schemaIdMin),
                                Optional.ofNullable(schemaIdMax));
            }

            @SuppressWarnings("unchecked")
            CastExecutor<InternalRow, BinaryString> partitionCastExecutor =
                    (CastExecutor<InternalRow, BinaryString>)
                            CastExecutors.resolveToString(
                                    dataTable.schema().logicalPartitionType());

            Iterator<InternalRow> rows =
                    Iterators.transform(
                            manifestFileMetas.iterator(),
                            meta -> toRow(meta, partitionCastExecutor));
            if (readType != null) {
                rows =
                        Iterators.transform(
                                rows,
                                row ->
                                        ProjectedRow.from(readType, ManifestsTable.TABLE_TYPE)
                                                .replaceRow(row));
            }
            return new IteratorRecordReader<>(rows);
        }

        private static List<ManifestFileMeta> filterBySchemaIds(
                List<ManifestFileMeta> metas, List<Long> schemaIds) {
            List<ManifestFileMeta> result = new ArrayList<>();
            for (ManifestFileMeta meta : metas) {
                if (schemaIds.contains(meta.schemaId())) {
                    result.add(meta);
                }
            }
            return result;
        }

        private static List<ManifestFileMeta> filterBySchemaIdRange(
                List<ManifestFileMeta> metas, Optional<Long> min, Optional<Long> max) {
            List<ManifestFileMeta> result = new ArrayList<>();
            for (ManifestFileMeta meta : metas) {
                long schemaId = meta.schemaId();
                if (min.isPresent() && schemaId < min.get()) {
                    continue;
                }
                if (max.isPresent() && schemaId > max.get()) {
                    continue;
                }
                result.add(meta);
            }
            return result;
        }

        private InternalRow toRow(
                ManifestFileMeta manifestFileMeta,
                CastExecutor<InternalRow, BinaryString> partitionCastExecutor) {
            return GenericRow.of(
                    BinaryString.fromString(manifestFileMeta.fileName()),
                    manifestFileMeta.fileSize(),
                    manifestFileMeta.numAddedFiles(),
                    manifestFileMeta.numDeletedFiles(),
                    manifestFileMeta.schemaId(),
                    partitionCastExecutor.cast(manifestFileMeta.partitionStats().minValues()),
                    partitionCastExecutor.cast(manifestFileMeta.partitionStats().maxValues()),
                    manifestFileMeta.minRowId(),
                    manifestFileMeta.maxRowId());
        }
    }

    private static List<ManifestFileMeta> allManifests(FileStoreTable dataTable) {
        CoreOptions options = dataTable.coreOptions();
        Snapshot snapshot = TimeTravelUtil.tryTravelOrLatest(dataTable);
        if (snapshot == null) {
            LOG.warn("Check if your snapshot is empty.");
            return Collections.emptyList();
        }
        FileStorePathFactory fileStorePathFactory = dataTable.store().pathFactory();
        ManifestList manifestList =
                new ManifestList.Factory(
                                dataTable.fileIO(),
                                FileFormat.manifestFormat(options),
                                options.manifestCompression(),
                                fileStorePathFactory,
                                null)
                        .create();
        return manifestList.readAllManifests(snapshot);
    }
}
