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
import org.apache.paimon.casting.CastExecutor;
import org.apache.paimon.casting.CastExecutors;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.Partition;
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
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.InternalRowUtils;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.SerializationUtils;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.paimon.catalog.Identifier.SYSTEM_TABLE_SPLITTER;

/** A {@link Table} for showing partitions info. */
public class PartitionsTable implements ReadonlyTable {

    private static final long serialVersionUID = 1L;

    public static final String PARTITIONS = "partitions";

    public static final RowType TABLE_TYPE =
            new RowType(
                    Arrays.asList(
                            new DataField(0, "partition", SerializationUtils.newStringType(true)),
                            new DataField(1, "record_count", new BigIntType(false)),
                            new DataField(2, "file_size_in_bytes", new BigIntType(false)),
                            new DataField(3, "file_count", new BigIntType(false)),
                            new DataField(4, "last_update_time", DataTypes.TIMESTAMP_MILLIS()),
                            new DataField(5, "created_at", DataTypes.TIMESTAMP_MILLIS()),
                            new DataField(6, "created_by", DataTypes.STRING()),
                            new DataField(7, "updated_by", DataTypes.STRING()),
                            new DataField(8, "options", DataTypes.STRING())));

    private final FileStoreTable storeTable;

    public PartitionsTable(FileStoreTable storeTable) {
        this.storeTable = storeTable;
    }

    @Override
    public String name() {
        return storeTable.name() + SYSTEM_TABLE_SPLITTER + PARTITIONS;
    }

    @Override
    public RowType rowType() {
        return TABLE_TYPE;
    }

    @Override
    public List<String> primaryKeys() {
        return Collections.singletonList("partition");
    }

    @Override
    public FileIO fileIO() {
        return storeTable.fileIO();
    }

    @Override
    public InnerTableScan newScan() {
        return new PartitionsScan();
    }

    @Override
    public InnerTableRead newRead() {
        return new PartitionsRead(storeTable);
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new PartitionsTable(storeTable.copy(dynamicOptions));
    }

    private static class PartitionsScan extends ReadOnceTableScan {

        @Override
        public InnerTableScan withFilter(Predicate predicate) {
            return this;
        }

        @Override
        public Plan innerPlan() {
            return () -> Collections.singletonList(new PartitionsSplit());
        }
    }

    private static class PartitionsSplit extends SingletonSplit {

        private static final long serialVersionUID = 1L;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            return o != null && getClass() == o.getClass();
        }

        @Override
        public int hashCode() {
            return 1;
        }
    }

    private static class PartitionsRead implements InnerTableRead {

        private final FileStoreTable fileStoreTable;

        private RowType readType;

        public PartitionsRead(FileStoreTable table) {
            this.fileStoreTable = table;
        }

        @Override
        public InnerTableRead withFilter(Predicate predicate) {
            // TODO
            return this;
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
        public RecordReader<InternalRow> createReader(Split split) throws IOException {
            if (!(split instanceof PartitionsSplit)) {
                throw new IllegalArgumentException("Unsupported split: " + split.getClass());
            }

            List<Partition> partitions = listPartitions();

            List<DataType> fieldTypes =
                    fileStoreTable.schema().logicalPartitionType().getFieldTypes();
            InternalRow.FieldGetter[] fieldGetters =
                    InternalRowUtils.createFieldGetters(fieldTypes);
            List<CastExecutor> castExecutors =
                    fieldTypes.stream()
                            .map(CastExecutors::resolveToString)
                            .collect(Collectors.toList());

            // sorted by partition
            Iterator<InternalRow> iterator =
                    partitions.stream()
                            .map(
                                    partition ->
                                            toRow(
                                                    partition,
                                                    fileStoreTable.partitionKeys(),
                                                    castExecutors,
                                                    fieldGetters,
                                                    fileStoreTable
                                                            .coreOptions()
                                                            .partitionDefaultName()))
                            .sorted(Comparator.comparing(row -> row.getString(0)))
                            .iterator();

            if (readType != null) {
                iterator =
                        Iterators.transform(
                                iterator,
                                row ->
                                        ProjectedRow.from(readType, PartitionsTable.TABLE_TYPE)
                                                .replaceRow(row));
            }
            return new IteratorRecordReader<>(iterator);
        }

        private InternalRow toRow(
                Partition partition,
                List<String> partitionKeys,
                List<CastExecutor> castExecutors,
                InternalRow.FieldGetter[] fieldGetters,
                String defaultPartitionName) {
            PartitionEntry entry = toPartitionEntry(partition);

            StringBuilder partitionStringBuilder = new StringBuilder();

            for (int i = 0; i < partitionKeys.size(); i++) {
                if (i > 0) {
                    partitionStringBuilder.append("/");
                }
                Object partitionValue = fieldGetters[i].getFieldOrNull(entry.partition());
                String partitionValueString =
                        partitionValue == null
                                ? defaultPartitionName
                                : castExecutors
                                        .get(i)
                                        .cast(fieldGetters[i].getFieldOrNull(entry.partition()))
                                        .toString();
                partitionStringBuilder
                        .append(partitionKeys.get(i))
                        .append("=")
                        .append(partitionValueString);
            }

            BinaryString createdByString = BinaryString.fromString(partition.createdBy());
            Timestamp createdAtTimestamp = toTimestamp(partition.createdAt());
            BinaryString updatedByString = BinaryString.fromString(partition.updatedBy());
            BinaryString optionsString = null;
            if (Objects.nonNull(partition.options())) {
                optionsString =
                        BinaryString.fromString(JsonSerdeUtil.toFlatJson(partition.options()));
            }

            return GenericRow.of(
                    BinaryString.fromString(partitionStringBuilder.toString()),
                    partition.recordCount(),
                    partition.fileSizeInBytes(),
                    partition.fileCount(),
                    toTimestamp(partition.lastFileCreationTime()),
                    createdAtTimestamp,
                    createdByString,
                    updatedByString,
                    optionsString);
        }

        private PartitionEntry toPartitionEntry(Partition partition) {
            RowType partitionType = fileStoreTable.schema().logicalPartitionType();
            String defaultPartitionName = fileStoreTable.coreOptions().partitionDefaultName();
            InternalRowSerializer serializer = new InternalRowSerializer(partitionType);
            GenericRow partitionRow =
                    InternalRowPartitionComputer.convertSpecToInternalRow(
                            partition.spec(), partitionType, defaultPartitionName);
            BinaryRow binaryPartition = serializer.toBinaryRow(partitionRow).copy();
            return new PartitionEntry(
                    binaryPartition,
                    partition.recordCount(),
                    partition.fileSizeInBytes(),
                    partition.fileCount(),
                    partition.lastFileCreationTime());
        }

        private Timestamp toTimestamp(Long epochMillis) {
            if (epochMillis == null) {
                return null;
            }
            return Timestamp.fromLocalDateTime(
                    LocalDateTime.ofInstant(
                            Instant.ofEpochMilli(epochMillis), ZoneId.systemDefault()));
        }

        private List<Partition> listPartitions() {
            CatalogLoader catalogLoader = fileStoreTable.catalogEnvironment().catalogLoader();
            if (TimeTravelUtil.hasTimeTravelOptions(new Options(fileStoreTable.options()))
                    || catalogLoader == null) {
                return listPartitionEntries();
            }

            try (Catalog catalog = catalogLoader.load()) {
                Identifier baseIdentifier = fileStoreTable.catalogEnvironment().identifier();
                if (baseIdentifier == null) {
                    return listPartitionEntries();
                }
                String branch = fileStoreTable.coreOptions().branch();
                Identifier identifier;
                if (branch != null && !branch.equals(CoreOptions.BRANCH.defaultValue())) {
                    identifier =
                            new Identifier(
                                    baseIdentifier.getDatabaseName(),
                                    baseIdentifier.getTableName(),
                                    branch);
                } else {
                    identifier = baseIdentifier;
                }
                return catalog.listPartitions(identifier);
            } catch (Exception e) {
                return listPartitionEntries();
            }
        }

        private List<Partition> listPartitionEntries() {
            List<PartitionEntry> partitionEntries =
                    fileStoreTable.newScan().withLevelFilter(level -> true).listPartitionEntries();
            RowType partitionType = fileStoreTable.schema().logicalPartitionType();
            String defaultPartitionName = fileStoreTable.coreOptions().partitionDefaultName();
            String[] partitionColumns = fileStoreTable.partitionKeys().toArray(new String[0]);
            InternalRowPartitionComputer computer =
                    new InternalRowPartitionComputer(
                            defaultPartitionName,
                            partitionType,
                            partitionColumns,
                            fileStoreTable.coreOptions().legacyPartitionName());
            return partitionEntries.stream()
                    .map(entry -> entry.toPartition(computer))
                    .collect(Collectors.toList());
        }
    }
}
