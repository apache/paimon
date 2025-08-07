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

package org.apache.paimon.table.format;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.format.FormatKey;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileRecordReader;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.ReaderSupplier;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.source.AbstractDataTableRead;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.FormatReaderMapping;
import org.apache.paimon.utils.InternalRowPartitionComputer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.paimon.partition.PartitionPredicate.createPartitionPredicate;
import static org.apache.paimon.partition.PartitionPredicate.fromPredicate;

/** {@link ReadBuilder} for {@link FormatTable}. */
public class FormatReadBuilder implements ReadBuilder {

    private static final long serialVersionUID = 1L;

    private final FormatTable table;
    private final CoreOptions options;
    protected final RowType partitionType;

    private final FileIO fileIO;
    private final TableSchema schema;
    private final FileFormatDiscover formatDiscover;
    private final Map<FormatKey, FormatReaderMapping> formatReaderMappings;
    private final InternalRowPartitionComputer partitionComputer;

    private RowType readRowType;
    @Nullable private List<Predicate> filters;

    private Integer limit = null;

    private @Nullable PartitionPredicate partitionFilter;
    private @Nullable RowType readType;
    private @Nullable Predicate predicate;
    private @Nullable int[] projection;

    public FormatReadBuilder(FormatTable table) {
        this.table = table;
        this.options = new CoreOptions(table.options());
        this.partitionType = table.partitionType();
        this.fileIO = table.fileIO();
        this.schema = table.schema();
        this.readRowType = schema.logicalRowType().notNull();
        this.formatDiscover = FileFormatDiscover.of(options);
        this.formatReaderMappings = new HashMap<>();
        this.partitionComputer =
                new InternalRowPartitionComputer(
                        options.partitionDefaultName(),
                        schema.logicalPartitionType(),
                        schema.partitionKeys().toArray(new String[0]),
                        options.legacyPartitionName());
    }

    @Override
    public String tableName() {
        return table.name();
    }

    @Override
    public RowType readType() {
        return readType != null ? readType : table.rowType();
    }

    @Override
    public ReadBuilder withFilter(Predicate predicate) {
        this.predicate = predicate;
        return this;
    }

    @Override
    public ReadBuilder withPartitionFilter(Map<String, String> partitionSpec) {
        if (partitionSpec != null) {
            RowType partitionType = table.partitionType();
            PartitionPredicate partitionPredicate =
                    fromPredicate(
                            partitionType,
                            createPartitionPredicate(
                                    partitionSpec, partitionType, table.defaultPartName()));
            withPartitionFilter(partitionPredicate);
        }
        return this;
    }

    @Override
    public ReadBuilder withPartitionFilter(PartitionPredicate partitionPredicate) {
        this.partitionFilter = partitionPredicate;
        return this;
    }

    @Override
    public ReadBuilder withReadType(RowType readType) {
        this.readType = readType;
        return this;
    }

    @Override
    public ReadBuilder withProjection(int[] projection) {
        if (projection == null) {
            return this;
        }
        this.projection = projection;
        return withReadType(table.rowType().project(projection));
    }

    @Override
    public ReadBuilder withLimit(int limit) {
        this.limit = limit;
        return this;
    }

    @Override
    public TableScan newScan() {
        TableScan scan = new FormatTableScan(table, predicate, projection);
        return scan;
    }

    @Override
    public TableRead newRead() {
        FormatReadBuilder read = this;
        return new AbstractDataTableRead(table.schema()) {
            @Override
            protected InnerTableRead innerWithFilter(Predicate predicate) {
                this.withFilter(predicate);
                return this;
            }

            @Override
            public void applyReadType(RowType readType) {
                this.withReadType(readType);
            }

            @Override
            public RecordReader<InternalRow> reader(Split split) throws IOException {
                FormatDataSplit dataSplit = (FormatDataSplit) split;
                return read.createReader(dataSplit);
            }
        };
    }

    private RecordReader<InternalRow> createReader(FormatDataSplit dataSplit) throws IOException {
        List<ReaderSupplier<InternalRow>> suppliers = new ArrayList<>();

        List<DataField> readTableFields = readRowType.getFields();
        FormatReaderMapping.Builder formatReaderMappingBuilder =
                new FormatReaderMapping.Builder(
                        formatDiscover, readTableFields, TableSchema::fields, filters, false);

        String formatIdentifier = options.formatType();
        Supplier<FormatReaderMapping> formatSupplier =
                () -> formatReaderMappingBuilder.build(formatIdentifier, schema, schema);

        FormatReaderMapping formatReaderMapping =
                formatReaderMappings.computeIfAbsent(
                        new FormatKey(0, formatIdentifier), key -> formatSupplier.get());
        suppliers.add(() -> createFileReader(dataSplit, formatReaderMapping));

        return ConcatRecordReader.create(suppliers);
    }

    private FileRecordReader<InternalRow> createFileReader(
            FormatDataSplit dataSplit, FormatReaderMapping formatReaderMapping) throws IOException {

        Path filePath = dataSplit.dataPath();
        FormatReaderContext formatReaderContext =
                new FormatReaderContext(fileIO, filePath, dataSplit.length(), null);
        FileRecordReader<InternalRow> fileRecordReader =
                new DataFileRecordReader(
                        schema.logicalRowType(),
                        formatReaderMapping.getReaderFactory(),
                        formatReaderContext,
                        formatReaderMapping.getIndexMapping(),
                        formatReaderMapping.getCastMapping(),
                        null,
                        false,
                        null,
                        0,
                        formatReaderMapping.getSystemFields());
        return fileRecordReader;
    }

    // ===================== Unsupported ===============================

    @Override
    public ReadBuilder dropStats() {
        return this;
    }

    @Override
    public ReadBuilder withBucketFilter(Filter<Integer> bucketFilter) {
        throw new UnsupportedOperationException("Format Table does not support withBucketFilter.");
    }

    @Override
    public ReadBuilder withBucket(int bucket) {
        throw new UnsupportedOperationException("Format Table does not support withBucket.");
    }

    @Override
    public ReadBuilder withShard(int indexOfThisSubtask, int numberOfParallelSubtasks) {
        throw new UnsupportedOperationException("Format Table does not support withShard.");
    }

    @Override
    public StreamTableScan newStreamScan() {
        throw new UnsupportedOperationException("Format Table does not support stream scan.");
    }
}
