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
import org.apache.paimon.data.variant.VariantAccessInfo;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileRecordReader;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.partition.PartitionUtils;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileUtils;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.paimon.partition.PartitionPredicate.createPartitionPredicate;
import static org.apache.paimon.partition.PartitionPredicate.fromPredicate;
import static org.apache.paimon.partition.PartitionPredicate.splitPartitionPredicatesAndDataPredicates;
import static org.apache.paimon.predicate.PredicateBuilder.excludePredicateWithFields;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** {@link ReadBuilder} for {@link FormatTable}. */
public class FormatReadBuilder implements ReadBuilder {

    private static final long serialVersionUID = 1L;

    private final FormatTable table;
    private RowType readType;
    private final CoreOptions options;
    @Nullable private Predicate filter;
    @Nullable private PartitionPredicate partitionFilter;
    @Nullable private Integer limit;

    public FormatReadBuilder(FormatTable table) {
        this.table = table;
        this.readType = this.table.rowType();
        this.options = new CoreOptions(table.options());
    }

    @Override
    public String tableName() {
        return this.table.name();
    }

    @Override
    public RowType readType() {
        return this.readType;
    }

    @Override
    public ReadBuilder withFilter(Predicate predicate) {
        if (this.filter == null) {
            this.filter = predicate;
        } else {
            this.filter = PredicateBuilder.and(this.filter, predicate);
        }
        return this;
    }

    @Override
    public ReadBuilder withPartitionFilter(Map<String, String> partitionSpec) {
        if (partitionSpec != null) {
            RowType partitionType = table.rowType().project(table.partitionKeys());
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
    public ReadBuilder withVariantAccess(VariantAccessInfo[] variantAccessInfo) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ReadBuilder withProjection(int[] projection) {
        if (projection == null) {
            return this;
        }
        return withReadType(readType().project(projection));
    }

    @Override
    public ReadBuilder withLimit(int limit) {
        this.limit = limit;
        return this;
    }

    @Override
    public TableScan newScan() {
        PartitionPredicate partitionFilter = this.partitionFilter;
        if (partitionFilter == null && this.filter != null && !table.partitionKeys().isEmpty()) {
            Optional<PartitionPredicate> partitionPredicateOpt =
                    splitPartitionPredicatesAndDataPredicates(
                                    filter, table.rowType(), table.partitionKeys())
                            .getLeft();
            if (partitionPredicateOpt.isPresent()) {
                partitionFilter = partitionPredicateOpt.get();
            }
        }
        return new FormatTableScan(table, partitionFilter, limit);
    }

    @Override
    public TableRead newRead() {
        return new FormatTableRead(readType(), table.rowType(), this, filter, limit);
    }

    protected RecordReader<InternalRow> createReader(FormatDataSplit dataSplit) throws IOException {
        Path filePath = dataSplit.dataPath();
        FormatReaderContext formatReaderContext =
                new FormatReaderContext(table.fileIO(), filePath, dataSplit.fileSize(), null);
        // Skip pushing down partition filters to reader.
        List<Predicate> readFilters =
                excludePredicateWithFields(
                        PredicateBuilder.splitAnd(filter), new HashSet<>(table.partitionKeys()));
        RowType dataRowType = getRowTypeWithoutPartition(table.rowType(), table.partitionKeys());
        RowType readRowType = getRowTypeWithoutPartition(readType(), table.partitionKeys());
        FormatReaderFactory readerFactory =
                FileFormatDiscover.of(options)
                        .discover(options.formatType())
                        .createReaderFactory(dataRowType, readRowType, readFilters);

        Pair<int[], RowType> partitionMapping =
                PartitionUtils.getPartitionMapping(
                        table.partitionKeys(), readType().getFields(), table.partitionType());
        try {
            FileRecordReader<InternalRow> reader;
            Long length = dataSplit.length();
            if (length != null) {
                reader =
                        readerFactory.createReader(formatReaderContext, dataSplit.offset(), length);
            } else {
                checkArgument(dataSplit.offset() == 0, "Offset must be 0.");
                reader = readerFactory.createReader(formatReaderContext);
            }
            return new DataFileRecordReader(
                    readType(),
                    reader,
                    null,
                    null,
                    PartitionUtils.create(partitionMapping, dataSplit.partition()),
                    false,
                    null,
                    0,
                    Collections.emptyMap(),
                    null);
        } catch (Exception e) {
            FileUtils.checkExists(formatReaderContext.fileIO(), formatReaderContext.filePath());
            throw e;
        }
    }

    private static RowType getRowTypeWithoutPartition(RowType rowType, List<String> partitionKeys) {
        return rowType.project(
                rowType.getFieldNames().stream()
                        .filter(name -> !partitionKeys.contains(name))
                        .collect(Collectors.toList()));
    }

    // ===================== Unsupported ===============================

    @Override
    public ReadBuilder withTopN(TopN topN) {
        return this;
    }

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
    public ReadBuilder withRowRanges(List<Range> rowRanges) {
        throw new UnsupportedOperationException("Format Table does not support withRowRanges.");
    }

    @Override
    public ReadBuilder withVectorSearch(VectorSearch vectorSearch) {
        throw new UnsupportedOperationException("Format Table does not support withRowRanges.");
    }

    @Override
    public StreamTableScan newStreamScan() {
        throw new UnsupportedOperationException("Format Table does not support stream scan.");
    }
}
