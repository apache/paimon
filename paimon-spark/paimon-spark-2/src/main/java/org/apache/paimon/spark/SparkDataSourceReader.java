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

package org.apache.paimon.spark;

import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.TypeUtils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.Statistics;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownFilters;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.v2.reader.SupportsReportStatistics;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.stream.Collectors;

/** A Spark {@link DataSourceReader} for paimon. */
public class SparkDataSourceReader
        implements SupportsPushDownFilters,
                SupportsPushDownRequiredColumns,
                SupportsReportStatistics {

    private final Table table;

    private List<Predicate> predicates = new ArrayList<>();
    private Filter[] pushedFilters;
    private int[] projectedFields;
    private List<Split> splits;

    public SparkDataSourceReader(Table table) {
        this.table = table;
    }

    @Override
    public Filter[] pushFilters(Filter[] filters) {
        SparkFilterConverter converter = new SparkFilterConverter(table.rowType());
        List<Predicate> predicates = new ArrayList<>();
        List<Filter> pushed = new ArrayList<>();
        for (Filter filter : filters) {
            try {
                predicates.add(converter.convert(filter));
                pushed.add(filter);
            } catch (UnsupportedOperationException ignore) {
            }
        }
        this.predicates = predicates;
        this.pushedFilters = pushed.toArray(new Filter[0]);
        return filters;
    }

    @Override
    public Filter[] pushedFilters() {
        return pushedFilters;
    }

    @Override
    public void pruneColumns(StructType requiredSchema) {
        String[] pruneFields = requiredSchema.fieldNames();
        List<String> fieldNames = table.rowType().getFieldNames();
        int[] projected = new int[pruneFields.length];
        for (int i = 0; i < projected.length; i++) {
            projected[i] = fieldNames.indexOf(pruneFields[i]);
        }
        this.projectedFields = projected;
    }

    @Override
    public Statistics estimateStatistics() {
        long rowCount = 0L;

        for (Split split : splits()) {
            rowCount += split.rowCount();
        }

        final long numRows = rowCount;
        final long sizeInBytes = readSchema().defaultSize() * numRows;

        return new Statistics() {
            @Override
            public OptionalLong sizeInBytes() {
                return OptionalLong.of(sizeInBytes);
            }

            @Override
            public OptionalLong numRows() {
                return OptionalLong.of(numRows);
            }
        };
    }

    @Override
    public StructType readSchema() {
        RowType rowType = table.rowType();
        return SparkTypeUtils.fromPaimonRowType(
                projectedFields == null ? rowType : TypeUtils.project(rowType, projectedFields));
    }

    private ReadBuilder readBuilder() {
        return table.newReadBuilder().withFilter(predicates).withProjection(projectedFields);
    }

    @Override
    public List<InputPartition<InternalRow>> planInputPartitions() {
        return splits().stream()
                .map(split -> new SparkInputPartition(readBuilder(), split))
                .collect(Collectors.toList());
    }

    protected List<Split> splits() {
        if (splits == null) {
            this.splits = readBuilder().newScan().plan().splits();
        }
        return splits;
    }
}
