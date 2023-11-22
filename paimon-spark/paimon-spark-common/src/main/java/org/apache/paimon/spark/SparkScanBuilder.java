/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark;

import org.apache.paimon.predicate.PartitionPredicateVisitor;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.Table;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/** A Spark {@link ScanBuilder} for paimon. */
public class SparkScanBuilder
        implements ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns {
    private static final Logger LOG = LoggerFactory.getLogger(SparkScanBuilder.class);
    private final Table table;

    private List<Predicate> predicates = new ArrayList<>();
    private Filter[] pushedFilters;
    private int[] projectedFields;

    public SparkScanBuilder(Table table) {
        this.table = table;
    }

    @Override
    public Filter[] pushFilters(Filter[] filters) {
        // There are 3 kinds of filters:
        // (1) pushable filters which don't need to be evaluated again after scanning, e.g. filter
        // partitions.
        // (2) pushable filters which still need to be evaluated after scanning.
        // (3) non-pushable filters.
        // case 1 and 2 are considered as pushable filters and will be returned by pushedFilters().
        // case 2 and 3 are considered as postScan filters and should be return by this method.
        List<Filter> pushable = new ArrayList<>(filters.length);
        List<Filter> postScan = new ArrayList<>(filters.length);
        List<Predicate> predicates = new ArrayList<>(filters.length);

        SparkFilterConverter converter = new SparkFilterConverter(table.rowType());
        PartitionPredicateVisitor visitor = new PartitionPredicateVisitor(table.partitionKeys());
        for (Filter filter : filters) {
            try {
                Predicate predicate = converter.convert(filter);
                predicates.add(predicate);
                pushable.add(filter);
                if (!predicate.visit(visitor)) {
                    postScan.add(filter);
                }
            } catch (UnsupportedOperationException e) {
                LOG.warn(e.getMessage());
                postScan.add(filter);
            }
        }
        this.predicates = predicates;
        this.pushedFilters = pushable.toArray(new Filter[0]);
        return postScan.toArray(new Filter[0]);
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
    public Scan build() {
        return new SparkScan(
                table,
                table.newReadBuilder().withFilter(predicates).withProjection(projectedFields));
    }
}
