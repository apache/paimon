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

import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.Table;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/** A Spark {@link ScanBuilder} for paimon. */
public class SparkScanBuilder
        implements ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns {

    private final Table table;

    private List<Predicate> predicates = new ArrayList<>();
    private Filter[] pushedFilters;
    private int[] projectedFields;

    public SparkScanBuilder(Table table) {
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
    public Scan build() {
        return new SparkScan(
                table,
                table.newReadBuilder().withFilter(predicates).withProjection(projectedFields));
    }
}
