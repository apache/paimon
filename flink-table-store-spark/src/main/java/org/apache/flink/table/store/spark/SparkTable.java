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

package org.apache.flink.table.store.spark;

import org.apache.flink.table.store.file.operation.Lock;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.table.SupportsPartition;
import org.apache.flink.table.store.table.Table;

import org.apache.spark.sql.connector.catalog.SupportsDelete;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.IdentityTransform;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/** A spark {@link org.apache.spark.sql.connector.catalog.Table} for table store. */
public class SparkTable
        implements org.apache.spark.sql.connector.catalog.Table,
                SupportsRead,
                SupportsWrite,
                SupportsDelete {

    private final Table table;
    private final Lock.Factory lockFactory;

    public SparkTable(Table table, Lock.Factory lockFactory) {
        this.table = table;
        this.lockFactory = lockFactory;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        // options is already merged into table
        return new SparkScanBuilder(table);
    }

    @Override
    public String name() {
        return table.name();
    }

    @Override
    public StructType schema() {
        return SparkTypeUtils.fromFlinkRowType(table.rowType());
    }

    @Override
    public Set<TableCapability> capabilities() {
        Set<TableCapability> capabilities = new HashSet<>();
        capabilities.add(TableCapability.BATCH_READ);
        capabilities.add(TableCapability.V1_BATCH_WRITE);
        return capabilities;
    }

    @Override
    public Transform[] partitioning() {
        if (table instanceof SupportsPartition) {
            return ((SupportsPartition) table)
                    .partitionKeys().stream()
                            .map(FieldReference::apply)
                            .map(IdentityTransform::apply)
                            .toArray(Transform[]::new);
        }
        return new Transform[0];
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        return new SparkWriteBuilder(castToWritable(table), info.queryId(), lockFactory);
    }

    @Override
    public void deleteWhere(Filter[] filters) {
        SparkFilterConverter converter = new SparkFilterConverter(table.rowType());
        List<Predicate> predicates = new ArrayList<>();
        for (Filter filter : filters) {
            if ("AlwaysTrue()".equals(filter.toString())) {
                continue;
            }

            predicates.add(converter.convert(filter));
        }

        String commitUser = UUID.randomUUID().toString();
        castToWritable(table).deleteWhere(commitUser, predicates, lockFactory);
    }

    private static org.apache.flink.table.store.table.SupportsWrite castToWritable(Table table) {
        if (!(table instanceof org.apache.flink.table.store.table.SupportsWrite)) {
            throw new UnsupportedOperationException(
                    "Unsupported table for writing: " + table.getClass());
        }

        return (org.apache.flink.table.store.table.SupportsWrite) table;
    }
}
