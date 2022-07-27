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

import org.apache.flink.table.store.table.FileStoreTable;

import org.apache.spark.sql.catalog.Table;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.IdentityTransform;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.HashSet;
import java.util.Set;

/** A spark {@link Table} for table store. */
public class SparkTable implements org.apache.spark.sql.connector.catalog.Table, SupportsRead {

    private final FileStoreTable table;

    public SparkTable(FileStoreTable table) {
        this.table = table;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        // options is already merged into table
        return new SparkScanBuilder(table);
    }

    @Override
    public String name() {
        return table.location().getName();
    }

    @Override
    public StructType schema() {
        return SparkTypeUtils.fromFlinkRowType(table.schema().logicalRowType());
    }

    @Override
    public Set<TableCapability> capabilities() {
        Set<TableCapability> capabilities = new HashSet<>();
        capabilities.add(TableCapability.BATCH_READ);
        return capabilities;
    }

    @Override
    public Transform[] partitioning() {
        return table.schema().partitionKeys().stream()
                .map(FieldReference::apply)
                .map(IdentityTransform::apply)
                .toArray(Transform[]::new);
    }
}
