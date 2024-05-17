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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.spark.schema.PaimonMetadataColumn$;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

import org.apache.spark.sql.connector.catalog.MetadataColumn;
import org.apache.spark.sql.connector.catalog.SupportsMetadataColumns;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.IdentityTransform;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** A spark {@link org.apache.spark.sql.connector.catalog.Table} for paimon. */
public class SparkTable
        implements org.apache.spark.sql.connector.catalog.Table,
                SupportsRead,
                SupportsWrite,
                SupportsMetadataColumns,
                PaimonPartitionManagement {

    private final Table table;

    public SparkTable(Table table) {
        this.table = table;
    }

    public Table getTable() {
        return table;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        Table newTable = table.copy(options.asCaseSensitiveMap());
        return new PaimonScanBuilder(newTable);
    }

    @Override
    public String name() {
        return table.name();
    }

    @Override
    public StructType schema() {
        return SparkTypeUtils.fromPaimonRowType(table.rowType());
    }

    @Override
    public Set<TableCapability> capabilities() {
        Set<TableCapability> capabilities = new HashSet<>();
        capabilities.add(TableCapability.BATCH_READ);
        capabilities.add(TableCapability.V1_BATCH_WRITE);
        capabilities.add(TableCapability.OVERWRITE_BY_FILTER);
        capabilities.add(TableCapability.OVERWRITE_DYNAMIC);
        capabilities.add(TableCapability.MICRO_BATCH_READ);
        return capabilities;
    }

    @Override
    public Transform[] partitioning() {
        return table.partitionKeys().stream()
                .map(FieldReference::apply)
                .map(IdentityTransform::apply)
                .toArray(Transform[]::new);
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        try {
            return new SparkWriteBuilder((FileStoreTable) table, Options.fromMap(info.options()));
        } catch (Exception e) {
            throw new RuntimeException("Only FileStoreTable can be written.");
        }
    }

    @Override
    public Map<String, String> properties() {
        if (table instanceof DataTable) {
            Map<String, String> properties =
                    new HashMap<>(((DataTable) table).coreOptions().toMap());
            if (!table.primaryKeys().isEmpty()) {
                properties.put(
                        CoreOptions.PRIMARY_KEY.key(), String.join(",", table.primaryKeys()));
            }
            properties.put(TableCatalog.PROP_PROVIDER, SparkSource.NAME());
            if (table.comment().isPresent()) {
                properties.put(TableCatalog.PROP_COMMENT, table.comment().get());
            }
            return properties;
        } else {
            return Collections.emptyMap();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SparkTable that = (SparkTable) o;
        return Objects.equals(table, that.table);
    }

    @Override
    public MetadataColumn[] metadataColumns() {
        return new MetadataColumn[] {
            PaimonMetadataColumn$.MODULE$.FILE_PATH(), PaimonMetadataColumn$.MODULE$.ROW_INDEX()
        };
    }
}
