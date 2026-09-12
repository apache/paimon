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

import org.apache.spark.sql.connector.catalog.MetadataColumn;
import org.apache.spark.sql.connector.catalog.SupportsMetadataColumns;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/** A Fluss LakeStream table which reads from Paimon and writes to Fluss. */
public final class FlussLakeStreamTable
        implements Table, SupportsRead, SupportsWrite, SupportsMetadataColumns {

    private final SparkTable paimonTable;
    private final Table flussTable;

    public FlussLakeStreamTable(SparkTable paimonTable, Table flussTable) {
        if (!(flussTable instanceof SupportsWrite)) {
            throw new IllegalArgumentException(
                    String.format("Fluss table '%s' does not support writes.", flussTable.name()));
        }
        this.paimonTable = paimonTable;
        this.flussTable = flussTable;
    }

    @Override
    public String name() {
        return paimonTable.name();
    }

    @Override
    public StructType schema() {
        // Fluss owns the user-visible schema used to validate writes. The Paimon lake table may
        // contain a different physical layout.
        return flussTable.schema();
    }

    @Override
    public Transform[] partitioning() {
        return paimonTable.partitioning();
    }

    @Override
    public Map<String, String> properties() {
        return paimonTable.properties();
    }

    @Override
    public Set<TableCapability> capabilities() {
        Set<TableCapability> capabilities = readCapabilities(paimonTable);
        addIfSupported(capabilities, flussTable, TableCapability.BATCH_WRITE);
        addIfSupported(capabilities, flussTable, TableCapability.STREAMING_WRITE);
        return capabilities;
    }

    @Override
    public MetadataColumn[] metadataColumns() {
        return paimonTable.metadataColumns();
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return paimonTable.newScanBuilder(options);
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        return ((SupportsWrite) flussTable).newWriteBuilder(info);
    }

    static Set<TableCapability> readCapabilities(Table table) {
        Set<TableCapability> capabilities = EnumSet.noneOf(TableCapability.class);
        for (TableCapability capability : table.capabilities()) {
            if (capability.name().endsWith("_READ")) {
                capabilities.add(capability);
            }
        }
        return capabilities;
    }

    private static void addIfSupported(
            Set<TableCapability> target, Table source, TableCapability capability) {
        if (source.capabilities().contains(capability)) {
            target.add(capability);
        }
    }
}
