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

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;
import java.util.Set;

/** A read-only Fluss LakeStream table used by the {@code $rt} suffix. */
public final class FlussLakeStreamReadTable implements Table, SupportsRead {

    private final Table flussTable;

    public FlussLakeStreamReadTable(Table flussTable) {
        if (!(flussTable instanceof SupportsRead)) {
            throw new IllegalArgumentException(
                    String.format("Fluss table '%s' does not support reads.", flussTable.name()));
        }
        this.flussTable = flussTable;
    }

    @Override
    public String name() {
        return flussTable.name();
    }

    @Override
    public StructType schema() {
        return flussTable.schema();
    }

    @Override
    public Transform[] partitioning() {
        return flussTable.partitioning();
    }

    @Override
    public Map<String, String> properties() {
        return flussTable.properties();
    }

    @Override
    public Set<TableCapability> capabilities() {
        return FlussLakeStreamTable.readCapabilities(flussTable);
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return ((SupportsRead) flussTable).newScanBuilder(options);
    }
}
