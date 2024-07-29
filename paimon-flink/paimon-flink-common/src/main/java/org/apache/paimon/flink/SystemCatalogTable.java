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

package org.apache.paimon.flink;

import org.apache.paimon.table.Table;
import org.apache.paimon.table.system.AuditLogTable;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.WatermarkSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.types.utils.TypeConversions;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.paimon.flink.LogicalTypeConversion.toLogicalType;
import static org.apache.paimon.flink.utils.FlinkCatalogPropertiesUtil.compoundKey;
import static org.apache.paimon.flink.utils.FlinkCatalogPropertiesUtil.deserializeWatermarkSpec;

/** A {@link CatalogTable} to represent system table. */
public class SystemCatalogTable implements CatalogTable {

    private final Table table;

    public SystemCatalogTable(Table table) {
        this.table = table;
    }

    public Table table() {
        return table;
    }

    @Override
    public Schema getUnresolvedSchema() {
        Schema.Builder builder = Schema.newBuilder();
        builder.fromRowDataType(
                TypeConversions.fromLogicalToDataType(toLogicalType(table.rowType())));
        if (table instanceof AuditLogTable) {
            Map<String, String> newOptions = new HashMap<>(table.options());
            if (newOptions.keySet().stream()
                    .anyMatch(key -> key.startsWith(compoundKey(SCHEMA, WATERMARK)))) {
                WatermarkSpec watermarkSpec = deserializeWatermarkSpec(newOptions);
                return builder.watermark(
                                watermarkSpec.getRowtimeAttribute(),
                                watermarkSpec.getWatermarkExpr())
                        .build();
            }
        }
        return builder.build();
    }

    @Override
    public boolean isPartitioned() {
        return false;
    }

    @Override
    public List<String> getPartitionKeys() {
        return Collections.emptyList();
    }

    @Override
    public CatalogTable copy(Map<String, String> map) {
        return new SystemCatalogTable(table.copy(map));
    }

    @Override
    public Map<String, String> getOptions() {
        return Collections.emptyMap();
    }

    @Override
    public String getComment() {
        return "";
    }

    @Override
    public CatalogTable copy() {
        return copy(Collections.emptyMap());
    }

    @Override
    public Optional<String> getDescription() {
        return Optional.empty();
    }

    @Override
    public Optional<String> getDetailedDescription() {
        return Optional.empty();
    }
}
