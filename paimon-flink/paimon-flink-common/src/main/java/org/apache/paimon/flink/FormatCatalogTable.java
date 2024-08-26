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

import org.apache.paimon.table.FormatTable;

import org.apache.flink.connector.file.table.FileSystemTableFactory;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.PATH;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;
import static org.apache.paimon.flink.LogicalTypeConversion.toLogicalType;

/** A {@link CatalogTable} to represent format table. */
public class FormatCatalogTable implements CatalogTable {

    private final FormatTable table;

    private Map<String, String> cachedOptions;

    public FormatCatalogTable(FormatTable table) {
        this.table = table;
    }

    public FormatTable table() {
        return table;
    }

    @Override
    public Schema getUnresolvedSchema() {
        return Schema.newBuilder()
                .fromRowDataType(fromLogicalToDataType(toLogicalType(table.rowType())))
                .build();
    }

    @Override
    public boolean isPartitioned() {
        return !table.partitionKeys().isEmpty();
    }

    @Override
    public List<String> getPartitionKeys() {
        return table.partitionKeys();
    }

    @Override
    public CatalogTable copy(Map<String, String> map) {
        return new FormatCatalogTable(table.copy(map));
    }

    @Override
    public Map<String, String> getOptions() {
        if (cachedOptions == null) {
            cachedOptions = new HashMap<>();
            FileSystemTableFactory fileSystemFactory = new FileSystemTableFactory();
            Set<String> validOptions = new HashSet<>();
            fileSystemFactory.requiredOptions().forEach(o -> validOptions.add(o.key()));
            fileSystemFactory.optionalOptions().forEach(o -> validOptions.add(o.key()));
            String format = table.format().name().toLowerCase();
            table.options()
                    .forEach(
                            (k, v) -> {
                                if (validOptions.contains(k) || k.startsWith(format + ".")) {
                                    cachedOptions.put(k, v);
                                }
                            });
            cachedOptions.put(CONNECTOR.key(), "filesystem");
            cachedOptions.put(PATH.key(), table.location());
            cachedOptions.put(FORMAT.key(), format);
        }
        return cachedOptions;
    }

    @Override
    public String getComment() {
        return table.comment().orElse("");
    }

    @Override
    public CatalogTable copy() {
        return copy(Collections.emptyMap());
    }

    @Override
    public Optional<String> getDescription() {
        return table.comment();
    }

    @Override
    public Optional<String> getDetailedDescription() {
        return getDescription();
    }

    public DynamicTableSource createTableSource(DynamicTableFactory.Context context) {
        return FactoryUtil.createDynamicTableSource(
                null,
                context.getObjectIdentifier(),
                context.getCatalogTable(),
                context.getConfiguration(),
                context.getClassLoader(),
                context.isTemporary());
    }

    public DynamicTableSink createTableSink(DynamicTableFactory.Context context) {
        return FactoryUtil.createDynamicTableSink(
                null,
                context.getObjectIdentifier(),
                context.getCatalogTable(),
                context.getConfiguration(),
                context.getClassLoader(),
                context.isTemporary());
    }
}
