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

package org.apache.paimon.flink.pulsar;

import org.apache.paimon.CoreOptions.LogChangelogMode;
import org.apache.paimon.CoreOptions.LogConsistency;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.log.LogStoreTableFactory;
import org.apache.paimon.table.sink.SinkRecord;
import org.apache.paimon.types.RowKind;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.paimon.CoreOptions.LOG_CHANGELOG_MODE;
import static org.apache.paimon.CoreOptions.LOG_CONSISTENCY;
import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.apache.paimon.mergetree.compact.MergeTreeCompactManagerTest.row;

/**
 * Utils for the test of {@link PulsarLogStoreFactory}.
 */
public class PulsarLogTestUtils {

    public static final DynamicTableSource.Context SOURCE_CONTEXT =
            new DynamicTableSource.Context() {
                @Override
                public <T> TypeInformation<T> createTypeInformation(DataType producedDataType) {
                    return createTypeInformation(
                            TypeConversions.fromDataToLogicalType(producedDataType));
                }

                @Override
                public <T> TypeInformation<T> createTypeInformation(
                        LogicalType producedLogicalType) {
                    return InternalTypeInfo.of(producedLogicalType);
                }

                @Override
                public DynamicTableSource.DataStructureConverter createDataStructureConverter(
                        DataType producedDataType) {
                    return ScanRuntimeProviderContext.INSTANCE.createDataStructureConverter(
                            producedDataType);
                }
            };

    public static final DynamicTableSink.Context SINK_CONTEXT =
            new DynamicTableSink.Context() {

                @Override
                public boolean isBounded() {
                    return false;
                }

                @Override
                public <T> TypeInformation<T> createTypeInformation(DataType producedDataType) {
                    return createTypeInformation(
                            TypeConversions.fromDataToLogicalType(producedDataType));
                }

                @Override
                public <T> TypeInformation<T> createTypeInformation(
                        LogicalType producedLogicalType) {
                    return InternalTypeInfo.of(producedLogicalType);
                }

                @Override
                public DynamicTableSink.DataStructureConverter createDataStructureConverter(
                        DataType producedDataType) {
                    return new SinkRuntimeProviderContext(isBounded())
                            .createDataStructureConverter(producedDataType);
                }
            };

    public static PulsarLogStoreFactory discoverPulsarLogFactory() {
        return (PulsarLogStoreFactory)
                LogStoreTableFactory.discoverLogStoreFactory(
                        Thread.currentThread().getContextClassLoader(),
                        PulsarLogStoreFactory.IDENTIFIER);
    }

    private static DynamicTableFactory.Context createContext(
            String name, RowType rowType, int[] pk, Map<String, String> options) {
        return new FactoryUtil.DefaultDynamicTableContext(
                ObjectIdentifier.of("catalog", "database", name),
                PulsarLogTestUtils.createResolvedTable(options, rowType, pk),
                Collections.emptyMap(),
                new Configuration(),
                Thread.currentThread().getContextClassLoader(),
                false);
    }

    static ResolvedCatalogTable createResolvedTable(
            Map<String, String> options, RowType rowType, int[] pk) {
        List<String> fieldNames = rowType.getFieldNames();
        List<DataType> fieldDataTypes =
                rowType.getChildren().stream()
                        .map(TypeConversions::fromLogicalToDataType)
                        .collect(Collectors.toList());
        CatalogTable origin =
                CatalogTable.of(
                        Schema.newBuilder().fromFields(fieldNames, fieldDataTypes).build(),
                        null,
                        Collections.emptyList(),
                        options);
        List<Column> resolvedColumns =
                IntStream.range(0, fieldNames.size())
                        .mapToObj(i -> Column.physical(fieldNames.get(i), fieldDataTypes.get(i)))
                        .collect(Collectors.toList());
        UniqueConstraint constraint = null;
        if (pk.length > 0) {
            List<String> pkNames =
                    Arrays.stream(pk).mapToObj(fieldNames::get).collect(Collectors.toList());
            constraint = UniqueConstraint.primaryKey("pk", pkNames);
        }
        return new ResolvedCatalogTable(
                origin, new ResolvedSchema(resolvedColumns, Collections.emptyList(), constraint));
    }

    static DynamicTableFactory.Context testContext(
            String serviceUrl, LogChangelogMode changelogMode, boolean keyed) {
        return testContext("table", serviceUrl, changelogMode, LogConsistency.TRANSACTIONAL, keyed);
    }

    static DynamicTableFactory.Context testContext(
            String name,
            String serviceUrl,
            LogChangelogMode changelogMode,
            LogConsistency consistency,
            boolean keyed) {
        return testContext(
                name,
                serviceUrl,
                changelogMode,
                consistency,
                RowType.of(new IntType(), new IntType()),
                keyed ? new int[]{0} : new int[0]);
    }

    public static DynamicTableFactory.Context testContext(
            String name,
            String serviceUrl,
            LogChangelogMode changelogMode,
            LogConsistency consistency,
            RowType type,
            int[] keys) {
        Map<String, String> options = new HashMap<>();
        options.put(LOG_CHANGELOG_MODE.key(), changelogMode.toString());
        options.put(LOG_CONSISTENCY.key(), consistency.toString());
        options.put(PulsarLogOptions.PULSAR_SERVICE_URL.key(), serviceUrl);
        options.put(PulsarLogOptions.TOPIC.key(), UUID.randomUUID().toString());
        return createContext(name, type, keys, options);
    }

    static SinkRecord testRecord(boolean hasPk, int bucket, int pk, int value, RowKind rowKind) {
        return new SinkRecord(
                EMPTY_ROW,
                bucket,
                hasPk ? row(pk) : EMPTY_ROW,
                GenericRow.ofKind(rowKind, pk, value));
    }
}
