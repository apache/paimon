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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory.Context;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.flink.factories.FlinkFactoryUtil;
import org.apache.paimon.flink.log.LogStoreTableFactory;
import org.apache.paimon.options.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.paimon.CoreOptions.LOG_CHANGELOG_MODE;
import static org.apache.paimon.CoreOptions.LOG_CONSISTENCY;
import static org.apache.paimon.CoreOptions.SCAN_TIMESTAMP_MILLIS;
import static org.apache.paimon.flink.factories.FlinkFactoryUtil.createFlinkTableFactoryHelper;
import static org.apache.paimon.flink.pulsar.PulsarLogOptions.PULSAR_ADMIN_URL;
import static org.apache.paimon.flink.pulsar.PulsarLogOptions.PULSAR_KEY_UPPER_SPLIT;
import static org.apache.paimon.flink.pulsar.PulsarLogOptions.PULSAR_SERVICE_URL;
import static org.apache.paimon.flink.pulsar.PulsarLogOptions.TOPIC;

/**
 * The Pulsar {@link LogStoreTableFactory} implementation.
 */
public class PulsarLogStoreFactory implements LogStoreTableFactory {
    private static final Logger LOG = LoggerFactory.getLogger(org.apache.paimon.flink.pulsar.PulsarLogStoreFactory.class);

    public static final String IDENTIFIER = "pulsar";

    public static final String PULSAR_PREFIX = IDENTIFIER + ".";

    public static Properties toPulsarProperties(Options options) {
        Properties properties = new Properties();
        Map<String, String> optionMap = options.toMap();
        optionMap.keySet().stream()
                .filter(key -> key.startsWith(PULSAR_PREFIX))
                .forEach(
                        key ->
                                properties.put(
                                        changeKeyStyle(key),
                                        optionMap.get(key)));

        // Pulsar read committed for transactional consistency mode is not supported.
        return properties;
    }

    public static String changeKeyStyle(String key) {
        if (!key.contains(PULSAR_KEY_UPPER_SPLIT)) {
            return key;
        }
        StringBuilder newKeyBuilder = new StringBuilder(key);
        for (int i = 0; i < newKeyBuilder.length() - 1; i++) {
            if (newKeyBuilder.charAt(i) == '-' && Character.isLowerCase(newKeyBuilder.charAt(i + 1))) {
                char upperCase = Character.toUpperCase(newKeyBuilder.charAt(i + 1));
                newKeyBuilder.deleteCharAt(i + 1);
                newKeyBuilder.setCharAt(i, upperCase);
            }
        }
        return newKeyBuilder.toString();
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    private String topic(Context context) {
        return context.getCatalogTable().getOptions().get(TOPIC.key());
    }

    private String adminUrl(Context context) {
        return context.getCatalogTable().getOptions().get(PULSAR_ADMIN_URL.key());
    }

    private String serviceUrl(Context context) {
        return context.getCatalogTable().getOptions().get(PULSAR_SERVICE_URL.key());
    }

    @Override
    public PulsarLogSourceProvider createSourceProvider(
            Context context,
            DynamicTableSource.Context sourceContext,
            @Nullable int[][] projectFields) {
        FlinkFactoryUtil.FlinkTableFactoryHelper helper =
                createFlinkTableFactoryHelper(this, context);
        ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
        DataType physicalType = schema.toPhysicalRowDataType();
        DeserializationSchema<RowData> primaryKeyDeserializer = null;
        int[] primaryKey = getPrimaryKeyIndexes(schema);
        if (primaryKey.length > 0) {
            DataType keyType = DataTypeUtils.projectRow(physicalType, primaryKey);
            primaryKeyDeserializer =
                    LogStoreTableFactory.getKeyDecodingFormat(helper)
                            .createRuntimeDecoder(sourceContext, keyType);
        }
        DeserializationSchema<RowData> valueDeserializer =
                LogStoreTableFactory.getValueDecodingFormat(helper)
                        .createRuntimeDecoder(sourceContext, physicalType);
        Options options = toOptions(helper.getOptions());
        return new PulsarLogSourceProvider(
                topic(context),
                toPulsarProperties(options),
                physicalType,
                primaryKey,
                primaryKeyDeserializer,
                valueDeserializer,
                projectFields,
                options.get(LOG_CONSISTENCY),
                // TODO visit all options through CoreOptions
                CoreOptions.startupMode(options),
                options.get(SCAN_TIMESTAMP_MILLIS));
    }

    @Override
    public PulsarLogSinkProvider createSinkProvider(
            Context context, DynamicTableSink.Context sinkContext) {
        FlinkFactoryUtil.FlinkTableFactoryHelper helper =
                createFlinkTableFactoryHelper(this, context);
        ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
        DataType physicalType = schema.toPhysicalRowDataType();
        SerializationSchema<RowData> primaryKeySerializer = null;
        int[] primaryKey = getPrimaryKeyIndexes(schema);
        if (primaryKey.length > 0) {
            DataType keyType = DataTypeUtils.projectRow(physicalType, primaryKey);
            primaryKeySerializer =
                    LogStoreTableFactory.getKeyEncodingFormat(helper)
                            .createRuntimeEncoder(sinkContext, keyType);
        }
        SerializationSchema<RowData> valueSerializer =
                LogStoreTableFactory.getValueEncodingFormat(helper)
                        .createRuntimeEncoder(sinkContext, physicalType);
        Options options = toOptions(helper.getOptions());
        LOG.info("CatalogTable" + context.getCatalogTable().getOptions());
        return new PulsarLogSinkProvider(
                serviceUrl(context),
                adminUrl(context),
                topic(context),
                toPulsarProperties(options),
                primaryKeySerializer,
                valueSerializer,
                options.get(LOG_CONSISTENCY),
                options.get(LOG_CHANGELOG_MODE));
    }

    private int[] getPrimaryKeyIndexes(ResolvedSchema schema) {
        final List<String> columns = schema.getColumnNames();
        return schema.getPrimaryKey()
                .map(UniqueConstraint::getColumns)
                .map(pkColumns -> pkColumns.stream().mapToInt(columns::indexOf).toArray())
                .orElseGet(() -> new int[]{});
    }

    private Options toOptions(ReadableConfig config) {
        Options options = new Options();
        ((Configuration) config).toMap().forEach(options::setString);
        return options;
    }
}
