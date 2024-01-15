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

package org.apache.paimon.flink.kafka;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.flink.factories.FlinkFactoryUtil.FlinkTableFactoryHelper;
import org.apache.paimon.flink.log.LogStoreRegister;
import org.apache.paimon.flink.log.LogStoreTableFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.DateTimeUtils;

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

import javax.annotation.Nullable;

import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;
import static org.apache.paimon.CoreOptions.LOG_CHANGELOG_MODE;
import static org.apache.paimon.CoreOptions.LOG_CONSISTENCY;
import static org.apache.paimon.CoreOptions.LogConsistency;
import static org.apache.paimon.CoreOptions.SCAN_TIMESTAMP;
import static org.apache.paimon.CoreOptions.SCAN_TIMESTAMP_MILLIS;
import static org.apache.paimon.flink.factories.FlinkFactoryUtil.createFlinkTableFactoryHelper;
import static org.apache.paimon.flink.kafka.KafkaLogOptions.TOPIC;
import static org.apache.paimon.options.OptionsUtils.convertToPropertiesPrefixKey;

/** The Kafka {@link LogStoreTableFactory} implementation. */
public class KafkaLogStoreFactory implements LogStoreTableFactory {

    public static final String IDENTIFIER = "kafka";

    public static final String KAFKA_PREFIX = IDENTIFIER + ".";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    private String topic(Context context) {
        return context.getCatalogTable().getOptions().get(TOPIC.key());
    }

    @Override
    public KafkaLogSourceProvider createSourceProvider(
            Context context,
            DynamicTableSource.Context sourceContext,
            @Nullable int[][] projectFields) {
        FlinkTableFactoryHelper helper = createFlinkTableFactoryHelper(this, context);
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
        Long timestampMills = options.get(SCAN_TIMESTAMP_MILLIS);
        String timestampString = options.get(SCAN_TIMESTAMP);

        if (timestampMills == null && timestampString != null) {
            timestampMills =
                    DateTimeUtils.parseTimestampData(timestampString, 3, TimeZone.getDefault())
                            .getMillisecond();
        }
        return new KafkaLogSourceProvider(
                topic(context),
                toKafkaProperties(options),
                physicalType,
                primaryKey,
                primaryKeyDeserializer,
                valueDeserializer,
                projectFields,
                options.get(LOG_CONSISTENCY),
                // TODO visit all options through CoreOptions
                CoreOptions.startupMode(options),
                timestampMills);
    }

    @Override
    public KafkaLogSinkProvider createSinkProvider(
            Context context, DynamicTableSink.Context sinkContext) {
        FlinkTableFactoryHelper helper = createFlinkTableFactoryHelper(this, context);
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
        return new KafkaLogSinkProvider(
                topic(context),
                toKafkaProperties(options),
                primaryKeySerializer,
                valueSerializer,
                options.get(LOG_CONSISTENCY),
                options.get(LOG_CHANGELOG_MODE));
    }

    @Override
    public LogStoreRegister createRegister(RegisterContext context) {
        return new KafkaLogStoreRegister(context);
    }

    private int[] getPrimaryKeyIndexes(ResolvedSchema schema) {
        final List<String> columns = schema.getColumnNames();
        return schema.getPrimaryKey()
                .map(UniqueConstraint::getColumns)
                .map(pkColumns -> pkColumns.stream().mapToInt(columns::indexOf).toArray())
                .orElseGet(() -> new int[] {});
    }

    public static Properties toKafkaProperties(Options options) {
        Properties properties = new Properties();
        properties.putAll(convertToPropertiesPrefixKey(options.toMap(), KAFKA_PREFIX));

        // Add read committed for transactional consistency mode.
        if (options.get(LOG_CONSISTENCY) == LogConsistency.TRANSACTIONAL) {
            properties.setProperty(ISOLATION_LEVEL_CONFIG, "read_committed");
        }
        return properties;
    }

    private Options toOptions(ReadableConfig config) {
        Options options = new Options();
        ((Configuration) config).toMap().forEach(options::setString);
        return options;
    }
}
