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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.cdc.CdcRecord;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.sink.AbstractStoreWriteOperator;
import org.apache.paimon.flink.sink.LogSinkFunction;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.SinkRecord;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.TypeUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * An {@link AbstractStoreWriteOperator} which is aware of schema changes.
 *
 * <p>When the input {@link CdcRecord} contains a field name not in the current {@link TableSchema},
 * it periodically queries the latest schema, until the latest schema contains that field name.
 */
public class SchemaAwareStoreWriteOperator extends AbstractStoreWriteOperator<CdcRecord> {

    static final ConfigOption<Duration> RETRY_SLEEP_TIME =
            ConfigOptions.key("cdc.retry-sleep-time")
                    .durationType()
                    .defaultValue(Duration.ofMillis(500));

    private final long retrySleepMillis;

    public SchemaAwareStoreWriteOperator(
            FileStoreTable table,
            @Nullable LogSinkFunction logSinkFunction,
            StoreSinkWrite.Provider storeSinkWriteProvider) {
        super(table, logSinkFunction, storeSinkWriteProvider);
        retrySleepMillis = table.coreOptions().toConfiguration().get(RETRY_SLEEP_TIME).toMillis();
    }

    @Override
    protected SinkRecord processRecord(CdcRecord record) throws Exception {
        Map<String, Object> convertedFields = tryConvert(record.fields());
        if (convertedFields == null) {
            while (true) {
                table = table.copyWithLatestSchema();
                convertedFields = tryConvert(record.fields());
                if (convertedFields != null) {
                    break;
                }
                Thread.sleep(retrySleepMillis);
            }
            write.replace(commitUser -> table.newWrite(commitUser));
        }

        TableSchema schema = table.schema();
        GenericRow row = new GenericRow(schema.fields().size());
        row.setRowKind(record.kind());
        for (Map.Entry<String, Object> convertedField : convertedFields.entrySet()) {
            String key = convertedField.getKey();
            Object value = convertedField.getValue();
            int idx = schema.fieldNames().indexOf(key);
            row.setField(idx, value);
        }

        try {
            return write.write(row);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private Map<String, Object> tryConvert(Map<String, String> fields) {
        Map<String, Object> converted = new HashMap<>();
        TableSchema schema = table.schema();
        for (Map.Entry<String, String> field : fields.entrySet()) {
            String key = field.getKey();
            String value = field.getValue();
            int idx = schema.fieldNames().indexOf(key);
            if (idx < 0) {
                return null;
            }
            DataType type = schema.fields().get(idx).type();
            // TODO TypeUtils.castFromString cannot deal with complex types like arrays and maps.
            //  Change type of CdcRecord#field if needed.
            try {
                converted.put(key, TypeUtils.castFromString(value, type));
            } catch (Exception e) {
                LOG.debug("Failed to convert value " + value + " to type " + type, e);
                return null;
            }
        }
        return converted;
    }
}
