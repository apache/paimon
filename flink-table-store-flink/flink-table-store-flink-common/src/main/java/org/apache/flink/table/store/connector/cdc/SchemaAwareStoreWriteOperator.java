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

package org.apache.flink.table.store.connector.cdc;

import org.apache.flink.table.store.cdc.Record;
import org.apache.flink.table.store.connector.sink.AbstractStoreWriteOperator;
import org.apache.flink.table.store.connector.sink.StoreSinkWrite;
import org.apache.flink.table.store.data.GenericRow;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.options.ConfigOption;
import org.apache.flink.table.store.options.ConfigOptions;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.sink.LogSinkFunction;
import org.apache.flink.table.store.table.sink.SinkRecord;
import org.apache.flink.table.store.types.DataType;
import org.apache.flink.table.store.utils.TypeUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;

/**
 * An {@link AbstractStoreWriteOperator} which is aware of schema changes.
 *
 * <p>When the input {@link Record} contains a field name not in the current {@link TableSchema}, it
 * periodically queries the latest schema, until the latest schema contains that field name.
 */
public class SchemaAwareStoreWriteOperator extends AbstractStoreWriteOperator<Record> {

    static final ConfigOption<Duration> SCHEMA_UPDATE_WAIT_TIME =
            ConfigOptions.key("cdc.schema-update-wait-time")
                    .durationType()
                    .defaultValue(Duration.ofMillis(500));

    private final long schemaUpdateWaitMillis;

    public SchemaAwareStoreWriteOperator(
            FileStoreTable table,
            @Nullable LogSinkFunction logSinkFunction,
            StoreSinkWrite.Provider storeSinkWriteProvider) {
        super(table, logSinkFunction, storeSinkWriteProvider);
        schemaUpdateWaitMillis =
                table.options().toConfiguration().get(SCHEMA_UPDATE_WAIT_TIME).toMillis();
    }

    @Override
    protected SinkRecord processRecord(Record record) throws Exception {
        Map<String, String> fields = record.fields();

        if (!schemaMatched(fields)) {
            while (true) {
                table = table.copyWithLatestSchema();
                if (schemaMatched(fields)) {
                    break;
                }
                Thread.sleep(schemaUpdateWaitMillis);
            }
            write.replace(commitUser -> table.newWrite(commitUser));
        }

        TableSchema schema = table.schema();
        GenericRow row = new GenericRow(schema.fields().size());
        for (Map.Entry<String, String> field : fields.entrySet()) {
            String key = field.getKey();
            String value = field.getValue();
            int idx = schema.fieldNames().indexOf(key);
            DataType type = schema.fields().get(idx).type();
            row.setField(idx, TypeUtils.castFromString(value, type));
        }

        try {
            return write.write(row);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private boolean schemaMatched(Map<String, String> fields) {
        TableSchema currentSchema = table.schema();
        return currentSchema.fieldNames().containsAll(fields.keySet());
    }
}
