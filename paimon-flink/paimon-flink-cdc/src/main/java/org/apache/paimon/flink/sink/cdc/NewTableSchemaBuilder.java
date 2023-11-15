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

import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataType;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.columnDuplicateErrMsg;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.listCaseConvert;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.mapKeyCaseConvert;

/** Build schema for new table found in database synchronization. */
public class NewTableSchemaBuilder implements Serializable {

    private final Map<String, String> tableConfig;
    private final boolean caseSensitive;

    public NewTableSchemaBuilder(Map<String, String> tableConfig, boolean caseSensitive) {
        this.tableConfig = tableConfig;
        this.caseSensitive = caseSensitive;
    }

    public Optional<Schema> build(RichCdcMultiplexRecord record) {
        Schema.Builder builder = Schema.newBuilder();
        builder.options(tableConfig);

        String tableName = record.tableName();
        tableName = tableName == null ? "UNKNOWN" : tableName;
        LinkedHashMap<String, DataType> fieldTypes =
                mapKeyCaseConvert(
                        record.fieldTypes(), caseSensitive, columnDuplicateErrMsg(tableName));

        for (Map.Entry<String, DataType> entry : fieldTypes.entrySet()) {
            builder.column(entry.getKey(), entry.getValue());
        }

        builder.primaryKey(listCaseConvert(record.primaryKeys(), caseSensitive));

        return Optional.of(builder.build());
    }
}
