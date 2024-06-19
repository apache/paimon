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

import org.apache.paimon.flink.action.cdc.CdcMetadataConverter;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataField;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.checkDuplicateFields;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.listCaseConvert;
import static org.apache.paimon.utils.StringUtils.caseSensitiveConversion;

/** Build schema for new table found in database synchronization. */
public class NewTableSchemaBuilder implements Serializable {

    private final Map<String, String> tableConfig;
    private final boolean caseSensitive;
    private final CdcMetadataConverter[] metadataConverters;

    public NewTableSchemaBuilder(
            Map<String, String> tableConfig,
            boolean caseSensitive,
            CdcMetadataConverter[] metadataConverters) {
        this.tableConfig = tableConfig;
        this.caseSensitive = caseSensitive;
        this.metadataConverters = metadataConverters;
    }

    public Optional<Schema> build(RichCdcMultiplexRecord record) {
        Schema.Builder builder = Schema.newBuilder();
        builder.options(tableConfig);

        // fields
        List<String> allFieldNames = new ArrayList<>();

        for (DataField dataField : record.fields()) {
            String fieldName = caseSensitiveConversion(dataField.name(), caseSensitive);
            allFieldNames.add(fieldName);
            builder.column(fieldName, dataField.type(), dataField.description());
        }

        for (CdcMetadataConverter metadataConverter : metadataConverters) {
            String metadataColumnName =
                    caseSensitiveConversion(metadataConverter.columnName(), caseSensitive);
            allFieldNames.add(metadataColumnName);
            builder.column(metadataColumnName, metadataConverter.dataType());
        }

        checkDuplicateFields(record.tableName(), allFieldNames);

        builder.primaryKey(listCaseConvert(record.primaryKeys(), caseSensitive));

        return Optional.of(builder.build());
    }
}
