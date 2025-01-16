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

package org.apache.paimon.flink.action.cdc.format.debezium;

import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.mongodb.BsonValueConvertor;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.TypeUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.FIELD_PAYLOAD;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.FIELD_SCHEMA;
import static org.apache.paimon.utils.JsonSerdeUtil.writeValueAsString;

/**
 * The {@code DebeziumRecordParser} class extends the {@link DebeziumJsonRecordParser} and is
 * designed to parse records from MongoDB's BSON change data capture (CDC) format via Debezium.
 *
 * <p>The class supports The following features:
 *
 * <p>Convert bson string to java object from before/after field
 *
 * <p>Parse the schema from bson, all fields are string type, and the _id field is the primary key
 */
public class DebeziumBsonRecordParser extends DebeziumJsonRecordParser {

    private static final Logger LOG = LoggerFactory.getLogger(DebeziumBsonRecordParser.class);

    private static final String FIELD_COLLECTION = "collection";
    private static final String FIELD_OBJECT_ID = "_id";
    private static final List<String> PRIMARY_KEYS = Collections.singletonList(FIELD_OBJECT_ID);

    public DebeziumBsonRecordParser(TypeMapping typeMapping, List<ComputedColumn> computedColumns) {
        super(typeMapping, computedColumns);
    }

    @Override
    protected void setRoot(CdcSourceRecord record) {
        JsonNode node = (JsonNode) record.getValue();
        if (node.has(FIELD_SCHEMA)) {
            root = node.get(FIELD_PAYLOAD);
        } else {
            root = node;
        }
    }

    @Override
    protected Map<String, String> extractRowData(JsonNode record, RowType.Builder rowTypeBuilder) {
        // bson record should be a string
        Preconditions.checkArgument(
                record.isTextual(),
                "debezium bson record expected to be STRING type, but actual is %s",
                record.getNodeType());

        BsonDocument document = BsonDocument.parse(record.asText());
        LinkedHashMap<String, String> resultMap = new LinkedHashMap<>();
        for (Map.Entry<String, BsonValue> entry : document.entrySet()) {
            String fieldName = entry.getKey();
            resultMap.put(fieldName, toJsonString(BsonValueConvertor.convert(entry.getValue())));
            rowTypeBuilder.field(fieldName, DataTypes.STRING());
        }

        evalComputedColumns(resultMap, rowTypeBuilder);

        return resultMap;
    }

    private static String toJsonString(Object entry) {
        if (entry == null) {
            return null;
        } else if (!TypeUtils.isBasicType(entry)) {
            try {
                return writeValueAsString(entry);
            } catch (JsonProcessingException e) {
                LOG.error("Failed to deserialize record.", e);
            }
        }
        return Objects.toString(entry);
    }

    @Override
    protected List<String> extractPrimaryKeys() {
        return PRIMARY_KEYS;
    }

    @Nullable
    @Override
    protected String getTableName() {
        return getFromSourceField(FIELD_COLLECTION);
    }

    @Override
    protected String format() {
        return "debezium-bson";
    }
}
