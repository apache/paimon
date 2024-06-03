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

package org.apache.paimon.flink.action.cdc.format;

import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.TypeUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.paimon.utils.JsonSerdeUtil.convertValue;
import static org.apache.paimon.utils.JsonSerdeUtil.getNodeAs;
import static org.apache.paimon.utils.JsonSerdeUtil.isNull;
import static org.apache.paimon.utils.JsonSerdeUtil.writeValueAsString;

/**
 * Provides a base implementation for parsing messages of various formats into {@link
 * RichCdcMultiplexRecord} objects.
 *
 * <p>This abstract class defines common functionalities and fields required for parsing messages.
 * Subclasses are expected to provide specific implementations for extracting records, validating
 * message formats, and other format-specific operations.
 */
public abstract class AbstractJsonRecordParser extends AbstractRecordParser {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractJsonRecordParser.class);

    protected JsonNode root;

    public AbstractJsonRecordParser(
            boolean caseSensitive, TypeMapping typeMapping, List<ComputedColumn> computedColumns) {
        super(caseSensitive, typeMapping, computedColumns);
    }

    protected void setRoot(CdcSourceRecord record) {
        root = (JsonNode) record.getValue();
    }

    protected abstract String primaryField();

    protected abstract String dataField();

    // use STRING type in default when we cannot get origin data types (most cases)
    protected void fillDefaultTypes(JsonNode record, RowType.Builder rowTypeBuilder) {
        record.fieldNames()
                .forEachRemaining(name -> rowTypeBuilder.field(name, DataTypes.STRING()));
    }

    protected Map<String, String> extractRowData(JsonNode record, RowType.Builder rowTypeBuilder) {
        fillDefaultTypes(record, rowTypeBuilder);
        Map<String, Object> recordMap =
                convertValue(record, new TypeReference<Map<String, Object>>() {});
        Map<String, String> rowData =
                recordMap.entrySet().stream()
                        .filter(entry -> Objects.nonNull(entry.getKey()))
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        entry -> {
                                            if (Objects.nonNull(entry.getValue())
                                                    && !TypeUtils.isBasicType(entry.getValue())) {
                                                try {
                                                    return writeValueAsString(entry.getValue());
                                                } catch (JsonProcessingException e) {
                                                    LOG.error("Failed to deserialize record.", e);
                                                    return Objects.toString(entry.getValue());
                                                }
                                            }
                                            return Objects.toString(entry.getValue());
                                        }));
        evalComputedColumns(rowData, rowTypeBuilder);
        return rowData;
    }

    @Override
    protected List<String> extractPrimaryKeys() {
        ArrayNode pkNames = getNodeAs(root, primaryField(), ArrayNode.class);
        if (pkNames == null) {
            return Collections.emptyList();
        }

        return StreamSupport.stream(pkNames.spliterator(), false)
                .map(JsonNode::asText)
                .collect(Collectors.toList());
    }

    protected void processRecord(
            JsonNode jsonNode, RowKind rowKind, List<RichCdcMultiplexRecord> records) {
        RowType.Builder rowTypeBuilder = RowType.builder();
        Map<String, String> rowData = this.extractRowData(jsonNode, rowTypeBuilder);
        records.add(createRecord(rowKind, rowData, rowTypeBuilder.build().getFields()));
    }

    protected JsonNode mergeOldRecord(JsonNode data, JsonNode oldNode) {
        JsonNode oldFullRecordNode = data.deepCopy();
        oldNode.fieldNames()
                .forEachRemaining(
                        fieldName ->
                                ((ObjectNode) oldFullRecordNode)
                                        .set(fieldName, oldNode.get(fieldName)));
        return oldFullRecordNode;
    }

    @Nullable
    @Override
    protected String getTableName() {
        JsonNode node = root.get(FIELD_TABLE);
        return isNull(node) ? null : node.asText();
    }

    @Nullable
    @Override
    protected String getDatabaseName() {
        JsonNode node = root.get(FIELD_DATABASE);
        return isNull(node) ? null : node.asText();
    }

    protected void checkNotNull(JsonNode node, String key) {
        if (isNull(node)) {
            throw new RuntimeException(
                    String.format("Invalid %s format: missing '%s' field.", format(), key));
        }
    }

    protected void checkNotNull(
            JsonNode node, String key, String conditionKey, String conditionValue) {
        if (isNull(node)) {
            throw new RuntimeException(
                    String.format(
                            "Invalid %s format: missing '%s' field when '%s' is '%s'.",
                            format(), key, conditionKey, conditionValue));
        }
    }

    protected JsonNode getAndCheck(String key) {
        JsonNode node = root.get(key);
        checkNotNull(node, key);
        return node;
    }

    protected JsonNode getAndCheck(String key, String conditionKey, String conditionValue) {
        JsonNode node = root.get(key);
        checkNotNull(node, key, conditionKey, conditionValue);
        return node;
    }
}
