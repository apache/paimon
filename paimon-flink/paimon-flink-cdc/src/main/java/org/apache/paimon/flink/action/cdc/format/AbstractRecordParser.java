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

import org.apache.paimon.flink.action.cdc.CdcMetadataConverter;
import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.sink.cdc.CdcSchema;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.RowKind;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Provides a base implementation for parsing messages of various formats into {@link
 * RichCdcMultiplexRecord} objects.
 *
 * <p>This abstract class defines common functionalities and fields required for parsing messages.
 * Subclasses are expected to provide specific implementations for extracting records, validating
 * message formats, and other format-specific operations.
 */
public abstract class AbstractRecordParser
        implements FlatMapFunction<CdcSourceRecord, RichCdcMultiplexRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractRecordParser.class);

    protected static final String FIELD_TABLE = "table";
    protected static final String FIELD_DATABASE = "database";
    protected final TypeMapping typeMapping;
    protected final List<ComputedColumn> computedColumns;
    protected CdcMetadataConverter[] metadataConverters;
    protected CdcSourceRecord currentRecord; // Store current record for metadata access

    public AbstractRecordParser(TypeMapping typeMapping, List<ComputedColumn> computedColumns) {
        this.typeMapping = typeMapping;
        this.computedColumns = computedColumns;
        this.metadataConverters = new CdcMetadataConverter[0];
    }

    /** Sets metadata converters and returns this parser for chaining. */
    public AbstractRecordParser withMetadataConverters(CdcMetadataConverter[] metadataConverters) {
        this.metadataConverters =
                metadataConverters != null ? metadataConverters : new CdcMetadataConverter[0];
        return this;
    }

    @Nullable
    public Schema buildSchema(CdcSourceRecord record) {
        try {
            setRoot(record);
            if (isDDL()) {
                return null;
            }

            Optional<RichCdcMultiplexRecord> recordOpt = extractRecords().stream().findFirst();
            return recordOpt.map(RichCdcMultiplexRecord::buildSchema).orElse(null);
        } catch (Exception e) {
            logInvalidSourceRecord(record);
            throw e;
        }
    }

    @Override
    public void flatMap(CdcSourceRecord value, Collector<RichCdcMultiplexRecord> out) {
        try {
            setRoot(value);
            extractRecords().forEach(out::collect);
        } catch (Exception e) {
            logInvalidSourceRecord(value);
            throw e;
        }
    }

    protected void setRoot(CdcSourceRecord record) {
        this.currentRecord = record;
        // Call the original setRoot method for backward compatibility
        // Subclasses can override this method as they used to
    }

    protected abstract List<RichCdcMultiplexRecord> extractRecords();

    protected boolean isDDL() {
        return false;
    }

    protected abstract List<String> extractPrimaryKeys();

    /**
     * Evaluate against source fields and metadata, then append computed columns before metadata.
     */
    protected void evalComputedAndMetadataColumns(
            Map<String, String> rowData, CdcSchema.Builder schemaBuilder) {
        Map<String, String> metadataValues = readMetadataColumns();
        evalComputedColumns(rowData, schemaBuilder, metadataValues);
        evalMetadataColumns(rowData, schemaBuilder, metadataValues);
    }

    /** Generate computed values from source fields for parsers using the original helper API. */
    protected void evalComputedColumns(
            Map<String, String> rowData, CdcSchema.Builder schemaBuilder) {
        evalComputedColumns(rowData, schemaBuilder, Collections.emptyMap());
    }

    private void evalComputedColumns(
            Map<String, String> rowData,
            CdcSchema.Builder schemaBuilder,
            Map<String, String> metadataValues) {
        Set<String> base64EncodedFields =
                computedColumns.isEmpty()
                        ? Collections.emptySet()
                        : new HashSet<>(extractBase64EncodedFields());
        computedColumns.forEach(
                computedColumn -> {
                    String result =
                            computedColumn.evalFromRecord(
                                    rowData, base64EncodedFields, metadataValues);

                    rowData.put(computedColumn.columnName(), result);
                    base64EncodedFields.remove(computedColumn.columnName());
                    schemaBuilder.column(computedColumn.columnName(), computedColumn.columnType());
                });
    }

    /** Append metadata outputs for parsers using the original helper API. */
    protected void evalMetadataColumns(
            Map<String, String> rowData, CdcSchema.Builder schemaBuilder) {
        evalMetadataColumns(rowData, schemaBuilder, readMetadataColumns());
    }

    private void evalMetadataColumns(
            Map<String, String> rowData,
            CdcSchema.Builder schemaBuilder,
            Map<String, String> metadataValues) {
        for (CdcMetadataConverter converter : metadataConverters) {
            if (metadataValues.containsKey(converter.columnName())) {
                String value = metadataValues.get(converter.columnName());
                if (value != null) {
                    rowData.put(converter.columnName(), value);
                }
                schemaBuilder.column(converter.columnName(), converter.dataType());
            }
        }
    }

    private Map<String, String> readMetadataColumns() {
        if (metadataConverters.length == 0) {
            return Collections.emptyMap();
        }
        Map<String, String> metadataValues = new HashMap<>();
        for (CdcMetadataConverter metadataConverter : metadataConverters) {
            try {
                metadataValues.put(
                        metadataConverter.columnName(), metadataConverter.read(currentRecord));
            } catch (UnsupportedOperationException e) {
                // This converter doesn't support CdcSourceRecord, skip it
                LOG.debug(
                        "Metadata converter {} does not support CdcSourceRecord",
                        metadataConverter.getClass().getSimpleName());
            }
        }
        return metadataValues;
    }

    /** Handle case sensitivity here. */
    protected RichCdcMultiplexRecord createRecord(
            RowKind rowKind, Map<String, String> data, CdcSchema.Builder schemaBuilder) {
        schemaBuilder.primaryKey(extractPrimaryKeys());
        Set<String> base64EncodedFields = new HashSet<>(extractBase64EncodedFields());
        computedColumns.forEach(column -> base64EncodedFields.remove(column.columnName()));
        for (CdcMetadataConverter converter : metadataConverters) {
            base64EncodedFields.remove(converter.columnName());
        }
        return new RichCdcMultiplexRecord(
                getDatabaseName(),
                getTableName(),
                schemaBuilder.build(),
                CdcRecord.withBase64EncodedFields(rowKind, data, base64EncodedFields));
    }

    /** Names of source fields whose non-null values contain Base64-encoded binary data. */
    protected Set<String> extractBase64EncodedFields() {
        return Collections.emptySet();
    }

    @Nullable
    protected abstract String getTableName();

    @Nullable
    protected abstract String getDatabaseName();

    private void logInvalidSourceRecord(CdcSourceRecord record) {
        LOG.error("Invalid source record:\n{}", record.toString());
    }

    protected abstract String format();
}
