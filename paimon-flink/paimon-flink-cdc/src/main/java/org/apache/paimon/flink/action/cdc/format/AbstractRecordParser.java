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
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

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

    public AbstractRecordParser(TypeMapping typeMapping, List<ComputedColumn> computedColumns) {
        this.typeMapping = typeMapping;
        this.computedColumns = computedColumns;
    }

    @Nullable
    public Schema buildSchema(CdcSourceRecord record) {
        try {
            setRoot(record);
            if (isDDL()) {
                return null;
            }

            Optional<RichCdcMultiplexRecord> recordOpt = extractRecords().stream().findFirst();
            if (!recordOpt.isPresent()) {
                return null;
            }

            Schema.Builder builder = Schema.newBuilder();
            recordOpt
                    .get()
                    .fields()
                    .forEach(
                            field ->
                                    builder.column(
                                            field.name(), field.type(), field.description()));
            builder.primaryKey(extractPrimaryKeys());
            return builder.build();
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

    protected abstract void setRoot(CdcSourceRecord record);

    protected abstract List<RichCdcMultiplexRecord> extractRecords();

    protected boolean isDDL() {
        return false;
    }

    protected abstract List<String> extractPrimaryKeys();

    /** generate values for computed columns. */
    protected void evalComputedColumns(
            Map<String, String> rowData, RowType.Builder rowTypeBuilder) {
        computedColumns.forEach(
                computedColumn -> {
                    rowData.put(
                            computedColumn.columnName(),
                            computedColumn.eval(rowData.get(computedColumn.fieldReference())));
                    rowTypeBuilder.field(computedColumn.columnName(), computedColumn.columnType());
                });
    }

    /** Handle case sensitivity here. */
    protected RichCdcMultiplexRecord createRecord(
            RowKind rowKind, Map<String, String> data, List<DataField> paimonFields) {
        return new RichCdcMultiplexRecord(
                getDatabaseName(),
                getTableName(),
                paimonFields,
                extractPrimaryKeys(),
                new CdcRecord(rowKind, data));
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
