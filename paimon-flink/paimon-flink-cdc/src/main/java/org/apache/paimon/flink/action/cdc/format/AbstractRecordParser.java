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
import org.apache.paimon.flink.sink.cdc.CdcSchema;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.RowKind;

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

    private boolean skipCorruptRecord = false;
    private boolean logCorruptRecord = false;

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
            return recordOpt.map(RichCdcMultiplexRecord::buildSchema).orElse(null);
        } catch (Exception e) {
            if (skipCorruptRecord) {
                logCorruptRecordMetadata(record, "schema build");
                if (logCorruptRecord) {
                    logInvalidSourceRecord(record);
                    LOG.warn(
                            "Skipping corrupt or unparsable source record during schema build.", e);
                } else {
                    LOG.warn(
                            "Skipping corrupt or unparsable source record during schema build (record details not logged due to PII concerns).",
                            e);
                }
                return null;
            } else {
                logInvalidSourceRecord(record);
                throw e;
            }
        }
    }

    @Override
    public void flatMap(CdcSourceRecord value, Collector<RichCdcMultiplexRecord> out) {
        try {
            setRoot(value);
            extractRecords().forEach(out::collect);
        } catch (Exception e) {
            if (skipCorruptRecord) {
                logCorruptRecordMetadata(value, "record processing");
                if (logCorruptRecord) {
                    logInvalidSourceRecord(value);
                    LOG.warn("Skipping corrupt or unparsable source record.", e);
                } else {
                    LOG.warn(
                            "Skipping corrupt or unparsable source record (record details not logged due to PII concerns).",
                            e);
                }
            } else {
                logInvalidSourceRecord(value);
                throw e;
            }
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
            Map<String, String> rowData, CdcSchema.Builder schemaBuilder) {
        computedColumns.forEach(
                computedColumn -> {
                    String result =
                            computedColumn.eval(rowData.get(computedColumn.fieldReference()));

                    rowData.put(computedColumn.columnName(), result);
                    schemaBuilder.column(computedColumn.columnName(), computedColumn.columnType());
                });
    }

    /** Handle case sensitivity here. */
    protected RichCdcMultiplexRecord createRecord(
            RowKind rowKind, Map<String, String> data, CdcSchema.Builder schemaBuilder) {
        schemaBuilder.primaryKey(extractPrimaryKeys());
        return new RichCdcMultiplexRecord(
                getDatabaseName(),
                getTableName(),
                schemaBuilder.build(),
                new CdcRecord(rowKind, data));
    }

    @Nullable
    protected abstract String getTableName();

    @Nullable
    protected abstract String getDatabaseName();

    private void logInvalidSourceRecord(CdcSourceRecord record) {
        StringBuilder msg = new StringBuilder("Invalid source record");
        if (record.getTopic() != null) {
            msg.append(" from topic: ").append(record.getTopic());
        }
        if (record.getPartition() != null) {
            msg.append(", partition: ").append(record.getPartition());
        }
        if (record.getOffset() != null) {
            msg.append(", offset: ").append(record.getOffset());
        }
        LOG.error("{}\n{}", msg.toString(), record.toString());
    }

    private void logCorruptRecordMetadata(CdcSourceRecord record, String context) {
        String topic = record.getTopic();
        if (topic != null) {
            LOG.warn("Corrupt record detected during {} from topic: {}", context, topic);
        } else {
            LOG.warn("Corrupt record detected during {} (no topic information available)", context);
        }
    }

    protected abstract String format();

    /**
     * Configure whether to skip corrupt records that fail parsing.
     *
     * @param skip if true, corrupt records will be skipped instead of causing job failure
     * @return this parser instance for chaining
     */
    public AbstractRecordParser withSkipCorruptRecord(boolean skip) {
        this.skipCorruptRecord = skip;
        return this;
    }

    /**
     * Configure whether to log details about corrupt records.
     *
     * @param log if true, corrupt records will be logged with details (may contain PII)
     * @return this parser instance for chaining
     */
    public AbstractRecordParser withLogCorruptRecord(boolean log) {
        this.logCorruptRecord = log;
        return this;
    }
}
