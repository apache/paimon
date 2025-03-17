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

package org.apache.paimon.io;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStatsEvolution;
import org.apache.paimon.stats.SimpleStatsEvolutions;
import org.apache.paimon.table.PrimaryKeyTableUtils;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

/** A factory to create {@link RecordReader} expires records by time. */
public class RecordLevelExpire {

    private static final Logger LOG = LoggerFactory.getLogger(RecordLevelExpire.class);

    private final int expireTime;
    private final Function<InternalRow, Optional<Integer>> fieldGetter;

    private final ConcurrentMap<Long, TableSchema> tableSchemas;
    private final TableSchema schema;
    private final SchemaManager schemaManager;
    private final SimpleStatsEvolutions fieldValueStatsConverters;

    @Nullable
    public static RecordLevelExpire create(
            CoreOptions options, TableSchema schema, SchemaManager schemaManager) {
        Duration expireTime = options.recordLevelExpireTime();
        if (expireTime == null) {
            return null;
        }

        String timeFieldName = options.recordLevelTimeField();
        if (timeFieldName == null) {
            throw new IllegalArgumentException(
                    "You should set time field for record-level expire.");
        }

        // should no project here, record level expire only works in compaction
        RowType rowType = schema.logicalRowType();
        int fieldIndex = rowType.getFieldIndex(timeFieldName);
        if (fieldIndex == -1) {
            throw new IllegalArgumentException(
                    String.format(
                            "Can not find time field %s for record level expire.", timeFieldName));
        }

        DataType dataType = rowType.getField(timeFieldName).type();
        Function<InternalRow, Optional<Integer>> fieldGetter =
                createFieldGetter(dataType, fieldIndex);

        LOG.info(
                "Create RecordExpire. expireTime is {}s,timeField is {}",
                (int) expireTime.getSeconds(),
                timeFieldName);
        return new RecordLevelExpire(
                (int) expireTime.getSeconds(), fieldGetter, schema, schemaManager);
    }

    private RecordLevelExpire(
            int expireTime,
            Function<InternalRow, Optional<Integer>> fieldGetter,
            TableSchema schema,
            SchemaManager schemaManager) {
        this.expireTime = expireTime;
        this.fieldGetter = fieldGetter;

        this.tableSchemas = new ConcurrentHashMap<>();
        this.schema = schema;
        this.schemaManager = schemaManager;

        KeyValueFieldsExtractor extractor =
                PrimaryKeyTableUtils.PrimaryKeyFieldsExtractor.EXTRACTOR;

        fieldValueStatsConverters =
                new SimpleStatsEvolutions(
                        sid -> extractor.valueFields(scanTableSchema(sid)), schema.id());
    }

    public boolean isExpireFile(DataFileMeta file) {
        InternalRow minValues = file.valueStats().minValues();

        if (file.schemaId() != schema.id() || file.valueStatsCols() != null) {
            // In the following cases, can not read minValues with field index directly
            //
            // 1. if the table had suffered schema evolution, read minValues with new field index
            // may cause exception.
            // 2. if metadata.stats-dense-store = true, minValues may not contain all data fields
            // which may cause exception when reading with origin field index
            SimpleStatsEvolution.Result result =
                    fieldValueStatsConverters
                            .getOrCreate(file.schemaId())
                            .evolution(file.valueStats(), file.rowCount(), file.valueStatsCols());
            minValues = result.minValues();
        }

        int currentTime = (int) (System.currentTimeMillis() / 1000);
        Optional<Integer> minTime = fieldGetter.apply(minValues);

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "expire time is {}, currentTime is {}, file min time for time field is {}. "
                            + "file name is {}, file level is {}, file schema id is {}, file valueStatsCols is {}",
                    expireTime,
                    currentTime,
                    minTime.isPresent() ? minTime.get() : "empty",
                    file.fileName(),
                    file.level(),
                    file.schemaId(),
                    file.valueStatsCols());
        }

        return minTime.map(minValue -> currentTime - expireTime > minValue).orElse(false);
    }

    public FileReaderFactory<KeyValue> wrap(FileReaderFactory<KeyValue> readerFactory) {
        return file -> wrap(readerFactory.createRecordReader(file));
    }

    private RecordReader<KeyValue> wrap(RecordReader<KeyValue> reader) {
        int currentTime = (int) (System.currentTimeMillis() / 1000);
        return reader.filter(
                keyValue ->
                        fieldGetter
                                .apply(keyValue.value())
                                .map(integer -> currentTime <= integer + expireTime)
                                .orElse(true));
    }

    private static Function<InternalRow, Optional<Integer>> createFieldGetter(
            DataType dataType, int fieldIndex) {
        final Function<InternalRow, Optional<Integer>> fieldGetter;
        if (dataType instanceof IntType) {
            fieldGetter =
                    row ->
                            row.isNullAt(fieldIndex)
                                    ? Optional.empty()
                                    : Optional.of(row.getInt(fieldIndex));
        } else if (dataType instanceof BigIntType) {
            fieldGetter =
                    row -> {
                        if (row.isNullAt(fieldIndex)) {
                            return Optional.empty();
                        }
                        long value = row.getLong(fieldIndex);
                        // If it is milliseconds, convert it to seconds.
                        return Optional.of(
                                (int) (value >= 1_000_000_000_000L ? value / 1000 : value));
                    };
        } else if (dataType instanceof TimestampType
                || dataType instanceof LocalZonedTimestampType) {
            int precision = DataTypeChecks.getPrecision(dataType);
            fieldGetter =
                    row ->
                            row.isNullAt(fieldIndex)
                                    ? Optional.empty()
                                    : Optional.of(
                                            (int)
                                                    (row.getTimestamp(fieldIndex, precision)
                                                                    .getMillisecond()
                                                            / 1000));
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "The record level time field type should be one of INT, BIGINT, or TIMESTAMP, but field type is %s.",
                            dataType));
        }

        return fieldGetter;
    }

    private TableSchema scanTableSchema(long id) {
        return tableSchemas.computeIfAbsent(
                id, key -> key == schema.id() ? schema : schemaManager.schema(id));
    }
}
