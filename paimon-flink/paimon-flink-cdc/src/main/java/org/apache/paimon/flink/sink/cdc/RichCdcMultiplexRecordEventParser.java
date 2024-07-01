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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.cdc.TableNameConverter;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataField;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** {@link EventParser} for {@link RichCdcMultiplexRecord}. */
public class RichCdcMultiplexRecordEventParser implements EventParser<RichCdcMultiplexRecord> {

    private static final Logger LOG =
            LoggerFactory.getLogger(RichCdcMultiplexRecordEventParser.class);

    @Nullable private final NewTableSchemaBuilder schemaBuilder;
    private final TableNameConverter tableNameConverter;
    private final Map<String, RichEventParser> parsers = new HashMap<>();
    private final Set<String> createdTables = new HashSet<>();

    private RichCdcMultiplexRecord record;
    private String currentTable;
    private RichEventParser currentParser;

    public RichCdcMultiplexRecordEventParser(boolean caseSensitive) {
        this(null, new TableNameConverter(caseSensitive));
    }

    public RichCdcMultiplexRecordEventParser(
            @Nullable NewTableSchemaBuilder schemaBuilder, TableNameConverter tableNameConverter) {
        this.schemaBuilder = schemaBuilder;
        this.tableNameConverter = tableNameConverter;
    }

    @Override
    public void setRawEvent(RichCdcMultiplexRecord record) {
        this.record = record;
        this.currentTable = record.tableName();
        this.currentParser = parsers.computeIfAbsent(currentTable, t -> new RichEventParser());
        this.currentParser.setRawEvent(record.toRichCdcRecord());
    }

    @Override
    public String parseTableName() {
        return tableNameConverter.convert(Identifier.create(record.databaseName(), currentTable));
    }

    @Override
    public List<DataField> parseSchemaChange() {
        return currentParser.parseSchemaChange();
    }

    @Override
    public List<CdcRecord> parseRecords() {
        return currentParser.parseRecords();
    }

    @Override
    public Optional<Schema> parseNewTable() {
        if (shouldCreateCurrentTable()) {
            checkNotNull(schemaBuilder, "NewTableSchemaBuilder hasn't been set.");
            return schemaBuilder.build(record);
        }
        return Optional.empty();
    }

    private boolean shouldCreateCurrentTable() {
        return !record.fields().isEmpty() && createdTables.add(parseTableName());
    }
}
