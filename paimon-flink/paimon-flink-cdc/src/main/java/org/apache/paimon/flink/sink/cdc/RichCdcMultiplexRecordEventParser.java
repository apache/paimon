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

import org.apache.paimon.flink.action.cdc.TableNameConverter;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataField;

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
import java.util.regex.Pattern;

/** {@link EventParser} for {@link RichCdcMultiplexRecord}. */
public class RichCdcMultiplexRecordEventParser implements EventParser<RichCdcMultiplexRecord> {

    private static final Logger LOG =
            LoggerFactory.getLogger(RichCdcMultiplexRecordEventParser.class);

    private final NewTableSchemaBuilder<RichCdcMultiplexRecord> schemaBuilder;
    @Nullable private final Pattern includingPattern;
    @Nullable private final Pattern excludingPattern;
    private final TableNameConverter tableNameConverter;
    private final Map<String, RichEventParser> parsers = new HashMap<>();
    private final Set<String> includedTables = new HashSet<>();
    private final Set<String> excludedTables = new HashSet<>();
    private final Set<String> createdTables = new HashSet<>();

    private RichCdcMultiplexRecord record;
    private String currentTable;
    private boolean shouldSynchronizeCurrentTable;
    private RichEventParser currentParser;

    public RichCdcMultiplexRecordEventParser(boolean caseSensitive) {
        this(record -> Optional.empty(), null, null, new TableNameConverter(caseSensitive));
    }

    public RichCdcMultiplexRecordEventParser(
            NewTableSchemaBuilder<RichCdcMultiplexRecord> schemaBuilder,
            @Nullable Pattern includingPattern,
            @Nullable Pattern excludingPattern,
            TableNameConverter tableNameConverter) {
        this.schemaBuilder = schemaBuilder;
        this.includingPattern = includingPattern;
        this.excludingPattern = excludingPattern;
        this.tableNameConverter = tableNameConverter;
    }

    @Override
    public void setRawEvent(RichCdcMultiplexRecord record) {
        this.record = record;
        this.currentTable = record.tableName();
        this.shouldSynchronizeCurrentTable = shouldSynchronizeCurrentTable();
        if (shouldSynchronizeCurrentTable) {
            this.currentParser = parsers.computeIfAbsent(currentTable, t -> new RichEventParser());
            this.currentParser.setRawEvent(record.toRichCdcRecord());
        }
    }

    @Override
    public String parseTableName() {
        return tableNameConverter.convert(currentTable);
    }

    @Override
    public List<DataField> parseSchemaChange() {
        return shouldSynchronizeCurrentTable
                ? currentParser.parseSchemaChange()
                : Collections.emptyList();
    }

    @Override
    public List<CdcRecord> parseRecords() {
        return shouldSynchronizeCurrentTable
                ? currentParser.parseRecords()
                : Collections.emptyList();
    }

    @Override
    public Optional<Schema> parseNewTable() {
        if (shouldCreateCurrentTable()) {
            return schemaBuilder.build(record);
        }

        return Optional.empty();
    }

    private boolean shouldSynchronizeCurrentTable() {
        if (includedTables.contains(currentTable)) {
            return true;
        }
        if (excludedTables.contains(currentTable)) {
            return false;
        }

        boolean shouldSynchronize = true;
        if (includingPattern != null) {
            shouldSynchronize = includingPattern.matcher(currentTable).matches();
        }
        if (excludingPattern != null) {
            shouldSynchronize =
                    shouldSynchronize && !excludingPattern.matcher(currentTable).matches();
        }
        if (!shouldSynchronize) {
            LOG.debug(
                    "Source table {} won't be synchronized because it was excluded. ",
                    currentTable);
            excludedTables.add(currentTable);
            return false;
        }

        includedTables.add(currentTable);
        return true;
    }

    private boolean shouldCreateCurrentTable() {
        return shouldSynchronizeCurrentTable && createdTables.add(currentTable);
    }
}
