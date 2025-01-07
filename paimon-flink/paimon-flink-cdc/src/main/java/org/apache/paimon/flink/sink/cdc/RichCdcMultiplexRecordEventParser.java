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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** {@link EventParser} for {@link RichCdcMultiplexRecord}. */
public class RichCdcMultiplexRecordEventParser implements EventParser<RichCdcMultiplexRecord> {

    private static final Logger LOG =
            LoggerFactory.getLogger(RichCdcMultiplexRecordEventParser.class);

    @Nullable private final NewTableSchemaBuilder schemaBuilder;
    @Nullable private final Pattern tblIncludingPattern;
    @Nullable private final Pattern tblExcludingPattern;
    @Nullable private final Pattern dbIncludingPattern;
    @Nullable private final Pattern dbExcludingPattern;
    private final TableNameConverter tableNameConverter;
    private final Set<String> createdTables;

    private final Map<String, RichEventParser> parsers = new HashMap<>();
    private final Set<String> includedTables = new HashSet<>();
    private final Set<String> excludedTables = new HashSet<>();

    private final Set<String> includedDbs = new HashSet<>();
    private final Set<String> excludedDbs = new HashSet<>();

    private RichCdcMultiplexRecord record;
    private String currentTable;
    private String currentDb;
    private boolean shouldSynchronizeCurrentTable;
    private RichEventParser currentParser;

    public RichCdcMultiplexRecordEventParser(boolean caseSensitive) {
        this(null, null, null, null, null, new TableNameConverter(caseSensitive), new HashSet<>());
    }

    public RichCdcMultiplexRecordEventParser(
            @Nullable NewTableSchemaBuilder schemaBuilder,
            @Nullable Pattern tblIncludingPattern,
            @Nullable Pattern tblExcludingPattern,
            @Nullable Pattern dbIncludingPattern,
            @Nullable Pattern dbExcludingPattern,
            TableNameConverter tableNameConverter,
            Set<String> createdTables) {
        this.schemaBuilder = schemaBuilder;
        this.tblIncludingPattern = tblIncludingPattern;
        this.tblExcludingPattern = tblExcludingPattern;
        this.dbIncludingPattern = dbIncludingPattern;
        this.dbExcludingPattern = dbExcludingPattern;
        this.tableNameConverter = tableNameConverter;
        this.createdTables = createdTables;
    }

    @Override
    public void setRawEvent(RichCdcMultiplexRecord record) {
        this.record = record;
        this.currentTable = record.tableName();
        this.currentDb = record.databaseName();
        this.shouldSynchronizeCurrentTable = shouldSynchronizeCurrentTable();
        if (shouldSynchronizeCurrentTable) {
            this.currentParser = parsers.computeIfAbsent(currentTable, t -> new RichEventParser());
            this.currentParser.setRawEvent(record.toRichCdcRecord());
        }
    }

    @Override
    public String parseTableName() {
        // database synchronization needs this, so we validate the record here
        if (record.databaseName() == null || record.tableName() == null) {
            throw new IllegalArgumentException(
                    "Cannot synchronize record when database name or table name is unknown. "
                            + "Invalid record is:\n"
                            + record);
        }
        return tableNameConverter.convert(Identifier.create(record.databaseName(), currentTable));
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
            checkNotNull(schemaBuilder, "NewTableSchemaBuilder hasn't been set.");
            return schemaBuilder.build(record);
        }

        return Optional.empty();
    }

    private boolean shouldSynchronizeCurrentDb() {
        // In case the record is incomplete, we let the null value pass validation
        // and handle the null value when we really need it
        if (currentDb == null) {
            return true;
        }
        if (includedDbs.contains(currentDb)) {
            return true;
        }
        if (excludedDbs.contains(currentDb)) {
            return false;
        }
        boolean shouldSynchronize = true;
        if (dbIncludingPattern != null) {
            shouldSynchronize = dbIncludingPattern.matcher(currentDb).matches();
        }
        if (dbExcludingPattern != null) {
            shouldSynchronize =
                    shouldSynchronize && !dbExcludingPattern.matcher(currentDb).matches();
        }
        if (!shouldSynchronize) {
            LOG.debug(
                    "Source database {} won't be synchronized because it was excluded. ",
                    currentDb);
            excludedDbs.add(currentDb);
            return false;
        }
        includedDbs.add(currentDb);
        return true;
    }

    private boolean shouldSynchronizeCurrentTable() {
        if (!shouldSynchronizeCurrentDb()) {
            return false;
        }
        // In case the record is incomplete, we let the null value pass validation
        // and handle the null value when we really need it
        if (currentTable == null) {
            return true;
        }

        if (includedTables.contains(currentTable)) {
            return true;
        }
        if (excludedTables.contains(currentTable)) {
            return false;
        }

        boolean shouldSynchronize = true;
        if (tblIncludingPattern != null) {
            shouldSynchronize = tblIncludingPattern.matcher(currentTable).matches();
        }
        if (tblExcludingPattern != null) {
            shouldSynchronize =
                    shouldSynchronize && !tblExcludingPattern.matcher(currentTable).matches();
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
        return shouldSynchronizeCurrentTable
                && !record.fields().isEmpty()
                && createdTables.add(parseTableName());
    }
}
