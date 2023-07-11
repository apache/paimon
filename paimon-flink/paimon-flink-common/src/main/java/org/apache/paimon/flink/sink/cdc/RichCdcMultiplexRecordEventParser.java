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

import org.apache.paimon.types.DataField;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** {@link EventParser} for {@link RichCdcMultiplexRecord}. */
public class RichCdcMultiplexRecordEventParser implements EventParser<RichCdcMultiplexRecord> {

    // TODO: currently we don't consider database
    private String currentTable;
    private RichEventParser currentParser;

    private final Map<String, RichEventParser> parsers = new HashMap<>();

    @Override
    public void setRawEvent(RichCdcMultiplexRecord record) {
        this.currentTable = record.tableName();
        this.currentParser = parsers.computeIfAbsent(currentTable, t -> new RichEventParser());
        currentParser.setRawEvent(record.toRichCdcRecord());
    }

    @Override
    public String parseTableName() {
        return currentTable;
    }

    @Override
    public List<DataField> parseSchemaChange() {
        return currentParser.parseSchemaChange();
    }

    @Override
    public List<CdcRecord> parseRecords() {
        return currentParser.parseRecords();
    }
}
