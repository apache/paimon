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

package org.apache.paimon.flink.action.cdc.format.json;

import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.format.RecordParser;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.RowKind;

import java.util.ArrayList;
import java.util.List;

/**
 * The {@code JsonRecordParser} class extends the abstract {@link RecordParser} class and is
 * designed to parse JSON records.
 *
 * <p>This class treats JSON records as special CDC data with only insert operation type and
 * generates {@link RichCdcMultiplexRecord} objects with only INSERT operation types.
 */
public class JsonRecordParser extends RecordParser {

    public JsonRecordParser(TypeMapping typeMapping, List<ComputedColumn> computedColumns) {
        super(typeMapping, computedColumns);
    }

    @Override
    protected List<RichCdcMultiplexRecord> extractRecords() {
        List<RichCdcMultiplexRecord> records = new ArrayList<>();
        processRecord(root, RowKind.INSERT, records);
        return records;
    }

    @Override
    protected String primaryField() {
        return null;
    }

    @Override
    protected String dataField() {
        return null;
    }

    @Override
    protected String format() {
        return "json";
    }
}
