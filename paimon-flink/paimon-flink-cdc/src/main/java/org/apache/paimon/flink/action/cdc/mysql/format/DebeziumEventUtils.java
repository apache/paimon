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

package org.apache.paimon.flink.action.cdc.mysql.format;

import io.debezium.document.Array;
import io.debezium.document.DocumentReader;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;
import org.apache.flink.cdc.debezium.history.FlinkJsonTableChangeSerializer;

import java.io.IOException;

/** A utility class that provide abilities for debezium event {@link DebeziumEvent}. */
public class DebeziumEventUtils {

    private static final DocumentReader DOCUMENT_READER = DocumentReader.defaultReader();
    private static final FlinkJsonTableChangeSerializer TABLE_CHANGE_SERIALIZER =
            new FlinkJsonTableChangeSerializer();

    public static HistoryRecord getHistoryRecord(String historyRecordStr) throws IOException {
        return new HistoryRecord(DOCUMENT_READER.read(historyRecordStr));
    }

    public static TableChanges getTableChanges(String historyRecordStr) throws IOException {
        HistoryRecord historyRecord = getHistoryRecord(historyRecordStr);
        Array tableChanges = historyRecord.document().getArray(HistoryRecord.Fields.TABLE_CHANGES);
        return TABLE_CHANGE_SERIALIZER.deserialize(tableChanges, true);
    }
}
