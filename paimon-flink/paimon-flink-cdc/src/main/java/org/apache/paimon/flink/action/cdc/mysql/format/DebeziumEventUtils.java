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
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.relational.Table;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;
import io.debezium.relational.history.TableChanges.TableChange;
import io.debezium.relational.history.TableChanges.TableChangeType;
import org.apache.flink.cdc.debezium.history.FlinkJsonTableChangeSerializer;

import java.io.IOException;

/** A utility class that provide abilities for debezium event {@link DebeziumEvent}. */
public class DebeziumEventUtils {

    private static final DocumentReader DOCUMENT_READER = DocumentReader.defaultReader();

    public static HistoryRecord getHistoryRecord(String historyRecordStr) throws IOException {
        return new HistoryRecord(DOCUMENT_READER.read(historyRecordStr));
    }

    public static TableChanges getTableChanges(String historyRecordStr) throws IOException {
        HistoryRecord historyRecord = getHistoryRecord(historyRecordStr);
        Array tableChangesDocument =
                historyRecord.document().getArray(HistoryRecord.Fields.TABLE_CHANGES);
        return deserialize(tableChangesDocument, true);
    }

    /**
     * Copy from {@link FlinkJsonTableChangeSerializer#deserialize}, add a method to supplement
     * table comment. TODO remove this method after the method is added to {@link
     * FlinkJsonTableChangeSerializer}.
     */
    private static TableChanges deserialize(Array array, boolean useCatalogBeforeSchema) {
        TableChanges tableChanges = new TableChanges();

        for (Array.Entry entry : array) {
            Document document = entry.getValue().asDocument();
            TableChange change =
                    FlinkJsonTableChangeSerializer.fromDocument(document, useCatalogBeforeSchema);

            if (change.getType() == TableChangeType.CREATE) {
                // tableChanges.create(change.getTable());
                tableChanges.create(supplementTableComment(document, change.getTable()));
            } else if (change.getType() == TableChangeType.ALTER) {
                // tableChanges.alter(change.getTable());
                tableChanges.alter(supplementTableComment(document, change.getTable()));
            } else if (change.getType() == TableChangeType.DROP) {
                tableChanges.drop(change.getTable());
            }
        }

        return tableChanges;
    }

    private static Table supplementTableComment(Document document, Table table) {
        if (table.comment() != null) {
            return table;
        }
        String comment = document.getString("comment");
        if (comment != null) {
            return table.edit().setComment(comment).create();
        }
        return table;
    }
}
