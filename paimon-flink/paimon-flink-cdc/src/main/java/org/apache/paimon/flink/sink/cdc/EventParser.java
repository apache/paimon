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

import org.apache.paimon.annotation.Experimental;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataField;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

/**
 * Parse a CDC change event to a list of {@link DataField}s or {@link CdcRecord}.
 *
 * @param <T> CDC change event type
 */
@Experimental
public interface EventParser<T> {

    /** Set current raw event to the parser. */
    void setRawEvent(T rawEvent);

    /** Parse the table name from raw event. */
    default String parseTableName() {
        throw new UnsupportedOperationException("Table name is not supported in this parser.");
    }

    /**
     * Parse new schema if this event contains schema change.
     *
     * @return empty if there is no schema change
     */
    List<DataField> parseSchemaChange();

    /**
     * Parse records from event.
     *
     * @return empty if there is no records
     */
    List<CdcRecord> parseRecords();

    /**
     * Parse newly added table schema from event.
     *
     * @return empty if there is no newly added table
     */
    default Optional<Schema> parseNewTable() {
        return Optional.empty();
    }

    /** Factory to create an {@link EventParser}. */
    interface Factory<T> extends Serializable {

        EventParser<T> create();
    }

    /** generate values for computed columns. */
    void evalComputedColumns(List<DataField> fields);
}
