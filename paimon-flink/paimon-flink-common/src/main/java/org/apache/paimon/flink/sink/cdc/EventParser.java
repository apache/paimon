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
public interface EventParser<T> {

    void setRawEvent(T rawEvent);

    String tableName();

    String databaseName();

    boolean isUpdatedDataFields();

    Optional<List<DataField>> getUpdatedDataFields();

    List<CdcRecord> getRecords();

    Optional<Schema> getNewlyAddedTableSchema(String databaseName);

    /** Factory to create an {@link EventParser}. */
    interface Factory<T> extends Serializable {

        EventParser<T> create();
    }
}
