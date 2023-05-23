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

import org.apache.paimon.types.RowKind;

import java.util.Map;

/**
 * A data change message from the CDC source. Compared to {@link CdcRecord}, MultiplexCdcRecord
 * contains database and table information.
 */
public class MultiplexCdcRecord extends CdcRecord {
    private final String databaseName;
    private final String tableName;

    public MultiplexCdcRecord(
            String databaseName, String tableName, RowKind kind, Map<String, String> fields) {
        super(kind, fields);
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    public static MultiplexCdcRecord fromCdcRecord(
            String databaseName, String tableName, CdcRecord record) {
        return new MultiplexCdcRecord(
                databaseName, tableName, record.getKind(), record.getFields());
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }
}
