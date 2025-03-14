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

import java.io.Serializable;
import java.util.List;

/** Testing CDC change event. */
public class TestCdcEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String tableName;
    private final CdcSchema cdcSchema;
    private final List<CdcRecord> records;
    private final int keyHash;

    public TestCdcEvent(String tableName, CdcSchema updatedSchema) {
        this.tableName = tableName;
        this.cdcSchema = updatedSchema;
        this.records = null;
        this.keyHash = 0;
    }

    public TestCdcEvent(String tableName, List<CdcRecord> records, int keyHash) {
        this.tableName = tableName;
        this.cdcSchema = null;
        this.records = records;
        this.keyHash = keyHash;
    }

    public String tableName() {
        return tableName;
    }

    public CdcSchema cdcSchema() {
        return cdcSchema;
    }

    public List<CdcRecord> records() {
        return records;
    }

    @Override
    public int hashCode() {
        return keyHash;
    }

    @Override
    public String toString() {
        return String.format(
                "{tableName = %s, schema = %s, records = %s}", tableName, cdcSchema, records);
    }
}
