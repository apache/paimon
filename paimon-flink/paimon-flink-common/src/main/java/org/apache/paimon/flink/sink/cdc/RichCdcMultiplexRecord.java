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

import org.apache.paimon.types.DataType;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Objects;

/** Compared to {@link CdcMultiplexRecord}, this contains schema information. */
public class RichCdcMultiplexRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    private final CdcRecord cdcRecord;
    private final LinkedHashMap<String, DataType> fieldTypes;
    private final String databaseName;
    private final String tableName;

    public RichCdcMultiplexRecord(
            CdcRecord cdcRecord,
            LinkedHashMap<String, DataType> fieldTypes,
            String databaseName,
            String tableName) {
        this.cdcRecord = cdcRecord;
        this.fieldTypes = fieldTypes;
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    public String tableName() {
        return tableName;
    }

    public RichCdcRecord toRichCdcRecord() {
        return new RichCdcRecord(cdcRecord, fieldTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cdcRecord, fieldTypes, databaseName, tableName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RichCdcMultiplexRecord that = (RichCdcMultiplexRecord) o;
        return Objects.equals(cdcRecord, that.cdcRecord)
                && Objects.equals(fieldTypes, that.fieldTypes)
                && databaseName.equals(that.databaseName)
                && tableName.equals(that.tableName);
    }

    @Override
    public String toString() {
        return "{"
                + "cdcRecord="
                + cdcRecord
                + ", fieldTypes="
                + fieldTypes
                + ", databaseName="
                + databaseName
                + ", tableName="
                + tableName
                + '}';
    }
}
