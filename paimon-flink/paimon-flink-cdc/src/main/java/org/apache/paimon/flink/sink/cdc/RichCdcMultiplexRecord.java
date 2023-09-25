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
import java.util.List;
import java.util.Objects;

/** Compared to {@link CdcMultiplexRecord}, this contains schema information. */
public class RichCdcMultiplexRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String databaseName;
    private final String tableName;
    private final LinkedHashMap<String, DataType> fieldTypes;
    private final List<String> primaryKeys;
    private final CdcRecord cdcRecord;

    public RichCdcMultiplexRecord(
            String databaseName,
            String tableName,
            LinkedHashMap<String, DataType> fieldTypes,
            List<String> primaryKeys,
            CdcRecord cdcRecord) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.fieldTypes = fieldTypes;
        this.primaryKeys = primaryKeys;
        this.cdcRecord = cdcRecord;
    }

    public String tableName() {
        return tableName;
    }

    public LinkedHashMap<String, DataType> fieldTypes() {
        return fieldTypes;
    }

    public List<String> primaryKeys() {
        return primaryKeys;
    }

    public RichCdcRecord toRichCdcRecord() {
        return new RichCdcRecord(cdcRecord, fieldTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, tableName, fieldTypes, primaryKeys, cdcRecord);
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
        return databaseName.equals(that.databaseName)
                && tableName.equals(that.tableName)
                && Objects.equals(fieldTypes, that.fieldTypes)
                && Objects.equals(primaryKeys, that.primaryKeys)
                && Objects.equals(cdcRecord, that.cdcRecord);
    }

    @Override
    public String toString() {
        return "{"
                + "databaseName="
                + databaseName
                + ", tableName="
                + tableName
                + ", fieldTypes="
                + fieldTypes
                + ", primaryKeys="
                + primaryKeys
                + ", cdcRecord="
                + cdcRecord
                + '}';
    }
}
