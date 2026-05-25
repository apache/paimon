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

package org.apache.paimon.flink.dataevolution;

import org.apache.paimon.flink.sink.NoneCopyVersionedSerializerTypeSerializerProxy;
import org.apache.paimon.types.RowType;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/** Type information of {@link UpsertRecord}. */
public class UpsertRecordTypeInfo extends TypeInformation<UpsertRecord> {

    private final RowType rowType;
    private final int partitionArity;

    public UpsertRecordTypeInfo(RowType rowType, int partitionArity) {
        this.rowType = rowType;
        this.partitionArity = partitionArity;
    }

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 1;
    }

    @Override
    public int getTotalFields() {
        return 1;
    }

    @Override
    public Class<UpsertRecord> getTypeClass() {
        return UpsertRecord.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    /**
     * Do not annotate with <code>@override</code> here to maintain compatibility with Flink 1.18-.
     */
    public TypeSerializer<UpsertRecord> createSerializer(SerializerConfig config) {
        return this.createSerializer((ExecutionConfig) null);
    }

    /**
     * Do not annotate with <code>@override</code> here to maintain compatibility with Flink 2.0+.
     */
    public TypeSerializer<UpsertRecord> createSerializer(ExecutionConfig config) {
        return new NoneCopyVersionedSerializerTypeSerializerProxy<UpsertRecord>(
                () -> new UpsertRecordSerializer(rowType, partitionArity)) {};
    }

    @Override
    public int hashCode() {
        return rowType.hashCode();
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof UpsertRecordTypeInfo;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof UpsertRecordTypeInfo)) {
            return false;
        }
        UpsertRecordTypeInfo other = (UpsertRecordTypeInfo) obj;
        return rowType.equals(other.rowType) && partitionArity == other.partitionArity;
    }

    @Override
    public String toString() {
        return "UpsertRecordTypeInfo";
    }
}
