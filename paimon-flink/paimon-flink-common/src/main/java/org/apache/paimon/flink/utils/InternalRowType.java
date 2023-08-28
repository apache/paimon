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

package org.apache.paimon.flink.utils;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataType;

import org.apache.paimon.shade.guava30.com.google.common.base.Objects;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.Arrays;

/** A {@link TypeInformation} for internal row serializer. */
public class InternalRowType extends TypeInformation<InternalRow> {

    private final DataType[] dataTypes;

    public InternalRowType(DataType... dataTypes) {
        this.dataTypes = dataTypes;
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
    public Class<InternalRow> getTypeClass() {
        return InternalRow.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<InternalRow> createSerializer(ExecutionConfig config) {
        return new InternalRowTypeSerializer(dataTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode((Object[]) dataTypes);
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof InternalRow;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof InternalRow;
    }

    @Override
    public String toString() {
        return "InternalRowType: " + Arrays.toString(dataTypes);
    }
}
