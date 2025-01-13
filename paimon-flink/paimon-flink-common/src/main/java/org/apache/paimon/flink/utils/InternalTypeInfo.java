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
import org.apache.paimon.types.RowType;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.Objects;

/** A {@link TypeInformation} for internal serializer. */
public class InternalTypeInfo<T> extends TypeInformation<T> {

    private static final long serialVersionUID = 1L;

    private final InternalTypeSerializer<T> serializer;

    public InternalTypeInfo(InternalTypeSerializer<T> serializer) {
        this.serializer = serializer;
    }

    public static InternalTypeInfo<InternalRow> fromRowType(RowType rowType) {
        return new InternalTypeInfo<>(new InternalRowTypeSerializer(rowType));
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

    @SuppressWarnings("unchecked")
    @Override
    public Class<T> getTypeClass() {
        return (Class<T>) serializer.createInstance().getClass();
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    /**
     * Do not annotate with <code>@override</code> here to maintain compatibility with Flink 1.18-.
     */
    public TypeSerializer<T> createSerializer(SerializerConfig config) {
        return this.createSerializer((ExecutionConfig) null);
    }

    /**
     * Do not annotate with <code>@override</code> here to maintain compatibility with Flink 2.0+.
     */
    public TypeSerializer<T> createSerializer(ExecutionConfig executionConfig) {
        return serializer.duplicate();
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof InternalTypeInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InternalTypeInfo<?> that = (InternalTypeInfo<?>) o;
        return Objects.equals(serializer, that.serializer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serializer);
    }

    @Override
    public String toString() {
        return "InternalTypeInfo{" + "serializer=" + serializer + '}';
    }
}
