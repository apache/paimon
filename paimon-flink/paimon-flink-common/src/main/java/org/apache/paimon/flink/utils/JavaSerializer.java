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

import org.apache.flink.api.common.typeutils.GenericTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A type serializer that serializes its type using the Java serialization. */
public class JavaSerializer<T extends Serializable> extends TypeSerializer<T> {

    private static final long serialVersionUID = 3L;

    private static final Logger LOG = LoggerFactory.getLogger(JavaSerializer.class);

    private final Class<T> type;

    public JavaSerializer(Class<T> type) {
        this.type = checkNotNull(type);
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public JavaSerializer<T> duplicate() {
        return new JavaSerializer<>(type);
    }

    @Override
    public T createInstance() {
        try {
            return type.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public T copy(T from) {
        try {
            return InstantiationUtil.clone(from);
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public T copy(T from, T reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(T record, DataOutputView target) throws IOException {
        InstantiationUtil.serializeObject(new DataOutputViewStream(target), record);
    }

    @Override
    public T deserialize(DataInputView source) throws IOException {
        try {
            return InstantiationUtil.deserializeObject(
                    new DataInputViewStream(source), getClass().getClassLoader());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public T deserialize(T reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public int hashCode() {
        return type.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof JavaSerializer) {
            JavaSerializer<?> other = (JavaSerializer<?>) obj;

            return type == other.type;
        } else {
            return false;
        }
    }

    @Override
    public TypeSerializerSnapshot<T> snapshotConfiguration() {
        return new JavaSerializerSnapshot<>(type);
    }

    /** {@link JavaSerializer} snapshot class. */
    public static final class JavaSerializerSnapshot<T extends Serializable>
            extends GenericTypeSerializerSnapshot<T, JavaSerializer> {

        @SuppressWarnings("unused")
        public JavaSerializerSnapshot() {}

        JavaSerializerSnapshot(Class<T> typeClass) {
            super(typeClass);
        }

        @Override
        protected TypeSerializer<T> createSerializer(Class<T> typeClass) {
            return new JavaSerializer<>(typeClass);
        }

        @SuppressWarnings("unchecked")
        @Override
        protected Class<T> getTypeClass(JavaSerializer serializer) {
            return serializer.type;
        }

        @Override
        protected Class<?> serializerClass() {
            return JavaSerializer.class;
        }
    }
}
