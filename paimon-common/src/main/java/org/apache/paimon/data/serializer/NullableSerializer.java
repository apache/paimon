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

package org.apache.paimon.data.serializer;

import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.io.DataOutputView;

import javax.annotation.Nonnull;

import java.io.IOException;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Serializer wrapper to add support of {@code null} value serialization. */
public class NullableSerializer<T> implements Serializer<T> {

    private static final long serialVersionUID = 1L;

    private final Serializer<T> originalSerializer;

    private NullableSerializer(Serializer<T> originalSerializer) {
        this.originalSerializer = originalSerializer;
    }

    /**
     * This method tries to serialize {@code null} value with the {@code originalSerializer} and
     * wraps it in case of {@link NullPointerException}, otherwise it returns the {@code
     * originalSerializer}.
     *
     * @param originalSerializer serializer to wrap and add {@code null} support
     * @return serializer which supports {@code null} values
     */
    public static <T> Serializer<T> wrapIfNullIsNotSupported(
            @Nonnull Serializer<T> originalSerializer) {
        return checkIfNullSupported(originalSerializer)
                ? originalSerializer
                : wrap(originalSerializer);
    }

    /**
     * This method checks if {@code serializer} supports {@code null} value.
     *
     * @param serializer serializer to check
     */
    public static <T> boolean checkIfNullSupported(Serializer<T> serializer) {
        DataOutputSerializer dos = new DataOutputSerializer(10);
        try {
            serializer.serialize(null, dos);
        } catch (IOException | RuntimeException e) {
            return false;
        }
        DataInputDeserializer dis = new DataInputDeserializer(dos.getSharedBuffer());
        try {
            checkArgument(serializer.deserialize(dis) == null);
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Unexpected failure to deserialize just serialized null value with %s",
                            serializer.getClass().getName()),
                    e);
        }
        checkArgument(
                serializer.copy(null) == null,
                "Serializer %s has to be able properly copy null value if it can serialize it",
                serializer.getClass().getName());
        return true;
    }

    private Serializer<T> originalSerializer() {
        return originalSerializer;
    }

    /**
     * This method wraps the {@code originalSerializer} with the {@code NullableSerializer} if not
     * already wrapped.
     *
     * @param originalSerializer serializer to wrap and add {@code null} support
     * @return wrapped serializer which supports {@code null} values
     */
    public static <T> Serializer<T> wrap(Serializer<T> originalSerializer) {
        return originalSerializer instanceof NullableSerializer
                ? originalSerializer
                : new NullableSerializer<>(originalSerializer);
    }

    @Override
    public Serializer<T> duplicate() {
        Serializer<T> duplicateOriginalSerializer = originalSerializer.duplicate();
        return duplicateOriginalSerializer == originalSerializer
                ? this
                : new NullableSerializer<>(originalSerializer.duplicate());
    }

    @Override
    public T copy(T from) {
        return from == null ? null : originalSerializer.copy(from);
    }

    @Override
    public void serialize(T record, DataOutputView target) throws IOException {
        if (record == null) {
            target.writeBoolean(true);
        } else {
            target.writeBoolean(false);
            originalSerializer.serialize(record, target);
        }
    }

    @Override
    public T deserialize(DataInputView source) throws IOException {
        boolean isNull = deserializeNull(source);
        return isNull ? null : originalSerializer.deserialize(source);
    }

    private boolean deserializeNull(DataInputView source) throws IOException {
        return source.readBoolean();
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this
                || (obj != null
                        && obj.getClass() == getClass()
                        && originalSerializer.equals(
                                ((NullableSerializer) obj).originalSerializer));
    }

    @Override
    public int hashCode() {
        return originalSerializer.hashCode();
    }
}
