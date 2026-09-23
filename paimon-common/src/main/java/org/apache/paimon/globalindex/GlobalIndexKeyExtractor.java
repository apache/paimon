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

package org.apache.paimon.globalindex;

import org.apache.paimon.types.DataType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;

/** Extracts zero or more normalized index keys from one source-column value. */
public interface GlobalIndexKeyExtractor extends Serializable {

    /** Type of the normalized keys emitted by {@link #extract(Object, KeyConsumer)}. */
    DataType keyType();

    /** Emits normalized keys. Implementations may ignore null source values and null elements. */
    void extract(@Nullable Object sourceValue, KeyConsumer consumer) throws IOException;

    /** Whether extraction emits exactly the source value, including null. */
    default boolean isIdentity() {
        return false;
    }

    /** Creates an extractor which emits exactly one key for every source value, including null. */
    static GlobalIndexKeyExtractor identity(DataType keyType) {
        return new IdentityKeyExtractor(keyType);
    }

    /** Identity extraction for scalar sorted indexes. */
    class IdentityKeyExtractor implements GlobalIndexKeyExtractor {

        private static final long serialVersionUID = 1L;

        private final DataType keyType;

        private IdentityKeyExtractor(DataType keyType) {
            this.keyType = keyType;
        }

        @Override
        public DataType keyType() {
            return keyType;
        }

        @Override
        public void extract(@Nullable Object sourceValue, KeyConsumer consumer) throws IOException {
            consumer.accept(sourceValue);
        }

        @Override
        public boolean isIdentity() {
            return true;
        }
    }

    /** Consumer which lets an index build propagate storage failures from the emitted keys. */
    @FunctionalInterface
    interface KeyConsumer {

        void accept(@Nullable Object key) throws IOException;
    }
}
