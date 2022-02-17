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

package org.apache.flink.table.store.connector.sink;

/** Committable produced by {@link StoreSinkWriter}. */
public class Committable {

    private final Kind kind;

    private final byte[] wrappedCommittable;

    private final int serializerVersion;

    public Committable(Kind kind, byte[] wrappedCommittable, int serializerVersion) {
        this.kind = kind;
        this.wrappedCommittable = wrappedCommittable;
        this.serializerVersion = serializerVersion;
    }

    public Kind kind() {
        return kind;
    }

    public byte[] wrappedCommittable() {
        return wrappedCommittable;
    }

    public int serializerVersion() {
        return serializerVersion;
    }

    enum Kind {
        FILE((byte) 0),

        LOG((byte) 1),

        LOG_OFFSET((byte) 2);

        private final byte value;

        Kind(byte value) {
            this.value = value;
        }

        public byte toByteValue() {
            return value;
        }

        public static Kind fromByteValue(byte value) {
            switch (value) {
                case 0:
                    return FILE;
                case 1:
                    return LOG;
                case 2:
                    return LOG_OFFSET;
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported byte value '" + value + "' for value kind.");
            }
        }
    }
}
