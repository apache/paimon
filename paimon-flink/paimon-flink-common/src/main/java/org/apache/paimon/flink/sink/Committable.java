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

package org.apache.paimon.flink.sink;

/** Committable produced by {@link PrepareCommitOperator}. */
public class Committable {

    private final long checkpointId;

    private final Kind kind;

    private final Object wrappedCommittable;

    public Committable(long checkpointId, Kind kind, Object wrappedCommittable) {
        this.checkpointId = checkpointId;
        this.kind = kind;
        this.wrappedCommittable = wrappedCommittable;
    }

    public long checkpointId() {
        return checkpointId;
    }

    public Kind kind() {
        return kind;
    }

    public Object wrappedCommittable() {
        return wrappedCommittable;
    }

    @Override
    public String toString() {
        return "Committable{"
                + "checkpointId="
                + checkpointId
                + ", kind="
                + kind
                + ", wrappedCommittable="
                + wrappedCommittable
                + '}';
    }

    /** Kind of the produced Committable. */
    public enum Kind {
        FILE((byte) 0),

        LOG_OFFSET((byte) 1);

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
                    return LOG_OFFSET;
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported byte value '" + value + "' for value kind.");
            }
        }
    }
}
