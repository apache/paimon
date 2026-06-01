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

package dev.vortex.api;

import org.immutables.value.Value;

import java.util.Optional;
import java.util.OptionalLong;

/** Options for configuring a Vortex scan. */
@Value.Immutable
public interface ScanOptions {

    Optional<Expression> projection();

    Optional<Expression> filter();

    OptionalLong rowRangeBegin();

    OptionalLong rowRangeEnd();

    Optional<long[]> selectionIndices();

    @Value.Default
    default SelectionMode selectionMode() {
        return SelectionMode.INCLUDE_ALL;
    }

    OptionalLong limit();

    @Value.Default
    default boolean ordered() {
        return false;
    }

    /** Selection mode for row indices. */
    enum SelectionMode {
        INCLUDE_ALL((byte) 0),
        INCLUDE((byte) 1),
        EXCLUDE((byte) 2);

        private final byte code;

        SelectionMode(byte code) {
            this.code = code;
        }

        public byte code() {
            return code;
        }
    }
}
