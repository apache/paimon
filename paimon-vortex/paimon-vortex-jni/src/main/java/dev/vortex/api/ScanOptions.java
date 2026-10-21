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

    /** Sorted ascending, unique row indices. The native scan rejects any other order. */
    Optional<long[]> selectionIndices();

    /** A portable serialized Roaring64 bitmap of row indices. */
    Optional<byte[]> selectionRoaringBitmap();

    @Value.Default
    default SelectionMode selectionMode() {
        return SelectionMode.INCLUDE_ALL;
    }

    OptionalLong limit();

    @Value.Default
    default boolean ordered() {
        return false;
    }

    @Value.Check
    default void validateSelectionPayload() {
        boolean hasIndices = selectionIndices().isPresent();
        boolean hasBitmap = selectionRoaringBitmap().isPresent();
        if (hasIndices && hasBitmap) {
            throw new IllegalArgumentException(
                    "row selection must use either indices or a roaring bitmap, not both");
        }

        switch (selectionMode()) {
            case INCLUDE_ALL:
                if (hasIndices || hasBitmap) {
                    throw new IllegalArgumentException(
                            "row selection payload requires a selection mode");
                }
                break;
            case INCLUDE:
            case EXCLUDE:
                if (!hasIndices) {
                    throw new IllegalArgumentException(
                            "selection indices are required for index selection modes");
                }
                break;
            case INCLUDE_ROARING:
            case EXCLUDE_ROARING:
                if (!hasBitmap || selectionRoaringBitmap().get().length == 0) {
                    throw new IllegalArgumentException(
                            "a non-empty roaring bitmap is required for roaring selection modes");
                }
                break;
            default:
                throw new IllegalArgumentException("unknown selection mode " + selectionMode());
        }
    }

    /** Selection mode for row indices. */
    enum SelectionMode {
        INCLUDE_ALL((byte) 0),
        INCLUDE((byte) 1),
        EXCLUDE((byte) 2),
        INCLUDE_ROARING((byte) 3),
        EXCLUDE_ROARING((byte) 4);

        private final byte code;

        SelectionMode(byte code) {
            this.code = code;
        }

        public byte code() {
            return code;
        }
    }
}
