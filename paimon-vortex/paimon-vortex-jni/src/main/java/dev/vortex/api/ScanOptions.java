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

import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * Create a new set of options for configuring the scan.
 */
@Value.Immutable
public interface ScanOptions {
    /**
     * Columns to project out.
     */
    List<String> columns();

    /**
     * Optional pruning expression that is pushed down to the scan.
     */
    Optional<Expression> predicate();

    /**
     * Optional start (inclusive) and end (exclusive) row indices to select a range of rows
     * in the scan.
     */
    Optional<long[]> rowRange();

    /**
     * Optional row indices to select specific rows.
     * These must be sorted in ascending order.
     */
    Optional<long[]> rowIndices();

    /**
     * Creates a new ScanOptions instance with default values.
     *
     * @return a ScanOptions instance with empty columns list, no predicate, no row range, and no row indices
     */
    static ScanOptions of() {
        return ImmutableScanOptions.builder().build();
    }

    /**
     * Creates a new builder for constructing ScanOptions instances.
     *
     * @return a new builder instance that can be used to configure and build ScanOptions
     */
    static ImmutableScanOptions.Builder builder() {
        return ImmutableScanOptions.builder();
    }
}
