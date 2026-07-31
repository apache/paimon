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

package org.apache.paimon.partition;

import java.time.LocalDateTime;
import java.time.temporal.TemporalAmount;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Resolves partition values to/from timestamp and extracts the minimum time step.
 *
 * <p>Use {@link #create(List, String, String)} to obtain an instance. If both {@code pattern} and
 * {@code formatter} are provided, a full pattern-based resolver is returned; otherwise a fallback
 * resolver that handles the unconfigured case is returned.
 */
public interface PartitionTimeResolvable {

    /** Parses partition column values into a {@link LocalDateTime}. */
    LocalDateTime parsePartitionValues(List<?> partitionValues);

    /** Formats a {@link LocalDateTime} into partition column values. */
    default LinkedHashMap<String, String> resolvePartitionValues(LocalDateTime dateTime) {
        throw new UnsupportedOperationException(
                "resolvePartitionValues is not supported by this resolver");
    }

    /** Extracts the minimum time step covered by the partition pattern and formatter. */
    default TemporalAmount extractMinStep() {
        throw new UnsupportedOperationException("extractMinStep is not supported by this resolver");
    }

    /**
     * Creates a {@link PartitionTimeResolvable}.
     *
     * <p>If both {@code pattern} and {@code formatter} are non-null, returns a normal {@link
     * PartitionTimeResolver}. If either is null, returns a fallback resolver that handles the
     * unconfigured case.
     */
    static PartitionTimeResolvable create(
            List<String> partitionColumns, String pattern, String formatter) {
        if (pattern == null || formatter == null) {
            return PartitionTimeResolver.createFallback(partitionColumns, pattern, formatter);
        }
        return new PartitionTimeResolver(partitionColumns, pattern, formatter);
    }
}
