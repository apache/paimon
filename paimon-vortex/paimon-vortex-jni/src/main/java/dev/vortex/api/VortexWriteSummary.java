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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * The size, row count and column statistics of a completed Vortex file.
 *
 * <p>The native writer calls the constructor through JNI, so its signature must not change.
 */
public final class VortexWriteSummary {

    private final long fileSize;
    private final long rowCount;
    private final List<VortexColumnStatistics> columnStatistics;

    public VortexWriteSummary(
            long fileSize, long rowCount, VortexColumnStatistics[] columnStatistics) {
        this.fileSize = fileSize;
        this.rowCount = rowCount;
        this.columnStatistics =
                Collections.unmodifiableList(Arrays.asList(columnStatistics.clone()));
    }

    /** Exact size of the completed file in bytes. */
    public long fileSize() {
        return fileSize;
    }

    /** Exact number of rows in the file. */
    public long rowCount() {
        return rowCount;
    }

    /** Per-column statistics in Arrow schema order. */
    public List<VortexColumnStatistics> columnStatistics() {
        return columnStatistics;
    }
}
