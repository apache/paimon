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

package org.apache.paimon.data;

import org.apache.paimon.memory.MemorySegment;

/** A {@link Segments} with limit in last segment. */
public class SingleSegments implements Segments {

    private final MemorySegment segment;
    private final int limit;

    public SingleSegments(MemorySegment segment, int limit) {
        this.segment = segment;
        this.limit = limit;
    }

    public MemorySegment segment() {
        return segment;
    }

    public int limit() {
        return limit;
    }

    @Override
    public long totalMemorySize() {
        return segment.size();
    }
}
