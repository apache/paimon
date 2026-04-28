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

package dev.vortex.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

/**
 * Utility class for managing Apache Arrow memory allocation.
 * <p>
 * This class provides a global shared root allocator for Arrow memory operations
 * used throughout the Vortex JNI layer. The allocator is configured with the maximum
 * available heap size to allow for efficient memory management.
 * </p>
 */
public final class ArrowAllocation {
    private static final RootAllocator ROOT_ALLOCATOR = new RootAllocator(Long.MAX_VALUE);

    private ArrowAllocation() {}

    /**
     * Returns the shared root allocator instance for Apache Arrow operations.
     * <p>
     * This allocator is shared across all Arrow operations in the JVM and is configured
     * to use the maximum available memory. It should be used as the parent allocator
     * for all Arrow memory operations within the Vortex system.
     * </p>
     *
     * @return the shared {@link BufferAllocator} instance
     */
    public static BufferAllocator rootAllocator() {
        return ROOT_ALLOCATOR;
    }
}
