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

import java.util.Iterator;

/**
 * An iterator interface for traversing Vortex arrays while providing type information
 * and resource management capabilities.
 *
 * <p>This interface extends both {@link Iterator} and {@link AutoCloseable}, allowing
 * for efficient iteration over array elements while ensuring proper cleanup of any
 * underlying resources. The iterator provides access to the data type of the arrays
 * being iterated over, which is useful for type-safe processing.</p>
 *
 * <p>Example usage:</p>
 * <pre>{@code
 * try (ArrayIterator iterator = array.getIterator()) {
 *     DType dataType = iterator.getDataType();
 *     while (iterator.hasNext()) {
 *         Array element = iterator.next();
 *         // Process element based on dataType
 *     }
 * }
 * }</pre>
 *
 * @see Array
 * @see DType
 * @see Iterator
 * @see AutoCloseable
 */
public interface ArrayIterator extends AutoCloseable, Iterator<Array> {
    /**
     * Returns the data type of the arrays that this iterator produces.
     *
     * <p>This method provides type information about the arrays that will be
     * returned by subsequent calls to {@link #next()}. The data type remains
     * constant throughout the lifetime of the iterator and can be used for
     * type-safe processing and validation.</p>
     *
     * @return the {@link DType} representing the data type of arrays produced by this iterator
     */
    DType getDataType();

    /**
     * Closes this iterator and releases any underlying resources.
     *
     * <p>This method should be called when the iterator is no longer needed to
     * ensure proper cleanup of any native resources or memory allocations.
     * After calling this method, the iterator should not be used for further
     * operations.</p>
     *
     * <p>It is recommended to use this iterator within a try-with-resources
     * statement to ensure automatic cleanup:</p>
     * <pre>{@code
     * try (ArrayIterator iterator = array.getIterator()) {
     *     // Use iterator
     * } // close() is called automatically
     * }</pre>
     *
     * <p>This method overrides {@link AutoCloseable#close()} and does not
     * throw any checked exceptions, making it safe to use in any context.</p>
     *
     * @see AutoCloseable#close()
     */
    @Override
    void close();
}
