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

/**
 * Interface for reading Vortex format files, providing access to schema information,
 * row metadata, and configurable scanning capabilities.
 *
 * <p>A {@code File} represents a Vortex format file that has been opened for reading.
 * It provides methods to inspect the file's schema, count rows, and create iterators
 * for scanning the data with various filtering and projection options. This interface
 * extends {@link AutoCloseable} to ensure proper resource cleanup when the file
 * is no longer needed.</p>
 *
 * <p>Example usage:</p>
 * <pre>{@code
 * try (File file = VortexReader.open(path)) {
 *     DType schema = file.getDType();
 *     long totalRows = file.rowCount();
 *
 *     ScanOptions options = ScanOptions.builder()
 *         .columns(List.of("name", "age"))
 *         .build();
 *
 *     try (ArrayIterator iterator = file.newScan(options)) {
 *         while (iterator.hasNext()) {
 *             Array batch = iterator.next();
 *             // Process batch
 *         }
 *     }
 * }
 * }</pre>
 *
 * @see ScanOptions
 * @see ArrayIterator
 * @see DType
 * @see AutoCloseable
 */
public interface File extends AutoCloseable {
    /**
     * Returns the data type (schema) of this Vortex file.
     *
     * <p>The returned {@link DType} describes the logical structure and types
     * of the data contained in this file. For structured data, this will typically
     * be a {@link DType.Variant#STRUCT} containing field names and their corresponding
     * data types. The schema remains constant for the lifetime of the file.</p>
     *
     * @return the {@link DType} representing the schema of this file
     */
    DType getDType();

    /**
     * Returns the total number of rows in this Vortex file.
     *
     * <p>This method provides the count of logical rows contained in the file,
     * which represents the number of records or tuples that can be read. This
     * count is independent of any filtering or projection that may be applied
     * during scanning operations.</p>
     *
     * @return the total number of rows as a non-negative long value
     */
    long rowCount();

    /**
     * Creates a new iterator for scanning this file with the specified options.
     *
     * <p>This method returns an {@link ArrayIterator} that can be used to traverse
     * the data in this file according to the provided {@link ScanOptions}. The
     * scan options allow for column projection, row filtering via predicates,
     * and row range or index selection. Each call to this method creates a new
     * independent iterator.</p>
     *
     * <p>The returned iterator must be properly closed when no longer needed to
     * release any underlying resources. It is recommended to use the iterator
     * within a try-with-resources statement.</p>
     *
     * @param options the {@link ScanOptions} configuring the scan behavior,
     *                including column selection, filtering, and row selection
     * @return a new {@link ArrayIterator} for scanning the file data
     * @throws RuntimeException if the scan options contain invalid
     *                                  column names or conflicting row selection criteria
     * @see ScanOptions
     * @see ArrayIterator
     */
    ArrayIterator newScan(ScanOptions options);

    /**
     * Closes this file and releases any associated resources.
     *
     * <p>This method should be called when the file is no longer needed to ensure
     * proper cleanup of any underlying file handles, native memory, or other resources.
     * After calling this method, the file should not be used for any further operations.
     * This method is idempotent and can be called multiple times safely.</p>
     *
     * <p>It is recommended to use this file within a try-with-resources statement
     * to ensure automatic cleanup:</p>
     * <pre>{@code
     * try (File file = VortexReader.open(path)) {
     *     // Use file
     * } // close() is called automatically
     * }</pre>
     */
    @Override
    void close();
}
