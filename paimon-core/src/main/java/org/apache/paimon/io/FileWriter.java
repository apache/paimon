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

package org.apache.paimon.io;

import org.apache.paimon.utils.CloseableIterator;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

/**
 * File writer to accept one record or a branch of records and generate metadata after closing it.
 *
 * @param <T> record type.
 * @param <R> file result to collect.
 */
public interface FileWriter<T, R> extends Closeable {

    /**
     * Add only one record to this file writer.
     *
     * <p>NOTE: If any exception occurs during writing, the writer should clean up useless files for
     * the user.
     *
     * @param record to write.
     * @throws IOException if encounter any IO error.
     */
    void write(T record) throws IOException;

    /**
     * Add records from {@link Iterator} to this file writer.
     *
     * <p>NOTE: If any exception occurs during writing, the writer should clean up useless files for
     * the user.
     *
     * @param records to write
     * @throws IOException if encounter any IO error.
     */
    default void write(Iterator<T> records) throws Exception {
        while (records.hasNext()) {
            write(records.next());
        }
    }

    /**
     * Add records from {@link CloseableIterator} to this file writer.
     *
     * <p>NOTE: If any exception occurs during writing, the writer should clean up useless files for
     * the user.
     *
     * @param records to write
     * @throws IOException if encounter any IO error.
     */
    default void write(CloseableIterator<T> records) throws Exception {
        try {
            while (records.hasNext()) {
                write(records.next());
            }
        } finally {
            records.close();
        }
    }

    /**
     * Add records from {@link Iterable} to file writer.
     *
     * <p>NOTE: If any exception occurs during writing, the writer should clean up useless files for
     * the user.
     *
     * @param records to write.
     * @throws IOException if encounter any IO error.
     */
    default void write(Iterable<T> records) throws IOException {
        for (T record : records) {
            write(record);
        }
    }

    /**
     * The total written record count.
     *
     * @return record count.
     */
    long recordCount();

    /**
     * Abort to clear orphan file(s) if encounter any error.
     *
     * <p>NOTE: This implementation must be reentrant.
     */
    void abort();

    /** @return the result for this closed file writer. */
    R result() throws IOException;
}
