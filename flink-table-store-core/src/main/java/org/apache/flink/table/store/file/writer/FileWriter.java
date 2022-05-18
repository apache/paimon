/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.table.store.file.writer;

import org.apache.flink.core.fs.Path;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
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
     * @param record to write.
     * @throws IOException if encounter any IO error.
     */
    void write(T record) throws IOException;

    /**
     * Add records from {@link Iterator} to this file writer.
     *
     * @param records to write
     * @throws IOException if encounter any IO error.
     */
    default void write(Iterator<T> records) throws IOException {
        while (records.hasNext()) {
            write(records.next());
        }
    }

    /**
     * Add records from {@link Iterable} to file writer.
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
     * The estimated length of the current writer.
     *
     * @return the estimated length.
     * @throws IOException if encounter any IO error.
     */
    long length() throws IOException;

    /** Abort to clear orphan file(s) if encounter any error. */
    void abort();

    /** @return the result for this closed file writer. */
    R result() throws IOException;

    /** A factory that creates a {@link FileWriter}. */
    interface Factory<T, R> extends Serializable {

        /**
         * Creates a writer that writes to the given stream.
         *
         * @param path the path to write records.
         * @return the file format writer.
         * @throws IOException if any IO error was encountered then open the writer.
         */
        FileWriter<T, R> create(Path path) throws IOException;
    }
}
