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

package org.apache.flink.table.store.file.utils;

import java.io.IOException;
import java.util.function.Consumer;

/** Utils for {@link RecordReader}. */
public class RecordReaderUtils {

    /**
     * Performs the given action for each remaining element in {@link RecordReader} until all
     * elements have been processed or the action throws an exception.
     */
    public static <T> void forEachRemaining(
            final RecordReader<T> reader, final Consumer<? super T> action) throws IOException {
        RecordReader.RecordIterator<T> batch;
        T record;

        try {
            while ((batch = reader.readBatch()) != null) {
                while ((record = batch.next()) != null) {
                    action.accept(record);
                }
                batch.releaseBatch();
            }
        } finally {
            reader.close();
        }
    }
}
