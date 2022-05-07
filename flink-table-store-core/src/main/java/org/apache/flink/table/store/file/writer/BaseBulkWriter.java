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

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.util.function.Function;

/**
 * Base bulk writer to delegate the generic data type writing into the {@link RowData} writer.
 *
 * @param <T> generic record type.
 */
public class BaseBulkWriter<T> implements BulkWriter<T> {

    private final BulkWriter<RowData> writer;

    // Convert the record from the generic type T to the concrete RowData type.
    private final Function<T, RowData> converter;

    public BaseBulkWriter(BulkWriter<RowData> writer, Function<T, RowData> converter) {
        this.writer = writer;
        this.converter = converter;
    }

    @Override
    public void addElement(T elem) throws IOException {
        writer.addElement(converter.apply(elem));
    }

    @Override
    public void flush() throws IOException {
        writer.flush();
    }

    @Override
    public void finish() throws IOException {
        writer.finish();
    }
}
