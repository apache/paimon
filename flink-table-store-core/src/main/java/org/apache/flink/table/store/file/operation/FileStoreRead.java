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

package org.apache.flink.table.store.file.operation;

import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.utils.RecordReader;

import java.io.IOException;
import java.util.List;

/**
 * Read operation which provides {@link RecordReader} creation.
 *
 * @param <T> type of record to read.
 */
public interface FileStoreRead<T> {

    /** Create a {@link RecordReader} from partition and bucket and files. */
    RecordReader<T> createReader(BinaryRowData partition, int bucket, List<DataFileMeta> files)
            throws IOException;
}
