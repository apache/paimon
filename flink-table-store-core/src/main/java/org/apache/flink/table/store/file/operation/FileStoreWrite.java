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
import org.apache.flink.table.store.file.writer.RecordWriter;

import java.util.List;
import java.util.concurrent.ExecutorService;

/** Write operation which provides {@link RecordWriter} creation. */
public interface FileStoreWrite {

    /** Create a {@link RecordWriter} from partition and bucket. */
    RecordWriter createWriter(BinaryRowData partition, int bucket, ExecutorService compactExecutor);

    /** Create an empty {@link RecordWriter} from partition and bucket. */
    RecordWriter createEmptyWriter(
            BinaryRowData partition, int bucket, ExecutorService compactExecutor);

    /** Create a compact {@link RecordWriter} from partition, bucket and compact units. */
    RecordWriter createCompactWriter(
            BinaryRowData partition, int bucket, List<DataFileMeta> restoredFiles);
}
