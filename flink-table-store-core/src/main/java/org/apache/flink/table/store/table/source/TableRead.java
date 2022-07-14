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

package org.apache.flink.table.store.table.source;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.operation.FileStoreRead;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.utils.RecordReader;

import java.io.IOException;
import java.util.Arrays;

/** An abstraction layer above {@link FileStoreRead} to provide reading of {@link RowData}. */
public interface TableRead {

    TableRead withFilter(Predicate predicate);

    default TableRead withProjection(int[] projection) {
        int[][] nestedProjection =
                Arrays.stream(projection).mapToObj(i -> new int[] {i}).toArray(int[][]::new);
        return withProjection(nestedProjection);
    }

    TableRead withProjection(int[][] projection);

    RecordReader<RowData> createReader(Split split) throws IOException;
}
