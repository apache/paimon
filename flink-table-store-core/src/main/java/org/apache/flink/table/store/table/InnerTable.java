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

package org.apache.flink.table.store.table;

import org.apache.flink.table.store.table.sink.BatchWriteBuilder;
import org.apache.flink.table.store.table.sink.BatchWriteBuilderImpl;
import org.apache.flink.table.store.table.sink.InnerTableCommit;
import org.apache.flink.table.store.table.sink.InnerTableWrite;
import org.apache.flink.table.store.table.sink.StreamWriteBuilder;
import org.apache.flink.table.store.table.sink.StreamWriteBuilderImpl;
import org.apache.flink.table.store.table.source.InnerStreamTableScan;
import org.apache.flink.table.store.table.source.InnerTableRead;
import org.apache.flink.table.store.table.source.InnerTableScan;
import org.apache.flink.table.store.table.source.ReadBuilder;
import org.apache.flink.table.store.table.source.ReadBuilderImpl;

/** Inner table for implementation, provide newScan, newRead ... directly. */
public interface InnerTable extends Table {

    InnerTableScan newScan();

    InnerStreamTableScan newStreamScan();

    InnerTableRead newRead();

    InnerTableWrite newWrite(String commitUser);

    InnerTableCommit newCommit(String commitUser);

    @Override
    default ReadBuilder newReadBuilder() {
        return new ReadBuilderImpl(this);
    }

    @Override
    default BatchWriteBuilder newBatchWriteBuilder() {
        return new BatchWriteBuilderImpl(this);
    }

    @Override
    default StreamWriteBuilder newStreamWriteBuilder() {
        return new StreamWriteBuilderImpl(this);
    }
}
