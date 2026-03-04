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

package org.apache.paimon.flink.lineage;

import org.apache.paimon.table.Table;

import org.apache.flink.table.connector.sink.DynamicTableSink.SinkRuntimeProvider;
import org.apache.flink.table.connector.source.ScanTableSource.ScanRuntimeProvider;

/** Stub factory for Flink 1.x. Returns providers unchanged since lineage is not supported. */
public class DataStreamProviderFactory {

    /** Returns the provider unchanged. Flink 1.x does not support lineage. */
    public static ScanRuntimeProvider getScanProvider(
            ScanRuntimeProvider provider, String name, Table table) {
        return provider;
    }

    /** Returns the provider unchanged. Flink 1.x does not support lineage. */
    public static SinkRuntimeProvider getSinkProvider(
            SinkRuntimeProvider provider, String name, Table table) {
        return provider;
    }
}
