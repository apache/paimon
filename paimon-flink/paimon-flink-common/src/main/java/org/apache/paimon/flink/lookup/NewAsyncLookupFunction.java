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

package org.apache.paimon.flink.lookup;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionContext;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/** New {@link AsyncLookupFunction} for 1.16+, it supports Flink's unordered output. */
public class NewAsyncLookupFunction extends AsyncLookupFunction {

    private final FileStoreLookupFunction lookupFunction;

    public NewAsyncLookupFunction(FileStoreLookupFunction lookupFunction) {
        this.lookupFunction = lookupFunction;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        lookupFunction.open(context);
    }

    @Override
    public CompletableFuture<Collection<RowData>> asyncLookup(RowData rowData) {
        return lookupFunction.asyncLookup(rowData);
    }

    @Override
    public void close() throws Exception {
        lookupFunction.close();
    }
}
