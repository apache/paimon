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

import org.apache.flink.table.connector.source.lookup.AsyncLookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.LookupFunction;

/** A provider to provide both sync and async function. */
public class SyncAsyncLookupFunctionProvider
        implements LookupFunctionProvider, AsyncLookupFunctionProvider {

    private final NewLookupFunction function;
    private final int asyncThreadNumber;

    public SyncAsyncLookupFunctionProvider(NewLookupFunction function, int asyncThreadNumber) {
        this.function = function;
        this.asyncThreadNumber = asyncThreadNumber;
    }

    @Override
    public AsyncLookupFunction createAsyncLookupFunction() {
        return new AsyncLookupFunctionWrapper(function, asyncThreadNumber);
    }

    @Override
    public LookupFunction createLookupFunction() {
        return function;
    }
}
