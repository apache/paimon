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
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;

import java.io.IOException;
import java.util.Collection;

/** New {@link LookupFunction} for 1.16+, it supports Flink retry join. */
public class NewLookupFunction extends LookupFunction {

    private static final long serialVersionUID = 1L;

    private final FileStoreLookupFunction function;

    public NewLookupFunction(FileStoreLookupFunction function) {
        this.function = function;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        function.open(context);
    }

    @Override
    public Collection<RowData> lookup(RowData keyRow) throws IOException {
        return function.lookup(keyRow);
    }

    @Override
    public void close() throws Exception {
        function.close();
    }
}
