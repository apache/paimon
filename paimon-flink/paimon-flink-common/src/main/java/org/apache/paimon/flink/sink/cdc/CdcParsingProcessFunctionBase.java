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

package org.apache.paimon.flink.sink.cdc;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;

/** Base class of {@link ProcessFunction} for CDC parsing. */
public abstract class CdcParsingProcessFunctionBase<T, O> extends ProcessFunction<T, O> {

    private final EventParser.Factory<T> parserFactory;
    protected final boolean caseSensitive;

    protected transient EventParser<T> parser;

    public CdcParsingProcessFunctionBase(
            EventParser.Factory<T> parserFactory, boolean caseSensitive) {
        this.parserFactory = parserFactory;
        this.caseSensitive = caseSensitive;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        parser = parserFactory.create();
        parser.setCaseSensitive(caseSensitive);
    }
}
