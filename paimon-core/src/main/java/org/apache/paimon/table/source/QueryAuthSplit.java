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

package org.apache.paimon.table.source;

import org.apache.paimon.catalog.TableQueryAuthResult;

import javax.annotation.Nullable;

/** A wrapper class for {@link Split} that adds query authorization information. */
public class QueryAuthSplit implements Split {

    private static final long serialVersionUID = 1L;

    private final Split split;
    private final TableQueryAuthResult authResult;

    public QueryAuthSplit(Split split, TableQueryAuthResult authResult) {
        this.split = split;
        this.authResult = authResult;
    }

    public Split split() {
        return split;
    }

    @Nullable
    public TableQueryAuthResult authResult() {
        return authResult;
    }

    @Override
    public long rowCount() {
        return split.rowCount();
    }
}
