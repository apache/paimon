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

package org.apache.paimon.catalog;

import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.Transform;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Map;

/** Auth result for table query, including row level filter and optional column masking rules. */
public class TableQueryAuthResult {

    @Nullable private final Predicate rowFilter;
    private final Map<String, Transform> columnMasking;

    public TableQueryAuthResult(
            @Nullable Predicate rowFilter, Map<String, Transform> columnMasking) {
        this.rowFilter = rowFilter;
        this.columnMasking = columnMasking == null ? Collections.emptyMap() : columnMasking;
    }

    public static TableQueryAuthResult empty() {
        return new TableQueryAuthResult(null, Collections.emptyMap());
    }

    @Nullable
    public Predicate rowFilter() {
        return rowFilter;
    }

    public Map<String, Transform> columnMasking() {
        return columnMasking;
    }
}
