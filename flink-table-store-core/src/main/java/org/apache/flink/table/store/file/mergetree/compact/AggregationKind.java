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

package org.apache.flink.table.store.file.mergetree.compact;

import java.util.Locale;

/** Aggregate kinds. */
public enum AggregationKind {
    Sum,
    Max,
    Min,
    Avg;

    public static AggregationKind fromString(String name) {
        switch (name.toLowerCase(Locale.ROOT)) {
            case "sum":
                return Sum;
            case "max":
                return Max;
            case "min":
                return Min;
            case "avg":
                return Avg;
            default:
                throw new IllegalArgumentException("Unknown aggregation kind: " + name);
        }
    }
}
