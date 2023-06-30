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

import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;

import java.util.Arrays;
import java.util.List;

/** Inner {@link TableRead} contains filter and projection push down. */
public interface InnerTableRead extends TableRead {

    default InnerTableRead withFilter(List<Predicate> predicates) {
        if (predicates == null || predicates.isEmpty()) {
            return this;
        }
        return withFilter(PredicateBuilder.and(predicates));
    }

    InnerTableRead withFilter(Predicate predicate);

    default InnerTableRead withProjection(int[] projection) {
        if (projection == null) {
            return this;
        }
        int[][] nestedProjection =
                Arrays.stream(projection).mapToObj(i -> new int[] {i}).toArray(int[][]::new);
        return withProjection(nestedProjection);
    }

    InnerTableRead withProjection(int[][] projection);

    default InnerTableRead forceKeepDelete() {
        return this;
    }
}
