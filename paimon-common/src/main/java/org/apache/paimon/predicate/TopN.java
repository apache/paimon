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

package org.apache.paimon.predicate;

import org.apache.paimon.predicate.SortValue.NullOrdering;
import org.apache.paimon.predicate.SortValue.SortDirection;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.ListUtils.isNullOrEmpty;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Represents the TopN predicate. */
public class TopN implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<SortValue> orders;
    private final int limit;

    public TopN(List<SortValue> orders, int limit) {
        checkArgument(!isNullOrEmpty(orders), "orders should not be null or empty");
        this.orders = orders;
        this.limit = limit;
    }

    public TopN(FieldRef ref, SortDirection direction, NullOrdering nullOrdering, int limit) {
        SortValue order = new SortValue(ref, direction, nullOrdering);
        this.orders = Collections.singletonList(order);
        this.limit = limit;
    }

    public List<SortValue> orders() {
        return orders;
    }

    public int limit() {
        return limit;
    }

    @Override
    public String toString() {
        String sort = orders.stream().map(SortValue::toString).collect(Collectors.joining(", "));
        return String.format("Sort(%s), Limit(%s)", sort, limit);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TopN topN = (TopN) o;
        return limit == topN.limit && Objects.equals(orders, topN.orders);
    }

    @Override
    public int hashCode() {
        return Objects.hash(orders, limit);
    }
}
