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

package org.apache.flink.table.store.file.predicate;

import org.apache.flink.table.store.file.stats.FieldStats;

import java.util.Optional;

/** A {@link LeafPredicate.Function} to eval is null. */
public class IsNull implements LeafPredicate.Function {

    public static final IsNull INSTANCE = new IsNull();

    private IsNull() {}

    @Override
    public boolean test(Object[] values, int index, Literal literal) {
        return values[index] == null;
    }

    @Override
    public boolean test(long rowCount, FieldStats[] fieldStats, int index, Literal literal) {
        return fieldStats[index].nullCount() > 0;
    }

    @Override
    public Optional<Predicate> negate(int index, Literal literal) {
        return Optional.of(new LeafPredicate(IsNotNull.INSTANCE, index, literal));
    }
}
