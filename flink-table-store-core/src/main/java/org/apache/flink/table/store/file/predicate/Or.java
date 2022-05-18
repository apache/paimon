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

import java.util.Objects;
import java.util.Optional;

/** A {@link Predicate} to eval or. */
public class Or implements Predicate {

    private static final long serialVersionUID = 1L;

    private final Predicate predicate1;
    private final Predicate predicate2;

    public Or(Predicate predicate1, Predicate predicate2) {
        this.predicate1 = predicate1;
        this.predicate2 = predicate2;
    }

    @Override
    public boolean test(Object[] values) {
        return predicate1.test(values) || predicate2.test(values);
    }

    @Override
    public boolean test(long rowCount, FieldStats[] fieldStats) {
        return predicate1.test(rowCount, fieldStats) || predicate2.test(rowCount, fieldStats);
    }

    @Override
    public Optional<Predicate> negate() {
        Optional<Predicate> negate1 = predicate1.negate();
        Optional<Predicate> negate2 = predicate2.negate();
        if (negate1.isPresent() && negate2.isPresent()) {
            return Optional.of(new And(negate1.get(), negate2.get()));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Or)) {
            return false;
        }
        Or or = (Or) o;
        return predicate1.equals(or.predicate1) && predicate2.equals(or.predicate2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(predicate1, predicate2);
    }
}
