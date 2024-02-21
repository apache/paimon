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

import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;

import java.io.Serializable;
import java.util.Optional;

/**
 * Predicate which returns Boolean and provides testing by stats.
 *
 * @see PredicateBuilder
 * @since 0.4.0
 */
@Public
public interface Predicate extends Serializable {

    /**
     * Test based on the specific input row.
     *
     * @return return true when hit, false when not hit.
     */
    boolean test(InternalRow row);

    /**
     * Test based on the statistical information to determine whether a hit is possible.
     *
     * @return return true is likely to hit (there may also be false positives), return false is
     *     absolutely not possible to hit.
     */
    boolean test(
            long rowCount, InternalRow minValues, InternalRow maxValues, InternalArray nullCounts);

    /** @return the negation predicate of this predicate if possible. */
    Optional<Predicate> negate();

    <T> T visit(PredicateVisitor<T> visitor);
}
