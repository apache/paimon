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

package org.apache.paimon.globalindex;

import org.apache.paimon.predicate.FieldRef;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** A contains index that can separate coarse candidate lookup from exact verification. */
public interface ContainsRefiningGlobalIndexReader extends GlobalIndexReader {

    /**
     * Returns a safe superset of rows matching all literals, or empty when the literals have no
     * useful coarse representation. False positives are allowed; false negatives are not. When
     * non-null, {@code candidates} contains exact matches from sibling predicates and should be
     * used as the initial posting-list intersection.
     */
    CompletableFuture<Optional<GlobalIndexResult>> visitContainsCandidates(
            FieldRef fieldRef, List<Object> literals, @Nullable GlobalIndexResult candidates);

    /**
     * Exactly evaluates all contains literals. When non-null, {@code candidates} is the
     * intersection of the available coarse results for these literals and any exact sibling
     * predicates. Implementations should verify the conjunction in one pass over their exact-value
     * store without repeating coarse lookups.
     */
    CompletableFuture<Optional<GlobalIndexResult>> visitContainsConjunction(
            FieldRef fieldRef, List<Object> literals, @Nullable GlobalIndexResult candidates);
}
