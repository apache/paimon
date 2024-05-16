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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Extract leaf predicate for field names. */
public class LeafPredicateExtractor implements PredicateVisitor<Map<String, LeafPredicate>> {

    public static final LeafPredicateExtractor INSTANT = new LeafPredicateExtractor();

    @Override
    public Map<String, LeafPredicate> visit(LeafPredicate predicate) {
        return Collections.singletonMap(predicate.fieldName(), predicate);
    }

    @Override
    public Map<String, LeafPredicate> visit(CompoundPredicate predicate) {
        if (predicate.function() instanceof And) {
            Map<String, LeafPredicate> leafPredicates = new HashMap<>();
            predicate.children().stream().map(p -> p.visit(this)).forEach(leafPredicates::putAll);
            return leafPredicates;
        }
        return Collections.emptyMap();
    }
}
