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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** A utils to handle {@link Predicate}. */
public class InPredicateVisitor {

    /**
     * Method for handling with In CompoundPredicate.
     *
     * @param predicate CompoundPredicate to traverse handle
     * @param leafName LeafPredicate name
     */
    public static Optional<List<Object>> extractInElements(Predicate predicate, String leafName) {
        if (!(predicate instanceof CompoundPredicate)) {
            return Optional.empty();
        }

        CompoundPredicate compoundPredicate = (CompoundPredicate) predicate;
        List<Object> leafValues = new ArrayList<>();
        List<Predicate> children = compoundPredicate.children();
        for (Predicate leaf : children) {
            if (leaf instanceof LeafPredicate
                    && (((LeafPredicate) leaf).function() instanceof Equal)
                    && leaf.visit(LeafPredicateExtractor.INSTANCE).get(leafName) != null) {
                leafValues.add(((LeafPredicate) leaf).literals().get(0));
            } else {
                return Optional.empty();
            }
        }
        return Optional.of(leafValues);
    }
}
