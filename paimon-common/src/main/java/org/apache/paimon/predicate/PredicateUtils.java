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

import java.util.List;
import java.util.function.Consumer;

/** A utils to handle {@link Predicate}. */
public class PredicateUtils {

    /**
     * Method for handling with CompoundPredicate.
     *
     * @param predicate CompoundPredicate to traverse handle
     * @param leafName LeafPredicate name
     * @param matchConsumer leafName matched handle
     * @param unMatchConsumer leafName unmatched handle
     */
    public static void traverseCompoundPredicate(
            Predicate predicate,
            String leafName,
            Consumer<Predicate> matchConsumer,
            Consumer<Predicate> unMatchConsumer) {
        if (!(predicate instanceof CompoundPredicate)) {
            throw new RuntimeException(
                    "PredicateUtils##handleCompoundPredicate should handle with a CompoundPredicate.");
        }
        CompoundPredicate compoundPredicate = (CompoundPredicate) predicate;
        List<Predicate> children = compoundPredicate.children();
        for (Predicate leaf : children) {
            if (leaf instanceof LeafPredicate
                    && (((LeafPredicate) leaf).function() instanceof Equal)
                    && leaf.visit(LeafPredicateExtractor.INSTANCE).get(leafName) != null) {
                matchConsumer.accept(leaf);
            } else {
                unMatchConsumer.accept(leaf);
            }
        }
    }
}
