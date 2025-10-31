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

/** Visit the predicate and check if it only contains partition key's predicate. */
public class PartitionPredicateVisitor implements PredicateVisitor<Boolean> {

    private final List<String> partitionKeys;

    public PartitionPredicateVisitor(List<String> partitionKeys) {
        this.partitionKeys = partitionKeys;
    }

    @Override
    public Boolean visit(LeafPredicate predicate) {
        return partitionKeys.contains(predicate.fieldName());
    }

    @Override
    public Boolean visit(CompoundPredicate predicate) {
        for (Predicate child : predicate.children()) {
            Boolean matched = child.visit(this);

            if (!matched) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Boolean visit(TransformPredicate predicate) {
        Transform transform = predicate.transform();
        for (Object input : transform.inputs()) {
            if (input instanceof FieldRef) {
                if (!partitionKeys.contains(((FieldRef) input).name())) {
                    return false;
                }
            }
        }
        return true;
    }
}
