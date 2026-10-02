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

import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Resolves field references by name to positional indices and types in a read schema. Every
 * referenced field must exist; unresolved fields and malformed compound predicates are rejected.
 */
public final class PredicateRemapper implements PredicateVisitor<Predicate> {

    private final RowType rowType;

    private PredicateRemapper(RowType rowType) {
        this.rowType = rowType;
    }

    public static Predicate remap(Predicate predicate, RowType rowType) {
        return predicate.visit(new PredicateRemapper(rowType));
    }

    public static Transform remap(Transform transform, RowType rowType) {
        return transform.copyWithNewInputs(new PredicateRemapper(rowType).remapInputs(transform));
    }

    private List<Object> remapInputs(Transform transform) {
        List<Object> inputs = new ArrayList<>();
        for (Object input : transform.inputs()) {
            if (input instanceof FieldRef) {
                FieldRef field = (FieldRef) input;
                int index = rowType.getFieldIndex(field.name());
                checkArgument(
                        index >= 0,
                        "Cannot resolve field '%s' in read schema %s.",
                        field.name(),
                        rowType);
                inputs.add(new FieldRef(index, field.name(), rowType.getTypeAt(index)));
            } else {
                inputs.add(input);
            }
        }
        return inputs;
    }

    @Override
    public Predicate visit(LeafPredicate predicate) {
        return predicate.copyWithNewInputs(remapInputs(predicate.transform()));
    }

    @Override
    public Predicate visit(CompoundPredicate predicate) {
        checkArgument(predicate.function() != null, "Compound predicate function cannot be null.");
        checkArgument(predicate.children() != null, "Compound predicate children cannot be null.");
        checkArgument(
                !predicate.children().isEmpty(), "Compound predicate must contain a predicate.");
        List<Predicate> children = new ArrayList<>();
        for (Predicate child : predicate.children()) {
            checkArgument(child != null, "Compound predicate child cannot be null.");
            children.add(child.visit(this));
        }
        return children.size() == 1
                ? children.get(0)
                : new CompoundPredicate(predicate.function(), children);
    }
}
