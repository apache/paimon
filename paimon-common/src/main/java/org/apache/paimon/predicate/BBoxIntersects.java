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

import org.apache.paimon.geo.BoundingBox;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

import java.util.List;
import java.util.Optional;

/**
 * Tests intersection of a geometry's XY bounding box with a closed rectangle.
 *
 * <p>This is an envelope predicate. Callers accelerating an exact spatial relation must retain
 * their original geometry predicate for refinement. Its complement is deliberately not pushed down.
 */
public class BBoxIntersects extends LeafFunction {

    private static final long serialVersionUID = 1L;

    public static final String NAME = "BBOX_INTERSECTS";
    public static final BBoxIntersects INSTANCE = new BBoxIntersects();

    private BBoxIntersects() {}

    @Override
    public boolean test(DataType type, Object field, List<Object> literals) {
        return field != null && BoundingBox.fromWkb((byte[]) field).intersects(bounds(literals));
    }

    @Override
    public boolean test(
            DataType type,
            long rowCount,
            Object min,
            Object max,
            Long nullCount,
            List<Object> literals) {
        // Binary min/max are not spatial bounds.
        return nullCount == null || nullCount != rowCount;
    }

    @Override
    public DataType literalType(DataType fieldType) {
        return DataTypes.DOUBLE();
    }

    @Override
    public Optional<LeafFunction> negate() {
        return Optional.empty();
    }

    @Override
    public <T> T visit(FunctionVisitor<T> visitor, FieldRef fieldRef, List<Object> literals) {
        BoundingBox box = bounds(literals);
        return visitor.visitBBoxIntersects(
                fieldRef, box.minX(), box.minY(), box.maxX(), box.maxY());
    }

    private static BoundingBox bounds(List<Object> literals) {
        return BoundingBox.of(
                (double) literals.get(0),
                (double) literals.get(1),
                (double) literals.get(2),
                (double) literals.get(3));
    }

    @Override
    public String toJson() {
        return NAME;
    }
}
