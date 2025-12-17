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

import org.apache.paimon.utils.Preconditions;

import java.io.Serializable;
import java.util.Objects;

/** Represents a sort order. */
public class SortValue implements Serializable {

    private static final long serialVersionUID = 1L;

    private final FieldRef field;
    private final SortDirection direction;
    private final NullOrdering nullOrdering;

    public SortValue(FieldRef field, SortDirection direction, NullOrdering nullOrdering) {
        this.field = Preconditions.checkNotNull(field);
        this.direction = Preconditions.checkNotNull(direction);
        this.nullOrdering = Preconditions.checkNotNull(nullOrdering);
    }

    public FieldRef field() {
        return field;
    }

    public SortDirection direction() {
        return direction;
    }

    public NullOrdering nullOrdering() {
        return nullOrdering;
    }

    @Override
    public String toString() {
        return String.format(
                "%s %s %s", field.name(), direction.toString(), nullOrdering.toString());
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SortValue sortValue = (SortValue) o;
        return Objects.equals(field, sortValue.field)
                && direction == sortValue.direction
                && nullOrdering == sortValue.nullOrdering;
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, direction, nullOrdering);
    }

    /** A null order used in sorting expressions. */
    public enum NullOrdering {
        NULLS_FIRST("NULLS FIRST"),
        NULLS_LAST("NULLS LAST");

        private final String name;

        NullOrdering(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /** A sort direction used in sorting expressions. */
    public enum SortDirection {
        ASCENDING("ASC"),
        DESCENDING("DESC");

        private final String name;

        SortDirection(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
