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

package org.apache.paimon.deletionvectors;

import org.apache.paimon.utils.Range;

import java.util.Objects;

/**
 * A deletion vector key backed by an inclusive row id range. Note that the deletion vector of this
 * key will store the relative offset within this range, just aligned with current FileNameKey.
 */
public final class RowIdRangeKey extends DeletionFileKey {

    private static final long serialVersionUID = 1L;

    private final Range range;

    public RowIdRangeKey(Range range) {
        this.range = range;
    }

    public Range range() {
        return range;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RowIdRangeKey that = (RowIdRangeKey) o;
        return Objects.equals(range, that.range);
    }

    @Override
    public int hashCode() {
        return Objects.hash(range);
    }

    @Override
    public String toString() {
        return "RowIdRangeKey{" + "range=" + range + '}';
    }

    @Override
    public Type type() {
        return Type.ROW_RANGE;
    }
}
