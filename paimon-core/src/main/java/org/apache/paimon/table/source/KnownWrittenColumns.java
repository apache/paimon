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

package org.apache.paimon.table.source;

import org.apache.paimon.annotation.Experimental;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;

/** A complete, immutable set of written field ids, ordered by field id. */
@Experimental
public final class KnownWrittenColumns implements WrittenColumns {

    private static final long serialVersionUID = 1L;

    private final List<Integer> fieldIds;

    public KnownWrittenColumns(Collection<Integer> fieldIds) {
        this.fieldIds = Collections.unmodifiableList(new ArrayList<>(new TreeSet<>(fieldIds)));
    }

    public List<Integer> fieldIds() {
        return fieldIds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KnownWrittenColumns)) {
            return false;
        }
        KnownWrittenColumns that = (KnownWrittenColumns) o;
        return fieldIds.equals(that.fieldIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldIds);
    }

    @Override
    public String toString() {
        return "KnownWrittenColumns{" + "fieldIds=" + fieldIds + '}';
    }
}
