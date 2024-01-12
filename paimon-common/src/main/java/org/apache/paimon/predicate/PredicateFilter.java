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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;

import javax.annotation.Nullable;

import java.util.List;

/** A {@link Filter} to filter {@link InternalRow}. */
public class PredicateFilter implements Filter<InternalRow> {

    private final RowDataToObjectArrayConverter arrayConverter;
    @Nullable private final Predicate predicate;

    public PredicateFilter(RowType rowType, List<Predicate> predicates) {
        this(rowType, predicates.isEmpty() ? null : PredicateBuilder.and(predicates));
    }

    public PredicateFilter(RowType rowType, @Nullable Predicate predicate) {
        this.arrayConverter = new RowDataToObjectArrayConverter(rowType);
        this.predicate = predicate;
    }

    @Override
    public boolean test(InternalRow rowData) {
        return predicate == null || predicate.test(arrayConverter.convert(rowData));
    }
}
