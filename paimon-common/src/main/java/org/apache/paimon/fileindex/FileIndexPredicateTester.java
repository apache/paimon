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

package org.apache.paimon.fileindex;

import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Or;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateVisitor;

/** Predicate test. */
public class FileIndexPredicateTester implements PredicateVisitor<Boolean> {

    private final String columnName;
    private final FileIndex fileIndex;

    public FileIndexPredicateTester(String columnName, FileIndex fileIndex) {
        this.columnName = columnName;
        this.fileIndex = fileIndex;
    }

    @Override
    public Boolean visit(LeafPredicate predicate) {
        if (columnName.equals(predicate.fieldName())) {
            return predicate
                    .function()
                    .visit(
                            fileIndex,
                            new FieldRef(
                                    predicate.index(), predicate.fieldName(), predicate.type()),
                            predicate.literals());
        }
        return true;
    }

    @Override
    public Boolean visit(CompoundPredicate predicate) {

        if (predicate.function() instanceof Or) {
            for (Predicate predicate1 : predicate.children()) {
                if (predicate1.visit(this)) {
                    return true;
                }
            }
            return false;

        } else {
            for (Predicate predicate1 : predicate.children()) {
                if (!predicate1.visit(this)) {
                    return false;
                }
            }
            return true;
        }
    }
}
