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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.types.DataType;

import java.util.List;
import java.util.Optional;

/**
 * A {@link NullFalseLeafBinaryFunction} to evaluate {@code filter like '%abc' or filter like
 * 'abc_'}.
 */
public class EndsWith extends NullFalseLeafBinaryFunction {

    public static final EndsWith INSTANCE = new EndsWith();

    private EndsWith() {}

    @Override
    public boolean test(DataType type, Object field, Object patternLiteral) {
        BinaryString fieldString = (BinaryString) field;
        return fieldString.endsWith((BinaryString) patternLiteral);
    }

    @Override
    public boolean test(
            DataType type,
            long rowCount,
            Object min,
            Object max,
            Long nullCount,
            Object patternLiteral) {
        BinaryString minStr = (BinaryString) min;
        BinaryString maxStr = (BinaryString) max;
        BinaryString pattern = (BinaryString) patternLiteral;
        return (minStr.endsWith(pattern) || minStr.compareTo(pattern) <= 0)
                && (maxStr.endsWith(pattern) || maxStr.compareTo(pattern) >= 0);
    }

    @Override
    public Optional<LeafFunction> negate() {
        return Optional.empty();
    }

    @Override
    public <T> T visit(FunctionVisitor<T> visitor, FieldRef fieldRef, List<Object> literals) {
        return visitor.visitEndsWith(fieldRef, literals.get(0));
    }
}
