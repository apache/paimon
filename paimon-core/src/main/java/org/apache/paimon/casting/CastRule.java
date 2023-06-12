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

package org.apache.paimon.casting;

import org.apache.paimon.types.DataType;

/**
 * A {@link CastRule} provides the logic to create a {@link CastExecutor} starting from the input
 * and the target types. A rule is matched using {@link CastRulePredicate}.
 *
 * @param <IN> Input internal type
 * @param <OUT> Output internal type
 */
public interface CastRule<IN, OUT> {

    /** @see CastRulePredicate for more details about a cast rule predicate definition. */
    CastRulePredicate getPredicateDefinition();

    /**
     * Create a {@link CastExecutor} starting from the provided input type. The returned {@link
     * CastExecutor} assumes the input value is using the internal data type, and it's a valid value
     * for the provided {@code targetType}.
     */
    CastExecutor<IN, OUT> create(DataType inputType, DataType targetType);
}
