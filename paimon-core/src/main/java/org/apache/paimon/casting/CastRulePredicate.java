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
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.DataTypeRoot;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;

/**
 * In order to apply a {@link CastRule}, the runtime checks if a particular rule matches the tuple
 * of input and target type using this class. In particular, a rule is applied if:
 *
 * <ol>
 *   <li>{@link #getTargetTypeRoots()} includes the {@link DataTypeRoot} of target type and either
 *       <ol>
 *         <li>{@link #getInputTypeRoots()} includes the {@link DataTypeRoot} of input type or
 *         <li>{@link #getInputTypeFamilies()} includes one of the {@link DataTypeFamily} of input
 *             type
 *       </ol>
 *   <li>Or {@link #getTargetTypeFamilies()} includes one of the {@link DataTypeFamily} of target
 *       type and either
 *       <ol>
 *         <li>{@link #getInputTypeRoots()} includes the {@link DataTypeRoot} of input type or
 *         <li>{@link #getInputTypeFamilies()} includes one of the {@link DataTypeFamily} of input
 *             type
 *       </ol>
 *   <li>Or, if {@link #getCustomPredicate()} is not null, the input {@link DataType} and target
 *       {@link DataType} matches the predicate.
 * </ol>
 *
 * <p>The {@code customPredicate} should be used in cases where {@link DataTypeRoot} and {@link
 * DataTypeFamily} are not enough to identify whether a rule is applicable or not, for example when
 * the matching depends on a field of the provided input {@link DataType} instance.
 */
public class CastRulePredicate {

    private final Set<DataType> targetTypes;

    private final Set<DataTypeRoot> inputTypeRoots;
    private final Set<DataTypeRoot> targetTypeRoots;

    private final Set<DataTypeFamily> inputTypeFamilies;
    private final Set<DataTypeFamily> targetTypeFamilies;

    private final BiPredicate<DataType, DataType> customPredicate;

    private CastRulePredicate(
            Set<DataType> targetTypes,
            Set<DataTypeRoot> inputTypeRoots,
            Set<DataTypeRoot> targetTypeRoots,
            Set<DataTypeFamily> inputTypeFamilies,
            Set<DataTypeFamily> targetTypeFamilies,
            BiPredicate<DataType, DataType> customPredicate) {
        this.targetTypes = targetTypes;
        this.inputTypeRoots = inputTypeRoots;
        this.targetTypeRoots = targetTypeRoots;
        this.inputTypeFamilies = inputTypeFamilies;
        this.targetTypeFamilies = targetTypeFamilies;
        this.customPredicate = customPredicate;
    }

    public Set<DataType> getTargetTypes() {
        return targetTypes;
    }

    public Set<DataTypeRoot> getInputTypeRoots() {
        return inputTypeRoots;
    }

    public Set<DataTypeRoot> getTargetTypeRoots() {
        return targetTypeRoots;
    }

    public Set<DataTypeFamily> getInputTypeFamilies() {
        return inputTypeFamilies;
    }

    public Set<DataTypeFamily> getTargetTypeFamilies() {
        return targetTypeFamilies;
    }

    public Optional<BiPredicate<DataType, DataType>> getCustomPredicate() {
        return Optional.ofNullable(customPredicate);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for the {@link CastRulePredicate}. */
    public static class Builder {
        private final Set<DataTypeRoot> inputTypeRoots = new HashSet<>();
        private final Set<DataTypeRoot> targetTypeRoots = new HashSet<>();
        private final Set<DataType> targetTypes = new HashSet<>();

        private final Set<DataTypeFamily> inputTypeFamilies = new HashSet<>();
        private final Set<DataTypeFamily> targetTypeFamilies = new HashSet<>();

        private BiPredicate<DataType, DataType> customPredicate;

        public Builder input(DataTypeRoot inputTypeRoot) {
            inputTypeRoots.add(inputTypeRoot);
            return this;
        }

        public Builder target(DataTypeRoot outputTypeRoot) {
            targetTypeRoots.add(outputTypeRoot);
            return this;
        }

        public Builder target(DataType outputType) {
            targetTypes.add(outputType);
            return this;
        }

        public Builder input(DataTypeFamily inputTypeFamily) {
            inputTypeFamilies.add(inputTypeFamily);
            return this;
        }

        public Builder target(DataTypeFamily outputTypeFamily) {
            targetTypeFamilies.add(outputTypeFamily);
            return this;
        }

        public Builder predicate(BiPredicate<DataType, DataType> customPredicate) {
            this.customPredicate = customPredicate;
            return this;
        }

        public CastRulePredicate build() {
            return new CastRulePredicate(
                    Collections.unmodifiableSet(targetTypes),
                    Collections.unmodifiableSet(inputTypeRoots),
                    Collections.unmodifiableSet(targetTypeRoots),
                    Collections.unmodifiableSet(inputTypeFamilies),
                    Collections.unmodifiableSet(targetTypeFamilies),
                    customPredicate);
        }
    }
}
