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

package org.apache.paimon.function;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.types.DataField;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Function implementation. */
public class FunctionImpl implements Function {

    private final Identifier identifier;

    @Nullable private final List<DataField> inputParams;

    @Nullable private final List<DataField> returnParams;

    private final boolean deterministic;

    private final Map<String, FunctionDefinition> definitions;

    @Nullable private final String comment;

    private final Map<String, String> options;

    public FunctionImpl(
            Identifier identifier,
            @Nullable List<DataField> inputParams,
            @Nullable List<DataField> returnParams,
            boolean deterministic,
            Map<String, FunctionDefinition> definitions,
            @Nullable String comment,
            Map<String, String> options) {
        this.identifier = identifier;
        this.inputParams = inputParams;
        this.returnParams = returnParams;
        this.deterministic = deterministic;
        this.definitions = definitions;
        this.comment = comment;
        this.options = options;
    }

    public FunctionImpl(Identifier identifier, Map<String, FunctionDefinition> definitions) {
        this.identifier = identifier;
        this.inputParams = null;
        this.returnParams = null;
        this.deterministic = true;
        this.definitions = definitions;
        this.comment = null;
        this.options = Collections.emptyMap();
    }

    @Override
    public String name() {
        return identifier.getObjectName();
    }

    @Override
    public String fullName() {
        return identifier.getFullName();
    }

    public Identifier identifier() {
        return identifier;
    }

    @Override
    public Optional<List<DataField>> inputParams() {
        return Optional.ofNullable(inputParams);
    }

    @Override
    public Optional<List<DataField>> returnParams() {
        return Optional.ofNullable(returnParams);
    }

    @Override
    public boolean isDeterministic() {
        return deterministic;
    }

    @Override
    public Map<String, FunctionDefinition> definitions() {
        return definitions;
    }

    @Override
    public FunctionDefinition definition(String name) {
        return definitions.get(name);
    }

    @Override
    public String comment() {
        return comment;
    }

    @Override
    public Map<String, String> options() {
        return options;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FunctionImpl function = (FunctionImpl) o;
        return deterministic == function.deterministic
                && Objects.equals(identifier, function.identifier)
                && Objects.equals(inputParams, function.inputParams)
                && Objects.equals(returnParams, function.returnParams)
                && Objects.equals(definitions, function.definitions)
                && Objects.equals(comment, function.comment)
                && Objects.equals(options, function.options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                identifier,
                inputParams,
                returnParams,
                deterministic,
                definitions,
                comment,
                options);
    }
}
