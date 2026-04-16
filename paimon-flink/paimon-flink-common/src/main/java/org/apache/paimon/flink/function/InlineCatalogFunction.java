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

package org.apache.paimon.flink.function;

import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.FunctionLanguage;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.resource.ResourceUri;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * A {@link CatalogFunction} that wraps an already-instantiated {@link ScalarFunction}. This allows
 * passing catalog-aware function instances that require constructor parameters (e.g. catalog
 * options) through the catalog function resolution path.
 */
public class InlineCatalogFunction implements CatalogFunction {

    private final ScalarFunction function;

    public InlineCatalogFunction(ScalarFunction function) {
        this.function = function;
    }

    public ScalarFunction getFunction() {
        return function;
    }

    @Override
    public String getClassName() {
        return function.getClass().getName();
    }

    @Override
    public CatalogFunction copy() {
        return new InlineCatalogFunction(function);
    }

    @Override
    public Optional<String> getDescription() {
        return Optional.empty();
    }

    @Override
    public Optional<String> getDetailedDescription() {
        return Optional.empty();
    }

    @Override
    public boolean isGeneric() {
        return true;
    }

    @Override
    public FunctionLanguage getFunctionLanguage() {
        return FunctionLanguage.JAVA;
    }

    @Override
    public List<ResourceUri> getFunctionResources() {
        return Collections.emptyList();
    }
}
