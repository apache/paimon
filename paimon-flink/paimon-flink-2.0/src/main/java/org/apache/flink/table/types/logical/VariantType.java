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

package org.apache.flink.table.types.logical;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Collections;
import java.util.List;

/**
 * Data type of semi-structured data.
 *
 * <p>The type supports storing any semi-structured data, including ARRAY, MAP, and scalar types.
 * VARIANT can only store MAP types with keys of type STRING.
 */
@PublicEvolving
public class VariantType extends LogicalType {

    public VariantType(boolean isNullable) {
        super(isNullable, LogicalTypeRoot.UNRESOLVED);
    }

    public VariantType() {
        this(true);
    }

    @Override
    public LogicalType copy(boolean b) {
        return null;
    }

    @Override
    public String asSerializableString() {
        return "";
    }

    @Override
    public boolean supportsInputConversion(Class<?> aClass) {
        return false;
    }

    @Override
    public boolean supportsOutputConversion(Class<?> aClass) {
        return false;
    }

    @Override
    public Class<?> getDefaultConversion() {
        return null;
    }

    @Override
    public List<LogicalType> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public <R> R accept(LogicalTypeVisitor<R> logicalTypeVisitor) {
        return null;
    }
}
