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

package org.apache.paimon.flink.utils;

import org.apache.flink.api.common.typeutils.TypeInformationTestBase;

import java.io.Serializable;

/** Test for {@link JavaTypeInfo}. */
class JavaTypeInfoTest extends TypeInformationTestBase<JavaTypeInfo<?>> {

    @Override
    protected JavaTypeInfo<?>[] getTestData() {
        return new JavaTypeInfo<?>[] {
            new JavaTypeInfo<>(TestClass.class), new JavaTypeInfo<>(AlternativeClass.class)
        };
    }

    static class TestClass implements Serializable {}

    static class AlternativeClass implements Serializable {}
}
