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

package org.apache.paimon.codegen;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.Serializable;

/** Tests for {@link CodeGeneratorContext}. */
public class CodeGeneratorContextTest {

    @Test
    public void testAddReusableObjectInternal() {
        Object o = new TestClass();
        Thread.currentThread()
                .setContextClassLoader(ClassLoader.getSystemClassLoader().getParent());

        Assertions.assertThatCode(
                        () ->
                                new CodeGeneratorContext()
                                        .addReusableObjectInternal(o, "careless", "careless"))
                .doesNotThrowAnyException();
    }

    /**
     * This class is only for test, loaded by app classloader, and will not be found by its parent.
     */
    public static class TestClass implements Serializable {}
}
