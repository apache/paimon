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

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.io.Serializable;
import java.util.Objects;
import java.util.Random;

/** Test for {@link JavaSerializer}. */
public class JavaSerializerTest extends SerializerTestBase<JavaSerializerTest.TestClass> {

    @Override
    protected TypeSerializer<TestClass> createSerializer() {
        return new JavaSerializer<>(TestClass.class);
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<TestClass> getTypeClass() {
        return TestClass.class;
    }

    @Override
    protected TestClass[] getTestData() {
        Random rnd = new Random();
        int rndInt = rnd.nextInt();

        Integer[] integers =
                new Integer[] {0, 1, -1, Integer.MAX_VALUE, Integer.MIN_VALUE, rndInt, -rndInt};
        TestClass[] testClasses = new TestClass[integers.length];
        for (int i = 0; i < integers.length; i++) {
            testClasses[i] = new TestClass(integers[i]);
        }
        return testClasses;
    }

    static class TestClass implements Serializable {
        int v;

        public TestClass() {}

        public TestClass(int v) {
            this.v = v;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestClass testClass = (TestClass) o;
            return v == testClass.v;
        }

        @Override
        public int hashCode() {
            return Objects.hash(v);
        }
    }
}
