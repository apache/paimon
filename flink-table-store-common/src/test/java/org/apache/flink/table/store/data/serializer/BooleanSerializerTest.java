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

package org.apache.flink.table.store.data.serializer;

import java.util.Random;

/** Test for {@link BooleanSerializer}. */
public class BooleanSerializerTest extends SerializerTestBase<Boolean> {

    @Override
    protected Serializer<Boolean> createSerializer() {
        return BooleanSerializer.INSTANCE;
    }

    @Override
    protected boolean deepEquals(Boolean t1, Boolean t2) {
        return t1.equals(t2);
    }

    @Override
    protected Boolean[] getTestData() {
        Random rnd = new Random();

        return new Boolean[] {
            Boolean.TRUE, Boolean.FALSE, rnd.nextBoolean(), rnd.nextBoolean(), rnd.nextBoolean()
        };
    }
}
