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

package org.apache.paimon.data.serializer;

import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.data.variant.Variant;

/** Test for {@link VariantSerializer}. */
public class VariantSerializerTest extends SerializerTestBase<Variant> {

    @Override
    protected Serializer<Variant> createSerializer() {
        return VariantSerializer.INSTANCE;
    }

    @Override
    protected boolean deepEquals(Variant t1, Variant t2) {
        return t1.equals(t2);
    }

    @Override
    protected Variant[] getTestData() {
        return new Variant[] {GenericVariant.fromJson("{\"age\":27,\"city\":\"Beijing\"}")};
    }
}
