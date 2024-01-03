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

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.utils.Pair;

import java.util.Arrays;
import java.util.List;

/** Test for {@link TimestampSerializer}. */
public abstract class TimestampSerializerTest extends SerializerTestBase<Timestamp> {

    @Override
    protected Serializer<Timestamp> createSerializer() {
        return new TimestampSerializer(getPrecision());
    }

    @Override
    protected boolean deepEquals(Timestamp t1, Timestamp t2) {
        return t1.equals(t2);
    }

    @Override
    protected Timestamp[] getTestData() {
        return new Timestamp[] {
            Timestamp.fromEpochMillis(1),
            Timestamp.fromEpochMillis(2),
            Timestamp.fromEpochMillis(3),
            Timestamp.fromEpochMillis(4)
        };
    }

    @Override
    protected List<Pair<Timestamp, String>> getSerializableToStringTestData() {
        return Arrays.asList(
                Pair.of(Timestamp.fromEpochMillis(1), "1970-01-01T00:00:00.001"),
                Pair.of(Timestamp.fromEpochMillis(2), "1970-01-01T00:00:00.002"),
                Pair.of(Timestamp.fromEpochMillis(3), "1970-01-01T00:00:00.003"),
                Pair.of(Timestamp.fromEpochMillis(4, 1), "1970-01-01T00:00:00.004000001"));
    }

    protected abstract int getPrecision();

    static final class TimestampSerializer0Test extends TimestampSerializerTest {
        @Override
        protected int getPrecision() {
            return 0;
        }
    }

    static final class TimestampSerializer3Test extends TimestampSerializerTest {
        @Override
        protected int getPrecision() {
            return 3;
        }
    }

    static final class TimestampSerializer6Test extends TimestampSerializerTest {
        @Override
        protected int getPrecision() {
            return 6;
        }
    }

    static final class TimestampSerializer8Test extends TimestampSerializerTest {
        @Override
        protected int getPrecision() {
            return 8;
        }
    }
}
