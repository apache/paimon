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

package org.apache.flink.table.store.file.stats;

import org.apache.flink.table.store.file.TestKeyValueGenerator;
import org.apache.flink.table.store.file.utils.ObjectSerializer;
import org.apache.flink.table.store.file.utils.ObjectSerializerTestBase;

import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FieldStatsArraySerializer}. */
public class FieldStatsArraySerializerTest extends ObjectSerializerTestBase<FieldStats[]> {

    public TestKeyValueGenerator gen = new TestKeyValueGenerator();

    @Override
    protected ObjectSerializer<FieldStats[]> serializer() {
        return new FieldStatsArraySerializer(TestKeyValueGenerator.ROW_TYPE);
    }

    @Override
    protected FieldStats[] object() {
        FieldStatsCollector collector = new FieldStatsCollector(TestKeyValueGenerator.ROW_TYPE);
        for (int i = 0; i < 10; i++) {
            collector.collect(gen.next().value());
        }
        FieldStats[] result = collector.extract();

        // as stated in RollingFile.Writer#finish, field stats are not collected currently so
        // min/max values are all nulls
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int numFieldsNotCollected = random.nextInt(result.length + 1);
        for (int i = 0; i < numFieldsNotCollected; i++) {
            result[random.nextInt(result.length)] = new FieldStats(null, null, 0);
        }

        return result;
    }

    @Override
    protected void checkResult(FieldStats[] expected, FieldStats[] actual) {
        assertThat(actual).isEqualTo(expected);
    }
}
