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

package org.apache.paimon.globalindex;

import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link GlobalIndexResultSerializer}. */
public class GlobalIndexSerDeUtilsTest {

    @Test
    public void testSerializeAndDeserializeGlobalIndexResult() throws IOException {
        RoaringNavigableMap64 bitmap = RoaringNavigableMap64.bitmapOf(1, 5, 10, 100, 1000);
        GlobalIndexResult original = GlobalIndexResult.create(() -> bitmap);

        byte[] serialized = serialize(original);
        GlobalIndexResult deserialized = deserialize(serialized);

        assertThat(deserialized).isNotInstanceOf(VectorSearchGlobalIndexResult.class);
        assertThat(deserialized.results()).isEqualTo(bitmap);
    }

    @Test
    public void testSerializeAndDeserializeEmptyGlobalIndexResult() throws IOException {
        GlobalIndexResult original = GlobalIndexResult.createEmpty();

        byte[] serialized = serialize(original);
        GlobalIndexResult deserialized = deserialize(serialized);

        assertThat(deserialized).isNotInstanceOf(VectorSearchGlobalIndexResult.class);
        assertThat(deserialized.results().isEmpty()).isTrue();
    }

    @Test
    public void testSerializeAndDeserializeTopkGlobalIndexResult() throws IOException {
        RoaringNavigableMap64 bitmap = RoaringNavigableMap64.bitmapOf(1, 5, 10, 100);
        Map<Long, Float> scoreMap = new HashMap<>();
        scoreMap.put(1L, 0.9f);
        scoreMap.put(5L, 0.8f);
        scoreMap.put(10L, 0.7f);
        scoreMap.put(100L, 0.6f);

        VectorSearchGlobalIndexResult original =
                VectorSearchGlobalIndexResult.create(() -> bitmap, scoreMap::get);

        byte[] serialized = serialize(original);
        GlobalIndexResult deserialized = deserialize(serialized);

        assertThat(deserialized).isInstanceOf(VectorSearchGlobalIndexResult.class);
        assertThat(deserialized.results()).isEqualTo(bitmap);

        VectorSearchGlobalIndexResult topkResult = (VectorSearchGlobalIndexResult) deserialized;
        ScoreGetter scoreGetter = topkResult.scoreGetter();
        assertThat(scoreGetter.score(1L)).isEqualTo(0.9f);
        assertThat(scoreGetter.score(5L)).isEqualTo(0.8f);
        assertThat(scoreGetter.score(10L)).isEqualTo(0.7f);
        assertThat(scoreGetter.score(100L)).isEqualTo(0.6f);
    }

    @Test
    public void testSerializeAndDeserializeTopkWithLargeRowIds() throws IOException {
        RoaringNavigableMap64 bitmap =
                RoaringNavigableMap64.bitmapOf(
                        Integer.MAX_VALUE + 1L, Integer.MAX_VALUE + 100L, Long.MAX_VALUE - 1);
        Map<Long, Float> scoreMap = new HashMap<>();
        scoreMap.put(Integer.MAX_VALUE + 1L, 0.5f);
        scoreMap.put(Integer.MAX_VALUE + 100L, 0.3f);
        scoreMap.put(Long.MAX_VALUE - 1, 0.1f);

        VectorSearchGlobalIndexResult original =
                VectorSearchGlobalIndexResult.create(() -> bitmap, scoreMap::get);

        byte[] serialized = serialize(original);
        GlobalIndexResult deserialized = deserialize(serialized);

        assertThat(deserialized).isInstanceOf(VectorSearchGlobalIndexResult.class);
        assertThat(deserialized.results()).isEqualTo(bitmap);

        VectorSearchGlobalIndexResult topkResult = (VectorSearchGlobalIndexResult) deserialized;
        ScoreGetter scoreGetter = topkResult.scoreGetter();
        assertThat(scoreGetter.score(Integer.MAX_VALUE + 1L)).isEqualTo(0.5f);
        assertThat(scoreGetter.score(Integer.MAX_VALUE + 100L)).isEqualTo(0.3f);
        assertThat(scoreGetter.score(Long.MAX_VALUE - 1)).isEqualTo(0.1f);
    }

    private byte[] serialize(GlobalIndexResult result) throws IOException {
        GlobalIndexResultSerializer globalIndexResultSerializer = new GlobalIndexResultSerializer();
        DataOutputSerializer dataOutputSerializer = new DataOutputSerializer(1024);
        globalIndexResultSerializer.serialize(result, dataOutputSerializer);
        return dataOutputSerializer.getCopyOfBuffer();
    }

    private GlobalIndexResult deserialize(byte[] data) throws IOException {
        GlobalIndexResultSerializer globalIndexResultSerializer = new GlobalIndexResultSerializer();
        DataInputDeserializer dataInputDeserializer = new DataInputDeserializer(data);
        return globalIndexResultSerializer.deserialize(dataInputDeserializer);
    }
}
