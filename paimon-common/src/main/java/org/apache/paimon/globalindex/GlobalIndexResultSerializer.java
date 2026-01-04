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

import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.utils.RoaringNavigableMap64;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** GlobalIndexResultSerializer to serialize and deserialize GlobalIndexResult. */
public class GlobalIndexResultSerializer implements Serializer<GlobalIndexResult> {

    private static final int VERSION = 1;

    @Override
    public Serializer<GlobalIndexResult> duplicate() {
        return this;
    }

    @Override
    public GlobalIndexResult copy(GlobalIndexResult from) {
        try {
            DataOutputSerializer dataOutputSerializer = new DataOutputSerializer(1024);
            serialize(from, dataOutputSerializer);

            DataInputDeserializer dataInputDeserializer =
                    new DataInputDeserializer(dataOutputSerializer.getCopyOfBuffer());
            return deserialize(dataInputDeserializer);
        } catch (IOException e) {
            throw new RuntimeException("Copy failed", e);
        }
    }

    @Override
    public void serialize(GlobalIndexResult globalIndexResult, DataOutputView dataOutput)
            throws IOException {
        dataOutput.writeInt(VERSION);

        RoaringNavigableMap64 roaringNavigableMap64 = globalIndexResult.results();
        byte[] bytes = roaringNavigableMap64.serialize();

        dataOutput.writeInt(bytes.length);
        dataOutput.write(bytes);

        if (globalIndexResult instanceof VectorSearchGlobalIndexResult) {
            VectorSearchGlobalIndexResult vectorSearchGlobalIndexResult =
                    (VectorSearchGlobalIndexResult) globalIndexResult;
            dataOutput.writeInt(roaringNavigableMap64.getIntCardinality());
            ScoreGetter scoreGetter = vectorSearchGlobalIndexResult.scoreGetter();
            for (Long rowId : roaringNavigableMap64) {
                dataOutput.writeFloat(scoreGetter.score(rowId));
            }
        } else {
            dataOutput.writeInt(0);
        }
    }

    @Override
    public GlobalIndexResult deserialize(DataInputView dataInput) throws IOException {
        int version = dataInput.readInt();
        if (version != VERSION) {
            throw new IllegalStateException("Invalid version: " + version);
        }

        int size = dataInput.readInt();
        byte[] bytes = new byte[size];
        dataInput.readFully(bytes);

        RoaringNavigableMap64 roaringNavigableMap64 = new RoaringNavigableMap64();
        roaringNavigableMap64.deserialize(bytes);
        int scoreSize = dataInput.readInt();

        if (scoreSize == 0) {
            return GlobalIndexResult.create(() -> roaringNavigableMap64);
        }
        checkArgument(
                scoreSize == roaringNavigableMap64.getIntCardinality(),
                "Error size of score: "
                        + scoreSize
                        + ", expected: "
                        + roaringNavigableMap64.getIntCardinality());

        float[] scores = new float[scoreSize];
        for (int i = 0; i < scoreSize; i++) {
            scores[i] = dataInput.readFloat();
        }

        Map<Long, Float> scoreMap = new HashMap<>();
        int i = 0;
        for (Long rowId : roaringNavigableMap64) {
            scoreMap.put(rowId, scores[i++]);
        }

        return VectorSearchGlobalIndexResult.create(() -> roaringNavigableMap64, scoreMap::get);
    }
}
