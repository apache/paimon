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

import org.apache.paimon.utils.RoaringNavigableMap64;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** Utils to serialize and deserialize GlobalIndexResult. */
public class GlobalIndexSerDeUtils {

    private static final long MAGIC = 30458034380294134L;
    private static final int VERSION = 1;

    static void serialize(DataOutput dataOutput, GlobalIndexResult globalIndexResult)
            throws IOException {
        dataOutput.writeLong(MAGIC);
        dataOutput.writeInt(VERSION);

        int type;
        if (globalIndexResult instanceof TopkGlobalIndexResult) {
            type = 1;
        } else {
            type = 0;
        }
        dataOutput.writeInt(type);

        RoaringNavigableMap64 roaringNavigableMap64 = globalIndexResult.results();
        byte[] bytes = roaringNavigableMap64.serialize();

        dataOutput.writeInt(bytes.length);
        dataOutput.write(bytes);

        if (type == 1) {
            TopkGlobalIndexResult topkGlobalIndexResult = (TopkGlobalIndexResult) globalIndexResult;
            ScoreGetter scoreGetter = topkGlobalIndexResult.scoreGetter();
            for (Long rowId : roaringNavigableMap64) {
                dataOutput.writeFloat(scoreGetter.score(rowId));
            }
        }
    }

    static GlobalIndexResult deserialize(DataInput dataInput) throws IOException {
        long magic = dataInput.readLong();
        if (magic != MAGIC) {
            throw new IllegalStateException("Invalid magic number: " + magic);
        }

        int version = dataInput.readInt();
        if (version != VERSION) {
            throw new IllegalStateException("Invalid version: " + version);
        }

        int type = dataInput.readInt();

        int size = dataInput.readInt();
        byte[] bytes = new byte[size];
        dataInput.readFully(bytes);

        RoaringNavigableMap64 roaringNavigableMap64 = new RoaringNavigableMap64();
        roaringNavigableMap64.deserialize(bytes);

        if (type == 0) {
            return GlobalIndexResult.create(() -> roaringNavigableMap64);
        }

        int scoreSize = roaringNavigableMap64.getIntCardinality();
        float[] scores = new float[scoreSize];
        for (int i = 0; i < scoreSize; i++) {
            scores[i] = dataInput.readFloat();
        }

        Map<Long, Float> scoreMap = new HashMap<>();
        int i = 0;
        for (Long rowId : roaringNavigableMap64) {
            scoreMap.put(rowId, scores[i++]);
        }

        return TopkGlobalIndexResult.create(() -> roaringNavigableMap64, scoreMap::get);
    }
}
