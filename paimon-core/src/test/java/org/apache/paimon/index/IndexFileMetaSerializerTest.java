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

package org.apache.paimon.index;

import org.apache.paimon.deletionvectors.DeletionVectorsIndexFile;
import org.apache.paimon.utils.ObjectSerializer;
import org.apache.paimon.utils.ObjectSerializerTestBase;
import org.apache.paimon.utils.Pair;

import java.util.LinkedHashMap;
import java.util.Random;

/** Test for {@link org.apache.paimon.index.IndexFileMetaSerializer}. */
public class IndexFileMetaSerializerTest extends ObjectSerializerTestBase<IndexFileMeta> {

    @Override
    protected ObjectSerializer<IndexFileMeta> serializer() {
        return new IndexFileMetaSerializer();
    }

    @Override
    protected IndexFileMeta object() {
        return randomIndexFile();
    }

    public static IndexFileMeta randomIndexFile() {
        Random rnd = new Random();
        if (rnd.nextBoolean()) {
            return new IndexFileMeta(
                    HashIndexFile.HASH_INDEX,
                    "my_file_name" + rnd.nextLong(),
                    rnd.nextInt(),
                    rnd.nextInt());
        } else {
            LinkedHashMap<String, Pair<Integer, Integer>> deletionVectorsRanges =
                    new LinkedHashMap<>();
            deletionVectorsRanges.put("my_file_name1", Pair.of(rnd.nextInt(), rnd.nextInt()));
            deletionVectorsRanges.put("my_file_name2", Pair.of(rnd.nextInt(), rnd.nextInt()));
            return new IndexFileMeta(
                    DeletionVectorsIndexFile.DELETION_VECTORS_INDEX,
                    "deletion_vectors_index_file_name" + rnd.nextLong(),
                    rnd.nextInt(),
                    rnd.nextInt(),
                    deletionVectorsRanges);
        }
    }
}
